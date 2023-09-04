/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"context"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.ibm.com/TYOS/objcache/common"
	"golang.org/x/sys/unix"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/takeshi-yoshimura/fuse"
	"github.com/takeshi-yoshimura/fuse/fuseops"
	"github.com/takeshi-yoshimura/fuse/fuseutil"

	"net/http"

	"github.com/sirupsen/logrus"
)

const ObjcacheDirName = ".objcache"

func GetFSWithoutMount(args *common.ObjcacheCmdlineArgs, flags *common.ObjcacheConfig) (fs *ObjcacheFileSystem, err error) {
	fs, err = NewObjcacheFileSystem(args, flags)
	if err != nil {
		log.Errorf("Failed: FuseMount, newGoofys")
		return nil, err
	}
	return fs, nil
}

func (fs *ObjcacheFileSystem) FuseMount(args *common.ObjcacheCmdlineArgs, flags *common.ObjcacheConfig) (mfs *fuse.MountedFileSystem, err error) {
	mountCfg := &fuse.MountConfig{
		FSName:                  "objcache",
		Options:                 flags.MountOptions,
		ErrorLogger:             common.GetStdLogger(common.NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: flags.DisableLocalWritebackCaching,
		UseVectoredRead:         true,
	}
	if flags.DebugFuse {
		fLog := common.GetLogger("fuse")
		fLog.Level = logrus.DebugLevel
		mountCfg.DebugLogger = common.GetStdLogger(fLog, logrus.DebugLevel)
	}
	fs.SetRoot()
	var notifier *fuse.Notifier
	if mfs, notifier, err = fuse.MountAndGetNotifier(args.MountPoint, fuseutil.NewFileSystemServer(fs), mountCfg); err != nil {
		log.Errorf("Failed: FuseMount, Mount, err=%v", err)
		return
	}
	fs.notifier = notifier
	return
}

type ObjcacheProc struct {
	nextHandleID fuseops.HandleID
	files        map[fuseops.HandleID]*FileHandle
	lock         *sync.RWMutex
}

func (p *ObjcacheProc) newFileHandleFromMeta(h MetaRWHandler, isReadOnly bool, disableLocalWriteBackCaching bool) fuseops.HandleID {
	p.lock.Lock()
	fh := NewFileHandle(h, isReadOnly, disableLocalWriteBackCaching)
	handleId := p.nextHandleID
	p.nextHandleID += 1
	p.files[handleId] = fh
	p.lock.Unlock()
	return handleId
}

func (p *ObjcacheProc) getFileHandle(handleId fuseops.HandleID) (*FileHandle, error) {
	p.lock.RLock()
	fh, ok := p.files[handleId]
	p.lock.RUnlock()
	if !ok {
		return nil, unix.EBADF
	}
	return fh, nil
}

func (p *ObjcacheProc) closeFileHandle(handleId fuseops.HandleID) (fh *FileHandle, err error) {
	p.lock.Lock()
	var ok bool
	fh, ok = p.files[handleId]
	if ok {
		delete(p.files, handleId)
		err = nil
	} else {
		err = unix.EBADF
	}
	p.lock.Unlock()
	return fh, err
}

func (p *ObjcacheProc) CheckReset() (ok bool) {
	if ok = p.lock.TryLock(); !ok {
		log.Errorf("Failed: ObjcacheProc.CheckReset, p.lock is taken")
	} else {
		if ok2 := len(p.files) == 0; !ok2 {
			log.Errorf("Failed: ObcjacheProc.CheckReset, len(p.files) != 0")
			ok = false
		}
		p.lock.Unlock()
	}
	return
}

type LocalInode struct {
	key            string // name for file, full path for directory
	version        uint32
	chunkSize      int64
	attr           fuseops.InodeAttributes
	xAttr          map[string][]byte
	children       map[string]MetaAttributes
	parentInodeKey InodeKeyType // the first parent since hard links may add parents but this is only used for first fetch from COS.
	writerCount    *int32
	storeCount     *int32 // block writer at OpenFile while calling NotifyStore
}

type StaleInode struct {
	parentId fuseops.InodeID
	childId  fuseops.InodeID
	name     string
}

func (c *LocalInode) toMetaRWHandler(inodeKey InodeKeyType) MetaRWHandler {
	return MetaRWHandler{inodeKey: inodeKey, version: c.version, size: int64(c.attr.Size), chunkSize: c.chunkSize, mode: uint32(c.attr.Mode), mTime: c.attr.Mtime.UnixNano()}
}

type ObjcacheFileSystem struct {
	back         *NodeServer
	inodes       map[fuseops.InodeID]LocalInode
	staleInodes  map[StaleInode]time.Time
	staleDeletes map[StaleInode]time.Time
	lock         *sync.RWMutex
	staleLock    *sync.Mutex
	proc         *ObjcacheProc // Note: currently, fuse does not pass precise PIDs for each op. So, we only have a single copy of ObjcacheProc.
	uid          uint32
	gid          uint32
	notifier     *fuse.Notifier
	args         *common.ObjcacheCmdlineArgs
	isResetting  int32
	resetCond    *sync.Cond
}

func NewObjcacheFileSystem(args *common.ObjcacheCmdlineArgs, flags *common.ObjcacheConfig) (*ObjcacheFileSystem, error) {
	var back *ObjCacheBackend = nil
	if !args.ClientMode {
		var err error
		back, err = NewObjCache(args.SecretFile, flags.DebugS3, int(flags.ChunkSizeBytes))
		if err != nil {
			log.Errorf("Failed: NewObjcacheFileSystem, NewBackend, Unable to setup backend, err=%v", err)
			return nil, err
		}
		err = back.Init("/" + RandString.Get(64))
		if err != nil {
			log.Errorf("Failed: NewObjcacheFileSystem, Init, err=%v", err)
			return nil, err
		}
	}
	node := NewNodeServer(back, args, flags)

	proc := &ObjcacheProc{nextHandleID: 4, files: make(map[fuseops.HandleID]*FileHandle), lock: new(sync.RWMutex)}
	fs := &ObjcacheFileSystem{
		back:         node,
		inodes:       make(map[fuseops.InodeID]LocalInode),
		staleInodes:  make(map[StaleInode]time.Time),
		staleDeletes: make(map[StaleInode]time.Time),
		lock:         new(sync.RWMutex),
		staleLock:    new(sync.Mutex),
		proc:         proc, /*procs: make(map[uint32]*ObjcacheProc)*/
		uid:          flags.Uid, gid: flags.Gid, args: args,
		resetCond: sync.NewCond(new(sync.Mutex)),
	}
	node.SetFs(fs)
	return fs, nil
}

func (fs *ObjcacheFileSystem) SetRoot() {
	parent := MetaAttributes{inodeKey: InodeKeyType(fuseops.RootInodeID), mode: fs.back.flags.DirMode}
	root, err := fs.back.GetMetaFromClusterOrCOS(InodeKeyType(fuseops.RootInodeID), "", parent)
	if err != nil {
		log.Fatalf("Failed: SetRoot, GetMetaFromClusterOrCOS, rootId=%v, err=%v", fuseops.RootInodeID, err)
	}
	c := int32(0)
	d := int32(0)
	fs.inodes[fuseops.RootInodeID] = LocalInode{
		key: "", version: root.version, chunkSize: root.chunkSize, attr: root.GetAttr(fs.uid, fs.gid),
		children: root.childAttrs, parentInodeKey: root.parentKey, writerCount: &c, storeCount: &d,
	}
}

func (fs *ObjcacheFileSystem) WaitReset() {
	fs.resetCond.L.Lock()
	for fs.isResetting > 0 {
		fs.resetCond.Wait()
	}
	fs.resetCond.L.Unlock()
}

func (fs *ObjcacheFileSystem) GetOpenInodes() map[InodeKeyType]bool {
	ret := make(map[InodeKeyType]bool)
	fs.proc.lock.RLock()
	for _, handle := range fs.proc.files {
		ret[handle.h.inodeKey] = true
	}
	fs.proc.lock.RUnlock()
	return ret
}

func (fs *ObjcacheFileSystem) getProc(_ fuseops.OpContext) *ObjcacheProc {
	/*fs.lock.RLock()
	proc, ok := fs.procs[opContext.Pid]
	fs.lock.RUnlock()
	if ok {
		return proc
	}
	fs.lock.Lock()
	proc, ok = fs.procs[opContext.Pid]
	if ok {
		fs.lock.Unlock()
		return proc
	}
	proc = &ObjcacheProc{nextHandleID: 4, files: make(map[fuseops.HandleID]*FileHandle), lock: new(sync.RWMutex)}
	fs.procs[opContext.Pid] = proc
	fs.lock.Unlock()*/
	return fs.proc
}

func (fs *ObjcacheFileSystem) prefetchLookUp(parent LocalInode, parentInode fuseops.InodeID, childInode fuseops.InodeID, name string) {
	fs.lock.RLock()
	child, ok3 := fs.inodes[childInode]
	fs.lock.RUnlock()
	key := filepath.Join(parent.key, name)
	var cmeta *WorkingMeta
	var err error
	parentAttr := MetaAttributes{inodeKey: InodeKeyType(parentInode), mode: uint32(parent.attr.Mode)}
	if cmeta, err = fs.back.GetMetaFromClusterOrCOS(InodeKeyType(childInode), key, parentAttr); err != nil {
		if err == unix.ENOENT {
			// child may be deleted remotely
			fs.lock.Lock()
			delete(parent.children, child.key)
			fs.inodes[fuseops.InodeID(parentInode)] = parent
			delete(fs.inodes, fuseops.InodeID(childInode))
			fs.lock.Unlock()
		}
		return
	}
	if err != nil {
		return
	}
	// see LookUpInode comment
	if !cmeta.IsDir() && ok3 && cmeta.version != child.version {
		fs.staleLock.Lock()
		staleInode := StaleInode{parentId: fuseops.InodeID(parentInode), name: child.key, childId: fuseops.InodeID(childInode)}
		if _, ok4 := fs.staleInodes[staleInode]; !ok4 {
			fs.staleInodes[staleInode] = time.Now()
		}
		fs.staleLock.Unlock()
		log.Warnf("prefetchLookUp, remote update is detected. the next read will return the content in the local cache (likely stale), inodeKey=%v, key=%v, version=%v->%v",
			childInode, key, child.version, child.version)
		return
	}
	fs.trackAndUpdateCacheInKernel(key, fuseops.InodeID(childInode), cmeta)
}

func (fs *ObjcacheFileSystem) readDirInode(inodeId fuseops.InodeID, offset fuseops.DirOffset, dst []byte) (c int, err error) {
	fs.WaitReset()
	fs.lock.RLock()
	inode, ok := fs.inodes[inodeId]
	if !ok {
		fs.lock.RUnlock()
		return 0, unix.ENOENT
	}
	c = 0
	// we need to maker sure consistent ordering among separate ReadDir calls
	children := make([]string, len(inode.children))
	var k = 0
	for name := range inode.children {
		children[k] = name
		k += 1
	}
	sort.Slice(children, func(x int, y int) bool {
		return children[x] < children[y]
	})

	keys := make(map[fuseops.InodeID]string)
	for j := offset; j < fuseops.DirOffset(len(children)); j++ {
		name := children[j]
		child := inode.children[name]
		e := fuseutil.Dirent{Name: name, Inode: fuseops.InodeID(child.inodeKey), Offset: j + 1, Type: fuseutil.DT_File}
		if child.IsDir() {
			e.Type = fuseutil.DT_Directory
		}
		if _, ok := fs.inodes[fuseops.InodeID(child.inodeKey)]; !ok {
			keys[fuseops.InodeID(child.inodeKey)] = name
		}
		n := fuseutil.WriteDirent(dst[c:], e)
		if n == 0 {
			break
		}
		c += n
		if c >= len(dst) {
			break
		}
	}
	fs.lock.RUnlock()
	for inodeKey, name := range keys {
		go func(inodeKey fuseops.InodeID, name string) {
			fs.prefetchLookUp(inode, inodeId, inodeKey, name)
		}(inodeKey, name)
	}
	return
}

func (fs *ObjcacheFileSystem) Shutdown() {
	log.Infof("Attempting to shutdown")
	if err := fs.Reset(); err == nil {
		fs.back.Shutdown(true)
	}
}

func (fs *ObjcacheFileSystem) StatFS(_ context.Context, op *fuseops.StatFSOp) (err error) {
	op.BlockSize = uint32(fs.back.flags.ChunkSizeBytes)
	op.Blocks = 1 * 1024 * 1024 * 1024 * 1024 * 1024 / uint64(op.BlockSize) // 1PB
	op.BlocksFree = op.Blocks
	op.BlocksAvailable = op.Blocks
	op.IoSize = uint32(fs.back.flags.ChunkSizeBytes)
	op.Inodes = 1 * 1000 * 1000 * 1000 // 1 billion
	op.InodesFree = op.Inodes
	return
}

func (fs *ObjcacheFileSystem) trackAndUpdateCacheInKernel(key string, inodeId fuseops.InodeID, meta *WorkingMeta) bool {
	fs.lock.Lock()
	deleted, needInvalidate := fs.trackCacheInKernel(key, inodeId, meta)
	fs.lock.Unlock()
	if !needInvalidate {
		return needInvalidate
	}
	if len(deleted) > 0 {
		fs.staleLock.Lock()
		for childInodeId, name := range deleted {
			stale := StaleInode{parentId: inodeId, childId: childInodeId, name: name}
			fs.staleDeletes[stale] = time.Now()
			log.Debugf("trackAndUpdateCacheInKernel, remote delete is detected, stale=%v, key=%v, version=%v", stale, key, meta.version)
		}
		fs.staleLock.Unlock()
	}
	if inodeId == fuseops.RootInodeID {
		return false
	}
	if err := fs.notifier.InvalidateInode(inodeId, 0, 0); err != nil {
		log.Warnf("Failed (ignore): trackAndUpdateCacheInKernel, InvalidateInode, inodeKey=%v, ver=%v, err=%v", inodeId, meta.version, err)
	} else {
		log.Debugf("Success: trackAndUpdateCacheInKernel, InvalidateInode, inodeKey=%v, ver=%v", inodeId, meta.version)
	}
	return needInvalidate
}

func (fs *ObjcacheFileSystem) trackCacheInKernel(key string, inodeId fuseops.InodeID, meta *WorkingMeta) (deleted map[fuseops.InodeID]string, needInvalidation bool) {
	dent, ok := fs.inodes[inodeId]
	if ok {
		if needInvalidation = dent.version != meta.version; !needInvalidation {
			return
		}
	}
	c := int32(0)
	d := int32(0)
	inode := LocalInode{
		version: meta.version, parentInodeKey: meta.parentKey, chunkSize: meta.chunkSize,
		attr: meta.GetAttr(fs.uid, fs.gid), xAttr: meta.GetMetadata(), writerCount: &c, storeCount: &d,
	}
	if meta.IsDir() {
		inode.key = key
		inode.children = make(map[string]MetaAttributes)
		inodes := make(map[fuseops.InodeID]bool)
		for name, child := range meta.childAttrs {
			if ok {
				if known, ok2 := dent.children[name]; ok2 {
					inode.children[name] = known
					continue
				}
			}
			newInodeId := fuseops.InodeID(child.inodeKey)
			inode.children[name] = child
			inodes[newInodeId] = true
		}
		if ok {
			deleted = make(map[fuseops.InodeID]string)
			for childName, child := range dent.children {
				if _, ok3 := meta.childAttrs[childName]; !ok3 {
					childId := fuseops.InodeID(child.inodeKey)
					deleted[childId] = childName
					if _, ok4 := inodes[childId]; !ok4 { // check rename
						delete(fs.inodes, childId)
					}
				}
			}
		}
	} else {
		inode.key = filepath.Base(key)
	}
	fs.inodes[inodeId] = inode
	return
}

func (fs *ObjcacheFileSystem) updateFileInodeAttr(meta *WorkingMeta) {
	if meta == nil {
		return
	}
	inodeId := fuseops.InodeID(meta.inodeKey)
	fs.lock.Lock()
	inode, ok := fs.inodes[inodeId]
	if !ok {
		fs.lock.Unlock()
		return
	}
	parent, ok := fs.inodes[fuseops.InodeID(inode.parentInodeKey)]
	if !ok {
		fs.lock.Unlock()
		return
	}
	inode.attr = meta.GetAttr(fs.uid, fs.gid)
	oldVer := inode.version
	inode.version = meta.version
	inode.chunkSize = meta.chunkSize
	fs.inodes[inodeId] = inode
	if meta.IsDir() {
		parent.children[filepath.Base(inode.key)] = meta.toMetaAttr()
	} else {
		parent.children[inode.key] = meta.toMetaAttr()
	}
	fs.inodes[fuseops.InodeID(inode.parentInodeKey)] = parent
	fs.lock.Unlock()
	log.Debugf("Success: updateFileInodeAttr, inodeKey=%v, version=%v->%v", meta.inodeKey, oldVer, inode.version)
}

func (fs *ObjcacheFileSystem) GetInodeAttributes(_ context.Context, op *fuseops.GetInodeAttributesOp) (err error) {
	fs.WaitReset()
	fs.lock.RLock()
	inode, ok := fs.inodes[op.Inode]
	fs.lock.RUnlock()
	if !ok {
		return unix.ENOENT
	}
	op.Attributes = inode.attr
	op.AttributesExpiration = MaxTime
	return
}

func (fs *ObjcacheFileSystem) GetXattr(_ context.Context, op *fuseops.GetXattrOp) (err error) {
	fs.WaitReset()
	if op.Name == "security.capability" {
		//Note: https://sourceforge.net/p/fuse/mailman/fuse-devel/thread/889E1F64-C59A-41EE-927E-456640E404ED@nortel.com/
		/**
		> > I recently discovered that prior to *every single block* being written
		> > there is a request for the "security.capability" extended attribute
		> > (which is never set).  I checked this against a no-op filesystem (just
		> > passes calls through to an underlying file system) and found that it
		> > occurs there too.

		> > Just for grins this morning, I've tried returning ENODATA, ENOTSUP/
		> > EOPNOTSUP, ENOATTR and 0, none of which prevent the continued attempts
		> > to get the caps.

		> Try ENOSYS.

		> Yes!  That works.  If returned, I see no more requests for that
		> extended attributes, not just on the original file, but on any file.
		*/
		log.Infof("GetXattr, ignore security.capability for surpressing too many requests of GetXattr. Inode=%v", op.Inode)
		return unix.ENOSYS
	}
	fs.lock.RLock()
	inode, ok := fs.inodes[op.Inode]
	if !ok {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	xAttr := inode.xAttr
	fs.lock.RUnlock()
	if xAttr == nil {
		err = fuse.ENOSYS
		return
	}
	value, ok2 := xAttr[op.Name]
	if !ok2 {
		err = unix.ENODATA
		return
	}
	if len(op.Dst) < len(value) {
		if op.Dst == nil {
			op.BytesRead = len(value)
			return
		}
		log.Errorf("Failed: GetXattr, len(op.Dst) < op.BytesRead")
		err = unix.ERANGE
		return
	}
	if len(op.Dst) > 0 {
		op.BytesRead = copy(op.Dst, value)
	}
	return
}

func (fs *ObjcacheFileSystem) ListXattr(_ context.Context, op *fuseops.ListXattrOp) (err error) {
	fs.WaitReset()
	fs.lock.RLock()
	inode, ok := fs.inodes[op.Inode]
	if !ok {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	xAttr := inode.xAttr
	fs.lock.RUnlock()
	if xAttr == nil {
		err = fuse.ENOSYS
		return
	}
	attrs := make([]byte, 0)
	for k := range xAttr {
		attrs = append(attrs, []byte(k)...)
		attrs = append(attrs, '\x00')
	}
	if len(op.Dst) < len(attrs) {
		if op.Dst == nil {
			op.BytesRead = len(attrs)
			return
		}
		log.Errorf("Failed: ListXattr, len(op.Dst)=%d < len(attrs)=%d", len(op.Dst), len(attrs))
		err = unix.ERANGE
		return
	}
	op.BytesRead += copy(op.Dst, attrs)
	return
}

func (fs *ObjcacheFileSystem) RemoveXattr(_ context.Context, op *fuseops.RemoveXattrOp) (err error) {
	fs.WaitReset()
	err = fs.back.RemoveInodeMetadataKey(op.Inode, op.Name)
	return
}

func (fs *ObjcacheFileSystem) SetXattr(_ context.Context, op *fuseops.SetXattrOp) (err error) {
	fs.WaitReset()
	err = fs.back.SetInodeMetadataKey(op.Inode, op.Name, op.Value, op.Flags)
	return
}

func (fs *ObjcacheFileSystem) invalidateAllStaleInodesAtLookUp(parent fuseops.InodeID) {
	for {
		// NOTE: InvalidateEntry and NotifyDelete must not be called with any locks held
		fs.staleLock.Lock()
		staleInodes := make([]StaleInode, 0)
		for staleInode := range fs.staleInodes {
			if staleInode.parentId != parent {
				staleInodes = append(staleInodes, staleInode)
				delete(fs.staleInodes, staleInode)
			}
		}
		deletedInodes := make([]StaleInode, 0)
		for staleInode := range fs.staleDeletes {
			if staleInode.parentId != parent {
				deletedInodes = append(deletedInodes, staleInode)
				delete(fs.staleDeletes, staleInode)
			}
		}
		fs.staleLock.Unlock()
		if len(staleInodes)+len(deletedInodes) == 0 {
			break
		}
		for _, staleInode := range staleInodes {
			if err2 := fs.notifier.InvalidateEntry(staleInode.parentId, staleInode.name); err2 != nil {
				log.Errorf("Failed (ignore): invalidateAllStaleInodesAtLookUp, InvalidateEntry, parent=%v, name=%v, err=%v", staleInode.parentId, staleInode.name, err2)
			} else {
				log.Debugf("Success: invalidateAllStaleInodesAtLookUp, InvalidateEntry, parent=%v, name=%v", staleInode.parentId, staleInode.name)
			}
		}
		for _, staleInode := range deletedInodes {
			if err2 := fs.notifier.NotifyDelete(staleInode.parentId, staleInode.childId, staleInode.name); err2 != nil {
				log.Warnf("Failed (ignore): invalidateAllStaleInodesAtLookUp, NotifyDelete, parent=%v, child=%v, name=%v, err=%v", staleInode.parentId, staleInode.childId, staleInode.name, err2)
			} else {
				log.Debugf("Success: invalidateAllStaleInodesAtLookUp, NotifyDelete, parent=%v, child=%v, name=%v", staleInode.parentId, staleInode.childId, staleInode.name)
			}
		}
	}
}

func (fs *ObjcacheFileSystem) invalidateAllStaleInodes(inodeId fuseops.InodeID) {
	for {
		// NOTE: InvalidateEntry and NotifyDelete must not be called with any locks held
		fs.staleLock.Lock()
		staleInodes := make([]StaleInode, 0)
		for staleInode := range fs.staleInodes {
			if inodeId != staleInode.childId {
				staleInodes = append(staleInodes, staleInode)
				delete(fs.staleInodes, staleInode)
			}
		}
		deletedInodes := make([]StaleInode, 0)
		for staleInode := range fs.staleDeletes {
			if inodeId != staleInode.childId {
				deletedInodes = append(deletedInodes, staleInode)
				delete(fs.staleDeletes, staleInode)
			}
		}
		fs.staleLock.Unlock()
		if len(staleInodes)+len(deletedInodes) == 0 {
			break
		}
		for _, staleInode := range staleInodes {
			if err2 := fs.notifier.InvalidateEntry(staleInode.parentId, staleInode.name); err2 != nil {
				log.Errorf("Failed (ignore): invalidateAllStaleInodes, InvalidateEntry, parent=%v, name=%v, err=%v", staleInode.parentId, staleInode.name, err2)
			} else {
				log.Debugf("Success: invalidateAllStaleInodes, InvalidateEntry, parent=%v, name=%v", staleInode.parentId, staleInode.name)
			}
		}
		for _, staleInode := range deletedInodes {
			if err2 := fs.notifier.NotifyDelete(staleInode.parentId, staleInode.childId, staleInode.name); err2 != nil {
				log.Warnf("Failed (ignore): invalidateAllStaleInodes, NotifyDelete, parent=%v, child=%v, name=%v, err=%v", staleInode.parentId, staleInode.childId, staleInode.name, err2)
			} else {
				log.Debugf("Success: invalidateAllStaleInodes, NotifyDelete, parent=%v, child=%v, name=%v", staleInode.parentId, staleInode.childId, staleInode.name)
			}
		}
	}
}

func (fs *ObjcacheFileSystem) LookUpInode(_ context.Context, op *fuseops.LookUpInodeOp) (err error) {
	fs.WaitReset()
	fs.invalidateAllStaleInodesAtLookUp(op.Parent)
	fs.lock.RLock()
	parent, ok := fs.inodes[op.Parent]
	if !ok {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	childAttr, ok2 := parent.children[op.Name]
	if !ok2 {
		fs.lock.RUnlock()
		return unix.ENOENT
	}
	curChildInode, ok3 := fs.inodes[fuseops.InodeID(childAttr.inodeKey)]
	fs.lock.RUnlock()
	key := filepath.Join(parent.key, op.Name)
	var child *WorkingMeta
	parentAttr := MetaAttributes{inodeKey: InodeKeyType(op.Parent), mode: uint32(parent.attr.Mode)}
	if child, err = fs.back.GetMetaFromClusterOrCOS(childAttr.inodeKey, key, parentAttr); err != nil {
		if err == unix.ENOENT {
			// child may be deleted remotely
			fs.lock.Lock()
			delete(parent.children, op.Name)
			fs.inodes[op.Parent] = parent
			delete(fs.inodes, fuseops.InodeID(childAttr.inodeKey))
			fs.lock.Unlock()
		}
		return
	}
	if err != nil {
		return
	}
	// Note: remote nodes may update inodes that is locally cached. We need to invalidate the cache to expose correct views of file attributes and existence.
	// However, InvalidateEntry at the time of LookUpInode with a file name and its parent inode causes deadlock (see the comment for InvalidateEntry).
	// Also, even if InvalidateEntry is delayed after LookUpInode with go routine, users still see stale attributes since FUSE already holds a lock with its inode cache.
	// For example, fstat and read after open may return inconsistent results between locally-cached inode attributes (e.g. size) and remotely-copied data.
	// So, we remember stale inodes and let invalidation handled later (next LookUp, Release, or timer)
	if !child.IsDir() && ok3 && child.version != curChildInode.version {
		fs.staleLock.Lock()
		staleInode := StaleInode{parentId: op.Parent, name: op.Name, childId: fuseops.InodeID(child.inodeKey)}
		if _, ok4 := fs.staleInodes[staleInode]; !ok4 {
			fs.staleInodes[staleInode] = time.Now()
		}
		fs.staleLock.Unlock()
		log.Warnf("LookUpInode, remote update is detected. the next read will return the content in the local cache (likely stale), inodeKey=%v, key=%v, version=%v->%v",
			childAttr.inodeKey, key, curChildInode.version, child.version)
		op.Entry.Child = fuseops.InodeID(childAttr.inodeKey)
		op.Entry.Attributes = curChildInode.attr
		op.Entry.AttributesExpiration = MaxTime
		return
	}
	fs.trackAndUpdateCacheInKernel(key, fuseops.InodeID(childAttr.inodeKey), child)
	op.Entry.Child = fuseops.InodeID(childAttr.inodeKey)
	op.Entry.Attributes = child.GetAttr(fs.uid, fs.gid)
	op.Entry.AttributesExpiration = MaxTime
	//op.Entry.EntryExpiration = MaxTime  // ensure the kernel calls LookUpInode everytime a user opens a file/directory.
	return
}

func (fs *ObjcacheFileSystem) BatchForget(_ context.Context, op *fuseops.BatchForgetOp) error {
	fs.lock.Lock()
	for _, entry := range op.Entries {
		delete(fs.inodes, entry.Inode)
		log.Debugf("BatchForget, inode=%v", entry.Inode)
	}
	fs.lock.Unlock()
	return nil
}

func (fs *ObjcacheFileSystem) ForgetInode(_ context.Context, op *fuseops.ForgetInodeOp) error {
	fs.lock.Lock()
	delete(fs.inodes, op.Inode)
	fs.lock.Unlock()
	log.Debugf("ForgetInode, inode=%v", op.Inode)
	return nil
}

func (fs *ObjcacheFileSystem) getFileHandle(handleId fuseops.HandleID, opContext fuseops.OpContext) (*FileHandle, error) {
	return fs.getProc(opContext).getFileHandle(handleId)
}

func (fs *ObjcacheFileSystem) closeFileHandle(handleId fuseops.HandleID, opContext fuseops.OpContext) error {
	fh, err := fs.getProc(opContext).closeFileHandle(handleId)
	inodeId := fuseops.InodeID(fh.h.inodeKey)
	fs.lock.RLock()
	inode, ok := fs.inodes[inodeId]
	if !ok {
		log.Warnf("Failed (ignore): closeFileHandle, cannot find inode for file handle, handleId=%v, inodeId=%v", handleId, inodeId)
	} else if !fh.isReadOnly {
		atomic.AddInt32(inode.writerCount, -1)
	}
	fs.lock.RUnlock()
	fs.invalidateAllStaleInodes(inodeId)
	return err
}

func (fs *ObjcacheFileSystem) OpenDir(_ context.Context, op *fuseops.OpenDirOp) (err error) {
	fs.WaitReset()
	fs.lock.RLock()
	inode, ok := fs.inodes[op.Inode]
	fs.lock.RUnlock()
	if !ok {
		err = unix.ENOENT
		return
	}
	if !inode.attr.Mode.IsDir() {
		err = unix.ENOTDIR
		return
	}
	op.Handle = fs.getProc(op.OpContext).newFileHandleFromMeta(inode.toMetaRWHandler(InodeKeyType(op.Inode)), true, true)
	return
}

func (fs *ObjcacheFileSystem) ReadDir(_ context.Context, op *fuseops.ReadDirOp) (err error) {
	fs.WaitReset()
	var fh *FileHandle
	if fh, err = fs.getFileHandle(op.Handle, op.OpContext); err != nil {
		return
	}
	op.BytesRead, err = fs.readDirInode(fuseops.InodeID(fh.h.inodeKey), op.Offset, op.Dst)
	return
}

func (fs *ObjcacheFileSystem) ReleaseDirHandle(_ context.Context, op *fuseops.ReleaseDirHandleOp) (err error) {
	fs.WaitReset()
	err = fs.closeFileHandle(op.Handle, op.OpContext)
	return
}

func (fs *ObjcacheFileSystem) OpenFile(_ context.Context, op *fuseops.OpenFileOp) (err error) {
	fs.WaitReset()
	fs.lock.RLock()
	inode, ok := fs.inodes[op.Inode]
	fs.lock.RUnlock()
	if !ok {
		err = unix.ENOENT
		return
	}
	op.KeepPageCache = !fs.back.flags.DisableLocalWritebackCaching
	isReadOnly := op.OpenFlags.IsReadOnly()
	if !isReadOnly {
		atomic.AddInt32(inode.writerCount, 1)
		for i := 0; i < 1000*1000; i++ {
			c := atomic.LoadInt32(inode.storeCount)
			if c == 0 {
				break
			}
			if i == 1000*1000-1 {
				log.Errorf("Failed: OpenFile, wait for NotifyStore ~1 sec. deadlocked? inodeId=%v, storeCount=%v", op.Inode, c)
				err = unix.ETIMEDOUT
				atomic.AddInt32(inode.writerCount, -1)
				return
			}
			if i > 0 && i%1000 == 0 {
				log.Warnf("OpenFile is waiting for very slow NotifyStore. recommend to avoid concurrent read and writes or using O_RDONLY if possible, inodeId=%v, i=%v storeCount=%v",
					op.Inode, i, c)
			}
			time.Sleep(time.Microsecond)
		}
	}
	op.Handle = fs.getProc(op.OpContext).newFileHandleFromMeta(inode.toMetaRWHandler(InodeKeyType(op.Inode)), isReadOnly, fs.back.flags.DisableLocalWritebackCaching)
	return
}

func (fs *ObjcacheFileSystem) ReadFile(_ context.Context, op *fuseops.ReadFileOp) (err error) {
	fs.WaitReset()
	begin := time.Now()
	var fh *FileHandle
	if fh, err = fs.getFileHandle(op.Handle, op.OpContext); err != nil {
		return
	}
	fs.lock.RLock()
	inode, ok := fs.inodes[fuseops.InodeID(fh.h.inodeKey)]
	if !ok {
		fs.lock.RUnlock()
		return unix.ENOENT
	}
	parent, ok2 := fs.inodes[fuseops.InodeID(inode.parentInodeKey)]
	if !ok2 {
		fs.lock.RUnlock()
		return unix.ENOENT
	}
	fs.lock.RUnlock()
	lock := time.Now()
	/*if op.Dst != nil {
		op.BytesRead, err = fh.ReadMinimum(filepath.Join(parent.key, inode.key), op.Offset, op.Dst, fs.back)
		if err == nil {
			log.Debugf("Success: ReadFile (op.Dst != nil), offset=%v, bytesRead=%v, elapsed=%v", op.Offset, op.BytesRead, time.Since(begin))
		}
	} else */
	if fs.back.flags.UseNotifyStore && atomic.LoadInt32(inode.writerCount) == 0 && atomic.LoadInt32(inode.storeCount) == 0 {
		/*
		 * Linux FUSE allows up to 256 KiB (128 KiB read + 128 KiB read ahead) requests for a single read, which increase kernel-FUSE context switching too much for large files.
		 * Objcache emulates bigger requests for each read by aggressively filling read buffer with notifier.Store.
		 * But note that we need to be careful not to overwrite concurrent writes().
		 * We ensure this optimization works only when we know all opened FDs are read-only,
		 *
		 * We also found oncurrent NotifyStore during ReadFile() on the same page cause a deadlock.
		 * Page-level locking should be best for performance, but currently we simplify it by exclusively calling notify_store on an inode at a time.
		 */
		var buffers [][]byte
		var count int
		buffers, count, err = fh.Read(filepath.Join(parent.key, inode.key), op.Offset, op.Size+fs.back.flags.ClientReadAheadSizeBytes, fs.back, op)
		if err != nil {
			return
		}
		var readAheadSize = int64(0)
		if int64(count) < op.Size {
			op.BytesRead = count
			op.Data = buffers
		} else {
			storeBuffers := make([][]byte, 0)
			op.BytesRead = 0
			var i int
			for i = 0; i < len(buffers) && int64(op.BytesRead) < op.Size; i++ {
				buffer := buffers[i]
				if int64(op.BytesRead+len(buffer)) > op.Size {
					readAheadSize = int64(len(buffer)) - (op.Size - int64(op.BytesRead))
					storeBuffers = append(storeBuffers, buffer[op.Size-int64(op.BytesRead):])
					buffer = buffer[:op.Size-int64(op.BytesRead)]
				}
				op.Data = append(op.Data, buffer)
				op.BytesRead += len(buffer)
			}
			for ; i < len(buffers); i++ {
				storeBuffers = append(storeBuffers, buffers[i])
				readAheadSize += int64(len(buffers[i]))
			}
			var notifyOffset = op.Offset + int64(op.BytesRead)
			if slop := notifyOffset % PageSize; slop != 0 {
				// pages at [op.Offset, op.Offset+op.Size] are locked. NotifyStore must avoid the range not to deadlock.
				skip := PageSize - slop
				log.Debugf("ReadFile, skip %v bytes for NotifyStore, notifyOffset=%v->%v, readAheadSize=%v->%v", skip, notifyOffset, notifyOffset+skip, readAheadSize, readAheadSize+skip)
				notifyOffset += skip
				readAheadSize -= skip
				for i = 0; i < len(storeBuffers) && 0 < skip; i++ {
					if int64(len(storeBuffers[i])) > skip {
						storeBuffers[i] = storeBuffers[i][skip:]
						skip = 0
						storeBuffers = storeBuffers[i:]
						break
					} else {
						skip -= int64(len(storeBuffers[i]))
					}
				}
				if 0 < skip {
					storeBuffers = nil
					readAheadSize = 0
				}
			}
			if len(storeBuffers) > 0 {
				c := atomic.AddInt32(inode.storeCount, 1)
				if c == 1 && atomic.LoadInt32(inode.writerCount) == 0 {
					log.Debugf("ReadFile, Try NotifyStore, inodeId=%v, offset=%v (op.BytesRead=%v), size=%v, len(storeBuffers)=%v",
						fh.h.inodeKey, notifyOffset, op.BytesRead, readAheadSize, len(storeBuffers))
					if err2 := fs.notifier.Store(fuseops.InodeID(fh.h.inodeKey), notifyOffset, storeBuffers); err2 != nil {
						log.Errorf("Failed (ignore): ReadFile, Store, inodeId=%v, offset=%v, size=%v, len(storeBuffers)=%v, err=%v",
							fh.h.inodeKey, notifyOffset, readAheadSize, len(storeBuffers), err2)
					} else {
						log.Debugf("Success: ReadFile, NotifyStore, inodeId=%v, offset=%v, size=%v, len(storeBuffers)=%v",
							fh.h.inodeKey, notifyOffset, readAheadSize, len(storeBuffers))
					}
					atomic.AddInt32(inode.storeCount, -1)
				} else {
					readAheadSize = 0
					log.Warnf("ReadFile, Skip NotifyStore because cocurrent reads/writes are detected. inodeId=%v, offset=%v, size=%v", op.Inode, op.Offset, op.Size)
					atomic.AddInt32(inode.storeCount, -1)
				}
			}
		}
		log.Debugf("Success: ReadFile (opt), offset=%v, reqSize=%v, BytesRead=%v, notifyStoreSize=%v, elapsed=%v", op.Offset, op.Size, op.BytesRead, readAheadSize, time.Since(begin))
	} else if fs.back.flags.DisableLocalWritebackCaching {
		op.Data, op.BytesRead, err = fh.ReadNoCache(filepath.Join(parent.key, inode.key), op.Offset, op.Size, fs.back, op)
	} else {
		op.Data, op.BytesRead, err = fh.Read(filepath.Join(parent.key, inode.key), op.Offset, op.Size, fs.back, op)
	}
	if err == nil {
		log.Debugf("Success: ReadFile, pid=%v, handle=%v, inode=%v, offset=%v, reqSize=%v, BytesRead=%v, readAheadSize=%v, lock=%v, elapsed=%v", op.OpContext.Pid, op.Handle, op.Inode, op.Offset, op.Size, op.BytesRead, fs.back.flags.ClientReadAheadSizeBytes, lock.Sub(begin), time.Since(begin))
	} else {
		log.Errorf("Failed: ReadFile, pid=%v, handle=%v, inode=%v, offset=%v, reqSize=%v, BytesRead=%v, readAheadSize=%v, lock=%v, elapsed=%v, err=%v", op.OpContext.Pid, op.Handle, op.Inode, op.Offset, op.Size, op.BytesRead, fs.back.flags.ClientReadAheadSizeBytes, lock.Sub(begin), time.Since(begin), err)
	}
	return
}

func (fs *ObjcacheFileSystem) PostOp(_ context.Context, op interface{}) {
	if readOp, ok := op.(*fuseops.ReadFileOp); ok {
		fs.WaitReset()
		fh, err := fs.getFileHandle(readOp.Handle, readOp.OpContext)
		if err != nil {
			return
		}
		fh.ReleaseFlyingBuffer(readOp)
	}
}

func (fs *ObjcacheFileSystem) SyncFile(_ context.Context, op *fuseops.SyncFileOp) (err error) {
	fs.WaitReset()
	var fh *FileHandle
	if fh, err = fs.getFileHandle(op.Handle, op.OpContext); err != nil {
		return
	}
	var meta *WorkingMeta
	if meta, err = fh.Flush(fs.back); err != nil {
		return
	}
	fs.updateFileInodeAttr(meta)
	meta, err = fs.back.PersistObject(op.Inode)
	if err == nil && meta != nil {
		fh.SetMeta(meta)
		fs.updateFileInodeAttr(meta)
	}
	return
}

func (fs *ObjcacheFileSystem) FlushFile(_ context.Context, op *fuseops.FlushFileOp) (err error) {
	fs.WaitReset()
	var fh *FileHandle
	if fh, err = fs.getFileHandle(op.Handle, op.OpContext); err != nil {
		return
	}
	var meta *WorkingMeta
	meta, err = fh.Flush(fs.back)
	if err == nil {
		fs.updateFileInodeAttr(meta)
	}
	return
}

func (fs *ObjcacheFileSystem) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) (err error) {
	fs.WaitReset()
	err = fs.closeFileHandle(op.Handle, op.OpContext)
	return
}

func (fs *ObjcacheFileSystem) createFileOrDir(parentId fuseops.InodeID, name string, mode uint32) (inodeKey InodeKeyType, inode LocalInode, entry fuseops.ChildInodeEntry, err error) {
	fs.lock.RLock()
	parentInode, ok := fs.inodes[parentId]
	fs.lock.RUnlock()
	if !ok {
		err = unix.ENOENT
		return
	}
	inodeKey = fs.back.inodeMgr.CreateInodeId()
	childAttr := NewMetaAttributes(inodeKey, mode)
	var meta *WorkingMeta
	if meta, err = fs.back.CreateObject(parentInode.key, NewMetaAttributes(InodeKeyType(parentId), uint32(parentInode.attr.Mode)), name, childAttr); err != nil {
		return
	}
	fs.lock.Lock()
	key := filepath.Join(parentInode.key, name)
	fs.trackCacheInKernel(key, fuseops.InodeID(inodeKey), meta)
	inode = fs.inodes[fuseops.InodeID(inodeKey)]
	parentInode.children[name] = meta.toMetaAttr()
	fs.inodes[parentId] = parentInode
	fs.lock.Unlock()
	entry.Child = fuseops.InodeID(inodeKey)
	entry.Attributes = meta.GetAttr(fs.uid, fs.gid)
	entry.AttributesExpiration = MaxTime
	//entry.EntryExpiration = MaxTime
	return
}

func (fs *ObjcacheFileSystem) CreateFile(_ context.Context, op *fuseops.CreateFileOp) (err error) {
	fs.WaitReset()
	var inodeKey InodeKeyType
	var inode LocalInode
	inodeKey, inode, op.Entry, err = fs.createFileOrDir(op.Parent, op.Name, uint32(op.Mode))
	if err != nil {
		return err
	}
	atomic.AddInt32(inode.writerCount, 1)
	op.Handle = fs.getProc(op.OpContext).newFileHandleFromMeta(inode.toMetaRWHandler(inodeKey), false, fs.back.flags.DisableLocalWritebackCaching)
	return
}

func (fs *ObjcacheFileSystem) MkDir(_ context.Context, op *fuseops.MkDirOp) (err error) {
	fs.WaitReset()
	_, _, op.Entry, err = fs.createFileOrDir(op.Parent, op.Name, uint32(op.Mode|os.ModeDir))
	if err != nil {
		return err
	}
	return
}

func (fs *ObjcacheFileSystem) unlinkFileOrDir(parentInodeId fuseops.InodeID, name string) error {
	fs.lock.RLock()
	parent, ok := fs.inodes[parentInodeId]
	if !ok {
		fs.lock.RUnlock()
		return unix.ENOENT
	}
	child, ok2 := parent.children[name]
	fs.lock.RUnlock()
	if !ok2 {
		return unix.ENOENT
	}
	if err := fs.back.UnlinkObject(parent.key, parentInodeId, name, child.inodeKey); err != nil {
		return err
	}
	fs.lock.Lock()
	delete(parent.children, name)
	fs.inodes[parentInodeId] = parent
	fs.lock.Unlock()
	return nil
}

func (fs *ObjcacheFileSystem) RmDir(_ context.Context, op *fuseops.RmDirOp) (err error) {
	fs.WaitReset()
	err = fs.unlinkFileOrDir(op.Parent, op.Name)
	return err
}

func (fs *ObjcacheFileSystem) SetInodeAttributes(_ context.Context, op *fuseops.SetInodeAttributesOp) (err error) {
	fs.WaitReset()
	fs.lock.RLock()
	inode, ok := fs.inodes[op.Inode]
	if !ok {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	parent, ok := fs.inodes[fuseops.InodeID(inode.parentInodeKey)]
	fs.lock.RUnlock()
	if !ok {
		err = unix.ENOENT
		return
	}

	var doTruncate = false
	var fh *FileHandle = nil
	if op.Handle != nil {
		fh, err = fs.getFileHandle(*op.Handle, op.OpContext)
		if err != nil {
			return
		}
		fh.SetModeMTime(op.Mode, op.Mtime)
		doTruncate = op.Size != nil && *op.Size != uint64(fh.h.size)
		if doTruncate {
			var meta *WorkingMeta
			meta, err = fh.Flush(fs.back)
			if err != nil {
				return
			}
			fs.updateFileInodeAttr(meta)
			doTruncate = op.Size != nil && *op.Size != uint64(fh.h.size)
		}
	} else {
		if op.Mode != nil {
			mode := uint32(*op.Mode)
			if inode.attr.Mode&os.ModeDir != 0 {
				mode |= uint32(os.ModeDir)
			}
			ts := inode.attr.Mtime.UnixNano()
			if op.Mtime != nil {
				ts = op.Mtime.UnixNano()
			}
			var meta *WorkingMeta
			meta, err = fs.back.UpdateObjectAttr(MetaAttributes{inodeKey: InodeKeyType(op.Inode), mode: mode}, ts)
			if err != nil {
				return
			}
			attr := meta.GetAttr(fs.uid, fs.gid)
			fs.lock.RLock()
			inode = fs.inodes[op.Inode]
			needUpdate := meta.version != inode.version
			fs.lock.RUnlock()
			if needUpdate {
				fs.updateFileInodeAttr(meta)
			} else {
				fs.lock.Lock()
				inode.attr = attr
				fs.inodes[op.Inode] = inode
				if meta.IsDir() {
					parent.children[filepath.Base(inode.key)] = meta.toMetaAttr()
				} else {
					parent.children[inode.key] = meta.toMetaAttr()
				}
				fs.inodes[fuseops.InodeID(inode.parentInodeKey)] = parent
				fs.lock.Unlock()
			}
			op.Attributes = attr
			return
		}
		doTruncate = op.Size != nil
	}
	if doTruncate {
		var meta *WorkingMeta
		meta, err = fs.back.TruncateObject(op.Inode, int64(*op.Size))
		if err != nil {
			return
		}
		if fh != nil {
			fh.SetMeta(meta) // TODO: race condition under op.Handle == nil?
		}
		fs.updateFileInodeAttr(meta)
		op.Attributes = meta.GetAttr(fs.uid, fs.gid)
	} else {
		fs.lock.Lock()
		if op.Mtime != nil {
			inode.attr.Mtime = *op.Mtime
		}
		if op.Mode != nil {
			if inode.attr.Mode&os.ModeDir != 0 {
				inode.attr.Mode = *op.Mode | os.ModeDir
			} else {
				inode.attr.Mode = *op.Mode
			}
		}
		fs.inodes[op.Inode] = inode
		if inode.attr.Mode&os.ModeDir != 0 {
			parent.children[filepath.Base(inode.key)] = NewMetaAttributes(InodeKeyType(op.Inode), uint32(inode.attr.Mode))
		} else {
			parent.children[inode.key] = NewMetaAttributes(InodeKeyType(op.Inode), uint32(inode.attr.Mode))
		}
		fs.inodes[fuseops.InodeID(inode.parentInodeKey)] = parent
		fs.lock.Unlock()
		op.Attributes = inode.attr
	}
	return
}

func (fs *ObjcacheFileSystem) WriteFile(_ context.Context, op *fuseops.WriteFileOp) (err error) {
	fs.WaitReset()
	// NOTE: when page cahce is on, multiple WriteFile() calls can be invoked in parallel.
	begin := time.Now()
	var fh *FileHandle
	if fh, err = fs.getFileHandle(op.Handle, op.OpContext); err != nil {
		return
	}
	var meta *WorkingMeta
	if meta, err = fh.Write(op.Offset, op.Data, fs.back); err != nil {
		return
	}
	if meta != nil {
		fs.updateFileInodeAttr(meta)
	}
	elapsed := time.Since(begin)
	log.Debugf("Success: WriteFile, pid=%v, handle=%v, inode=%v, offset=%v, size=%v, elapsed=%v", op.OpContext.Pid, op.Handle, op.Inode, op.Offset, len(op.Data), elapsed)
	return
}

func (fs *ObjcacheFileSystem) Unlink(_ context.Context, op *fuseops.UnlinkOp) (err error) {
	fs.WaitReset()
	err = fs.unlinkFileOrDir(op.Parent, op.Name)
	return err
}

func (fs *ObjcacheFileSystem) Rename(_ context.Context, op *fuseops.RenameOp) (err error) {
	fs.WaitReset()
	fs.lock.RLock()
	parent, ok := fs.inodes[op.OldParent]
	if !ok {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	newParent, ok := fs.inodes[op.NewParent]
	if !ok {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	child, ok2 := parent.children[op.OldName]
	fs.lock.RUnlock()
	if !ok2 {
		return unix.ENOENT
	}
	err = fs.back.RenameObject(parent.key, NewMetaAttributes(InodeKeyType(op.OldParent), uint32(parent.attr.Mode)), newParent.key, op.NewParent, op.OldName, op.NewName, child)
	fs.lock.Lock()
	delete(parent.children, op.OldName)
	newParent.children[op.NewName] = child
	fs.inodes[op.OldParent] = parent
	if op.OldParent != op.NewParent {
		fs.inodes[op.NewParent] = newParent
	}
	fs.lock.Unlock()
	return
}

func (fs *ObjcacheFileSystem) MkNode(_ context.Context, op *fuseops.MkNodeOp) (err error) {
	fs.WaitReset()
	special := ""
	switch op.Mode.Type() {
	case os.ModeDir:
		special = "dir"
	case os.ModeCharDevice:
		special = "chardev"
	case os.ModeDevice:
		special = "dev"
	case os.ModeNamedPipe:
		special = "pipe"
	case os.ModeSymlink:
		special = "symlink"
	case os.ModeSocket:
		special = "socket"
	}
	log.Infof("NotImplemented: MkNode, parent=%v, name=%v, mode=%v, special=%v", op.Parent, op.Name, op.Mode.String(), special)
	err = fuse.ENOSYS
	return
}

func (fs *ObjcacheFileSystem) CreateSymlink(_ context.Context, op *fuseops.CreateSymlinkOp) (err error) {
	fs.WaitReset()
	log.Infof("NotImplemented: CreateSymlink, parent=%v, name=%v, target=%v", op.Parent, op.Name, op.Target)
	err = fuse.ENOSYS
	return
}

func (fs *ObjcacheFileSystem) CreateLink(_ context.Context, op *fuseops.CreateLinkOp) (err error) {
	fs.WaitReset()
	fs.lock.RLock()
	inode, ok := fs.inodes[op.Parent]
	if !ok {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	child, ok2 := inode.children[op.Name]
	if !ok2 {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	_, ok3 := fs.inodes[fuseops.InodeID(child.inodeKey)]
	if !ok3 {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	if child.IsDir() {
		fs.lock.RUnlock()
		err = unix.EPERM
		return
	}
	dst, ok4 := fs.inodes[op.Target]
	if !ok4 {
		fs.lock.RUnlock()
		err = unix.ENOENT
		return
	}
	fs.lock.RUnlock()
	var meta *WorkingMeta
	if meta, err = fs.back.HardLinkObject(op.Parent, NewMetaAttributes(InodeKeyType(op.Parent), uint32(inode.attr.Mode)), dst.key, op.Target, op.Name, child); err != nil {
		return
	}
	fs.lock.Lock()
	dst.children[op.Name] = child
	fs.lock.Unlock()
	op.Entry.Child = fuseops.InodeID(child.inodeKey)
	op.Entry.Attributes = meta.GetAttr(fs.uid, fs.gid)
	op.Entry.AttributesExpiration = MaxTime
	//entry.EntryExpiration = MaxTime
	return
}

func (fs *ObjcacheFileSystem) ReadSymlink(_ context.Context, op *fuseops.ReadSymlinkOp) (err error) {
	fs.WaitReset()
	log.Infof("NotImplemented: ReadSymlink, inode=%v, target=%v", op.Inode, op.Target)
	err = fuse.ENOSYS
	return
}

func (fs *ObjcacheFileSystem) Fallocate(_ context.Context, op *fuseops.FallocateOp) (err error) {
	fs.WaitReset()
	var fh *FileHandle
	if fh, err = fs.getFileHandle(op.Handle, op.OpContext); err != nil {
		return
	}
	var meta *WorkingMeta
	if meta, err = fh.Flush(fs.back); err != nil {
		return
	}
	fs.updateFileInodeAttr(meta)
	newSize := int64(op.Offset + op.Length)
	length := fh.GetLength()
	var doTruncate = false
	if op.Mode == 0x0 && length < newSize {
		doTruncate = true
	} else if op.Mode == 0x2 && int64(op.Offset) < length && newSize >= length {
		doTruncate = true
		newSize = int64(op.Offset)
	}
	if doTruncate {
		if meta, err = fs.back.TruncateObject(op.Inode, newSize); err != nil {
			return
		}
		if err == nil {
			fh.SetMeta(meta)
			fs.updateFileInodeAttr(meta)
		}
	}
	return
}

func (fs *ObjcacheFileSystem) RequestJoinLocal(headWorkerAddr string, headWorkerPort int) (err error) {
	err = fs.back.RequestJoinLocal(headWorkerAddr, headWorkerPort)
	return
}

func (fs *ObjcacheFileSystem) InitNodeListAsClient() (err error) {
	err = fs.back.UpdateNodeListAsClient()
	return
}

func (fs *ObjcacheFileSystem) Destroy() {
}

func (fs *ObjcacheFileSystem) Reset() error {
	fs.resetCond.L.Lock()
	if fs.isResetting > 0 {
		fs.resetCond.L.Unlock()
		return unix.EBUSY
	}
	fs.isResetting = 1
	fs.resetCond.L.Unlock()

	fs.proc.lock.RLock()
	ok := len(fs.proc.files) == 0
	fs.proc.lock.RUnlock()
	if !ok {
		fs.EndReset()
		return unix.EBUSY
	}
	fs.invalidateAllStaleInodes(fuseops.InodeID(math.MaxUint64))
	fs.staleLock.Lock()
	ok = len(fs.staleDeletes) == 0 && len(fs.staleInodes) == 0
	fs.staleLock.Unlock()
	if !ok {
		fs.EndReset()
		return unix.EBUSY
	}
	fs.lock.Lock()
	fs.inodes = make(map[fuseops.InodeID]LocalInode, 0)
	fs.SetRoot()
	fs.lock.Unlock()
	// caller must call fs.EndReset() to resume filesystem
	return nil
}

func (fs *ObjcacheFileSystem) CheckReset() (ok bool) {
	if ok = fs.lock.TryLock(); !ok {
		log.Errorf("Failed: ObjcacheFileSystem.CheckReset, fs.lock is taken")
	} else {
		if ok2 := len(fs.inodes) <= 1; !ok2 { // Root inode is not invalidated
			log.Errorf("Failed: ObjcacheFileSystem.CheckReset, len(fs.inodes) != 1")
			ok = false
		}
		fs.lock.Unlock()
	}
	if ok2 := fs.staleLock.TryLock(); !ok2 {
		log.Errorf("Failed: ObjcacheFileSystem.CheckReset, fs.staleLock is taken")
		ok = false
	} else {
		if ok2 := len(fs.staleInodes) == 0; !ok2 {
			log.Errorf("Failed: ObjcacheFileSystem.CheckReset, len(fs.staleInodes) != 0")
			ok = false
		}
		if ok2 := len(fs.staleDeletes) == 0; !ok2 {
			log.Errorf("Failed: ObjcacheFileSystem.CheckReset, len(fs.staleDeletes) != 0")
			ok = false
		}
		fs.staleLock.Unlock()
	}
	if ok2 := fs.resetCond.L.(*sync.Mutex).TryLock(); !ok2 {
		log.Errorf("Failed: ObjcacheFileSystem.CheckReset, fs.resetCond is taken")
		ok = false
	} else {
		fs.resetCond.L.Unlock()
	}
	if ok2 := fs.proc.CheckReset(); !ok2 {
		log.Errorf("Failed: ObjcacheFileSystem.CheckReset, proc")
		ok = false
	}
	return
}

func (fs *ObjcacheFileSystem) EndReset() {
	fs.resetCond.L.Lock()
	fs.isResetting = 0
	fs.resetCond.Broadcast()
	fs.resetCond.L.Unlock()
}

func mapHttpError(status int) error {
	switch status {
	case 400:
		return fuse.EINVAL
	case 401:
		return unix.EACCES
	case 403:
		return unix.EACCES
	case 404:
		return fuse.ENOENT
	case 405:
		return unix.ENOTSUP
	case http.StatusConflict:
		return unix.EINTR
	case 429:
		return unix.EAGAIN
	case 500:
		return unix.EAGAIN
	default:
		return nil
	}
}

func mapAwsError(err error) error {
	if err == nil {
		return nil
	}

	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case "BucketRegionError":
			// don't need to log anything, we should detect region after
			return err
		case "NoSuchBucket":
			return unix.ENXIO
		case "BucketAlreadyOwnedByYou":
			return fuse.EEXIST
		case "InvalidRange":
			return unix.EFAULT
		}

		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// A service error occurred
			err = mapHttpError(reqErr.StatusCode())
			if err != nil {
				return err
			} else {
				s3Log.Errorf("http=%v %v s3=%v request=%v\n",
					reqErr.StatusCode(), reqErr.Message(),
					awsErr.Code(), reqErr.RequestID())
				return reqErr
			}
		} else {
			// Generic AWS Error with Code, Message, and original error (if any)
			s3Log.Errorf("code=%v msg=%v, err=%v\n", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			return awsErr
		}
	} else {
		return err
	}
}

// note that this is NOT the same as url.PathEscape in golang 1.8,
// as this preserves / and url.PathEscape converts / to %2F
func pathEscape(path string) string {
	u := url.URL{Path: path}
	return u.EscapedPath()
}
