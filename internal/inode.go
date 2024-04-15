/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/IBM/objcache/common"
	"github.com/serialx/hashring"
	"github.com/takeshi-yoshimura/fuse/fuseops"
	"golang.org/x/sys/unix"
)

type InodeKeyType uint64

type Inode struct {
	files    map[string]bool //required for flushing at server side
	meta     *WorkingMeta
	chunkMap map[int64]*Chunk
	children map[string]InodeKeyType //Directories' children file name and inode keys. TODO: should be recorded as chunks

	metaLock    *sync.RWMutex
	chunkLock   *sync.RWMutex
	metaTxLock  *sync.Mutex
	persistLock *sync.Mutex
}

func NewInode() *Inode {
	return &Inode{
		files: make(map[string]bool), chunkMap: make(map[int64]*Chunk), children: nil,
		metaLock: new(sync.RWMutex), chunkLock: new(sync.RWMutex), metaTxLock: new(sync.Mutex), persistLock: new(sync.Mutex),
	}
}

func (i *Inode) UpdateMeta(meta *WorkingMeta, children map[string]InodeKeyType) {
	i.metaLock.Lock()
	i.meta = meta
	i.children = children
	i.metaLock.Unlock()
}

func (i *Inode) CopyChildren() map[string]InodeKeyType {
	ret := make(map[string]InodeKeyType)
	for key, value := range i.children {
		ret[key] = value
	}
	return ret
}

func (i *Inode) CopyMeta() *WorkingMeta {
	if i.meta == nil {
		return nil
	}
	ret := *i.meta
	return &ret
}

func (i *Inode) CopyFiles() []string {
	ret := make([]string, 0)
	for key := range i.files {
		ret = append(ret, key)
	}
	return ret
}

type InodeMgr struct {
	nextInodeId uint32 //InodeKeyType(uint64(raft.selfId)<<32 + uint64(txMgr.GetNextClock()))
	inodes      map[InodeKeyType]*Inode
	inodeLock   *sync.RWMutex

	defaultDirMode  uint32
	defaultFileMode uint32

	uploadSem chan struct{}

	back      *ObjCacheBackend
	raft      *RaftInstance
	readCache *ReaderBufferCache
}

const (
	MaxNrCacheForUpdateChunk  int = 32
	MaxNrWriterForUpdateChunk int = 16
)

func NewInodeMgr(back *ObjCacheBackend, raft *RaftInstance, flags *common.ObjcacheConfig) *InodeMgr {
	ret := &InodeMgr{
		nextInodeId:     0,
		inodes:          make(map[InodeKeyType]*Inode),
		inodeLock:       new(sync.RWMutex),
		defaultDirMode:  flags.DirMode | uint32(os.ModeDir),
		defaultFileMode: flags.FileMode,
		back:            back,
		raft:            raft,
		readCache:       NewReaderBufferCache(flags),
		uploadSem:       make(chan struct{}, flags.UploadParallel),
	}
	if back != nil {
		root := ret.GetInode(fuseops.RootInodeID)
		root.files[""] = true
		meta, children := ret.NewWorkingMetaForRoot()
		root.UpdateMeta(meta, children)
	}
	return ret
}

func (n *InodeMgr) Clean(dirtyMgr *DirtyMgr) int32 {
	n.readCache.DropAll()
	inodeKeys := make([]uint64, 0)
	keys := make([]string, 0)
	n.inodeLock.RLock()
	inodes := make([]*Inode, 0)
	for inodeKey, inode := range n.inodes {
		if dirtyMgr.IsDirtyMeta(inodeKey) {
			log.Warnf("InodeMgr.Clean, key is dirty, inodeKey=%v", inodeKey)
			continue
		}
		if inodeKey == InodeKeyType(fuseops.RootInodeID) {
			continue
		}
		for key := range inode.files {
			inodeKeys = append(inodeKeys, uint64(inodeKey))
			keys = append(keys, key)
		}
		if len(inode.files) == 0 {
			inodeKeys = append(inodeKeys, uint64(inodeKey))
			keys = append(keys, "")
		}
		inode.metaTxLock.Lock()
		inodes = append(inodes, inode)
	}
	n.inodeLock.RUnlock()
	reply := n.raft.AppendExtendedLogEntry(NewDeleteInodeFileMapCommand(inodeKeys, keys))
	if reply != RaftReplyOk {
		log.Errorf("Failed: InodeMgr.Clean, AppendExtendedLogEntry, reply=%v", reply)
	}
	for _, inode := range inodes {
		inode.metaTxLock.Unlock()
	}
	return reply
}

func (n *InodeMgr) CheckReset() (ok bool) {
	ok = true
	var metaLock = true
	if ok2 := n.inodeLock.TryLock(); !ok2 {
		log.Errorf("Failed: InodeMgr.CheckReset, metadataMapLock is taken")
		metaLock = false
		ok = false
	}
	if metaLock {
		if ok2 := len(n.inodes) == 1; !ok2 {
			log.Errorf("Failed: InodeMgr.CheckReset, len(n.inodes) != 1")
			ok = false
		}
		n.inodeLock.Unlock()
	}
	if ok2 := n.readCache.CheckReset(); !ok2 {
		log.Errorf("Failed: InodeMgr.CheckReset, readCache")
		ok = false
	}
	return
}

func (n *InodeMgr) CreateInodeId() (InodeKeyType, error) {
	for {
		cur := atomic.LoadUint32(&n.nextInodeId)
		if cur >= uint32(math.MaxUint32) {
			return 0, unix.ENOSPC
		}
		if atomic.CompareAndSwapUint32(&n.nextInodeId, cur, cur+1) {
			return InodeKeyType((uint64(n.raft.selfId) << 32) | uint64(cur)), nil
		}
	}
}

/*******************************************
 * methods to read meta
 * inodes can be on the cluster or COS
 *******************************************/

func (n *InodeMgr) GetInode(inodeKey InodeKeyType) *Inode {
	n.inodeLock.Lock()
	inode, ok := n.inodes[inodeKey]
	if !ok {
		inode = NewInode()
		n.inodes[inodeKey] = inode
		log.Debugf("GetMetaHandler, create a new Meta, inodeKey=%v", inodeKey)
	}
	n.inodeLock.Unlock()
	return inode
}

func (n *InodeMgr) NewWorkingMetaForRoot() (*WorkingMeta, map[string]InodeKeyType) {
	inodeKey := InodeKeyType(fuseops.RootInodeID)
	ret := NewWorkingMeta(inodeKey, 4096, -1, n.defaultDirMode, uint32(1+len(n.back.buckets)), "")
	dirNames := make([]string, 0)
	for dirName := range n.back.buckets {
		dirNames = append(dirNames, dirName)
	}
	sort.Strings(dirNames)
	children := make(map[string]InodeKeyType)
	for i := 0; i < len(dirNames); i++ {
		children[dirNames[i]] = InodeKeyType(fuseops.RootInodeID + i + 1)
		ret.size += int64(unsafe.Sizeof(dirNames[i]) + unsafe.Sizeof(InodeKeyType(fuseops.RootInodeID+i+1)))
	}
	return ret, children
}

func (n *InodeMgr) __fetchWorkingMeta(inodeKey InodeKeyType, key string, chunkSize int64, expireMs int32, parent InodeKeyType) (*WorkingMeta, map[string]InodeKeyType, int32) {
	// must hold metadataMap.RLock() (No states are updated here. Raft will do it)
	working := NewWorkingMeta(inodeKey, chunkSize, expireMs, n.defaultFileMode, 1, key)
	head, awsErr := n.back.headBlob(&HeadBlobInput{Key: key})
	var mayDir = false
	if r := AwsErrToReply(awsErr); r != RaftReplyOk {
		if r == ErrnoToReply(unix.ENOENT) {
			// users may not explicitly create a directory. so, ignore fuse.ENOENT here.
			mayDir = true
		} else {
			log.Errorf("Failed: __fetchWorkingMeta, headBlob, name=%v, awsErr=%v, r=%v", key, awsErr, r)
			return nil, nil, r
		}
	} else if head != nil {
		working.size = int64(head.Size)
		if head.IsDirBlob {
			working.mode = n.defaultDirMode
		} else {
			working.mode = n.defaultFileMode
		}
		if head.LastModified != nil {
			working.mTime = head.LastModified.UnixNano()
		}
	}
	keyWithSuffix := key + "/"
	var children map[string]InodeKeyType = nil
	if mayDir || working.IsDir() {
		head, awsErr = n.back.headBlob(&HeadBlobInput{Key: keyWithSuffix})
		if r := AwsErrToReply(awsErr); r != RaftReplyOk {
			if r != ErrnoToReply(unix.ENOENT) {
				// users may not explicitly create a directory. so, ignore fuse.ENOENT here.
				log.Errorf("Failed: __fetchWorkingMeta, headBlob, name=%v, awsErr=%v, r=%v", keyWithSuffix, awsErr, r)
				return nil, nil, r
			}
		} else if head != nil {
			working.size = int64(head.Size)
			if head.IsDirBlob {
				working.mode = n.defaultDirMode
			} else {
				working.mode = n.defaultFileMode
			}
			if head.LastModified != nil {
				working.mTime = head.LastModified.UnixNano()
			}
			if working.IsDir() {
				working.size = 0
				mayDir = false
			}
		}
		// we may already know dirty files on local due to migration/log replay.
		if knownInode, ok := n.inodes[inodeKey]; ok {
			working.size = 0
			if len(knownInode.children) > 0 {
				working.mode = n.defaultDirMode
				for childName, childInodeKey := range knownInode.children {
					working.size += int64(unsafe.Sizeof(childName) + unsafe.Sizeof(childInodeKey))
					if strings.HasSuffix(childName, "/") {
						working.nlink += 1
					}
				}
				return working, nil, RaftReplyOk
			}
		}
		sep := string(os.PathSeparator)
		arg := &ListBlobsInput{
			Prefix:    &keyWithSuffix,
			Delimiter: &sep,
		}
		children = make(map[string]InodeKeyType)
		children["."] = inodeKey
		children[".."] = parent
		var res *ListBlobsOutput = nil
		for {
			if res != nil && res.NextContinuationToken != nil {
				arg.ContinuationToken = res.NextContinuationToken
			}
			res, awsErr = n.back.listBlobs(arg)
			if r := AwsErrToReply(awsErr); r != RaftReplyOk {
				log.Errorf("Failed: __fetchWorkingMeta, headBlob and listBlobs, name=%v, awsErr=%v, r=%v", *arg.Prefix, awsErr, r)
				return nil, nil, r
			}
			if mayDir {
				var count = 0
				for _, item := range res.Items {
					if strings.HasPrefix(*item.Key, keyWithSuffix) {
						count += 1
					}
				}
				if count+len(res.Prefixes) == 0 {
					// no persisted objects
					log.Errorf("Failed: __fetchWorkingMeta, headBlob with empty listBlobs, name=%v", *arg.Prefix)
					return nil, nil, ErrnoToReply(unix.ENOENT)
				} else {
					// an empty directory is persisted
					mayDir = false
					working.size = 0
					working.mode = n.defaultDirMode
				}
			}
			working.size = int64(unsafe.Sizeof(".") + unsafe.Sizeof(inodeKey))
			working.size += int64(unsafe.Sizeof("..") + unsafe.Sizeof(inodeKey))
			var c = 0
			for _, item := range res.Items {
				if item.Key == nil {
					continue
				}
				childKey := strings.TrimSuffix(*item.Key, "/")
				childKeyWithSuffix := childKey + "/"
				if childKey == key || childKeyWithSuffix == keyWithSuffix {
					continue
				}
				childInodeKey, err2 := n.CreateInodeId()
				if err2 != nil {
					log.Errorf("Failed: __fetchWorkingMeta, GetOrCreateInodeId, childKey=%v, err=%v", childKey, err2)
					return nil, nil, ErrnoToReply(err2)
				}
				childName := filepath.Base(childKey)
				children[childName] = childInodeKey
				working.size += int64(unsafe.Sizeof(childName) + unsafe.Sizeof(childInodeKey))
				working.nlink += 1
				c += 1
				if c <= 30 {
					log.Debugf("__fetchWorkingMeta: file %v is a child of %v", *item.Key, key)
				}
			}
			for _, prefix := range res.Prefixes {
				if prefix.Prefix == nil {
					continue
				}
				childKey := strings.TrimSuffix(*prefix.Prefix, "/")
				childKeyWithSuffix := childKey + "/"
				if childKey == key || childKeyWithSuffix == keyWithSuffix {
					continue
				}
				childInodeKey, err2 := n.CreateInodeId()
				if err2 != nil {
					log.Errorf("Failed: __fetchWorkingMeta, GetOrCreateInodeId, childKey=%v, err=%v", childKey, err2)
					return nil, nil, ErrnoToReply(err2)
				}
				childName := filepath.Base(childKey)
				children[childName] = childInodeKey
				working.size += int64(unsafe.Sizeof(childName) + unsafe.Sizeof(childInodeKey))
				working.nlink += 1
				c += 1
				if c <= 30 {
					log.Debugf("__fetchWorkingMeta: directory %v is a child of %v", *prefix.Prefix, key)
				}
			}
			if c == 0 || res.NextContinuationToken == nil {
				break
			}
		}
	}
	return working, children, RaftReplyOk
}

func (n *InodeMgr) GetOrFetchWorkingMeta(inodeKey InodeKeyType, key string, chunkSize int64, expireMs int32, parent InodeKeyType) (*WorkingMeta, map[string]InodeKeyType, int32) {
	if inodeKey == fuseops.RootInodeID {
		inode := n.GetInode(inodeKey)
		inode.metaLock.RLock()
		meta := inode.CopyMeta()
		children := inode.CopyChildren()
		inode.metaLock.RUnlock()
		if meta == nil {
			log.Warnf("BUG: GetOrFetchWorkingMeta, no root meta")
			return nil, nil, RaftReplyFail
		}
		return meta, children, RaftReplyOk
	}
	inode := n.GetInode(inodeKey)
	inode.metaLock.RLock()
	working := inode.CopyMeta()
	if working != nil {
		children := inode.CopyChildren()
		inode.metaLock.RUnlock()
		if working.IsDeleted() {
			log.Debugf("GetOrFetchWorkingMeta: meta is deleted, inodeKey=%v, key=%v, version=%v, mTime=%v", inodeKey, key, working.version, working.mTime)
			return nil, nil, ErrnoToReply(unix.ENOENT)
		}
		// TODO: check cache TTL
		log.Debugf("GetOrFetchWorkingMeta: get a cached meta, inodeKey=%v, key=%v, version=%v, mTime=%v", inodeKey, key, working.version, working.mTime)
		return working, children, RaftReplyOk
	}
	working, children, reply := n.__fetchWorkingMeta(inodeKey, key, chunkSize, expireMs, parent)
	if reply != RaftReplyOk {
		inode.metaLock.RUnlock()
		return nil, nil, reply
	}
	inode.metaLock.RUnlock()
	if reply = n.raft.AppendExtendedLogEntry(NewAddInodeFileMapCommand(working, children, key)); reply != RaftReplyOk {
		log.Errorf("Failed: GetOrFetchMeta, AppendExtendedLogEntry, key=%v, reply=%v", key, reply)
		return nil, nil, reply
	}
	inode.metaLock.RLock()
	children = inode.CopyChildren()
	inode.metaLock.RUnlock()
	log.Debugf("Success: GetOrFetchWorkingMeta: instantiate new metadata, inodeKey=%v, key=%v", inodeKey, key)
	return working, children, RaftReplyOk
}

func (n *InodeMgr) GetAllMeta() ([]*common.CopiedMetaMsg, []*common.InodeToFileMsg) {
	metas := make([]*common.CopiedMetaMsg, 0)
	files := make([]*common.InodeToFileMsg, 0)
	n.inodeLock.RLock()
	for inodeKey, inode := range n.inodes {
		inode.metaLock.RLock()
		metas = append(metas, inode.meta.toMsg())
		for f := range inode.files {
			files = append(files, &common.InodeToFileMsg{InodeKey: uint64(inodeKey), FilePath: f})
		}
		inode.metaLock.RUnlock()
	}
	n.inodeLock.RUnlock()
	return metas, files
}

func (n *InodeMgr) GetMetaForNodeLeave(migrationId MigrationId, nodeList *RaftNodeList, dirtyMetas map[string]map[InodeKeyType]bool) map[string]*Snapshot {
	ret := make(map[string]*Snapshot)
	n.inodeLock.RLock()
	for inodeKey, inode := range n.inodes {
		g, ok := GetGroupForMeta(nodeList.ring, inodeKey)
		if !ok {
			continue
		}
		if _, ok2 := ret[g]; !ok2 {
			ret[g] = &Snapshot{migrationId: migrationId}
		}
		if inode.meta != nil {
			ret[g].metas = append(ret[g].metas, inode.meta.toMsg())
		}
		if len(inode.children) > 0 {
			tree := &common.InodeTreeMsg{InodeKey: uint64(inodeKey)}
			for childName, childInodeKey := range inode.children {
				tree.Children = append(tree.Children, &common.CopiedMetaChildMsg{InodeKey: uint64(childInodeKey), Name: childName})
			}
			ret[g].dirents = append(ret[g].dirents, tree)
		}
		for file := range inode.files {
			ret[g].files = append(ret[g].files, &common.InodeToFileMsg{InodeKey: uint64(inodeKey), FilePath: file})
		}
		if dm, ok := dirtyMetas[g]; ok {
			if _, ok2 := dm[inodeKey]; ok2 {
				ret[g].dirtyMetas = append(ret[g].dirtyMetas, &common.DirtyMetaInfoMsg{
					InodeKey: uint64(inodeKey), Version: inode.meta.version, Timestamp: inode.meta.mTime, ExpireMs: inode.meta.expireMs,
				})
			}
		}
	}
	n.inodeLock.RUnlock()
	return ret
}

func (n *InodeMgr) GetMetaForNodeJoin(migrationId MigrationId, selfGroup string, joinGroup string, nodeList *RaftNodeList, newRing *hashring.HashRing, dirtyMetas map[InodeKeyType]bool) *Snapshot {
	ret := &Snapshot{migrationId: migrationId}
	n.inodeLock.RLock()
	for inodeKey, inode := range n.inodes {
		oldOwner, ok := GetGroupForMeta(nodeList.ring, inodeKey)
		if !ok || oldOwner != selfGroup {
			continue
		}
		newOwner, ok := GetGroupForMeta(newRing, inodeKey)
		if ok && newOwner == joinGroup {
			if inode.meta != nil {
				ret.metas = append(ret.metas, inode.meta.toMsg())
			}
			if len(inode.children) > 0 {
				tree := &common.InodeTreeMsg{InodeKey: uint64(inodeKey)}
				for childName, childInodeKey := range inode.children {
					tree.Children = append(tree.Children, &common.CopiedMetaChildMsg{InodeKey: uint64(childInodeKey), Name: childName})
				}
				ret.dirents = append(ret.dirents, tree)
			}
			for file := range inode.files {
				ret.files = append(ret.files, &common.InodeToFileMsg{InodeKey: uint64(inodeKey), FilePath: file})
			}
			if _, ok2 := dirtyMetas[inodeKey]; ok2 {
				ret.dirtyMetas = append(ret.dirtyMetas, &common.DirtyMetaInfoMsg{
					InodeKey: uint64(inodeKey), Version: inode.meta.version, Timestamp: inode.meta.mTime, ExpireMs: inode.meta.expireMs,
				})
			}
		}
	}
	n.inodeLock.RUnlock()
	return ret
}

func (n *InodeMgr) GetAllMetaIn(inodeKeys []uint64) ([]*common.CopiedMetaMsg, []*common.InodeToFileMsg) {
	metas := make([]*common.CopiedMetaMsg, 0)
	files := make([]*common.InodeToFileMsg, 0)
	n.inodeLock.RLock()
	for _, inodeKey := range inodeKeys {
		ik := InodeKeyType(inodeKey)
		if inode, ok := n.inodes[ik]; ok {
			inode.metaLock.RLock()
			metas = append(metas, inode.meta.toMsg())
			for f := range inode.files {
				files = append(files, &common.InodeToFileMsg{InodeKey: uint64(inodeKey), FilePath: f})
			}
			inode.metaLock.RUnlock()
		}
	}
	n.inodeLock.RUnlock()
	return metas, files
}

func (n *InodeMgr) GetAllDirInodes() []*common.InodeTreeMsg {
	keys := make([]*common.InodeTreeMsg, 0)
	n.inodeLock.Lock()
	for inodeKey, inode := range n.inodes {
		if len(inode.children) > 0 && inodeKey != InodeKeyType(fuseops.RootInodeID) {
			children := make([]*common.CopiedMetaChildMsg, 0)
			for childName, childInodeKey := range inode.children {
				children = append(children, &common.CopiedMetaChildMsg{Name: childName, InodeKey: uint64(childInodeKey)})
			}
			keys = append(keys, &common.InodeTreeMsg{InodeKey: uint64(inodeKey), Children: children})
		}
	}
	n.inodeLock.Unlock()
	return keys
}

func (n *InodeMgr) GetAllDirectoryMetaAsUint64() []uint64 {
	keys := make([]uint64, 0)
	n.inodeLock.Lock()
	for inodeKey, inode := range n.inodes {
		if inode.meta != nil && inode.meta.IsDir() && inode.meta.inodeKey != InodeKeyType(fuseops.RootInodeID) {
			keys = append(keys, uint64(inodeKey))
		}
	}
	n.inodeLock.Unlock()
	return keys
}

func (n *InodeMgr) GetAllChunks(chunks map[InodeKeyType]DirtyChunkInfo) ([]*common.AppendCommitUpdateChunksMsg, error) {
	ret := make([]*common.AppendCommitUpdateChunksMsg, 0)
	for inodeKey, cInfo := range chunks {
		for offset, version := range cInfo.OffsetVersions {
			chunk := n.GetChunk(inodeKey, offset, cInfo.chunkSize)
			chunk.lock.RLock()
			working, err := chunk.GetWorkingChunk(version, false)
			if err != nil {
				chunk.lock.RUnlock()
				log.Errorf("Failed: GetAllChunks, GetWorkingChunk, inodeKey=%v, offset=%v, version=%v, not found", inodeKey, offset, version)
				return nil, err
			}
			msg := &common.AppendCommitUpdateChunksMsg{
				InodeKey: uint64(inodeKey), ChunkSize: cInfo.chunkSize, Version: working.chunkVer, ObjectSize: cInfo.objectSize,
				Chunks: []*common.WorkingChunkAddMsg{{Offset: offset, Stagings: working.toStagingChunkAddMsg()}},
			}
			ret = append(ret, msg)
		}
	}
	return ret, nil
}

/****************************************************
 * methods to update metadata without transactions
 ****************************************************/

func (n *InodeMgr) CommitUpdateMetaAttr(inodeKey InodeKeyType, mode uint32, ts int64) {
	inode := n.GetInode(inodeKey)
	inode.metaLock.Lock()
	if inode.meta != nil {
		inode.meta.mode = mode
		inode.meta.mTime = ts
	} else {
		log.Warnf("Failed (ignore): CommitUpdateMetaAttr, working is null, inodeKey=%v", inodeKey)
	}
	inode.metaLock.Unlock()
}

func (n *InodeMgr) UpdateMetaAttr(inodeKey InodeKeyType, mode uint32, ts int64) (*WorkingMeta, int32) {
	inode := n.GetInode(inodeKey)
	inode.metaLock.RLock()
	if inode.meta == nil || inode.meta.IsDeleted() {
		inode.metaLock.RUnlock()
		log.Errorf("Failed: UpdateMetaAttr, inode is not created or deleted, inodeKey=%v", inodeKey)
		return nil, ErrnoToReply(unix.ENOENT)
	}
	oldMode := inode.meta.mode
	oldTs := inode.meta.mTime
	if oldMode == mode && oldTs == ts {
		working := inode.CopyMeta()
		inode.metaLock.RUnlock()
		log.Debugf("Success: UpdateMetaAttr (no update), inodeKey=%v, mode=%v, mTime=%v", inodeKey, oldMode, oldTs)
		return working, RaftReplyOk
	}
	inode.metaLock.RUnlock()
	reply := n.raft.AppendExtendedLogEntry(NewUpdateMetaAttrCommand(inodeKey, mode, ts))
	if reply != RaftReplyOk {
		log.Errorf("Failed: UpdateMetaAttr, AppendExtendedLogEntry, inodeKey=%v, reply=%v", inodeKey, reply)
		return nil, reply
	}
	inode.metaLock.RLock()
	working := inode.CopyMeta()
	inode.metaLock.RUnlock()
	log.Debugf("Success: UpdateMetaAttr, inodeKey=%v, mode=%v->%v, mTime=%v->%v",
		inodeKey, oldMode, working.mode, oldTs, working.mTime)
	return working, RaftReplyOk
}

func (n *InodeMgr) UpdateMetaXattr(inodeKey InodeKeyType, expireMs int32, dirtyMgr *DirtyMgr) int32 {
	//TODO
	return RaftReplyOk
}

/****************************************************
 * methods to update objects
 * updating meta is separated into prepare and commit
 ****************************************************/

func (n *InodeMgr) PrepareCreateMeta(inodeKey InodeKeyType, chunkSize int64, expireMs int32, mode uint32) (*WorkingMeta, func(*NodeServer), int32) {
	inode := n.GetInode(inodeKey)
	inode.metaTxLock.Lock() // NOTE: TryLock increases latency too much

	working := NewWorkingMeta(inodeKey, chunkSize, expireMs, mode, 1, "")
	inode.metaLock.RLock()
	if inode.meta != nil {
		working.mTime = time.Now().UnixNano()
		working.version = inode.meta.version + 1
		working.prevVer = inode.meta
		if inode.meta.IsDeleted() {
			working.nlink = 1
			working.chunkSize = chunkSize
			working.mode = mode
			working.expireMs = expireMs
		}
	}
	unlock := func(n *NodeServer) {
		inode.metaTxLock.Unlock()
	}
	inode.metaLock.RUnlock()
	log.Debugf("Success: PrepareCreateMeta, inodeKey=%v", inodeKey)
	return working, unlock, RaftReplyOk
}

func (n *InodeMgr) PrepareUpdateMetaKey(inodeKey InodeKeyType, oldKey string, parent InodeKeyType, chunkSize int64, expireMs int32) (*WorkingMeta, map[string]InodeKeyType, func(*NodeServer), int32) {
	inode := n.GetInode(inodeKey)
	inode.metaTxLock.Lock() // NOTE: TryLock increases latency too much
	inode.metaLock.RLock()
	prev := inode.meta
	children := inode.CopyChildren()
	if prev == nil {
		var reply int32
		prev, children, reply = n.__fetchWorkingMeta(inodeKey, oldKey, chunkSize, expireMs, parent)
		if reply != RaftReplyOk {
			inode.metaLock.Unlock()
			return nil, nil, nil, reply
		}
	}
	working := *prev
	working.mTime = time.Now().UnixNano()
	working.version += 1
	working.prevVer = prev
	inode.metaLock.RUnlock()
	unlock := func(n *NodeServer) {
		inode.metaTxLock.Unlock()
	}
	log.Debugf("Success: PrepareUpdateMetaKey, inodeKey=%v, oldKey=%v", inodeKey, oldKey)
	return &working, children, unlock, RaftReplyOk
}

func (n *InodeMgr) PrepareUpdateParent(inodeKey InodeKeyType, delete bool) (*WorkingMeta, map[string]InodeKeyType, func(*NodeServer), int32) {
	inode := n.GetInode(inodeKey)
	inode.metaTxLock.Lock() // NOTE: TryLock increases latency too much
	inode.metaLock.RLock()
	prev := inode.CopyMeta()
	children := inode.CopyChildren()
	inode.metaLock.RUnlock()
	if prev == nil {
		inode.metaTxLock.Unlock()
		log.Errorf("Failed: PrepareUpdateParent, GetWorkingMeta, key does not exist, inodeKey=%v", inodeKey)
		return nil, nil, nil, ErrnoToReply(unix.ENOENT)
	}
	if !prev.IsDir() {
		inode.metaTxLock.Unlock()
		log.Errorf("Failed: PrepareUpdateParent, key is not a directory, inodeKey=%v", inodeKey)
		return nil, nil, nil, ErrnoToReply(unix.ENOTDIR)
	}
	unlock := func(n *NodeServer) {
		inode.metaTxLock.Unlock()
	}
	log.Debugf("Success: PrepareUpdateParent, inodeKey=%v", inodeKey)
	return prev, children, unlock, RaftReplyOk
}

func (n *InodeMgr) PrepareUpdateMeta(inodeKey InodeKeyType, delete bool) (*WorkingMeta, func(*NodeServer), int32) {
	inode := n.GetInode(inodeKey)
	inode.metaTxLock.Lock() // NOTE: TryLock increases latency too much
	inode.metaLock.RLock()
	if inode.meta == nil {
		inode.metaLock.RUnlock()
		inode.metaTxLock.Unlock()
		log.Errorf("Failed: PrepareUpdateMeta, GetWorkingMeta, key does not exist, inodeKey=%v", inodeKey)
		return nil, nil, ErrnoToReply(unix.ENOENT)
	}
	working := *inode.meta
	if !delete && working.IsDeleted() {
		working.nlink += 1
	}
	if delete && working.IsDir() && len(inode.children) > 2 {
		inode.metaLock.RUnlock()
		inode.metaTxLock.Unlock()
		log.Errorf("Failed: PrepareUpdateMeta, tried to delete a non-empty dir, inodeKey=%v", inodeKey)
		return nil, nil, ErrnoToReply(unix.ENOTEMPTY)
	}
	unlock := func(n *NodeServer) {
		inode.metaTxLock.Unlock()
	}
	working.version = inode.meta.version + 1
	working.mTime = time.Now().UnixNano()
	working.prevVer = inode.meta
	inode.metaLock.RUnlock()
	log.Debugf("Success: PrepareUpdateMeta, inodeKey=%v, version=%v->%v", inodeKey, working.version-1, working.version)
	return &working, unlock, RaftReplyOk
}

func (n *InodeMgr) PreparePersistMeta(inodeKey InodeKeyType, dirtyMgr *DirtyMgr) (*Inode, []string, *WorkingMeta, int32) {
	inode := n.GetInode(inodeKey)
	inode.persistLock.Lock() // unlock at UnlockPersistInode
	inode.metaLock.RLock()
	working := inode.CopyMeta()
	metaKeys := inode.CopyFiles()
	inode.metaLock.RUnlock()
	if working == nil || working.IsDeleted() {
		inode.persistLock.Unlock()
		log.Debugf("PreparePersistMeta, inodeKey=%v does not exist", inodeKey)
		return nil, nil, nil, ObjCacheIsNotDirty
	}
	if len(metaKeys) == 0 {
		inode.persistLock.Unlock()
		log.Debugf("PreparePersistMeta, inodeKey=%v has no keys", inodeKey)
		return nil, nil, working, ObjCacheIsNotDirty
	}
	if ok := dirtyMgr.IsDirtyMeta(inodeKey); !ok {
		inode.persistLock.Unlock()
		log.Debugf("PreparePersistMeta, inode is not dirty, inodeKey=%v", inodeKey)
		return nil, nil, nil, ObjCacheIsNotDirty
	}
	log.Debugf("Success: PreparePersistMeta, inodeKey=%v", inodeKey)
	return inode, metaKeys, working, RaftReplyOk
}

func (n *InodeMgr) PreparePersistDeleteMeta(key string, dirtyMgr *DirtyMgr) int32 {
	_, ok := dirtyMgr.GetDeleteKey(key)
	if !ok {
		log.Debugf("PreparePersistDeleteMeta, key=%v, is not dirty", key)
		return ObjCacheIsNotDirty
	}
	log.Debugf("Success: PreparePersistDeleteMeta, key=%v", key)
	return RaftReplyOk
}

func (n *InodeMgr) UnlockPersistInode(inode *Inode, inodeKey InodeKeyType) {
	inode.persistLock.Unlock()
	log.Debugf("Success: UnlockPersistInode, inodeKey=%v", inodeKey)
}

func (n *InodeMgr) CommitSetMetaAndInodeFile(working *WorkingMeta, children map[string]InodeKeyType, key string) {
	inode := n.GetInode(working.inodeKey)
	inode.metaLock.Lock()
	inode.meta = working
	inode.files[key] = true
	high := uint32(working.inodeKey >> 32)
	low := uint32(working.inodeKey << 32)
	if n.raft.selfId == high && n.nextInodeId < low {
		n.nextInodeId = low + 1
	}
	if len(children) > 0 {
		//if inode.children != nil {
		//	log.Warnf("BUG: CommitSetMetaAndInodeFile, overwrite childMap, inodeKey=%v, key=%v", working.inodeKey, key)
		//}
		inode.children = make(map[string]InodeKeyType)
		for name, child := range children {
			high = uint32(child >> 32)
			low = uint32(child << 32)
			if n.raft.selfId == high && n.nextInodeId < low {
				n.nextInodeId = low + 1
			}
			inode.children[name] = child
		}
	}
	inode.metaLock.Unlock()
}

func (n *InodeMgr) CommitDeleteInodeMap(inodeKeys []uint64, keys []string) {
	n.inodeLock.Lock()
	for i := 0; i < len(inodeKeys); i++ {
		inodeKey := InodeKeyType(inodeKeys[i])
		deletedKey := keys[i]
		if inode, ok := n.inodes[inodeKey]; ok {
			inode.metaLock.Lock()
			delete(inode.files, deletedKey)
			inode.metaLock.Unlock()
		}
	}
	n.inodeLock.Unlock()
}

func (n *InodeMgr) CommitUpdateMeta(working *WorkingMeta, dirtyMgr *DirtyMgr) {
	inode := n.GetInode(working.inodeKey)
	inode.metaLock.Lock()
	inode.meta = working
	if working.prevVer != nil {
		working.prevVer.DropPrev()
	}
	inode.metaLock.Unlock()
	dirtyMgr.lock.Lock()
	dirtyMgr.AddMetaNoLock(working)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitUpdateMeta, inodeKey=%v, version=%v", working.inodeKey, working.version)
}

func (n *InodeMgr) CommitUpdateParentMeta(info *UpdateParentInfo, dirtyMgr *DirtyMgr) {
	inode := n.GetInode(info.inodeKey)
	inode.metaLock.Lock()
	inode.files[info.key] = true
	current := inode.CopyMeta()
	if current == nil {
		log.Warnf("BUG: CommimtUpdateParentMeta, inodeKey does not exist. info=%v", info)
		inode.metaLock.Unlock()
		return
	}
	copied := *current
	if inode.children == nil {
		log.Warnf("BUG: CommitUpdateParentMeta, inode.children does not exist, info=%v", info)
		inode.children = make(map[string]InodeKeyType)
		copied.size = 0
	}
	if info.removeChild != "" {
		if removed, ok := inode.children[info.removeChild]; ok {
			delete(inode.children, info.removeChild)
			copied.size -= int64(unsafe.Sizeof(info.removeChild) + unsafe.Sizeof(removed))
			if info.childIsDir {
				copied.nlink -= 1
			}
		}
	}
	if info.addChild != "" {
		if overwrite, ok := inode.children[info.addChild]; ok {
			copied.size -= int64(unsafe.Sizeof(info.addChild) + unsafe.Sizeof(overwrite))
			if info.childIsDir {
				copied.nlink -= 1
			}
		}
		inode.children[info.addChild] = info.addInodeKey
		copied.size += int64(unsafe.Sizeof(info.addChild) + unsafe.Sizeof(info.addInodeKey))
		if info.childIsDir {
			copied.nlink += 1
		}
	}
	copied.version = current.version + 1
	current.DropPrev()
	copied.prevVer = current
	inode.meta = &copied
	inode.metaLock.Unlock()

	dirtyMgr.lock.Lock()
	dirtyMgr.AddMetaNoLock(&copied)
	dirtyMgr.RemoveDeleteKeyNoLock(info.key)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitUpdateParentMeta, info=%v", info)
}

func (n *InodeMgr) CommitCreateMeta(working *WorkingMeta, parent InodeKeyType, newKey string, dirtyMgr *DirtyMgr) {
	inode := n.GetInode(working.inodeKey)
	inode.metaLock.Lock()
	if working.IsDir() {
		inode.children = make(map[string]InodeKeyType)
		inode.children["."] = working.inodeKey
		inode.children[".."] = parent
	}
	inode.files[newKey] = true
	inode.meta = working
	if working.prevVer != nil {
		working.prevVer.DropPrev()
	}
	inode.metaLock.Unlock()
	dirtyMgr.lock.Lock()
	dirtyMgr.AddMetaNoLock(working)
	dirtyMgr.RemoveDeleteKeyNoLock(newKey)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitCreateMeta, newKey=%v, inodeKey=%v, version=%v", newKey, working.inodeKey, working.version)
}

func (n *InodeMgr) CommitUpdateInodeToFile(working *WorkingMeta, children map[string]InodeKeyType, removedKey string, newKey string, dirtyMgr *DirtyMgr) *WorkingMeta {
	inode := n.GetInode(working.inodeKey)
	inode.metaLock.Lock()
	if working.IsDir() && len(children) > 0 {
		inode.children = children
	}
	nlink := working.nlink
	if removedKey != "" {
		delete(inode.files, removedKey)
		if working.nlink > 0 {
			working.nlink -= 1
		}
	}
	if newKey != "" {
		inode.files[newKey] = true
		working.nlink += 1
	}
	inode.meta = working
	if working.prevVer != nil {
		working.prevVer.DropPrev()
	}
	inode.metaLock.Unlock()
	dirtyMgr.lock.Lock()
	if removedKey != "" {
		dirtyMgr.AddDeleteKeyNoLock(removedKey, working)
	}
	if newKey != "" {
		dirtyMgr.RemoveDeleteKeyNoLock(newKey)
	}
	if nlink > 0 && working.nlink == 0 { // delete
		dirtyMgr.RemoveMetaNoLock(working.inodeKey)
	}
	if (nlink == 0 && working.nlink > 0) || (nlink == working.nlink && removedKey != "" && newKey != "") { //hardlink or rename
		// rename is also tracked since we need to upload renamed keys
		dirtyMgr.AddMetaNoLock(working)
	}
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitUpdateInodeToFile, inodeKey=%v, removedKey=%v, newKey=%v", working.inodeKey, removedKey, newKey)
	return working
}

func (n *InodeMgr) CommitPersistMeta(inodeKey InodeKeyType, newFetchKey string, version uint32, ts int64, dirtyMgr *DirtyMgr) {
	dirtyMgr.lock.Lock()
	ok := dirtyMgr.RemoveMetaNoLockIfLatest(inodeKey, version)
	dirtyMgr.lock.Unlock()
	if ok {
		inode := n.GetInode(inodeKey)
		inode.metaLock.Lock()
		if inode.meta != nil {
			inode.meta.mTime = ts
			inode.meta.fetchKey = newFetchKey
		}
		inode.metaLock.Unlock()
		log.Debugf("Success: CommitPersistMeta, inodeKey=%v, key=%v, ts=%v", inodeKey, newFetchKey, ts)
	} else {
		log.Infof("CommitPersistMeta, skip updating mTime, inodeKey=%v, version=%v", inodeKey, version)
	}
}

func (n *InodeMgr) CommitDeletePersistMeta(key string, dirtyMgr *DirtyMgr) {
	dirtyMgr.lock.Lock()
	inodeKey, ok := dirtyMgr.RemoveDeleteKeyNoLock(key)
	dirtyMgr.lock.Unlock()
	if ok {
		n.inodeLock.Lock()
		inode := n.inodes[inodeKey]
		if inode != nil {
			inode.meta = nil
		}
		n.inodeLock.Unlock()
		log.Debugf("Success: CommitDeletePersistMeta, key=%v, inodeKey=%v", key, inodeKey)
	} else {
		log.Infof("CommitDeletePersistMeta, skip: key is already deleted. key=%v, inodeKey=%v", key, inodeKey)
	}
}

func (n *InodeMgr) RestoreMetas(metas []*common.CopiedMetaMsg, files []*common.InodeToFileMsg) {
	for _, metaMsg := range metas {
		working := NewWorkingMetaFromMsg(metaMsg)
		inode := n.GetInode(working.inodeKey)
		inode.metaLock.Lock()
		inode.meta = working
		inode.metaLock.Unlock()
	}
	for _, fileMsg := range files {
		inodeKey := InodeKeyType(fileMsg.GetInodeKey())
		inode := n.GetInode(inodeKey)
		inode.metaLock.Lock()
		inode.files[fileMsg.GetFilePath()] = true
		inode.metaLock.Unlock()
	}
}

func (n *InodeMgr) RestoreInodeTree(inodes []*common.InodeTreeMsg) {
	for _, inodeMsg := range inodes {
		inode := n.GetInode(InodeKeyType(inodeMsg.GetInodeKey()))
		inode.metaLock.Lock()
		if inode.children == nil {
			inode.children = make(map[string]InodeKeyType)
		}
		for _, childMsg := range inodeMsg.GetChildren() {
			inode.children[childMsg.GetName()] = InodeKeyType(childMsg.GetInodeKey())
		}
		inode.metaLock.Unlock()
	}
}

func (n *InodeMgr) DeleteInode(inodeKeys []uint64) {
	n.inodeLock.Lock()
	for _, i := range inodeKeys {
		inodeKey := InodeKeyType(i)
		inode := n.inodes[inodeKey]
		if inode != nil {
			inode.metaLock.Lock()
			inode.meta = nil
			inode.metaLock.Unlock()
		}
	}
	n.inodeLock.Unlock()
}

/***************************
 * methods to persist object
 ***************************/

func (n *InodeMgr) PutDirObject(inode *Inode, meta *WorkingMeta, _ int) (ts int64, reply int32) {
	inode.metaLock.RLock()
	defer inode.metaLock.RUnlock()
	if len(inode.children) != 2 {
		ts = time.Now().UnixNano()
		reply = RaftReplyOk
		return
	}
	for metaKey := range inode.files {
		if !strings.Contains(metaKey, "/") {
			log.Debugf("Success: PutDirObject (skipped), inodeKey=%v, key=%v", meta.inodeKey, metaKey)
			reply = RaftReplyOk
			continue
		}
		var key = metaKey
		if !strings.HasPrefix(key, "/") {
			key += "/"
		}
		res, awsErr := n.back.putBlob(&PutBlobInput{Key: key, DirBlob: true, Body: nil})
		if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
			log.Errorf("Failed: PutDirObject, putBlob, meta=%v, awsErr=%v, reply=%v", *meta, awsErr, reply)
			return
		}
		if res.LastModified != nil {
			ts = res.LastModified.UTC().UnixNano()
		} else {
			ts = time.Now().UnixNano()
		}
		log.Infof("Success: PutDirObject, inodeKey=%v, key=%v", meta.inodeKey, key)
	}
	return
}

func (n *InodeMgr) PersistDeleteObject(key string, _ int) (reply int32) {
	_, awsErr := n.back.deleteBlob(&DeleteBlobInput{Key: key})
	if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
		if reply != ErrnoToReply(unix.ENOENT) {
			log.Errorf("Failed: PersistDeleteObject, deleteBlob, awsErr=%v, reply=%v", awsErr, reply)
			return
		}
		log.Debugf("Success: PersistDeleteObject (already deleted), metaKey=%v", key)
		return
	}
	nrRetries := 256
	var backOff = time.Millisecond * 10
	for j := 0; j < nrRetries; j++ {
		_, awsErr := n.back.headBlob(&HeadBlobInput{Key: key})
		if reply = AwsErrToReply(awsErr); reply != ErrnoToReply(unix.ENOENT) {
			log.Debugf("Failed: PersistDeleteObject name %v remains visible or other failures (awsErr=%v, r=%v), Retry after %v secs", key, awsErr, reply, backOff)
			time.Sleep(backOff)
			continue
		}
		break
	}
	log.Infof("Success: PersistDeleteObject, key=%v", key)
	reply = RaftReplyOk
	return
}

/************************
 * methods to read chunks
 ************************/

// SetChunkNoLock: caller must hold chunk.lock.Lock()
func (n *InodeMgr) SetChunkNoLock(chunk *Chunk, h MetaRWHandler) (*WorkingChunk, int32) {
	working, err := chunk.GetWorkingChunk(h.version, true)
	if err == nil {
		//log.Debugf("Success: SetChunkNoLock (cached), inodeKey=%v, offset=%v", h.inodeKey, alignedOffset)
		return working, RaftReplyOk //fast path
	}

	length := h.size - chunk.offset
	if length > h.chunkSize {
		length = h.chunkSize
	}
	logId := NewLogIdTypeFromInodeKey(h.inodeKey, chunk.offset, h.chunkSize)
	logOffset := n.raft.extLogger.ReserveRange(logId, length)
	if reply := n.raft.AppendExtendedLogEntry(NewCreateChunkCommand(h.inodeKey, chunk.offset, h.version, logOffset, length, h.fetchKey, logId.upper)); reply != RaftReplyOk {
		log.Errorf("Failed: SetChunkNoLock, AppendExtendedLogEntry, reply=%v", reply)
		return nil, reply
	}
	working, err = chunk.GetWorkingChunk(h.version, true)
	if h.fetchKey != "" && err != nil {
		log.Errorf("BUG: SetChunkNoLock, GetWorkingChunk, inodeKey=%v, fetchKey=%v, offset=%v, version=%v, err=%v",
			h.inodeKey, h.fetchKey, chunk.offset, h.version, err)
	} else {
		log.Debugf("Success: SetChunkNoLock (new), inodeKey=%v, key=%v, offset=%v", h.inodeKey, h.fetchKey, chunk.offset)
	}
	return working, ErrnoToReply(err)
}

func (n *InodeMgr) GetChunk(inodeKey InodeKeyType, offset int64, chunkSize int64) *Chunk {
	alignedOffset := offset - offset%chunkSize
	inode := n.GetInode(inodeKey)
	inode.chunkLock.Lock()
	chunk, ok2 := inode.chunkMap[alignedOffset]
	if !ok2 {
		chunk = CreateNewChunk(inodeKey, alignedOffset, uint32(offset/chunkSize))
		inode.chunkMap[alignedOffset] = chunk
	}
	inode.chunkLock.Unlock()
	//log.Debugf("Success: GetChunk, inodeKey=%v, offset=%v", inodeKey, alignedOffset)
	return chunk
}

func (n *InodeMgr) VectorReadChunk(h MetaRWHandler, offset int64, size int, blocking bool) (bufs []SlicedPageBuffer, count int, reply int32) {
	chunk := n.GetChunk(h.inodeKey, offset, h.chunkSize)
	chunk.lock.Lock()
	working, reply := n.SetChunkNoLock(chunk, h)
	chunk.lock.Unlock()
	if reply != RaftReplyOk {
		log.Errorf("Failed: VectorReadChunk, GetChunkOrSetAtRemote, inodeKey=%v, fetchKey=%v, offset=%v, chunkSize=%v, reply=%v", h.inodeKey, h.fetchKey, offset, h.chunkSize, reply)
		return nil, 0, reply
	}
	reader := working.GetReader(h.chunkSize, h.size, offset, n, blocking)
	var err error
	bufs, count, err = reader.GetBufferZeroCopy(size)
	reader.Close()
	if err != nil {
		if err == io.EOF {
			err = nil
		} else if blocking || (!blocking && err != unix.EAGAIN) {
			log.Errorf("Failed: VectorReadChunk, GetBufferZeroCopy, err=%v", err)
		}
		return nil, 0, ErrnoToReply(err)
	}
	return bufs, count, RaftReplyOk
}

func (n *InodeMgr) PrefetchChunkThread(h MetaRWHandler, offset int64) {
	chunk := n.GetChunk(h.inodeKey, offset, h.chunkSize)
	chunk.lock.Lock()
	working, reply := n.SetChunkNoLock(chunk, h)
	chunk.lock.Unlock()
	if reply != RaftReplyOk {
		log.Errorf("Failed: PrefetchChunk, GetChunkOrSetAtRemote, inodeKey=%v, offset=%v, chunkSize=%v, reply=%v", h.inodeKey, offset, h.chunkSize, reply)
		return
	}
	working.Prefetch(n)
	//log.Debugf("PrefetchChunk, inodeKey=%v, offset=%v", h.inodeKey, offset)
}

func (n *InodeMgr) PrepareUpdateChunk(newMeta *WorkingMeta, alignedOffset int64) (chunk *Chunk, working *WorkingChunk) {
	h := NewMetaRWHandlerFromMeta(newMeta)
	h.version = newMeta.prevVer.version
	chunk = n.GetChunk(h.inodeKey, alignedOffset, h.chunkSize)
	chunk.lock.Lock()
	prev, _ := n.SetChunkNoLock(chunk, h)
	if prev != nil {
		working = prev.Copy(newMeta.version)
		working.prevVer = prev
	} else {
		working = chunk.NewWorkingChunk(newMeta.version)
	}
	log.Debugf("Success: PrepareUpdateChunk, inodeKey=%v, offset=%v", newMeta.inodeKey, alignedOffset)
	return
}

func (n *InodeMgr) QuickPrepareChunk(newMeta *WorkingMeta, alignedOffset int64) (chunk *Chunk, working *WorkingChunk) {
	chunk = n.GetChunk(newMeta.inodeKey, alignedOffset, newMeta.chunkSize)
	chunk.lock.Lock()
	if prev, err := chunk.GetWorkingChunk(newMeta.prevVer.version, true); err == nil {
		working = prev.Copy(newMeta.version)
		working.prevVer = prev
	} else if newMeta.fetchKey != "" {
		if _, err := chunk.GetWorkingChunk(newMeta.prevVer.version, true); err != nil {
			length := newMeta.size - chunk.offset
			if length > newMeta.chunkSize {
				length = newMeta.chunkSize
			}
			logId := NewLogIdTypeFromInodeKey(newMeta.inodeKey, chunk.offset, newMeta.chunkSize)
			logOffset := n.raft.extLogger.ReserveRange(logId, length)
			working = chunk.NewWorkingChunk(newMeta.version)
			working.AddStag(NewStagingChunk(0, length, StagingChunkData, logOffset, newMeta.fetchKey, 0))
		}
	}
	if working == nil {
		working = chunk.NewWorkingChunk(newMeta.version)
	}
	return
}

func (n *InodeMgr) AppendStagingChunkLog(inodeKey InodeKeyType, offset int64, logId LogIdType, logOffset int64, dataLength uint32, extBufChksum [4]byte) (reply int32) {
	begin := time.Now()
	checksum := time.Now()
	_, _, reply = n.raft.AppendEntriesLocal(NewExtLogCommand(AppendEntryUpdateChunkCmdId, logId, logOffset, dataLength, extBufChksum), NewUpdateChunkCommand())
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendStagingChunkLog, AppendCommand, logId=%v, reply=%v", logId, reply)
		return reply
	}
	appendEntries := time.Now()
	log.Debugf("Success: AppendStagingChunkLog, inodeKey=%v, offset=%v, length=%v, checksum=%v, appendEntriesTime=%v",
		inodeKey, offset, dataLength, checksum.Sub(begin), appendEntries.Sub(checksum))
	return RaftReplyOk
}

func (n *InodeMgr) AppendStagingChunkBuffer(inodeKey InodeKeyType, offset int64, chunkSize int64, buf []byte) (logOffset int64, reply int32) {
	begin := time.Now()
	// update a chunk at the same node as a FUSE caller
	logId := NewLogIdTypeFromInodeKey(inodeKey, offset, chunkSize)
	var err error
	logOffset, err = n.raft.extLogger.AppendSingleBuffer(logId, buf)
	if err != nil {
		log.Errorf("Failed: AppendStagingChunkBuffer, AppendSingleBuffer, logId=%v, err=%v", logId, err)
		return -1, ErrnoToReply(err)
	}

	write := time.Now()
	_, _, reply = n.raft.AppendEntriesLocal(NewExtLogCommandFromExtBuf(AppendEntryUpdateChunkCmdId, logId, logOffset, buf), NewUpdateChunkCommand())
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendStagingChunkBuffer, AppendCommand, logId=%v, reply=%v", logId, reply)
		return
	}
	appendEntries := time.Now()
	log.Debugf("Success: AppendStagingChunkBuffer, inodeKey=%v, offset=%v, length=%v, writeTime=%v, appendEntriesTime=%v",
		inodeKey, offset, len(buf), write.Sub(begin), appendEntries.Sub(write))
	return
}

func (n *InodeMgr) DropLRUChunk(keys []uint64, offsets []int64) {
	for i := 0; i < len(keys); i++ {
		k := InodeKeyType(keys[i])
		n.inodeLock.Lock()
		inode, ok := n.inodes[k]
		n.inodeLock.Unlock()
		if !ok {
			continue
		}
		inode.chunkLock.Lock()
		chunk, ok2 := inode.chunkMap[offsets[i]]
		if !ok2 {
			inode.chunkLock.Unlock()
			continue
		}
		delete(inode.chunkMap, offsets[i])
		inode.chunkLock.Unlock()
		chunk.Drop(n, n.raft)
	}
}

func (n *InodeMgr) PreparePersistChunk(meta *WorkingMeta, offset int64) (chunk *Chunk, working *WorkingChunk, reply int32) {
	h := NewMetaRWHandlerFromMeta(meta)
	chunk = n.GetChunk(h.inodeKey, offset, h.chunkSize)
	chunk.lock.Lock()
	working, reply = n.SetChunkNoLock(chunk, h)
	chunk.lock.Unlock()
	if reply != RaftReplyOk {
		log.Errorf("Failed: PreparePersistChunk, GetChunkOrSetAtRemote, inodeKey=%v, offset=%v, not found", meta.inodeKey, offset)
	}
	return chunk, working, reply
}

func (n *InodeMgr) PutEmptyObject(metaKeys []string, meta *WorkingMeta, _ int) (ts int64, reply int32) {
	for _, metaKey := range metaKeys {
		size := uint64(meta.size)
		res, awsErr := n.back.putBlob(&PutBlobInput{Key: metaKey, Body: nil, Size: &size})
		if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
			log.Errorf("Failed: PutEmptyObject, putBlob, meta=%v, awsErr=%v, reply=%v", *meta, awsErr, reply)
			return
		}
		if res.LastModified != nil {
			ts = res.LastModified.UTC().UnixNano()
		} else {
			ts = time.Now().UTC().UnixNano()
		}
		log.Infof("Success: PutEmptyObject, inode=%v, key=%v", meta.inodeKey, metaKey)
	}
	return
}

func (n *InodeMgr) PutObject(tm *TxIdMgr, metaKeys []string, meta *WorkingMeta, priority int, selfGroup string, dirtyExpireInterval time.Duration) (ts int64, chunks []*common.PersistedChunkInfoMsg, reply int32) {
	for _, metaKey := range metaKeys {
		var working *WorkingChunk
		_, working, reply = n.PreparePersistChunk(meta, 0)
		if reply != RaftReplyOk {
			return
		}
		txId := tm.GetNextId()
		chunks = []*common.PersistedChunkInfoMsg{{TxId: txId.toMsg(), GroupId: selfGroup, Offsets: []int64{0}, CVers: []uint32{meta.version}}}
		reader := working.GetReader(meta.chunkSize, meta.size, 0, n, true)
		size := uint64(meta.size)
		res, awsErr := n.back.putBlob(&PutBlobInput{Key: metaKey, Body: reader, Size: &size})
		if err2 := reader.Close(); err2 != nil {
			log.Errorf("Failed: PutObject, Close, err=%v", err2)
		}
		if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
			log.Errorf("Failed: PutObject, putBlob, meta=%v, awsErr=%v, reply=%v", *meta, awsErr, reply)
			return
		}
		if res.LastModified != nil {
			ts = res.LastModified.UTC().UnixNano()
		} else {
			ts = time.Now().UTC().UnixNano()
		}
		log.Infof("Success: PutObject, inode=%v, key=%v, size=%v", meta.inodeKey, metaKey, size)
	}
	return
}

func (n *InodeMgr) MpuBegin(key string) (uploadId string, reply int32) {
	arg := &MultipartBlobBeginInput{
		Key:         key,
		ContentType: nil,
	}
	input, awsErr := n.back.multipartBlobBegin(arg)
	if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
		log.Errorf("Failed: MpuBegin, multipartBlobBegin, name=%v, content-type=%v, awsErr=%v, r=%v", arg.Key, arg.ContentType, awsErr, reply)
		return
	}
	uploadId = *input.UploadId
	log.Infof("Success: MpuBegin, multipartBlobBegin, meta=%v, uploadId=%v", key, uploadId)
	return
}

type MpuAddOut struct {
	idx  uint32
	etag string
	cVer uint32
}

func (n *InodeMgr) MpuAdd(metaKey string, meta *WorkingMeta, offset int64, uploadId string, priority int, dirtyExpireInterval time.Duration) (out MpuAddOut, unlock func(*NodeServer), reply int32) {
	_, working, reply := n.PreparePersistChunk(meta, offset)
	if reply != RaftReplyOk {
		log.Errorf("Failed: MpuAdd, PreparePersistChunk, inodeKey=%v, offset=%v, reply=%v", meta.inodeKey, offset, reply)
		return MpuAddOut{}, nil, reply
	}
	var maxIdx = uint32(0)
	maxIdx += 1
	input := &MultipartBlobCommitInput{
		Key:         &metaKey,
		UploadId:    &uploadId,
		Parts:       make([]*string, 1),
		NumParts:    0,
		backendData: nil, // TODO: add backendData for GCS
	}

	idx := uint32(offset / meta.chunkSize)
	isLast := int64(idx+1)*meta.chunkSize >= meta.size
	reader := working.GetReader(meta.chunkSize, meta.size, offset, n, true)
	size := uint64(working.Size())
	// NOTE: modified original s3 backend: MultipartBlobAdd
	_, awsErr := n.back.multipartBlobAdd(&MultipartBlobAddInput{
		Commit:     input,
		PartNumber: idx + 1,
		Body:       reader, //bytes.NewReader(optBuf),
		Size:       size,
		Last:       isLast,
		Offset:     uint64(offset),
	})
	if err := reader.Close(); err != nil {
		log.Errorf("Failed (ignore): MpuAdd, multipartBlobAdd, Close, err=%c", err)
	}
	if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
		log.Errorf("Failed: MpuAdd, multipartBlobAdd: inodeKey=%v, offset=%v, awsErr=%v, reply=%v", meta.inodeKey, offset, awsErr, reply)
		return MpuAddOut{}, nil, reply
	}
	log.Infof("Success: MpuAdd, inodeKey=%v, offset=%v, id=%v, etag=%v, size=%v", meta.inodeKey, offset, idx, *input.Parts[0], size)
	return MpuAddOut{idx: idx, etag: *input.Parts[0], cVer: working.chunkVer}, nil, RaftReplyOk
}

func (n *InodeMgr) MpuCommit(key string, uploadId string, eTags []string) (ts int64, reply int32) {
	nrEtags := uint32(len(eTags))
	input := &MultipartBlobCommitInput{
		Key:      &key,
		UploadId: &uploadId,
		Parts:    make([]*string, nrEtags),
		NumParts: nrEtags,
	}
	for i := uint32(0); i < nrEtags; i++ {
		input.Parts[i] = &eTags[i]
	}
	res, awsErr := n.back.multipartBlobCommit(input)
	if r := AwsErrToReply(awsErr); r != RaftReplyOk {
		log.Errorf("Failed: MpuCommit, multipartBlobCommit, awsErr=%v, r=%v", awsErr, r)
		return ts, r
	}
	if res.LastModified != nil {
		ts = res.LastModified.UTC().UnixNano()
	}
	log.Infof("Success: MpuCommit, multipartBlobCommit, name=%v", key)
	return ts, RaftReplyOk
}

func (n *InodeMgr) CommitUpdateChunk(inodeKey InodeKeyType, offset int64, chunkSize int64, chunkVer uint32, stags []*common.StagingChunkAddMsg, objectSize int64, dirtyMgr *DirtyMgr) {
	chunk := n.GetChunk(inodeKey, offset, chunkSize)
	working := chunk.NewWorkingChunk(chunkVer)
	for _, stag := range stags {
		working.AddNewStagFromMsg(stag)
	}
	chunk.AddWorkingChunk(n, working, working.prevVer)
	dirtyMgr.lock.Lock()
	dirtyMgr.AddChunkNoLock(inodeKey, chunkSize, working.chunkVer, offset, objectSize)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitUpdateChunk, inodeKey=%v, offset=%v, version=%v", inodeKey, offset, working.chunkVer)
}

func (n *InodeMgr) CommitDeleteChunk(inodeKey InodeKeyType, offset int64, chunkSize int64, chunkVer uint32, stags []*common.StagingChunkAddMsg, dirtyMgr *DirtyMgr) {
	chunk := n.GetChunk(inodeKey, offset, chunkSize)
	working := chunk.NewWorkingChunk(chunkVer)
	for _, stag := range stags {
		working.AddNewStagFromMsg(stag)
	}
	chunk.AddWorkingChunk(n, working, working.prevVer)
	dirtyMgr.lock.Lock()
	dirtyMgr.RemoveChunkNoLock(inodeKey, offset, working.prevVer.chunkVer)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitDeleteChunk, inodeKey=%v, version=%v", inodeKey, working.chunkVer)
}

func (n *InodeMgr) QuickCommitUpdateChunk(meta *WorkingMeta, selfGroup string, ucs []*common.UpdateChunkRecordMsg, dirtyMgr *DirtyMgr) {
	chunks := make([]*Chunk, 0)
	offStags := map[int64][]*common.StagingChunkMsg{}
	for _, chunk := range ucs {
		if chunk.GroupId == selfGroup {
			for _, stags := range chunk.GetStags() {
				align := stags.GetOffset() - stags.GetOffset()%meta.chunkSize
				if _, ok := offStags[align]; !ok {
					offStags[align] = make([]*common.StagingChunkMsg, 0)
				}
				offStags[align] = append(offStags[align], stags)
			}
		}
	}
	for off, stags := range offStags {
		chunk, working := n.QuickPrepareChunk(meta, off)
		for _, stag := range stags {
			working.AddStag(&StagingChunk{
				slop: stag.GetOffset() % meta.chunkSize, filled: 1, length: stag.GetLength(),
				updateType: StagingChunkData, logOffset: stag.GetLogOffset(),
			})
		}
		chunk.AddWorkingChunk(n, working, working.prevVer)
		dirtyMgr.lock.Lock()
		dirtyMgr.AddChunkNoLock(chunk.inodeKey, meta.chunkSize, working.chunkVer, off, meta.size)
		dirtyMgr.lock.Unlock()
		log.Debugf("Success: QuickCommitUpdateChunk, inodeKey=%v, offset=%v, version=%v", chunk.inodeKey, off, working.chunkVer)
		chunks = append(chunks, chunk)
	}
	for _, chunk := range chunks {
		chunk.lock.Unlock()
	}
}

func (n *InodeMgr) QuickCommitDeleteChunk(localOffsets map[int64]int64, meta *WorkingMeta, dirtyMgr *DirtyMgr) {
	for offset, length := range localOffsets {
		chunk, working := n.QuickPrepareChunk(meta, offset)
		working.AddStag(&StagingChunk{
			filled:     1,
			length:     length,
			slop:       offset,
			updateType: StagingChunkDelete,
		})
		working.chunkPtr.AddWorkingChunk(n, working, working.prevVer)
		dirtyMgr.lock.Lock()
		dirtyMgr.RemoveChunkNoLock(chunk.inodeKey, offset, working.chunkVer)
		dirtyMgr.lock.Unlock()
		chunk.lock.Unlock()
	}
}

func (n *InodeMgr) QuickCommitExpandChunk(localOffsets map[int64]int64, meta *WorkingMeta, dirtyMgr *DirtyMgr) {
	for offset, length := range localOffsets {
		chunk, working := n.QuickPrepareChunk(meta, offset)
		working.AddStag(&StagingChunk{
			filled:     1,
			length:     length,
			slop:       offset,
			updateType: StagingChunkBlank,
		})
		working.chunkPtr.AddWorkingChunk(n, working, working.prevVer)
		dirtyMgr.lock.Lock()
		dirtyMgr.AddChunkNoLock(chunk.inodeKey, meta.chunkSize, working.chunkVer, offset, meta.size)
		dirtyMgr.lock.Unlock()
		chunk.lock.Unlock()
	}
}

func (n *InodeMgr) CommitCreateChunk(inodeKey InodeKeyType, offset int64, version uint32, logOffset int64, length int64, key string, chunkIdx uint32) {
	begin := time.Now()
	var offsetCreate = false
	inode := n.GetInode(inodeKey)
	inode.chunkLock.Lock()
	c, ok2 := inode.chunkMap[offset]
	if !ok2 {
		c = CreateNewChunk(inodeKey, offset, chunkIdx)
		inode.chunkMap[offset] = c
	}
	inode.chunkLock.Unlock()
	lock := time.Now()
	if key != "" {
		if _, err := c.GetWorkingChunk(version, true); err != nil {
			working := c.NewWorkingChunk(version)
			newStag := NewStagingChunk(0, length, StagingChunkData, logOffset, key, 0)
			working.AddStag(newStag)
			c.AddWorkingChunk(n, working, c.workingHead.prevVer)
		}
	}
	log.Debugf("Success: CommitCreateChunk, inodeKey=%v, key=%v, offset=%v, lock=%v, elapsed=%v, offsetCreate=%v", inodeKey, key, offset, lock.Sub(begin), time.Since(begin), offsetCreate)
}

func (n *InodeMgr) CommitPersistChunk(inodeKey InodeKeyType, offsets []int64, cVers []uint32, dirtyMgr *DirtyMgr) {
	dirtyMgr.lock.Lock()
	for i := 0; i < len(offsets); i++ {
		dirtyMgr.RemoveChunkNoLock(inodeKey, offsets[i], cVers[i])
	}
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitPersistChunk, inodeKey=%v, offset=%v, cVer=%v", inodeKey, offsets, cVers)
}
