/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/serialx/hashring"
	"github.com/takeshi-yoshimura/fuse/fuseops"
	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

type InodeKeyType uint64

type InodeMgr struct {
	nextInodeId  uint64 //InodeKeyType(uint64(raft.selfId)<<32 + uint64(txMgr.GetNextClock()))
	inodeToFiles map[InodeKeyType]map[string]bool
	metadataMap  map[InodeKeyType]*Meta
	chunkMap     map[InodeKeyType]map[int64]*Chunk
	suspending   int32

	nodeLock        *sync.RWMutex
	metadataMapLock *sync.RWMutex
	chunkMapLock    *sync.RWMutex

	defaultDirMode  uint32
	defaultFileMode uint32

	uploadSem chan struct{}

	back      *ObjCacheBackend
	raft      *RaftInstance
	readCache *ReaderBufferCache
}

func NewInodeMgr(back *ObjCacheBackend, raft *RaftInstance, flags *common.ObjcacheConfig) *InodeMgr {
	ret := &InodeMgr{
		nextInodeId:     uint64(raft.selfId) << 32,
		inodeToFiles:    make(map[InodeKeyType]map[string]bool),
		metadataMap:     make(map[InodeKeyType]*Meta),
		chunkMap:        make(map[InodeKeyType]map[int64]*Chunk),
		suspending:      0,
		nodeLock:        new(sync.RWMutex),
		metadataMapLock: new(sync.RWMutex),
		chunkMapLock:    new(sync.RWMutex),
		defaultDirMode:  flags.DirMode | uint32(os.ModeDir),
		defaultFileMode: flags.FileMode,
		back:            back,
		raft:            raft,
		readCache:       NewReaderBufferCache(flags),
		uploadSem:       make(chan struct{}, flags.UploadParallel),
	}
	if back != nil {
		ret.inodeToFiles[InodeKeyType(fuseops.RootInodeID)] = make(map[string]bool)
		ret.inodeToFiles[InodeKeyType(fuseops.RootInodeID)][""] = true
		root := ret.GetMetaHandler(fuseops.RootInodeID)
		root.AddWorkingMeta(ret.NewWorkingMetaForRoot())
	}
	return ret
}

func (n *InodeMgr) Clean(dirtyMgr *DirtyMgr) int32 {
	n.readCache.DropAll()
	msg := &common.DeleteInodeMapArgs{InodeKey: make([]uint64, 0), Key: make([]string, 0)}
	n.metadataMapLock.RLock()
	metas := make([]*Meta, 0)
	for inodeKey, meta := range n.metadataMap {
		if dirtyMgr.IsDirtyMeta(inodeKey) {
			log.Warnf("InodeMgr.Clean, key is dirty, inodeKey=%v", inodeKey)
			continue
		}
		if inodeKey == InodeKeyType(fuseops.RootInodeID) {
			continue
		}
		keys := n.inodeToFiles[inodeKey]
		for key := range keys {
			msg.InodeKey = append(msg.InodeKey, uint64(inodeKey))
			msg.Key = append(msg.Key, key)
		}
		if len(keys) == 0 {
			msg.InodeKey = append(msg.InodeKey, uint64(inodeKey))
			msg.Key = append(msg.Key, "")
		}
		meta.lock.Lock()
		metas = append(metas, meta)
	}
	n.metadataMapLock.RUnlock()
	reply := n.raft.AppendExtendedLogEntry(AppendEntryDeleteInodeFileMapCmdId, msg)
	if reply != RaftReplyOk {
		log.Errorf("Failed: InodeMgr.Clean, AppendExtendedLogEntry, reply=%v", reply)
	}
	for _, meta := range metas {
		meta.lock.Unlock()
	}
	return reply
}

func (n *InodeMgr) CheckReset() (ok bool) {
	ok = true
	var metaLock = true
	if ok2 := n.metadataMapLock.TryLock(); !ok2 {
		log.Errorf("Failed: InodeMgr.CheckReset, metadataMapLock is taken")
		metaLock = false
		ok = false
	}
	if metaLock {
		if ok2 := len(n.metadataMap) == 1; !ok2 {
			log.Errorf("Failed: InodeMgr.CheckReset, len(n.metadataMap) != 1")
			ok = false
		}
		if ok2 := len(n.inodeToFiles) == 1; !ok2 {
			log.Errorf("Failed: InodeMgr.CheckReset, len(n.inodeToFiles) != 1")
			ok = false
		}
		n.metadataMapLock.Unlock()
	}
	var chunkLock = true
	if ok2 := n.chunkMapLock.TryLock(); !ok2 {
		log.Errorf("Failed: InodeMgr.CheckReset, chunkMapLock is taken")
		chunkLock = false
		ok = false
	}
	if chunkLock {
		if ok2 := len(n.chunkMap) == 0; !ok2 {
			log.Errorf("Failed: InodeMgr.CheckReset, len(n.chunkMap) != 0")
			ok = false
		}
		n.chunkMapLock.Unlock()
	}
	if ok2 := n.nodeLock.TryLock(); !ok2 {
		log.Errorf("Failed: InodeMgr.CheckReset, nodeLock is taken")
		ok = false
	} else {
		n.nodeLock.Unlock()
	}
	if ok2 := n.readCache.CheckReset(); !ok2 {
		log.Errorf("Failed: InodeMgr.CheckReset, readCache")
		ok = false
	}
	return
}

func (n *InodeMgr) CreateInodeId() InodeKeyType {
	return InodeKeyType(atomic.AddUint64(&n.nextInodeId, 1) - 1)
}

func (n *InodeMgr) SuspendNode() {
	now := atomic.AddInt32(&n.suspending, 1)
	log.Debugf("SuspendNode, suspending=%v->%v", now-1, now)
}

func (n *InodeMgr) ResumeNode() {
	now := atomic.AddInt32(&n.suspending, -1)
	log.Debugf("ResumeNode, suspending=%v->%v", now+1, now)
}

// must hold n.nodeLock.RLock
func (n *InodeMgr) IsNodeSuspending() bool {
	return atomic.LoadInt32(&n.suspending) > 0
}

func (n *InodeMgr) waitNodeResume() {
	for atomic.LoadInt32(&n.suspending) > 0 {
		time.Sleep(time.Millisecond * 10)
	}
}

/*******************************************
 * methods to read meta
 * files/inodes can be on the cluster or COS
 *******************************************/

func (n *InodeMgr) GetMetaHandler(inodeKey InodeKeyType) *Meta {
	n.metadataMapLock.Lock()
	meta, ok := n.metadataMap[inodeKey]
	if !ok {
		meta = &Meta{
			metaLock:    new(sync.RWMutex),
			workingHead: WorkingMeta{prevVer: nil},
			lock:        new(sync.Mutex),
			persistLock: new(sync.Mutex),
		}
		n.metadataMap[inodeKey] = meta
		log.Debugf("GetMetaHandler, create a new Meta, inodeKey=%v", inodeKey)
	}
	n.metadataMapLock.Unlock()
	return meta
}

func (n *InodeMgr) NewWorkingMetaForRoot() *WorkingMeta {
	inodeKey := InodeKeyType(fuseops.RootInodeID)
	ret := NewWorkingMeta(inodeKey, MetaAttributes{inodeKey: inodeKey, mode: n.defaultDirMode}, 4096, -1, n.defaultDirMode, "")
	s := n.back
	dirNames := make([]string, 0)
	for dirName := range s.buckets {
		dirNames = append(dirNames, dirName)
	}
	sort.Strings(dirNames)
	for i := 0; i < len(dirNames); i++ {
		ret.childAttrs[dirNames[i]] = NewMetaAttributes(InodeKeyType(fuseops.RootInodeID+i+1), n.defaultDirMode)
	}
	return ret
}

func (n *InodeMgr) __fetchWorkingMeta(inodeKey InodeKeyType, key string, chunkSize int64, expireMs int32, parent MetaAttributes) (*WorkingMeta, int32) {
	working := NewWorkingMeta(inodeKey, parent, chunkSize, expireMs, n.defaultFileMode, key)
	head, awsErr := n.back.headBlob(&HeadBlobInput{Key: key})
	var mayDir = false
	if r := AwsErrToReply(awsErr); r != RaftReplyOk {
		if r == ErrnoToReply(unix.ENOENT) {
			// users may not explicitly create a directory. so, ignore fuse.ENOENT here.
			mayDir = true
		} else {
			log.Errorf("Failed: __fetchWorkingMeta, headBlob, name=%v, awsErr=%v, r=%v", key, awsErr, r)
			return nil, r
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
	if mayDir || working.IsDir() {
		head, awsErr = n.back.headBlob(&HeadBlobInput{Key: keyWithSuffix})
		if r := AwsErrToReply(awsErr); r != RaftReplyOk {
			if r != ErrnoToReply(unix.ENOENT) {
				// users may not explicitly create a directory. so, ignore fuse.ENOENT here.
				log.Errorf("Failed: __fetchWorkingMeta, headBlob, name=%v, awsErr=%v, r=%v", keyWithSuffix, awsErr, r)
				return nil, r
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
				mayDir = false
			}
		}
		sep := string(os.PathSeparator)
		arg := &ListBlobsInput{
			Prefix:    &keyWithSuffix,
			Delimiter: &sep,
		}
		working.childAttrs = make(map[string]MetaAttributes)
		working.childAttrs["."] = NewMetaAttributes(inodeKey, working.mode)
		working.childAttrs[".."] = NewMetaAttributes(parent.inodeKey, parent.mode)
		var res *ListBlobsOutput = nil
		for {
			if res != nil && res.NextContinuationToken != nil {
				arg.ContinuationToken = res.NextContinuationToken
			}
			res, awsErr = n.back.listBlobs(arg)
			if r := AwsErrToReply(awsErr); r != RaftReplyOk {
				log.Errorf("Failed: __fetchWorkingMeta, headBlob and listBlobs, name=%v, awsErr=%v, r=%v", *arg.Prefix, awsErr, r)
				return nil, r
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
					return nil, ErrnoToReply(unix.ENOENT)
				} else {
					// an empty directory is persisted
					mayDir = false
					working.mode = n.defaultDirMode
				}
			}
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
				working.childAttrs[filepath.Base(childKey)] = NewMetaAttributes(n.CreateInodeId(), n.defaultFileMode)
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
				working.childAttrs[filepath.Base(childKey)] = NewMetaAttributes(n.CreateInodeId(), n.defaultDirMode)
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
	return working, RaftReplyOk
}

func (n *InodeMgr) GetOrFetchWorkingMeta(inodeKey InodeKeyType, key string, chunkSize int64, expireMs int32, parent MetaAttributes) (*WorkingMeta, int32) {
	n.nodeLock.RLock()
	defer n.nodeLock.RUnlock()
	if n.IsNodeSuspending() {
		// current migration sends all the directory stuctures.
		log.Errorf("TryAgain: GetOrFetchWorkingMeta, inodeKey=%v, Node is suspending", inodeKey)
		return nil, ObjCacheReplySuspending
	}
	if inodeKey == fuseops.RootInodeID {
		meta := n.GetMetaHandler(inodeKey)
		meta.metaLock.RLock()
		working := meta.GetLatestWorkingMeta()
		meta.metaLock.RUnlock()
		return working, RaftReplyOk
	}
	meta := n.GetMetaHandler(inodeKey)
	meta.metaLock.RLock()
	working := meta.GetLatestWorkingMeta()
	if working != nil {
		meta.metaLock.RUnlock()
		// TODO: check cache TTL
		log.Debugf("GetOrFetchWorkingMeta: get a cached meta, key=%v, mTime=%v", key, working.mTime)
		return working, RaftReplyOk
	}
	working, reply := n.__fetchWorkingMeta(inodeKey, key, chunkSize, expireMs, parent)
	if reply != RaftReplyOk {
		meta.metaLock.RUnlock()
		return nil, reply
	}
	meta.metaLock.RUnlock()
	msg := &common.AddInodeFileMapMsg{Meta: working.toMsg(), Key: key}
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryAddInodeFileMapCmdId, msg); reply != RaftReplyOk {
		log.Errorf("Failed: GetOrFetchMeta, AppendExtendedLogEntry, key=%v, reply=%v", key, reply)
		return nil, reply
	}
	log.Debugf("Success: GetOrFetchWorkingMeta: new working, key=%v, inodeKey=%v, version=%v", key, inodeKey, working.version)
	return working, RaftReplyOk
}

func (n *InodeMgr) GetFilePathAndWorkingMeta(inodeId InodeKeyType) (string, *WorkingMeta, int32) {
	meta := n.GetMetaHandler(inodeId)
	meta.metaLock.RLock()
	keys, ok2 := n.inodeToFiles[inodeId]
	cachedMeta := meta.GetLatestWorkingMeta()
	meta.metaLock.RUnlock()
	if cachedMeta == nil || !ok2 {
		return "", nil, ErrnoToReply(unix.ENOENT)
	}
	var ret string
	for key := range keys {
		ret = key
		break
	}
	return ret, cachedMeta, RaftReplyOk
}

func (n *InodeMgr) GetAllMeta(inodeKeys []InodeKeyType) ([]*common.CopiedMetaMsg, []*common.InodeToFileMsg) {
	metas := make([]*common.CopiedMetaMsg, 0)
	files := make([]*common.InodeToFileMsg, 0)
	n.metadataMapLock.RLock()
	for _, inodeKey := range inodeKeys {
		meta, ok := n.metadataMap[inodeKey]
		fs := n.inodeToFiles[inodeKey]
		if ok {
			meta.metaLock.RLock()
			working := meta.GetLatestWorkingMeta()
			meta.metaLock.RUnlock()
			metas = append(metas, working.toMsg())
		}
		for f := range fs {
			files = append(files, &common.InodeToFileMsg{InodeKey: uint64(inodeKey), FilePath: f})
		}
	}
	n.metadataMapLock.RUnlock()
	return metas, files
}

func (n *InodeMgr) GetAllDirectoryMeta() []InodeKeyType {
	keys := make([]InodeKeyType, 0)
	n.metadataMapLock.Lock()
	for inodeKey, meta := range n.metadataMap {
		working := meta.GetLatestWorkingMeta()
		if working != nil && working.IsDir() && working.inodeKey != InodeKeyType(fuseops.RootInodeID) {
			keys = append(keys, inodeKey)
		}
	}
	n.metadataMapLock.Unlock()
	return keys
}

/****************************************************
 * methods to update metadata without transactions
 ****************************************************/

func (n *InodeMgr) UpdateMetaAttr(attr MetaAttributes, ts int64) (*WorkingMeta, int32) {
	n.nodeLock.RLock()
	if n.IsNodeSuspending() {
		n.nodeLock.RUnlock()
		log.Errorf("TryAgain: UpdateMetaAttr, Node is suspending, inodeKey=%v", attr.inodeKey)
		return nil, ObjCacheReplySuspending
	}
	meta := n.GetMetaHandler(attr.inodeKey)
	meta.metaLock.RLock()
	working := meta.GetLatestWorkingMeta()
	if working == nil {
		meta.metaLock.RUnlock()
		log.Errorf("Failed: UpdateMetaAttr, inode is not created, inodeKey=%v", attr.inodeKey)
		return nil, ErrnoToReply(unix.ENOENT)
	}
	oldMode := working.mode
	oldTs := working.mTime
	if oldMode == attr.mode && oldTs == ts {
		meta.metaLock.RUnlock()
		log.Debugf("Success: UpdateMetaAttr (no update), inodeKey=%v, mode=%v, mTime=%v", attr.inodeKey, oldMode, oldTs)
		return working, RaftReplyOk
	}
	meta.metaLock.RUnlock()

	msg := &common.UpdateMetaAttrMsg{InodeKey: uint64(attr.inodeKey), Mode: attr.mode, Ts: ts}
	reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateMetaAttrCmdId, msg)
	if reply != RaftReplyOk {
		log.Errorf("Failed: UpdateMetaAttr, AppendExtendedLogEntry, inodeKey=%v, reply=%v", attr.inodeKey, reply)
		n.nodeLock.RUnlock()
		return nil, reply
	}
	meta.metaLock.RLock()
	working = meta.GetLatestWorkingMeta()
	meta.metaLock.RUnlock()
	n.nodeLock.RUnlock()
	log.Debugf("Success: UpdateMetaAttr, inodeKey=%v, mode=%v->%v, mTime=%v->%v",
		attr.inodeKey, oldMode, working.mode, oldTs, working.mTime)
	return working, RaftReplyOk
}

func (n *InodeMgr) ApplyAsUpdateMetaAttr(extBuf []byte) (reply int32) {
	msg := &common.UpdateMetaAttrMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsUpdateMetaAttr, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	inodeKey := InodeKeyType(msg.GetInodeKey())
	meta := n.GetMetaHandler(inodeKey)
	meta.metaLock.Lock()
	working := meta.GetLatestWorkingMeta()
	if working != nil {
		working.mode = msg.GetMode()
		working.mTime = msg.GetTs()
	} else {
		log.Warnf("Failed (ignore): ApplyAsUpdateMetaAttr, working is null, inodeKey=%v", inodeKey)
	}
	meta.metaLock.Unlock()
	return RaftReplyOk
}

func (n *InodeMgr) UpdateMetaXattr(inodeKey InodeKeyType, expireMs int32, dirtyMgr *DirtyMgr) int32 {
	//TODO
	return RaftReplyOk
}

/****************************************************
 * methods to update objects
 * updating meta is separated into prepare and commit
 ****************************************************/

func (n *InodeMgr) PrepareCreateMeta(inodeKey InodeKeyType, parent MetaAttributes, chunkSize int64, expireMs int32, mode uint32) (*WorkingMeta, func(*NodeServer), int32) {
	n.nodeLock.RLock()
	if n.IsNodeSuspending() {
		n.nodeLock.RUnlock()
		log.Errorf("TryAgain: PrepareCreateMeta, Node is suspending, inodeKey=%v", inodeKey)
		return nil, nil, ObjCacheReplySuspending
	}
	meta := n.GetMetaHandler(inodeKey)
	meta.lock.Lock() // NOTE: TryLock increases latency too much
	meta.metaLock.RLock()
	working := meta.GetLatestWorkingMeta()
	meta.metaLock.RUnlock()
	if working == nil {
		working = NewWorkingMeta(inodeKey, parent, chunkSize, expireMs, mode, "")
	} else {
		log.Warnf("PrepareCreateMeta, inodeKey already exists, inodeKey=%v", inodeKey)
	}
	unlock := func(n *NodeServer) {
		meta.lock.Unlock()
		n.inodeMgr.nodeLock.RUnlock()
	}
	log.Debugf("Success: PrepareCreateMeta, inodeKey=%v", inodeKey)
	return working, unlock, RaftReplyOk
}

func (n *InodeMgr) PrepareUpdateMetaKey(inodeKey InodeKeyType, oldKey string, parent MetaAttributes, chunkSize int64, expireMs int32) (*Meta, *WorkingMeta, func(*NodeServer), int32) {
	n.nodeLock.RLock()
	if n.IsNodeSuspending() {
		n.nodeLock.RUnlock()
		log.Errorf("TryAgain: PrepareUpdateMetaKey, Node is suspending, inodeKey=%v", inodeKey)
		return nil, nil, nil, ObjCacheReplySuspending
	}
	meta := n.GetMetaHandler(inodeKey)
	meta.lock.Lock() // NOTE: TryLock increases latency too much
	meta.metaLock.Lock()
	working := meta.GetLatestWorkingMeta()
	if working == nil {
		var reply int32
		working, reply = n.__fetchWorkingMeta(inodeKey, oldKey, chunkSize, expireMs, parent)
		if reply != RaftReplyOk {
			meta.metaLock.Unlock()
			return nil, nil, nil, reply
		}
	} else {
		prev := *working
		if working.IsDir() {
			working.childAttrs = map[string]MetaAttributes{}
			for k, child := range prev.childAttrs {
				working.childAttrs[k] = child
			}
		}
		prev.mTime = time.Now().UnixNano()
		prev.version += 1
		prev.prevVer = working
		working = &prev
	}
	meta.metaLock.Unlock()
	unlock := func(n *NodeServer) {
		meta.lock.Unlock()
		n.inodeMgr.nodeLock.RUnlock()
	}
	log.Debugf("Success: PrepareUpdateMetaKey, inodeKey=%v, oldKey=%v", inodeKey, oldKey)
	return meta, working, unlock, RaftReplyOk
}

func (n *InodeMgr) PrepareUpdateMeta(inodeKey InodeKeyType) (*Meta, *WorkingMeta, func(*NodeServer), int32) {
	n.nodeLock.RLock()
	if n.IsNodeSuspending() {
		n.nodeLock.RUnlock()
		log.Errorf("TryAgain: PrepareUpdateMetaKey, Node is suspending, inodeKey=%v", inodeKey)
		return nil, nil, nil, ObjCacheReplySuspending
	}
	meta := n.GetMetaHandler(inodeKey)
	meta.lock.Lock() // NOTE: TryLock increases latency too much
	meta.metaLock.RLock()
	prev := meta.GetLatestWorkingMeta()
	meta.metaLock.RUnlock()
	if prev == nil {
		meta.lock.Unlock()
		n.nodeLock.RUnlock()
		log.Errorf("Failed: PrepareUpdateMeta, GetWorkingMeta, key does not exist, inodeKey=%v", inodeKey)
		return nil, nil, nil, ErrnoToReply(unix.ENOENT)
	}
	unlock := func(n *NodeServer) {
		meta.lock.Unlock()
		n.inodeMgr.nodeLock.RUnlock()
	}
	working := *prev
	if working.IsDir() {
		working.childAttrs = map[string]MetaAttributes{}
		for k, child := range prev.childAttrs {
			working.childAttrs[k] = child
		}
	}
	working.version = prev.version + 1
	working.mTime = time.Now().UnixNano()
	working.prevVer = prev
	log.Debugf("Success: PrepareUpdateMeta, inodeKey=%v, version=%v->%v", inodeKey, prev.version, working.version)
	return meta, &working, unlock, RaftReplyOk
}

func (n *InodeMgr) PreparePersistMeta(inodeKey InodeKeyType, dirtyMgr *DirtyMgr) (*Meta, []string, *WorkingMeta, int32) {
	n.nodeLock.RLock() // unlock at unlockPersistMetaLocal
	if n.IsNodeSuspending() {
		n.nodeLock.RUnlock()
		log.Errorf("TryAgain: PreparePersistMeta, node is suspending, inodeKey=%v", inodeKey)
		return nil, nil, nil, ObjCacheReplySuspending
	}
	meta := n.GetMetaHandler(inodeKey)
	meta.persistLock.Lock() // unlock at unlockPersistMetaLocal
	meta.metaLock.RLock()
	working := meta.GetLatestWorkingMeta()
	metaKeys, ok2 := n.inodeToFiles[inodeKey]
	meta.metaLock.RUnlock()
	if working == nil {
		meta.persistLock.Unlock()
		n.nodeLock.RUnlock()
		log.Debugf("PreparePersistMeta, inodeKey=%v does not exist", inodeKey)
		return nil, nil, nil, ObjCacheIsNotDirty
	}
	if !ok2 {
		meta.persistLock.Unlock()
		n.nodeLock.RUnlock()
		log.Debugf("PreparePersistMeta, inodeKey=%v has no keys", inodeKey)
		return nil, nil, working, ObjCacheIsNotDirty
	}
	if ok := dirtyMgr.IsDirtyMeta(inodeKey); !ok {
		meta.persistLock.Unlock()
		n.nodeLock.RUnlock()
		log.Debugf("PreparePersistMeta, inode is not dirty, inodeKey=%v", inodeKey)
		return nil, nil, nil, ObjCacheIsNotDirty
	}
	ret := make([]string, len(metaKeys))
	var i = 0
	for key := range metaKeys {
		ret[i] = key
		i += 1
	}
	log.Debugf("Success: PreparePersistMeta, inodeKey=%v", inodeKey)
	return meta, ret, working, RaftReplyOk
}

func (n *InodeMgr) PreparePersistDeleteMeta(inodeKey InodeKeyType, key string, dirtyMgr *DirtyMgr) (*Meta, *WorkingMeta, int32) {
	n.nodeLock.RLock() // unlock at unlockPersistMetaLocal
	if n.IsNodeSuspending() {
		n.nodeLock.RUnlock()
		log.Errorf("TryAgain: PreparePersistDeleteMeta, Node is suspending, key=%v, inodeKey=%v", key, inodeKey)
		return nil, nil, ObjCacheReplySuspending
	}
	n.metadataMapLock.RLock()
	meta, ok := n.metadataMap[inodeKey]
	n.metadataMapLock.RUnlock()
	if !ok {
		n.nodeLock.RUnlock()
		log.Debugf("PreparePersistDeleteMeta, inodeKey=%v does not exist", inodeKey)
		return nil, nil, ObjCacheIsNotDirty
	}
	meta.persistLock.Lock() // unlock at unlockPersistMetaLocal
	meta.metaLock.RLock()
	working := meta.GetLatestWorkingMeta()
	meta.metaLock.RUnlock()
	if working == nil {
		meta.persistLock.Unlock()
		n.nodeLock.RUnlock()
		log.Errorf("PreparePersistMeta, inodeKey=%v does not exist", inodeKey)
		return nil, nil, ObjCacheIsNotDirty
	}
	deleted, ok := dirtyMgr.GetDeleteKey(key)
	if !ok || deleted != inodeKey {
		meta.persistLock.Unlock()
		n.nodeLock.RUnlock()
		log.Debugf("PreparePersistDeleteMeta, key=%v, inodeKey=%v is not dirty", key, inodeKey)
		return nil, nil, ObjCacheIsNotDirty
	}
	log.Debugf("Success: PreparePersistDeleteMeta, inodeKey=%v, key=%v", inodeKey, key)
	return meta, working, RaftReplyOk
}

func (n *InodeMgr) UnlockPersistMeta(meta *Meta, inodeKey InodeKeyType) {
	meta.persistLock.Unlock()
	n.nodeLock.RUnlock()
	log.Debugf("Success: UnlockPersistMeta, inodeKey=%v", inodeKey)
}

func (n *InodeMgr) CommitSetMetaAndInodeFile(working *WorkingMeta, key string) {
	meta := n.GetMetaHandler(working.inodeKey)
	meta.metaLock.Lock()
	meta.AddWorkingMeta(working)
	meta.metaLock.Unlock()
	n.metadataMapLock.Lock()
	if _, ok := n.inodeToFiles[working.inodeKey]; !ok {
		n.inodeToFiles[working.inodeKey] = make(map[string]bool)
	}
	n.inodeToFiles[working.inodeKey][key] = true
	n.metadataMapLock.Unlock()
}

func (n *InodeMgr) ApplyAsAddInodeFileMap(extBuf []byte) (reply int32) {
	msg := &common.AddInodeFileMapMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsAddInodeFileMap, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	n.CommitSetMetaAndInodeFile(NewWorkingMetaFromMsg(msg.GetMeta()), msg.GetKey())
	return RaftReplyOk
}

func (n *InodeMgr) CommitDeleteInodeMap(deleted *common.DeleteInodeMapArgs) {
	n.metadataMapLock.Lock()
	for i := 0; i < len(deleted.InodeKey); i++ {
		inodeKey := InodeKeyType(deleted.InodeKey[i])
		deletedKey := deleted.Key[i]
		delete(n.metadataMap, inodeKey)
		if keys, ok := n.inodeToFiles[inodeKey]; ok {
			delete(keys, deletedKey)
			if len(keys) == 0 {
				delete(n.inodeToFiles, inodeKey)
			}
		}
	}
	n.metadataMapLock.Unlock()
}

func (n *InodeMgr) ApplyAsDeleteInodeMap(extBuf []byte) (reply int32) {
	msg := &common.DeleteInodeMapArgs{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsDeleteInodeMap, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	n.CommitDeleteInodeMap(msg)
	return RaftReplyOk
}

func (n *InodeMgr) CommitUpdateMeta(working *WorkingMeta, dirtyMgr *DirtyMgr) {
	meta := n.GetMetaHandler(working.inodeKey)
	meta.metaLock.Lock()
	if working.prevVer != nil {
		working.prevVer.DropPrev()
	}
	meta.AddWorkingMeta(working)
	meta.metaLock.Unlock()
	dirtyMgr.lock.Lock()
	dirtyMgr.AddMetaNoLock(working)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitUpdateMeta, inodeKey=%v, version=%v", working.inodeKey, working.version)
}

func (n *InodeMgr) CommitUpdateParentMeta(working *WorkingMeta, key string, dirtyMgr *DirtyMgr) {
	meta := n.GetMetaHandler(working.inodeKey)
	meta.metaLock.Lock()
	if working.prevVer != nil {
		working.prevVer.DropPrev()
	}
	// NOTE: record inodeToFiles here to reconstruct correct inodeToFiles after crash recovery.
	// Without this, the recovery procedure only restores inode information and cannot access it as files.
	if _, ok := n.inodeToFiles[working.inodeKey]; !ok {
		n.inodeToFiles[working.inodeKey] = make(map[string]bool)
	}
	if _, exist := n.inodeToFiles[working.inodeKey]; !exist {
		// should only be here at log apply during crash recovery
		n.inodeToFiles[working.inodeKey][key] = true
	}
	meta.AddWorkingMeta(working)
	meta.metaLock.Unlock()
	dirtyMgr.lock.Lock()
	dirtyMgr.AddMetaNoLock(working)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitUpdateParentMeta, inodeKey=%v, version=%v", working.inodeKey, working.version)
}

func (n *InodeMgr) CommitDeleteMeta(working *WorkingMeta, key string, dirtyMgr *DirtyMgr) {
	meta := n.GetMetaHandler(working.inodeKey)
	meta.metaLock.Lock()
	newKeys := make(map[string]bool)
	keys := n.inodeToFiles[working.inodeKey]
	for known := range keys {
		if known != key {
			newKeys[known] = true
		}
	}
	if len(newKeys) == 0 {
		delete(n.inodeToFiles, working.inodeKey)
	} else {
		n.inodeToFiles[working.inodeKey] = newKeys
	}
	meta.metaLock.Unlock()
	dirtyMgr.lock.Lock()
	dirtyMgr.RemoveMetaNoLock(working.inodeKey)
	dirtyMgr.AddDeleteKeyNoLock(key, working)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitDeleteMeta, key=%v, inodeKey=%v, version=%v", key, working.inodeKey, working.version)
}

func (n *InodeMgr) CommitCreateMeta(working *WorkingMeta, newKey string, dirtyMgr *DirtyMgr) {
	meta := n.GetMetaHandler(working.inodeKey)
	meta.metaLock.Lock()
	if _, ok := n.inodeToFiles[working.inodeKey]; !ok {
		n.inodeToFiles[working.inodeKey] = make(map[string]bool)
	}
	n.inodeToFiles[working.inodeKey][newKey] = true
	if working.prevVer != nil {
		working.prevVer.DropPrev()
	}
	meta.AddWorkingMeta(working)
	meta.metaLock.Unlock()
	dirtyMgr.lock.Lock()
	dirtyMgr.AddMetaNoLock(working)
	dirtyMgr.RemoveDeleteKeyNoLock(newKey)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitCreateMeta, newKey=%v, metaKey=%v, version=%v", newKey, working.inodeKey, working.version)
}

func (n *InodeMgr) CommitUpdateMetaKey(working *WorkingMeta, removedKey string, newKey string, dirtyMgr *DirtyMgr) *WorkingMeta {
	meta := n.GetMetaHandler(working.inodeKey)
	meta.metaLock.Lock()
	if _, ok := n.inodeToFiles[working.inodeKey]; !ok {
		n.inodeToFiles[working.inodeKey] = make(map[string]bool)
	}
	if removedKey != "" {
		delete(n.inodeToFiles[working.inodeKey], removedKey)
	}
	if newKey != "" {
		n.inodeToFiles[working.inodeKey][newKey] = true
	}
	if working.prevVer != nil {
		working.prevVer.DropPrev()
	}
	meta.AddWorkingMeta(working)
	meta.metaLock.Unlock()
	dirtyMgr.lock.Lock()
	if removedKey != "" {
		dirtyMgr.AddDeleteKeyNoLock(removedKey, working)
	}
	if newKey != "" {
		dirtyMgr.RemoveDeleteKeyNoLock(newKey)
	}
	if removedKey != "" || newKey != "" {
		dirtyMgr.AddMetaNoLock(working)
	}
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitUpdateMetaKey, inodeKey=%v, removedKey=%v, newKey=%v", working.inodeKey, removedKey, newKey)
	return working
}

func (n *InodeMgr) CommitPersistMeta(inodeKey InodeKeyType, version uint32, ts int64, dirtyMgr *DirtyMgr) {
	dirtyMgr.lock.Lock()
	ok := dirtyMgr.RemoveMetaNoLockIfLatest(inodeKey, version)
	dirtyMgr.lock.Unlock()
	if ok {
		meta := n.GetMetaHandler(inodeKey)
		meta.metaLock.Lock()
		working := meta.GetLatestWorkingMeta()
		if working != nil {
			working.mTime = ts
		}
		meta.metaLock.Unlock()
		log.Debugf("Success: CommitPersistMeta, inodeKey=%v, ts=%v", inodeKey, ts)
	} else {
		log.Infof("CommitPersistMeta, skip updating mTime, inodeKey=%v, version=%v", inodeKey, version)
	}
}

func (n *InodeMgr) CommitDeletePersistMeta(key string, dirtyMgr *DirtyMgr) {
	dirtyMgr.lock.Lock()
	inodeKey, ok := dirtyMgr.RemoveDeleteKeyNoLock(key)
	dirtyMgr.lock.Unlock()
	if ok {
		n.metadataMapLock.Lock()
		meta, ok := n.metadataMap[inodeKey]
		delete(n.metadataMap, inodeKey)
		n.metadataMapLock.Unlock()
		if ok {
			meta.metaLock.Lock()
			meta.workingHead.prevVer = nil
			meta.metaLock.Unlock()
		}
		log.Debugf("Success: CommitDeletePersistMeta, key=%v, inodeKey=%v", key, inodeKey)
	} else {
		log.Infof("CommitDeletePersistMeta, skip: key is already deleted. key=%v, inodeKey=%v", key, inodeKey)
	}
}

func (n *InodeMgr) ApplyAsDeletePersist(extBuf []byte, dirtyMgr *DirtyMgr) int32 {
	ops := &common.TwoPCPersistRecordMsg{}
	if err := proto.Unmarshal(extBuf, ops); err != nil {
		log.Errorf("Failed: ApplyAsDeletePersist, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(ops.TxId)
	n.CommitDeletePersistMeta(ops.GetMetaKeys()[0], dirtyMgr)
	log.Debugf("Success: ApplyAsDeletePersist, txId=%v", txId)
	return RaftReplyOk
}

func (n *InodeMgr) RestoreMetas(metas []*common.CopiedMetaMsg, files []*common.InodeToFileMsg) {
	for _, metaMsg := range metas {
		working := NewWorkingMetaFromMsg(metaMsg)
		meta := n.GetMetaHandler(working.inodeKey)
		meta.lock.Lock()
		meta.AddWorkingMeta(working)
		meta.lock.Unlock()
	}
	for _, fileMsg := range files {
		inodeKey := InodeKeyType(fileMsg.GetInodeKey())
		meta := n.GetMetaHandler(inodeKey)
		meta.metaLock.Lock()
		if _, ok := n.inodeToFiles[inodeKey]; !ok {
			n.inodeToFiles[inodeKey] = make(map[string]bool)
		}
		n.inodeToFiles[inodeKey][fileMsg.GetFilePath()] = true
		meta.metaLock.Unlock()
	}
}

func (n *InodeMgr) DeleteInodeToFiles(inodeKeys []uint64) {
	for _, i := range inodeKeys {
		inodeKey := InodeKeyType(i)
		meta := n.GetMetaHandler(inodeKey)
		meta.metaLock.Lock()
		delete(n.inodeToFiles, inodeKey)
		meta.metaLock.Unlock()
	}
}

/***************************
 * methods to persist object
 ***************************/

func (n *InodeMgr) PutDirObject(ret *common.TwoPCPersistRecordMsg, metaKeys []string, meta *WorkingMeta, _ int) (reply int32) {
	for _, metaKey := range metaKeys {
		if !strings.Contains(metaKey, "/") {
			log.Debugf("Success: PutDirObject (skipped), inodeKey=%v, key=%v", meta.inodeKey, metaKey)
			reply = RaftReplyOk
			continue
		}
		var key = metaKey
		if !strings.HasPrefix(key, "/") {
			key += "/"
		}
		if len(meta.childAttrs) == 0 {
			res, awsErr := n.back.putBlob(&PutBlobInput{Key: key, DirBlob: true, Body: nil})
			if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
				log.Errorf("Failed: PutDirObject, putBlob, meta=%v, awsErr=%v, reply=%v", *meta, awsErr, reply)
				return
			}
			if res.LastModified != nil {
				ret.PrimaryMeta.Ts = res.LastModified.UTC().UnixNano()
			}
		} else {
			reply = RaftReplyOk
		}
		log.Infof("Success: PutDirObject, inodeKey=%v, key=%v", meta.inodeKey, key)
	}
	return
}

func (n *InodeMgr) PersistDeleteObject(ret *common.TwoPCPersistRecordMsg, metaKey string, meta *WorkingMeta, _ int) (reply int32) {
	var key = metaKey
	if meta.IsDir() && !strings.HasSuffix(key, "/") {
		key += "/"
	}
	_, awsErr := n.back.deleteBlob(&DeleteBlobInput{Key: key})
	if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
		if reply != ErrnoToReply(unix.ENOENT) {
			log.Errorf("Failed: PersistDeleteObject, deleteBlob, awsErr=%v, reply=%v", awsErr, reply)
			return
		}
		log.Debugf("Success: PersistDeleteObject (already deleted), inodeKey=%v, metaKey=%v", meta.inodeKey, key)
		return
	}
	nrRetries := 256
	var backOff = time.Millisecond * 10
	for j := 0; j < nrRetries; j++ {
		_, awsErr = n.back.headBlob(&HeadBlobInput{Key: key})
		if reply = AwsErrToReply(awsErr); reply != ErrnoToReply(unix.ENOENT) {
			log.Debugf("Failed: PersistDeleteObject name %v remains visible or other failures (awsErr=%v, r=%v), Retry after %v secs", key, awsErr, reply, backOff)
			time.Sleep(backOff)
			continue
		}
		ts := time.Now().UnixNano()
		ret.PrimaryMeta.Ts = ts
		if meta.mTime > ts {
			log.Warnf("PersistDeleteObject, System time is inaccurate (now: %v, prev: %v). use 1 msec after the prev for new one", ts, meta.mTime)
			ret.PrimaryMeta.Ts = meta.mTime + 1000
		}
		break
	}
	log.Infof("Success: PersistDeleteObject, inodeKey=%v, metaKey=%v", meta.inodeKey, key)
	reply = RaftReplyOk
	return
}

/************************
 * methods to read chunks
 ************************/

// SetChunkNoLock: caller must hold chunk.lock.Lock()
func (n *InodeMgr) SetChunkNoLock(chunk *Chunk, h MetaRWHandler, key string) (*WorkingChunk, int32) {
	working, err := chunk.GetWorkingChunk(h.version, true)
	if err == nil {
		//log.Debugf("Success: SetChunkNoLock (cached), inodeKey=%v, offset=%v", h.inodeKey, alignedOffset)
		return working, RaftReplyOk //fast path
	}

	length := h.size - chunk.offset
	if length > h.chunkSize {
		length = h.chunkSize
	}
	fileId := NewFileIdTypeFromInodeKey(h.inodeKey, chunk.offset, h.chunkSize)
	fileOffset := n.raft.files.ReserveRange(fileId, length)
	msg := &common.CreateChunkArgs{InodeKey: uint64(h.inodeKey), Key: key, Version: h.version, Offset: chunk.offset, FileOffset: fileOffset, Length: length}
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryCreateChunkCmdId, msg); reply != RaftReplyOk {
		log.Errorf("Failed: SetChunkNoLock, AppendExtendedLogEntry, msg=%v, reply=%v", msg, reply)
		return nil, reply
	}
	working, err = chunk.GetWorkingChunk(h.version, true)
	if key != "" && err != nil {
		log.Errorf("BUG: SetChunkNoLock, GetWrokingChunk, inodeKey=%v, key=%v, offset=%v, version=%v, err=%v",
			h.inodeKey, key, chunk.offset, h.version, err)
	} else {
		log.Debugf("Success: SetChunkNoLock (new), inodeKey=%v, key=%v, offset=%v", h.inodeKey, key, chunk.offset)
	}
	return working, ErrnoToReply(err)
}

func (n *InodeMgr) GetChunk(inodeKey InodeKeyType, offset int64, chunkSize int64) *Chunk {
	alignedOffset := offset - offset%chunkSize
	n.chunkMapLock.Lock()
	chunks, ok := n.chunkMap[inodeKey]
	if !ok {
		chunks = make(map[int64]*Chunk)
		n.chunkMap[inodeKey] = chunks
	}
	chunk, ok2 := chunks[alignedOffset]
	if !ok2 {
		chunk = CreateNewChunk(inodeKey, alignedOffset, n.raft.files)
		n.chunkMap[inodeKey][alignedOffset] = chunk
	}
	n.chunkMapLock.Unlock()
	//log.Debugf("Success: GetChunk, inodeKey=%v, offset=%v", inodeKey, alignedOffset)
	return chunk
}

func (n *InodeMgr) VectorReadChunk(h MetaRWHandler, key string, offset int64, size int, blocking bool) (bufs []SlicedPageBuffer, count int, reply int32) {
	n.nodeLock.RLock()
	defer n.nodeLock.RUnlock()
	if n.IsNodeSuspending() {
		log.Errorf("TryAgain: VectorReadChunk, Node is suspending, h=%v, offset=%v", h, offset)
		return nil, 0, ObjCacheReplySuspending
	}
	chunk := n.GetChunk(h.inodeKey, offset, h.chunkSize)
	chunk.lock.Lock()
	working, reply := n.SetChunkNoLock(chunk, h, key)
	chunk.lock.Unlock()
	if reply != RaftReplyOk {
		log.Errorf("Failed: VectorReadChunk, GetChunkOrSetAtRemote, inodeKey=%v, key=%v, offset=%v, chunkSize=%v, reply=%v", h.inodeKey, key, offset, h.chunkSize, reply)
		return nil, 0, reply
	}
	reader := working.GetReader(h.chunkSize, h.size, offset, n, blocking)
	var err error
	bufs, count, err = reader.GetBufferDirect(size)
	reader.Close()
	if err != nil {
		if err == io.EOF {
			err = nil
		} else if blocking || (!blocking && err != unix.EAGAIN) {
			log.Errorf("Failed: VectorReadChunk, GetBufferDirect, err=%v", err)
		}
		return nil, 0, ErrnoToReply(err)
	}
	return bufs, count, RaftReplyOk
}

func (n *InodeMgr) PrefetchChunkThread(h MetaRWHandler, key string, offset int64) {
	chunk := n.GetChunk(h.inodeKey, offset, h.chunkSize)
	chunk.lock.Lock()
	working, reply := n.SetChunkNoLock(chunk, h, key)
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
	prev, _ := n.SetChunkNoLock(chunk, h, newMeta.fetchKey)
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
			fileId := NewFileIdTypeFromInodeKey(newMeta.inodeKey, chunk.offset, newMeta.chunkSize)
			fileOffset := n.raft.files.ReserveRange(fileId, length)
			working = chunk.NewWorkingChunk(newMeta.version)
			working.AddStag(NewStagingChunk(0, length, StagingChunkData, fileOffset, newMeta.fetchKey, 0))
		}
	}
	if working == nil {
		working = chunk.NewWorkingChunk(newMeta.version)
	}
	return
}

func (n *InodeMgr) AppendStagingChunkFile(inodeKey InodeKeyType, offset int64, fileId FileIdType, fileOffset int64, dataLength uint32) (reply int32) {
	n.nodeLock.RLock()
	begin := time.Now()
	cmd, err := NewAppendEntryFileCommandFromFile(n.raft.currentTerm.Get(), AppendEntryUpdateChunkCmdId, fileId, fileOffset, dataLength, n.raft.files.GetFileName(fileId))
	if reply = ErrnoToReply(err); reply != RaftReplyOk {
		n.nodeLock.RUnlock()
		log.Errorf("Failed: AppendStagingChunkFile, CalcCheckSum, fileId=%v, reply=%v", fileId, reply)
		return reply
	}
	checksum := time.Now()
	_, _, reply = n.raft.AppendEntriesLocal(cmd)
	n.nodeLock.RUnlock()
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendStagingChunkFile, AppendCommand, fileId=%v, reply=%v", fileId, reply)
		return reply
	}
	appendEntries := time.Now()
	log.Debugf("Success: AppendStagingChunkFile, inodeKey=%v, offset=%v, length=%v, checksum=%v, appendEntriesTime=%v",
		inodeKey, offset, dataLength, checksum.Sub(begin), appendEntries.Sub(checksum))
	return RaftReplyOk
}

func (n *InodeMgr) AppendStagingChunkBuffer(inodeKey InodeKeyType, offset int64, chunkSize int64, buf []byte) (fileOffset int64, reply int32) {
	n.nodeLock.RLock()
	begin := time.Now()
	// update a chunk at the same node as a FUSE caller
	fileId := NewFileIdTypeFromInodeKey(inodeKey, offset, chunkSize)
	fileOffset, _, reply = n.raft.files.SyncWrite(fileId, buf)
	if reply != RaftReplyOk {
		n.nodeLock.RUnlock()
		log.Errorf("Failed: AppendStagingChunkBuffer, OpenAndWriteCache, reply=%v", reply)
		return
	}
	write := time.Now()
	cmd := NewAppendEntryFileCommand(n.raft.currentTerm.Get(), AppendEntryUpdateChunkCmdId, fileId, fileOffset, buf)
	_, _, reply = n.raft.AppendEntriesLocal(cmd)
	n.nodeLock.RUnlock()
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendStagingChunkBuffer, AppendCommand, fileId=%v, reply=%v", fileId, reply)
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
		n.chunkMapLock.Lock()
		chunks, ok := n.chunkMap[k]
		if !ok {
			n.chunkMapLock.Unlock()
			continue
		}
		chunk, ok2 := chunks[offsets[i]]
		if !ok2 {
			n.chunkMapLock.Unlock()
			continue
		}
		delete(chunks, offsets[i])
		if len(chunks) == 0 {
			delete(n.chunkMap, k)
		}
		n.chunkMapLock.Unlock()
		chunk.Drop(n, n.raft)
	}
}

func (n *InodeMgr) ApplyAsDropLRUChunks(extBuf []byte) (reply int32) {
	msg := &common.DropLRUChunksArgs{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsDropLRUChunks, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	n.DropLRUChunk(msg.GetInodeKeys(), msg.GetOffsets())
	return RaftReplyOk
}

func (n *InodeMgr) PreparePersistChunk(meta *WorkingMeta, offset int64) (chunk *Chunk, working *WorkingChunk, reply int32) {
	h := NewMetaRWHandlerFromMeta(meta)
	chunk = n.GetChunk(h.inodeKey, offset, h.chunkSize)
	chunk.lock.Lock()
	working, reply = n.SetChunkNoLock(chunk, h, meta.fetchKey)
	chunk.lock.Unlock()
	if reply != RaftReplyOk {
		log.Errorf("Failed: PreparePersistChunk, GetChunkOrSetAtRemote, inodeKey=%v, offset=%v, not found", meta.inodeKey, offset)
	}
	return chunk, working, reply
}

func (n *InodeMgr) PutEmptyObject(ret *common.TwoPCPersistRecordMsg, metaKeys []string, meta *WorkingMeta, _ int) (reply int32) {
	for _, metaKey := range metaKeys {
		size := uint64(meta.size)
		res, awsErr := n.back.putBlob(&PutBlobInput{Key: metaKey, Body: nil, Size: &size})
		if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
			log.Errorf("Failed: PutEmptyObject, putBlob, meta=%v, awsErr=%v, reply=%v", *meta, awsErr, reply)
			return
		}
		if res.LastModified != nil {
			ret.PrimaryMeta.Ts = res.LastModified.UTC().UnixNano()
		}
		log.Infof("Success: PutEmptyObject, inode=%v, key=%v", meta.inodeKey, metaKey)
	}
	return
}

func (n *InodeMgr) PutObject(ret *common.TwoPCPersistRecordMsg, metaKeys []string, meta *WorkingMeta, priority int, selfGroup string, dirtyExpireInterval time.Duration) (reply int32) {
	for _, metaKey := range metaKeys {
		var chunk *Chunk
		var working *WorkingChunk
		chunk, working, reply = n.PreparePersistChunk(meta, 0)
		if reply != RaftReplyOk {
			return
		}
		ret.PrimaryMeta.Chunks = []*common.PersistedChunkInfoMsg{{TxSeqNum: ret.GetTxId().GetTxSeqNum(), GroupId: selfGroup, Offsets: []int64{0}, CVers: []uint32{meta.version}}}
		reader := working.GetReader(meta.chunkSize, meta.size, 0, n, true)
		size := uint64(meta.size)
		res, awsErr := n.back.putBlob(&PutBlobInput{Key: metaKey, Body: reader, Size: &size})
		if priority <= 0 {
			if time.Now().UTC().Sub(chunk.lastAccess) > dirtyExpireInterval/2 {
				reader.DontNeed(n.raft)
			}
		}
		if err2 := reader.Close(); err2 != nil {
			log.Errorf("Failed: PutObject, Close, err=%v", err2)
		}
		if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
			log.Errorf("Failed: PutObject, putBlob, meta=%v, awsErr=%v, reply=%v", *meta, awsErr, reply)
			return
		}
		if res.LastModified != nil {
			ret.PrimaryMeta.Ts = res.LastModified.UTC().UnixNano()
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
	n.nodeLock.RLock()
	if n.IsNodeSuspending() {
		n.nodeLock.RUnlock()
		log.Errorf("TryAgain: MpuAdd, Node is suspending, meta=%v", *meta)
		return MpuAddOut{}, nil, ObjCacheReplySuspending
	}
	_, working, reply := n.PreparePersistChunk(meta, offset)
	if reply != RaftReplyOk {
		n.nodeLock.RUnlock()
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
	if priority <= 0 {
		if time.Now().UTC().Sub(working.chunkPtr.lastAccess) > dirtyExpireInterval/2 {
			reader.DontNeed(n.raft)
		}
	}
	if err := reader.Close(); err != nil {
		log.Errorf("Failed (ignore): MpuAdd, multipartBlobAdd, Close, err=%c", err)
	}
	if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
		n.nodeLock.RUnlock()
		log.Errorf("Failed: MpuAdd, multipartBlobAdd: meta=%v, offset=%v, awsErr=%v, reply=%v", meta.inodeKey, offset, awsErr, reply)
		return MpuAddOut{}, nil, reply
	}
	unlock = func(n *NodeServer) {
		n.inodeMgr.nodeLock.RUnlock()
	}
	log.Infof("Success: MpuAdd, meta=%v, offset=%v, id=%v, etag=%v, size=%v", meta.inodeKey, offset, idx, *input.Parts[0], size)
	return MpuAddOut{idx: idx, etag: *input.Parts[0], cVer: working.chunkVer}, unlock, RaftReplyOk
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

func (n *InodeMgr) CommitUpdateChunk(working *WorkingChunk, offset int64, chunkSize int64, dirtyMgr *DirtyMgr, objectSize int64) {
	c := working.chunkPtr
	c.AddWorkingChunk(n, working, working.prevVer)
	dirtyMgr.lock.Lock()
	dirtyMgr.AddChunkNoLock(c.inodeKey, chunkSize, working.chunkVer, offset, objectSize)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitUpdateChunk, inodeKey=%v, offset=%v, version=%v", c.inodeKey, offset, working.chunkVer)
}

func (n *InodeMgr) CommitDeleteChunk(working *WorkingChunk, offset int64, dirtyMgr *DirtyMgr) {
	c := working.chunkPtr
	c.AddWorkingChunk(n, working, working.prevVer)
	dirtyMgr.lock.Lock()
	dirtyMgr.RemoveChunkNoLock(c.inodeKey, offset, working.prevVer.chunkVer)
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitDeleteChunk, inodeKey=%v, version=%v", c.inodeKey, working.chunkVer)
}

func (n *InodeMgr) ApplyAsCommitUpdateChunk(extBuf []byte, dirtyMgr *DirtyMgr) int32 {
	msg := &common.AppendCommitUpdateChunksMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsCommitUpdateChunk, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	inodeKey := InodeKeyType(msg.GetInodeKey())
	chunks := make(map[int64]*WorkingChunk)
	for _, chunk := range msg.GetChunks() {
		c := n.GetChunk(inodeKey, chunk.GetOffset(), msg.GetChunkSize())
		working := c.NewWorkingChunk(msg.GetVersion())
		working.AddStagingChunkFromAddMsg(chunk)
		chunks[chunk.GetOffset()] = working
	}
	if msg.GetIsDelete() {
		for offset, chunk := range chunks {
			n.CommitDeleteChunk(chunk, offset, dirtyMgr)
		}
	} else {
		for offset, chunk := range chunks {
			n.CommitUpdateChunk(chunk, offset, msg.GetChunkSize(), dirtyMgr, msg.GetObjectSize())
		}
	}
	return RaftReplyOk
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
				updateType: StagingChunkData, fileOffset: stag.GetFileOffset(),
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

func (n *InodeMgr) QuickCommitDeleteChunk(ring *hashring.HashRing, selfGroup string, meta *WorkingMeta, dirtyMgr *DirtyMgr) {
	chunks := make([]*Chunk, 0)
	for offset := meta.size; offset < meta.prevVer.size; {
		var length = meta.chunkSize
		slop := offset % meta.chunkSize
		if slop > 0 {
			length -= slop
		}
		if offset+length > meta.prevVer.size {
			length = meta.prevVer.size - offset
		}
		groupId, ok := GetGroupForChunk(ring, meta.inodeKey, offset, meta.chunkSize)
		if ok && groupId == selfGroup {
			chunk, working := n.QuickPrepareChunk(meta, offset)
			working.AddStag(&StagingChunk{
				filled:     1,
				length:     length,
				slop:       offset,
				updateType: StagingChunkDelete,
			})
			working.chunkPtr.AddWorkingChunk(n, working, working.prevVer)
			chunks = append(chunks, chunk)
			dirtyMgr.lock.Lock()
			dirtyMgr.RemoveChunkNoLock(chunk.inodeKey, offset, working.chunkVer)
			dirtyMgr.lock.Unlock()
		}
		offset += length
	}
	for _, chunk := range chunks {
		chunk.lock.Unlock()
	}
}

func (n *InodeMgr) QuickCommitExpandChunk(ring *hashring.HashRing, selfGroup string, meta *WorkingMeta, dirtyMgr *DirtyMgr) {
	chunks := make([]*Chunk, 0)
	for offset := meta.prevVer.size; offset < meta.size; {
		var length = meta.chunkSize
		slop := offset % meta.chunkSize
		if slop > 0 {
			length -= slop
		}
		if offset+length > meta.size {
			length = meta.size - offset
		}
		groupId, ok := GetGroupForChunk(ring, meta.inodeKey, offset, meta.chunkSize)
		if ok && groupId == selfGroup {
			chunk, working := n.QuickPrepareChunk(meta, offset)
			working.AddStag(&StagingChunk{
				filled:     1,
				length:     length,
				slop:       offset,
				updateType: StagingChunkBlank,
			})
			working.chunkPtr.AddWorkingChunk(n, working, working.prevVer)
			chunks = append(chunks, chunk)
			dirtyMgr.lock.Lock()
			dirtyMgr.AddChunkNoLock(chunk.inodeKey, meta.chunkSize, working.chunkVer, offset, meta.size)
			dirtyMgr.lock.Unlock()
		}
		offset += length
	}
	for _, chunk := range chunks {
		chunk.lock.Unlock()
	}
}

func (n *InodeMgr) CommitCreateChunk(inodeKey InodeKeyType, offset int64, version uint32, fileOffset int64, length int64, key string) {
	begin := time.Now()
	var offsetCreate = false
	n.chunkMapLock.Lock()
	cmap, ok := n.chunkMap[inodeKey]
	var c *Chunk
	if !ok {
		cmap = make(map[int64]*Chunk)
		n.chunkMap[inodeKey] = cmap
	}
	c, ok2 := cmap[offset]
	if !ok2 {
		c = CreateNewChunk(inodeKey, offset, n.raft.files)
		cmap[offset] = c
	}
	n.chunkMapLock.Unlock()
	lock := time.Now()
	if key != "" {
		if _, err := c.GetWorkingChunk(version, true); err != nil {
			working := c.NewWorkingChunk(version)
			newStag := NewStagingChunk(0, length, StagingChunkData, fileOffset, key, 0)
			working.AddStag(newStag)
			c.AddWorkingChunk(n, working, c.workingHead.prevVer)
		}
	}
	log.Debugf("Success: CommitCreateChunk, inodeKey=%v, key=%v, offset=%v, lock=%v, elapsed=%v, offsetCreate=%v", inodeKey, key, offset, lock.Sub(begin), time.Since(begin), offsetCreate)
}

func (n *InodeMgr) ApplyAsCreateChunk(extBuf []byte) (reply int32) {
	begin := time.Now()
	msg := &common.CreateChunkArgs{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsCreateChunk, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	inodeKey := InodeKeyType(msg.GetInodeKey())
	offset := msg.GetOffset()
	version := msg.GetVersion()
	fileOffset := msg.GetFileOffset()
	length := msg.GetLength()
	key := msg.GetKey()

	marshaling := time.Now()
	n.CommitCreateChunk(inodeKey, offset, version, fileOffset, length, key)
	log.Debugf("Success: ApplyAsCreateChunk, len(extBuf)=%v, marshaling=%v, elapsed=%v", len(extBuf), marshaling.Sub(begin), time.Since(begin))
	return RaftReplyOk
}

func (n *InodeMgr) CommitPersistChunk(inodeKey InodeKeyType, metaKey string, offsets []int64, cVers []uint32, dirtyMgr *DirtyMgr) {
	dirtyMgr.lock.Lock()
	for i := 0; i < len(offsets); i++ {
		dirtyMgr.RemoveChunkNoLock(inodeKey, offsets[i], cVers[i])
	}
	dirtyMgr.lock.Unlock()
	log.Debugf("Success: CommitPersistChunk, inodeKey=%v, offset=%v, cVer=%v", inodeKey, offsets, cVers)
}

func (n *InodeMgr) ApplyAsPersistChunk(extBuf []byte, dirtyMgr *DirtyMgr) int32 {
	msg := &common.PersistedChunkInfoMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsPersistChunk, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	n.CommitPersistChunk(InodeKeyType(msg.GetInodeKey()), msg.GetNewFetchKey(), msg.GetOffsets(), msg.GetCVers(), dirtyMgr)
	return RaftReplyOk
}
