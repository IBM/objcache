/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/takeshi-yoshimura/fuse/fuseops"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/serialx/hashring"
	"github.com/takeshi-yoshimura/fuse"
	"github.com/IBM/objcache/api"
	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const TrackerNodeKey = InodeKeyType(math.MaxUint64)

type NodeServer struct {
	args       *common.ObjcacheCmdlineArgs
	flags      *common.ObjcacheConfig
	restarted  bool
	shutdown   int32
	signalChan chan os.Signal

	selfNode   RaftNode
	nodeCond   *sync.Cond
	suspending int
	runOps     int

	inodeMgr     *InodeMgr
	raft         *RaftInstance
	raftGroup    *RaftGroupMgr
	dirtyMgr     *DirtyMgr
	txMgr        *TxMgr
	rpcMgr       *RpcMgr
	remoteCache  *RemoteBufferCache
	localHistory *LocalReadHistory

	rpcClient   *RpcClientV2
	replyClient *RpcReplyClient

	fs *ObjcacheFileSystem // for Rejuvenate...

	s *grpc.Server
	api.UnimplementedObjcacheApiServer
}

/******************************
 * Methods to initialize a Node
 ******************************/

func (n *NodeServer) StartGrpcServer() error {
	listenAddr := fmt.Sprintf("%s:%d", n.args.ListenIp, n.args.ApiPort)
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Errorf("Failed: StartGrpcServer, Listen, listenAddr=%v, err=%v", listenAddr, err)
		return err
	}
	n.s = grpc.NewServer()
	api.RegisterObjcacheApiServer(n.s, n)
	go func() {
		if err2 := n.s.Serve(lis); err2 != nil {
			log.Fatalf("Failed: StartGrpcServer, Serve, listenAddr=%v, err=%v", listenAddr, err2)
		}
	}()
	return err
}

func ProfilerThread(blockProfileRate int, mutexProfileRate int, listenIp string, profilePort int) {
	runtime.SetBlockProfileRate(blockProfileRate)
	runtime.SetMutexProfileFraction(mutexProfileRate)
	common.StdouterrLogger.Println(http.ListenAndServe(fmt.Sprintf("%s:%d", listenIp, profilePort), nil))
}

var SectorSize = 512

func NewNodeServer(back *ObjCacheBackend, args *common.ObjcacheCmdlineArgs, flags *common.ObjcacheConfig) *NodeServer {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, unix.SIGTERM, unix.SIGINT)
	if args.ProfilePort > 0 {
		go ProfilerThread(flags.BlockProfileRate, flags.MutexProfileRate, args.ListenIp, args.ProfilePort)
	}
	if args.ClientMode {
		args.RaftGroupId = "client"
	}

	SectorSize = int(PageSize) // difficult to get precise size (using df --output=source . and /sys/block/*/queue/hw_sector is too simple for LVM setting)

	server := NodeServer{args: args, flags: flags, signalChan: signalChan}
	if err := server.Init(back); err != nil {
		return nil
	}
	return &server
}

func (n *NodeServer) WaitShutdown() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			recv := <-n.signalChan
			if recv == unix.SIGTERM {
				if n.fs != nil {
					log.Infof("Received %v, attempting to unmount...", recv)
					if err := n.fs.Reset(true); err != nil {
						log.Warnf("Failed: WaitShutdown, fs.Reset, err=%v", err)
					}
					fuse.Unmount(n.args.MountPoint)
				}
				n.Shutdown(true)
				break
			} else {
				log.Debugf("Ignore signal, recv=%v", recv)
			}
		}
		wg.Done()
	}()
	wg.Wait()
	signal.Reset(unix.SIGTERM)
}

func (n *NodeServer) Init(back *ObjCacheBackend) error {
	InitMemoryPool()
	InitAccessLinkHead()

	n.remoteCache = NewRemotePageBufferCache(n.flags.RemoteCacheSizeBytes)
	n.localHistory = NewLocalBufferCacheHistory()
	rpcClient, err := NewRpcClientV2(n.flags.IfName)
	if err != nil {
		log.Errorf("Failed: NodeServer.Init, NewRpcClientV2, err=%v", err)
		return err
	}
	replyClient, err := NewRpcReplyClient()
	if err != nil {
		log.Errorf("Failed: NodeServer.Init, NewRpcReplyClient, err=%v", err)
		return err
	}
	n.rpcClient = rpcClient
	n.replyClient = replyClient
	n.raftGroup = NewRaftGroupMgr(n.args.RaftGroupId, 1)
	n.txMgr = NewTxMgr()
	n.dirtyMgr = NewDirtyMgr()

	var logLength uint64
	n.raft, logLength = NewRaftInstance(n)
	if n.raft == nil {
		log.Error("Failed: NodeServer.Init, NewRaftInstance")
		return unix.EINVAL
	}
	n.selfNode = RaftNode{nodeId: n.raft.selfId, addr: n.raft.selfAddr, groupId: n.raftGroup.selfGroup}
	n.nodeCond = sync.NewCond(new(sync.Mutex))
	n.suspending = 0
	n.runOps = 0
	n.rpcMgr = NewRpcManager(n)
	n.inodeMgr = NewInodeMgr(back, n.raft, n.flags)
	n.raft.Init(n.args.PassiveRaftInit)
	n.restarted = logLength > 1

	if !n.args.ClientMode {
		if !n.restarted {
			if !n.args.PassiveRaftInit {
				log.Infof("NodeServer.Init, this is the very first initialization.")
				if reply := n.raft.AppendBootstrapLogs(n.raftGroup.selfGroup); reply != RaftReplyOk {
					log.Errorf("Failed: NodeServer.Init, AppendBootstrapLogs, reply=%v", reply)
					return ReplyToFuseErr(reply)
				}
			}
		} else {
			log.Infof("NodeServer.Init, log is detected at disk. restart from Index=%v", logLength-1)
			if reply := n.raft.ReplayAll(); reply != RaftReplyOk {
				log.Errorf("Failed: NodeServer.Init, ReplayAll, reply=%v", reply)
				return ReplyToFuseErr(reply)
			}
		}
		cacheCapacityBytes, err := resource.ParseQuantity(n.args.CacheCapacity)
		if err != nil {
			log.Errorf("Failed: NodeServer.Init, FromHumanSize, CacheCapacity=%v, err=%v", n.args.CacheCapacity, err)
			return err
		}
		go n.FlusherThread()
		go n.EvictionThread(cacheCapacityBytes.Value())
	}
	if err := n.StartGrpcServer(); err != nil {
		return err
	}
	maxCpus := runtime.NumCPU()
	maxProcesses := runtime.GOMAXPROCS(0)
	log.Infof("Success: NodeServer.Init, addr=%v, maxCpus=%v, GOMAXPROCS=%v", n.selfNode, maxCpus, maxProcesses)
	n.shutdown = 0
	return nil
}

func (n *NodeServer) SetFs(fs *ObjcacheFileSystem) {
	n.fs = fs
}

func (n *NodeServer) IsReady(_ context.Context, _ *api.Void) (*api.ApiRet, error) {
	if n == nil {
		return &api.ApiRet{Status: RaftReplyRetry}, nil
	}
	return &api.ApiRet{Status: RaftReplyOk}, nil
}

func (n *NodeServer) getDataDir() string {
	return n.args.RootDir + "/data"
}

func (n *NodeServer) getStateFile() string {
	return n.args.RootDir + "/state"
}

/*****************************************************************
 * methods to block all the operations during migration/compaction
 *****************************************************************/

func (n *NodeServer) TryBeginOp() bool {
	n.nodeCond.L.Lock()
	ret := n.suspending == 0
	if ret {
		n.runOps += 1
	}
	n.nodeCond.L.Unlock()
	return ret
}

func (n *NodeServer) EndOp() {
	n.nodeCond.L.Lock()
	n.runOps -= 1
	if n.runOps < 0 {
		log.Warnf("BUG: EndOp, runOps (%v) < 0", n.runOps)
	}
	n.nodeCond.Broadcast()
	n.nodeCond.L.Unlock()
}

func (n *NodeServer) LockNode() {
	n.nodeCond.L.Lock()
	for n.suspending > 0 || n.runOps > 0 {
		n.nodeCond.Wait()
	}
	n.suspending += 1
	n.nodeCond.L.Unlock()
}

func (n *NodeServer) TryLockNode() bool {
	n.nodeCond.L.Lock()
	ret := n.suspending == 0 && n.runOps == 0
	if ret {
		n.suspending += 1
	}
	n.nodeCond.L.Unlock()
	return ret
}

func (n *NodeServer) UnlockNode() {
	n.nodeCond.L.Lock()
	old := n.suspending
	n.suspending = old - 1
	n.nodeCond.Broadcast()
	n.nodeCond.L.Unlock()
}

func (n *NodeServer) WaitNodeUnlocked() {
	n.nodeCond.L.Lock()
	for n.suspending > 0 || n.runOps > 0 {
		n.nodeCond.Wait()
	}
	n.nodeCond.L.Unlock()
}

/*******************************************
 * methods to read inodes
 *******************************************/

func (n *NodeServer) GetMeta(inodeKey InodeKeyType, key string, parent InodeKeyType) (*WorkingMeta, map[string]InodeKeyType, error) {
	ret, r := n.rpcMgr.CallRpc(NewGetMetaOp(inodeKey, key, n.flags.ChunkSizeBytes, n.flags.DirtyExpireIntervalMs, parent).GetCaller(n))
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: GetMeta, inodeKey=%v, key=%v, reply=%v", inodeKey, key, r.reply)
		return nil, nil, ReplyToFuseErr(r.reply)
	}
	ret2 := ret.ext.(*GetWorkingMetaRet)
	return ret2.meta, ret2.children, nil
}

func (n *NodeServer) ReadChunk(h MetaRWHandler, offset int64, size int, blocking bool) (bufs []SlicedPageBuffer, count int, err error) {
	if h.size == 0 {
		return nil, 0, nil
	}
	ret, r := n.rpcMgr.CallRpc(NewReadChunkOp(h, offset, size, blocking, false).GetCaller(n))
	if r.reply != RaftReplyOk {
		return nil, 0, ReplyToFuseErr(r.reply)
	}
	ret2 := ret.ext.(ReadChunkOpRet)
	return ret2.bufs, ret2.count, nil
}

func (n *NodeServer) ReadAheadChunk(h MetaRWHandler, offset int64, size int64) {
	if n.flags.DisableLocalWritebackCaching {
		return
	}
	if offset+size > h.size {
		size = h.size - offset
	}
	for off := offset; off < offset+size && off < h.size; {
		alignedOffset := off - off%h.chunkSize
		length := int(offset + size - alignedOffset)
		if length > int(h.chunkSize) {
			length = int(h.chunkSize)
		}
		n.rpcMgr.CallRpc(NewReadChunkOp(h, alignedOffset, length, false, true).GetCaller(n))
		off = alignedOffset + int64(length)
	}
}

func (n *NodeServer) PrefetchChunk(h MetaRWHandler, offset int64, length int64) {
	groups := make(map[string]bool)
	l := n.raftGroup.GetNodeListLocal()
	for off := offset - offset%h.chunkSize; off < offset+length && off < h.size; off += h.chunkSize {
		if _, ok := n.localHistory.Has(h.inodeKey, off); ok {
			continue
		}
		if n.remoteCache.Has(h.inodeKey, off) {
			continue
		}
		groupId, ok := GetGroupForChunk(l.ring, h.inodeKey, off, h.chunkSize)
		if !ok {
			log.Errorf("Failed: PrefetchChunk, GetGroupForChunk, l.version=%v, inodeKey=%v, off=%v", l.version, h.inodeKey, off)
			return
		}
		groups[groupId] = true
		//log.Debugf("PrefetchChunk, need inodeKey=%v, off=%v", h.inodeKey, off)
	}
	for groupId := range groups {
		leader, ok := n.raftGroup.GetGroupLeader(groupId, l)
		if ok {
			go n.rpcMgr.CallRpc(NewPrefetchChunkOp(leader, offset, length, h, l.version).GetCaller(n))
		} else {
			log.Errorf("Failed: PrefetchChunk, GetGroupLeader, l.version=%v, inodeKey=%v, groupId=%v", l.version, h.inodeKey, groupId)
		}
	}
}

/*************************
 * methods to write inodes
 *************************/

func (n *NodeServer) WriteChunk(inodeKey InodeKeyType, chunkSize int64, offset int64, size int64, buf []byte) ([]*common.UpdateChunkRecordMsg, int32) {
	begin := time.Now()
	id := n.raft.GenerateCoordinatorId()
	groupRets := map[string][]*common.StagingChunkMsg{}
	var bufOff = int64(0)
	var i = uint64(1)
	for off := offset; off < offset+size; {
		slop := off % chunkSize
		var dataLen = size
		if dataLen > chunkSize-slop {
			dataLen = chunkSize - slop
		}
		if dataLen > n.flags.RpcChunkSizeBytes {
			dataLen = n.flags.RpcChunkSizeBytes
		}
		if bufOff+dataLen > int64(len(buf)) {
			dataLen = int64(len(buf)) - bufOff
		}
		txId := id.toTxId(i)
		op, r := n.rpcMgr.CallRpc(NewUpdateChunkOp(txId, inodeKey, chunkSize, off, buf[bufOff:bufOff+dataLen]).GetCaller(n))
		if _, ok := groupRets[op.node.groupId]; !ok {
			groupRets[op.node.groupId] = make([]*common.StagingChunkMsg, 0)
		}
		if op.ext != nil {
			groupRets[op.node.groupId] = append(groupRets[op.node.groupId], op.ext.(*common.StagingChunkMsg))
		}
		if r.reply != RaftReplyOk {
			ret := make([]*common.UpdateChunkRecordMsg, 0)
			for groupId, stags := range groupRets {
				ret = append(ret, &common.UpdateChunkRecordMsg{GroupId: groupId, Stags: stags})
			}
			log.Debugf("Failed: WriteChunk, inodeKey=%v, offset=%v, size=%v, elapsed=%v, r=%v", inodeKey, offset, size, time.Since(begin), r)
			return ret, r.reply
		}
		off += dataLen
		bufOff += dataLen
		i += 1
	}
	ret := make([]*common.UpdateChunkRecordMsg, 0)
	for groupId, stags := range groupRets {
		ret = append(ret, &common.UpdateChunkRecordMsg{GroupId: groupId, Stags: stags})
	}
	log.Debugf("Success: WriteChunk, inodeKey=%v, offset=%v, size=%v, i=%v, elapsed=%v", inodeKey, offset, size, i, time.Since(begin))
	return ret, RaftReplyOk
}

/*********************************************************
 * methods to persist cached files to cloud object storage
 *********************************************************/

func (n *NodeServer) persistMultipleChunksMeta(tm *TxIdMgr, metaKeys []string, meta *WorkingMeta, priority int) (ts int64, chunks []*common.PersistedChunkInfoMsg, reply int32) {
	chunks = make([]*common.PersistedChunkInfoMsg, 0)
	keyUploadIds := make(map[string]string)
	uploadIds := make([]string, 0)

	begin := time.Now()
	var startMpu, mpuAdd, commit time.Time
	var keyETags map[string][]string
	var lastErrReply = RaftReplyOk
	ctxs := make([]MpuContext, 0)
	var localOps []MpuAddOp = nil
	nodeList := n.raftGroup.GetNodeListLocal()

	for _, metaKey := range metaKeys {
		var uploadId string
		uploadId, lastErrReply = n.inodeMgr.MpuBegin(metaKey)
		if lastErrReply != RaftReplyOk {
			log.Errorf("Failed: persistMultipleChunksMeta, MpuBegin, name=%v, reply=%v", metaKey, lastErrReply)
			goto mpuAbort
		}
		uploadIds = append(uploadIds, uploadId)
		keyUploadIds[metaKey] = uploadId
	}
	if lastErrReply = n.raft.AppendExtendedLogEntry(NewBeginPersistCommand(tm.GetNextId(), metaKeys, uploadIds)); lastErrReply != RaftReplyOk {
		log.Errorf("Failed: persistMultipleChunksMeta, AppendBeginPersistLog, keys=%v, reply=%v", metaKeys, lastErrReply)
		goto mpuAbort
	}
	startMpu = time.Now()

	keyETags = make(map[string][]string)
	for metaKey, uploadId := range keyUploadIds {
		keyETags[metaKey] = make([]string, (meta.size+meta.chunkSize-1)/meta.chunkSize)
		offsets := make(map[RaftNode][]int64)
		for off := int64(0); off < meta.size; off += meta.chunkSize {
			node, ok := n.raftGroup.getChunkOwnerNodeLocalNew(nodeList, meta.inodeKey, off, meta.chunkSize)
			if !ok {
				log.Errorf("Failed: persistMultipleChunksMeta, getChunkOwnerNodeLocalNew, group not found, inodeKey=%v, offset=%v", meta.inodeKey, off)
				goto mpuAbort
			}
			if _, ok := offsets[node]; !ok {
				offsets[node] = nil
			}
			offsets[node] = append(offsets[node], off)
			if len(offsets[node]) > 1024 {
				offs := make([]int64, len(offsets[node]))
				copy(offs, offsets[node])
				op := NewMpuAddOp(node, nodeList.version, tm.GetNextId(), metaKey, meta, offs, uploadId, priority)
				if node.nodeId == n.raft.selfId {
					localOps = append(localOps, op)
				} else {
					ctx, r := op.remoteAsync(n)
					if r == RaftReplyOk {
						ctxs = append(ctxs, ctx)
					}
				}
				delete(offsets, node)
			}
		}
		for node, offsets := range offsets {
			op := NewMpuAddOp(node, nodeList.version, tm.GetNextId(), metaKey, meta, offsets, uploadId, priority)
			if node.nodeId == n.raft.selfId {
				localOps = append(localOps, op)
			} else {
				ctx, r := op.remoteAsync(n)
				if r == RaftReplyOk {
					ctxs = append(ctxs, ctx)
				}
			}
		}
		lastErrReply = RaftReplyOk
		for _, localOp := range localOps {
			rpcRet, r := n.rpcMgr.CallRpc(localOp.GetCaller(n))
			lastErrReply = r.reply
			if r.reply == RaftReplyOk {
				outs := rpcRet.ext.([]MpuAddOut)
				c := &common.PersistedChunkInfoMsg{
					TxId: localOp.txId.toMsg(), GroupId: n.raftGroup.selfGroup,
					Offsets: make([]int64, len(outs)), CVers: make([]uint32, len(outs)),
				}
				for i, out := range outs {
					keyETags[metaKey][out.idx] = out.etag
					c.Offsets[i] = int64(out.idx) * meta.chunkSize
					c.CVers[i] = out.cVer
				}
				chunks = append(chunks, c)
				tm.AddTx(localOp.txId, rpcRet)
			}
		}
		for _, ctx := range ctxs {
			outs, r := ctx.WaitRet(n)
			lastErrReply = r.reply
			if r.reply == RaftReplyOk {
				c := &common.PersistedChunkInfoMsg{
					TxId: ctx.txId.toMsg(), GroupId: ctx.node.groupId,
					Offsets: make([]int64, len(outs)), CVers: make([]uint32, len(outs)),
				}
				for i, out := range outs {
					keyETags[metaKey][out.idx] = out.etag
					c.Offsets[i] = int64(out.idx) * meta.chunkSize
					c.CVers[i] = out.cVer
				}
				chunks = append(chunks, c)
				tm.AddTx(ctx.txId, RpcRet{node: ctx.node})
			}
		}
		if lastErrReply != RaftReplyOk {
			goto mpuAbort
		}
	}
	mpuAdd = time.Now()

	for metaKey, uploadId := range keyUploadIds {
		ts, lastErrReply = n.inodeMgr.MpuCommit(metaKey, uploadId, keyETags[metaKey])
		if lastErrReply != RaftReplyOk {
			log.Errorf("Failed: persistMultipleChunksMeta, MpuCommit, reply=%v", lastErrReply)
			goto mpuAbort
		}
		commit = time.Now()
		log.Debugf("Success: persistMultipleChunksMeta, metaKey=%v, inodeKey=%v, length=%v, mpubegin=%v, mpuadd=%v, commit=%v",
			metaKey, meta.inodeKey, meta.size, startMpu.Sub(begin), mpuAdd.Sub(startMpu), commit.Sub(mpuAdd))
	}
	return ts, chunks, RaftReplyOk
mpuAbort:
	for metaKey, uploadId := range keyUploadIds {
		n.inodeMgr.MpuAbort(metaKey, uploadId)
	}
	return ts, nil, lastErrReply
}

func (n *NodeServer) flushExpired() {
	if r := n.raft.IsLeader(); r.reply != RaftReplyOk {
		return
	}
	dirtyKeys := n.dirtyMgr.CopyAllExpiredPrimaryDirtyMeta()
	for _, inodeKey := range dirtyKeys {
		log.Debugf("flushExpired: Start flushing inodeKey=%v", inodeKey)
		_, r := n.rpcMgr.CallRpc(NewCoordinatorPersistOp(n.raft.GenerateCoordinatorId(), inodeKey, 0).GetCaller(n))
		if r.reply != RaftReplyOk {
			break
		}
	}
	deletedKeys := n.dirtyMgr.CopyAllExpiredPrimaryDeletedDirtyMeta()
	for key, inodeKey := range deletedKeys {
		log.Debugf("flushExpired: Start flushing deleted inodeKey=%v", inodeKey)
		_, r := n.rpcMgr.CallRpc(NewCoordinatorDeletePersistOp(n.raft.GenerateCoordinatorId(), key, inodeKey, 0).GetCaller(n))
		if r.reply != RaftReplyOk {
			break
		}
	}
}

func (n *NodeServer) dropChunkMetaCache() int32 {
	n.localHistory.DropAll()
	n.remoteCache.DropAll()
	n.inodeMgr.Clean(n.dirtyMgr)
	current := n.raft.extLogger.GetDiskUsage()
	n.inodeMgr.inodeLock.RLock()
	chunks := make([]*Chunk, 0)
	inodeKeys := make([]uint64, 0)
	offsets := make([]int64, 0)
	for inodeKey, inode := range n.inodeMgr.inodes {
		inode.chunkLock.RLock()
		for off, chunk := range inode.chunkMap {
			if n.dirtyMgr.IsDirtyChunk(chunk) {
				log.Warnf("dropChunkMetaCache, chunk is dirty, inodeKey=%v, offset=%v", inodeKey, off)
				continue
			}
			chunks = append(chunks, chunk)
			inodeKeys = append(inodeKeys, uint64(inodeKey))
			offsets = append(offsets, off)
			chunk.lock.Lock()
		}
		inode.chunkLock.RUnlock()
	}
	n.inodeMgr.inodeLock.RUnlock()
	if len(chunks) > 0 {
		reply := n.raft.AppendExtendedLogEntry(NewDropLRUChunksCommand(inodeKeys, offsets))
		if reply != RaftReplyOk {
			log.Errorf("Failed: dropChunkMetaCache, AppendExtendedLogEntry, keys=%v, offsets=%v, reply=%v", inodeKeys, offsets, reply)
			return reply
		}
		for _, c := range chunks {
			c.lock.Unlock()
		}
	}
	n.raft.extLogger.Clear()
	updated := n.raft.extLogger.GetDiskUsage()
	log.Infof("Success: dropChunkMetaCache, prev=%v, now=%v, size=%v", current, updated, current-updated)
	return RaftReplyOk
}

func (n *NodeServer) DropCache(_ context.Context, _ *api.Void) (*api.ApiRet, error) {
	r := n.raft.IsLeader()
	r.leaderAddr.Port = n.args.ApiPort
	if r.reply != RaftReplyOk {
		return &api.ApiRet{Status: r.reply}, nil
	}
	n.LockNode()
	if n.fs != nil {
		if err := n.fs.Reset(false); err != nil {
			log.Errorf("Failed: DropCache, fs.Reset, err=%v", err)
			return &api.ApiRet{Status: ErrnoToReply(err)}, nil
		}
	}
	if r.reply = n.dirtyMgr.AppendForgetAllDirtyLog(n.raft); r.reply != RaftReplyOk {
		log.Errorf("Failed: DropCache, AppendForgetAllDirtyLog, reply=%v", r.reply)
		n.UnlockNode()
		if n.fs != nil {
			n.fs.EndReset() // unblock FS operations
		}
		return &api.ApiRet{Status: r.reply}, nil
	}
	reply := n.dropChunkMetaCache()
	if reply == RaftReplyOk {
		log.Infof("Success: DropCache")
	}
	if n.fs != nil {
		n.fs.EndReset() // unblock FS operations
	}
	n.UnlockNode()
	return &api.ApiRet{Status: reply}, nil
}

func (n *NodeServer) dropLRUCacheOnDisk(cacheCapacityBytes int64) {
	if r := n.raft.IsLeader(); r.reply != RaftReplyOk {
		return
	}
	current := n.raft.extLogger.GetDiskUsage()
	if current <= cacheCapacityBytes {
		return
	}
	opened := make(map[InodeKeyType]bool)
	if n.fs != nil {
		opened = n.fs.GetOpenInodes()
	}
	targetDropSize := (current - cacheCapacityBytes) * 2
	inodeKeys, offsets := CollectLRUChunks(n.dirtyMgr, n.raft, targetDropSize)
	if len(inodeKeys) == 0 {
		keys := CollectLRUDirtyKeys(n.dirtyMgr, n.raft, targetDropSize)
		falsePositives := make([]uint64, 0)
		for inodeKey := range keys {
			if _, ok := opened[inodeKey]; ok {
				log.Debugf("dropLRUCacheOnDisk: skip flushing opening inodeKey=%v", inodeKey)
				continue
			}
			log.Debugf("dropLRUCacheOnDisk: Start flushing inodeKey=%v", inodeKey)
			ret, r := n.rpcMgr.CallRpc(NewCoordinatorPersistOp(n.raft.GenerateCoordinatorId(), inodeKey, 0).GetCaller(n))
			if r.reply == RaftReplyOk {
				if ret.ext.(*CoordinatorRet).extReply == ObjCacheIsNotDirty {
					falsePositives = append(falsePositives, uint64(inodeKey))
				}
			} else {
				break
			}
		}
		if reply := n.dirtyMgr.AppendRemoveNonDirtyChunksLog(n.raft, falsePositives); reply != RaftReplyOk {
			log.Errorf("Failed (ignore): dropLRUCacheOnDisk, AppendRemoveNonDirtyChunksLog, falsePositives=%v", falsePositives)
		}
		inodeKeys, offsets = CollectLRUChunks(n.dirtyMgr, n.raft, targetDropSize)
		if len(inodeKeys) == 0 {
			return
		}
	}
	chunks := make([]*Chunk, 0)
	n.inodeMgr.inodeLock.RLock()
	for _, key := range inodeKeys {
		inodeKey := InodeKeyType(key)
		inode, ok := n.inodeMgr.inodes[inodeKey]
		if !ok {
			continue
		}
		inode.chunkLock.RLock()
		for _, c := range inode.chunkMap {
			if c.lock.TryLock() {
				chunks = append(chunks, c)
			}
		}
		inode.chunkLock.RUnlock()
	}
	n.inodeMgr.inodeLock.RUnlock()
	reply := n.raft.AppendExtendedLogEntry(NewDropLRUChunksCommand(inodeKeys, offsets))
	if reply != RaftReplyOk {
		log.Errorf("Failed: dropLRUCacheOnDisk, AppendExtendedLogEntry, inodeKeys=%v, offsets=%v, reply=%v", inodeKeys, offsets, reply)
		return
	}
	for _, c := range chunks {
		c.lock.Unlock()
	}
	updated := n.raft.extLogger.GetDiskUsage()
	if updated < current {
		log.Infof("Success: dropLRUCacheOnDisk, prev=%v, now=%v, size=%v", current, updated, current-updated)
	}
}

func (n *NodeServer) FlusherThread() {
	for atomic.LoadInt32(&n.shutdown) == 0 {
		n.flushExpired()
		time.Sleep(n.flags.DirtyFlusherIntervalDuration)
	}
}

func (n *NodeServer) EvictionThread(cacheCapacityBytes int64) {
	for atomic.LoadInt32(&n.shutdown) == 0 {
		n.dropLRUCacheOnDisk(cacheCapacityBytes)
		time.Sleep(n.flags.EvictionIntervalDuration)
	}
}

/************************************************
 * methods to execute RPC
 * Data RPC transfers many bytes utilizing splice
 ************************************************/

func (n *NodeServer) ExecRpcThread(msg RpcMsg, sa common.NodeAddrInet4, fd int) {
	payload := msg.GetCmdPayload()
	rpcId := msg.GetExecProtoBufRpcId(payload)
	seqNum := msg.GetExecProtoBufRpcSeqNum(payload)
	var buffer []SlicedPageBuffer = nil
	var ret proto.Message
	if newFn, ok := SingleShotOpMap[rpcId]; ok {
		fn, nodeListVer, reply := newFn(msg)
		if reply != RaftReplyOk {
			ret, buffer = fn.RetToMsg(nil, RaftBasicReply{reply: reply})
		} else {
			orig, r := fn.GetCaller(n).ExecLocalInRpc(n, nodeListVer)
			ret, buffer = fn.RetToMsg(orig.ext, r)
		}
	} else if newFn, ok := ParticipantOpMap[rpcId]; ok {
		fn, nodeListVer, reply := newFn(msg)
		if reply != RaftReplyOk {
			ret, buffer = fn.RetToMsg(nil, RaftBasicReply{reply: reply})
		} else {
			orig, r := fn.GetCaller(n).ExecLocalInRpc(n, nodeListVer)
			ret, buffer = fn.RetToMsg(orig.ext, r)
		}
	} else if newFn, ok := CoordinatorOpMap[rpcId]; ok {
		fn, nodeListVer, reply := newFn(msg)
		if reply != RaftReplyOk {
			ret = fn.RetToMsg(nil, RaftBasicReply{reply: reply})
		} else {
			orig, r := fn.GetCaller(n).ExecLocalInRpc(n, nodeListVer)
			ret = fn.RetToMsg(orig.ext, r)
		}
	} else if rpcId == RpcRestoreDirtyChunksCmdId || rpcId == RpcUpdateChunkCmdId {
		log.Errorf("BUG (ignore): ExecRpc, rpcId=%v should be handled at ExecDataRpc", rpcId)
		return
	} else {
		log.Errorf("Failed (ignore): ExecRpc, unknown rpcId=%v", rpcId)
		return
	}
	var dataBuf [][]byte = nil
	var count = 0
	for _, b := range buffer {
		count += len(b.Buf)
		dataBuf = append(dataBuf, b.Buf)
	}
	if reply := msg.FillExecProtoBufArgs(rpcId, seqNum, ret, count, true); reply != RaftReplyOk {
		log.Errorf("Failed: ExecRpc, FillExecProtoBufArgs, rpcId=%v, reply=%v", rpcId, reply)
		for _, b := range buffer {
			b.SetEvictable()
		}
		return
	}
	if reply := n.replyClient.ReplyRpcMsg(msg, fd, sa, nil, n.flags.RpcTimeoutDuration, dataBuf); reply != RaftReplyOk {
		log.Errorf("Failed: ExecRpc, ReplyRpcMsg, rpcId=%v, reply=%v", rpcId, reply)
	}
	for _, b := range buffer {
		b.SetEvictable()
	}
	//log.Debugf("Success: ExecRpc, rpcId=%v, seqNum=%v", rpcId, seqNum)
}

func (n *NodeServer) __replyFn(msg RpcMsg, sa common.NodeAddrInet4, fd int, state *ReadRpcMsgState, rpcId uint16, seqNum uint64, m proto.Message, disk *OnDiskLog, vec *DiskWriteVector, logId LogIdType, dataLength uint32) {
	reply := RaftReplyFail
	logOffset := int64(0)
	if disk != nil && vec != nil {
		sizeIncrease, err := disk.EndWrite(vec)
		reply = ErrnoToReply(err)
		if err != nil {
			log.Errorf("Failed: ExecDataRpc, EndWrite, err=%v", err)
		} else {
			logOffset = vec.logOffset
		}
		n.raft.extLogger.PutDiskLog(logId, vec, sizeIncrease)
	}

	var ret proto.Message
	switch rpcId {
	case RpcUpdateChunkCmdId:
		ret = n.rpcMgr.UpdateChunkBottomHalf(m, logId, logOffset, dataLength, reply)
	case RpcRestoreDirtyChunksCmdId:
		ret, _ = n.rpcMgr.RestoreDirtyChunksBottomHalf(m, logId, logOffset, dataLength, reply)
	}
	if reply := msg.FillExecProtoBufArgs(rpcId, seqNum, ret, 0, true); reply != RaftReplyOk {
		log.Errorf("Failed: ExecDataRpc, FillExecProtoBufArgs, rpcId=%v, reply=%v", rpcId, reply)
		state.failed = ReplyToFuseErr(reply)
		return
	}
	if reply := n.replyClient.ReplyRpcMsg(msg, fd, sa, nil, n.flags.RpcTimeoutDuration, nil); reply != RaftReplyOk {
		log.Errorf("Failed: ExecDataRpc, ReplyRpcMsg, rpcId=%v, reply=%v", rpcId, reply)
		state.failed = ReplyToFuseErr(reply)
		return
	}
}

func (n *NodeServer) ExecDataRpc(msg RpcMsg, sa common.NodeAddrInet4, fd int, pipeFds [2]int, state *ReadRpcMsgState) (noData bool) {
	payload := msg.GetCmdPayload()
	rpcId := msg.GetExecProtoBufRpcId(payload)
	noData = rpcId&DataCmdIdBit == 0
	if noData {
		state.completed = true
		return
	}
	var m proto.Message
	var reply int32
	var logId LogIdType
	switch rpcId {
	case RpcUpdateChunkCmdId:
		args := &common.UpdateChunkArgs{}
		if reply = msg.ParseExecProtoBufMessage(args); reply == RaftReplyOk {
			m = args
			logId = NewLogIdTypeFromInodeKey(InodeKeyType(args.GetInodeKey()), args.GetOffset(), args.GetChunkSize())
		}
	case RpcRestoreDirtyChunksCmdId:
		args := &common.RestoreDirtyChunksArgs{}
		if reply = msg.ParseExecProtoBufMessage(args); reply == RaftReplyOk {
			m = args
			logId = NewLogIdTypeFromInodeKey(InodeKeyType(args.GetInodeKey()), args.GetOffset(), args.GetChunkSize())
		}
	default:
		log.Errorf("Failed (ignore): ExecDataRpc, unknown rpcId, rpcId=%v", rpcId)
		noData = true
		state.completed = true
		return
	}

	dataLength, _ := msg.GetOptControlHeader()

	var disk *OnDiskLog
	if reply != RaftReplyOk {
		state.failed = ReplyToFuseErr(reply)
	} else if n.raft.extLogger.IsFreezed(logId) {
		log.Errorf("Failed: ExecDataRpc, logger is snapshotting, fd=%v, dataLength=%v", fd, dataLength)
		state.failed = unix.EROFS
	} else {
		disk = n.raft.extLogger.GetDiskLog(logId, MaxNrCacheForUpdateChunk, MaxNrWriterForUpdateChunk)
		if state.optComplete == 1 && state.vec == nil {
			vec, err := disk.BeginAppendSplice(fd, int64(dataLength))
			if err != nil {
				log.Errorf("Failed: ExecDataRpc, BeginAppendSplice, fd=%v, dataLength=%v, err=%v", fd, dataLength, err)
				state.failed = err
			} else {
				state.vec = vec
			}
		}
		if state.vec != nil {
			err := disk.Splice(state.vec, pipeFds, &state.bufOffForSplice)
			if err != nil {
				log.Errorf("Failed: ExecDataRpc, Splice, err=%v", err)
				state.failed = err
			} else {
				state.completed = uint32(state.bufOffForSplice) >= dataLength
			}
		}
	}

	if !state.completed && state.failed == nil {
		// We still have fragmented messages in the next loop
		return
	}

	seqNum := msg.GetExecProtoBufRpcSeqNum(payload)
	if state.failed != nil {
		// Note: state.failed != nil will forcefully abort the connection later. we do not need to care about unread network buffers here.
		// But we ensure notifying the failure before the abort.
		n.__replyFn(msg, sa, fd, state, rpcId, seqNum, m, disk, state.vec, logId, dataLength)
	} else {
		go n.__replyFn(msg, sa, fd, state, rpcId, seqNum, m, disk, state.vec, logId, dataLength)
	}
	//log.Debugf("Success: ExecDataRpc, RpcUpdateChunk, seqNum=%v, optHeaderLength=%v, dataLength=%v", seqNum, h.optHeaderLength, dataLength)
	return
}

/////////////////////////////////////////////////////////////////////////////////

func (n *NodeServer) RemoveInodeMetadataKey(_ fuseops.InodeID, _ string) error {
	return unix.ENODATA
}

func (n *NodeServer) SetInodeMetadataKey(_ fuseops.InodeID, name string, value []byte, flags uint32) error {
	if flags == 0x1 {
		if name == "user.chunk_size" || name == "user.dirty_expire" {
			return fuse.EEXIST
		}
		return fuse.EINVAL
	} else if flags == 0x2 {
		var args = &common.SetXAttrArgs{ChunkSize: -1, ExpireMs: -1}
		if name == "user.chunk_size" {
			chunkSize, err := strconv.ParseInt(string(value), 10, 64)
			if err != nil {
				log.Errorf("Failed: SetInodeMetadataKey, ParseInt, value=%v, err=%v", value, err)
				return fuse.EINVAL
			}
			args.ChunkSize = chunkSize
		} else if name == "user.dirty_expire" {
			dirtyExpire, err := time.ParseDuration(string(value))
			if err != nil {
				log.Errorf("Failed: SetInodeMetadataKey, ParseDuration, value=%v, err=%v", value, err)
				return fuse.EINVAL
			}
			if dirtyExpire.Milliseconds() > int64(math.MaxInt32) {
				log.Errorf("Failed: SetInodeMetadataKey, user.dirty_expire must be < %d ms", math.MaxInt32)
			}
			if dirtyExpire.Milliseconds() < int64(math.MinInt32) {
				dirtyExpire = time.Millisecond * -1
			}
			args.ExpireMs = int32(dirtyExpire.Milliseconds())
		} else {
			return unix.ENODATA
		}
		//TODO
	}
	return nil
}

func (n *NodeServer) UpdateObjectAttr(inodeKeyType InodeKeyType, mode uint32, ts int64) (meta *WorkingMeta, err error) {
	coordinatorId := n.raft.GenerateCoordinatorId()
	txId := coordinatorId.toCoordinatorTxId()
	ret, r := n.rpcMgr.CallRpc(NewUpdateMetaAttrOp(txId, inodeKeyType, mode, ts).GetCaller(n))
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: UpdateObjectAttr, CallAny, inodeKeyType=%v, reply=%v", inodeKeyType, r.reply)
	} else {
		log.Debugf("Success: UpdateObjectAttr, inodeKeyType=%v", inodeKeyType)
	}
	return ret.ext.(*WorkingMeta), ReplyToFuseErr(r.reply)
}

func (n *NodeServer) TruncateObject(inodeId fuseops.InodeID, size int64) (meta *WorkingMeta, err error) {
	inodeKey := InodeKeyType(inodeId)
	ret, r := n.rpcMgr.CallRpc(NewCoordinatorTruncateObjectOp(n.raft.GenerateCoordinatorId(), inodeKey, size).GetCaller(n))
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: TruncateObject, inodeKey=%v, size=%v, r=%v", inodeKey, size, r)
		return nil, ReplyToFuseErr(r.reply)
	}
	log.Debugf("Success: TruncateObject, inodeKey=%v, size=%v", inodeKey, size)
	return ret.ext.(*CoordinatorRet).meta, ReplyToFuseErr(r.reply)
}

func (n *NodeServer) UnlinkObject(parentFullPath string, parentId fuseops.InodeID, name string, childKey InodeKeyType) (err error) {
	// NOTE: childId and childIsDir are required to make coordinator idempotent (== "declare that an inode is now deleted (do not care if the inode exists right now)")
	parentKey := InodeKeyType(parentId)
	_, r := n.rpcMgr.CallRpc(NewCoordinatorDeleteObjectOp(n.raft.GenerateCoordinatorId(), parentFullPath, parentKey, name, childKey).GetCaller(n))
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: UnlinkObject, CallCoordinator, parentKey=%v, name=%v, r=%v", parentKey, name, r)
	} else {
		log.Debugf("Success: UnlinkObject, parentKey=%v, name=%v", parentKey, name)
	}
	return ReplyToFuseErr(r.reply)
}

func (n *NodeServer) HardLinkObject(srcInodeId fuseops.InodeID, srcParent InodeKeyType, dstParentKey string, dstParentInodeId fuseops.InodeID, dstName string, childInodeKey InodeKeyType) (meta *WorkingMeta, err error) {
	// NOTE: childId and childIsDir are required to make coordinator idempotent
	srcInodeKey := InodeKeyType(srcInodeId)
	dstParentInodeKey := InodeKeyType(dstParentInodeId)
	ret, r := n.rpcMgr.CallRpc(NewCoordinatorHardLinkOp(n.raft.GenerateCoordinatorId(), srcInodeKey, srcParent, dstParentKey, dstParentInodeKey, dstName, childInodeKey).GetCaller(n))
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: HardLinkObject, CallCoordinator, srcInodeId=%v, dstParentInodeId=%v, dstName=%v, r=%v", srcInodeKey, dstParentInodeKey, dstName, r)
		return nil, ReplyToFuseErr(r.reply)
	}
	log.Errorf("Success: HardLinkObject, srcInodeId=%v, dstParentInodeId=%v, dstName=%v", srcInodeKey, dstParentInodeKey, dstName)
	return ret.ext.(*CoordinatorRet).meta, ReplyToFuseErr(r.reply)
}

func (n *NodeServer) RenameObject(srcParentKey string, srcParent InodeKeyType, dstParentKey string, dstParentId fuseops.InodeID, srcName string, dstName string, childInodeKey InodeKeyType) (err error) {
	// NOTE: childId and childIsDir are required to make coordinator idempotent
	dstParentInodeKey := InodeKeyType(dstParentId)
	_, r := n.rpcMgr.CallRpc(NewCoordinatorRenameObjectOp(n.raft.GenerateCoordinatorId(), srcParentKey, srcParent, srcName, dstParentKey, dstParentInodeKey, dstName, childInodeKey).GetCaller(n))
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: RenameObject, CallCoordinator, srcParentKey=%v, srcName=%v, dstParentKey=%v, dstName=%v, r=%v",
			srcParentKey, srcName, dstParentKey, dstName, r)
	} else {
		log.Debugf("Success: RenameObject, srcParentKey=%v, srcName=%v, dstParentKey=%v, dstName=%v",
			srcParentKey, srcName, dstParentKey, dstName)
	}
	return ReplyToFuseErr(r.reply)
}

func (n *NodeServer) CreateObject(parentKey string, parentInodeKey InodeKeyType, name string, childInodeKey InodeKeyType, mode uint32) (meta *WorkingMeta, err error) {
	ret, r := n.rpcMgr.CallRpc(NewCoordinatorCreateObjectOp(n.raft.GenerateCoordinatorId(), parentKey, parentInodeKey, name, childInodeKey, mode, n.flags.ChunkSizeBytes, n.flags.DirtyExpireIntervalMs).GetCaller(n))
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: CreateObject, CallCoordinator, parentKey=%v, name=%v, r=%v", parentInodeKey, name, r)
		return nil, ReplyToFuseErr(r.reply)
	}
	log.Debugf("Success: CreateObject, parentKey=%v, name=%v, inodeKey=%v", parentInodeKey, name, childInodeKey)
	return ret.ext.(*CoordinatorRet).meta, ReplyToFuseErr(r.reply)
}

func (n *NodeServer) FlushObject(inodeKey InodeKeyType, records []*common.UpdateChunkRecordMsg, mTime int64, mode uint32) (meta *WorkingMeta, err error) {
	begin := time.Now()
	ret, r := n.rpcMgr.CallRpc(NewCoordinatorFlushObjectOp(n.raft.GenerateCoordinatorId(), inodeKey, records, mTime, mode).GetCaller(n))
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: FlushObject, CallCoordinator, inodeKey=%v, r=%v", inodeKey, r)
		return nil, ReplyToFuseErr(r.reply)
	}
	log.Debugf("Success: FlushObject, inodeKey=%v, len(records)=%v, elapsed=%v", inodeKey, len(records), time.Since(begin))
	return ret.ext.(*CoordinatorRet).meta, ReplyToFuseErr(r.reply)
}

func (n *NodeServer) AbortWriteObject(recordMap map[string][]*common.StagingChunkMsg) {
	// Abort is directly requested from client. This means failures at client loose target TxIds to be aborted.
	// We cannot reclaim disk space for the non-aborted staging chunks, but no h corruption in the FS.
	// Note that client roles are taken by some participant servers in a cluster, so, our end-users, FS
	// applications in this case, cannot intentionally cause disk leakages by exploiting this procedure.
	id := n.raft.GenerateCoordinatorId()
	txId := id.toTxId(1)
	for groupId, stags := range recordMap {
		txIds := make([]*common.TxIdMsg, 0)
		for _, stag := range stags {
			txIds = append(txIds, stag.GetTxId())
		}
		ret, _ := n.rpcMgr.CallRpc(NewAbortParticipantOp(txId, txIds, groupId, false).GetCaller(n))
		txId = txId.GetNext()
		if ret.unlock != nil {
			ret.unlock(n)
		}
	}
}

func (n *NodeServer) PersistObject(inodeId fuseops.InodeID) (meta *WorkingMeta, err error) {
	inodeKey := InodeKeyType(inodeId)
	ret, r := n.rpcMgr.CallRpc(NewCoordinatorPersistOp(n.raft.GenerateCoordinatorId(), inodeKey, 1000).GetCaller(n))
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: PersistObject, inodeKey=%v, r=%v", inodeKey, r)
		return nil, ReplyToFuseErr(r.reply)
	}
	log.Debugf("Success: PersistObject, inodeKey=%v", inodeKey)
	return ret.ext.(*CoordinatorRet).meta, ReplyToFuseErr(r.reply)
}

/**************************************************
 * methods to migrate data at node addition/removal
 **************************************************/

func (n *NodeServer) sendDirtyChunkAny(migrationId MigrationId, nodeListVer uint64, target RaftNode, inodeKey InodeKeyType, offset int64, version uint32, chunkSize int64, objectSize int64, rpcChunkSize int64) int32 {
	chunk := n.inodeMgr.GetChunk(inodeKey, offset, chunkSize)
	chunk.lock.RLock()
	working, err := chunk.GetWorkingChunk(version, false)
	if err != nil {
		chunk.lock.RUnlock()
		log.Errorf("Failed: sendDirtyChunkAny, GetWorkingChunk, inodeKey=%v, offset=%v, version=%v, not found", inodeKey, offset, version)
		return ErrnoToReply(err)
	}
	slices := make([]SlicedPageBuffer, 0)
	var count = int64(0)
	for count < chunkSize && offset+count < objectSize {
		length := objectSize - offset - count
		if length > chunkSize {
			length = chunkSize
		}
		reader := working.GetReader(chunkSize, objectSize, offset+count, n.inodeMgr, true)
		bs, c, err := reader.GetBufferZeroCopy(int(length))
		reader.Close()
		if err != nil {
			log.Errorf("Failed: sendDirtyChunkAny, GetBufferZeroCopy, err=%v", err)
			chunk.lock.RUnlock()
			for _, slice := range slices {
				slice.SetEvictable()
			}
			return ErrnoToReply(err)
		}
		count += int64(c)
		slices = append(slices, bs...)
	}
	chunk.lock.RUnlock()
	// we do not need to send fetchKey here since all the content is already fetched and random writes later will need it neither

	bufs := make([][]byte, 0)
	for _, slice := range slices {
		bufs = append(bufs, slice.Buf)
	}
	_, r := n.rpcMgr.CallRpc(NewRestoreDirtyChunkOp(migrationId, target, inodeKey, chunkSize, offset, objectSize, working.chunkVer, bufs).GetCaller(n))
	for _, slice := range slices {
		slice.SetEvictable()
	}
	if r.reply != RaftReplyOk {
		log.Error("Failed: sendDirtyChunkAny, CallRemote(RestoreDirtyChunksBottomHalf)")
		return r.reply
	}
	log.Debugf("Success: sendDirtyChunkAny, inodeKey=%v, offset=%v, chunkVer=%v, count=%v", inodeKey, offset, working.chunkVer, count)
	return RaftReplyOk
}

/**************************************
 * methods to update cluster membership
 **************************************/

func (n *NodeServer) GetConfig(context.Context, *api.Void) (*api.GetConfigRet, error) {
	r := n.raft.IsLeader()
	r.leaderAddr.Port = n.args.ApiPort
	ret := &api.GetConfigRet{Status: r.reply, LeaderIp: r.leaderAddr.Addr[:], LeaderPort: int32(r.leaderAddr.Port)}
	if r.reply != RaftReplyOk {
		log.Errorf("Rejected: GetConfig, IsLeader, reply=%v", r.reply)
		return ret, nil
	}
	f, err := os.Open(n.args.ConfigFile)
	if err != nil {
		log.Errorf("Failed: GetConfig, yamlFile=%v, err=%v", n.args.ConfigFile, err)
		return ret, nil
	}
	buf, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		log.Errorf("Failed: GetConfig, ReadAll, err=%v", err)
		return ret, nil
	}
	ret.ConfigBytes = buf
	return ret, nil
}

func (n *NodeServer) GetTrackerNode(context.Context, *api.Void) (*api.GetTrackerNodeRet, error) {
	r := n.raft.IsLeader()
	ret := &api.GetTrackerNodeRet{Status: r.reply, Leader: r.GetApiNodeMsg(n.raftGroup.selfGroup)}
	if r.reply != RaftReplyOk {
		log.Errorf("Rejected: GetTrackerNode, IsLeader, reply=%v", r.reply)
		return ret, nil
	}
	leader, nodeList, reply := n.raftGroup.getMetaOwnerNodeLocal(TrackerNodeKey)
	ret.Status = reply
	ret.Leader = leader.toApiMsg()
	if reply != RaftReplyOk {
		log.Errorf("Rejected: GetTrackerNode, getOwnerNodeLocalSync, r=%v", reply)
		return ret, nil
	}
	ret.NodeListVer = nodeList.version
	ret.Nodes = make([]*api.ApiNodeMsg, 0)
	for gId := range nodeList.groupNode {
		if no, ok := n.raftGroup.GetGroupLeader(gId, nodeList); ok {
			ret.Nodes = append(ret.Nodes, no.toApiMsg())
		}
	}
	return ret, nil
}

func (n *NodeServer) getTrackerNodeRemote(randomAddr common.NodeAddrInet4) (leader RaftNode, nodes []*api.ApiNodeMsg, nodeListVer uint64, reply int32) {
	addr := randomAddr.String()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Errorf("Failed: getServerConfig, Dial, addr=%v, err=%v", addr, err)
		return RaftNode{}, nil, 0, RaftReplyRetry
	}
	client := api.NewObjcacheApiClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), n.flags.RpcTimeoutDuration)
	msg, err := client.GetTrackerNode(ctx, &api.Void{})
	cancel()
	_ = conn.Close()
	if err != nil {
		log.Errorf("Failed: getTrackerNodeRemote, GetTrackerNode, randomAddr=%v, err=%v", randomAddr, err)
		return RaftNode{}, nil, 0, RaftReplyRetry
	}
	leader = NewRaftNodeFromApiMsg(msg.GetLeader())
	if msg.GetStatus() != RaftReplyOk {
		log.Errorf("Rejected: getTrackerNodeRemote, getTrackerNodeRemote, reply=%v, leaderHint=%v", msg.GetStatus(), msg.GetLeader())
		if msg.GetStatus() == RaftReplyNotLeader && randomAddr != leader.addr {
			return leader, nil, 0, msg.GetStatus()
		}
		return RaftNode{}, nil, 0, msg.GetStatus()
	}
	log.Debugf("Success: getTrackerNodeRemote, leader=%v, nodeListVer=%v", msg.GetLeader(), msg.GetNodeListVer())
	return leader, msg.GetNodes(), msg.GetNodeListVer(), RaftReplyOk
}

func getServerConfig(randomAddr common.NodeAddrInet4, timeout time.Duration) (common.NodeAddrInet4, *common.ObjcacheConfig, int32) {
	addr := randomAddr.String()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Errorf("Failed: getServerConfig, Dial, addr=%v, err=%v", addr, err)
		return randomAddr, nil, RaftReplyRetry
	}

	client := api.NewObjcacheApiClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	msg, err := client.GetConfig(ctx, &api.Void{})
	cancel()
	_ = conn.Close()
	if err != nil {
		log.Errorf("Failed: getServerConfig, GetConfig, addr=%v, err=%v", addr, err)
		return randomAddr, nil, RaftReplyRetry
	}
	leaderIp := msg.GetLeaderIp()
	leaderPort := msg.GetLeaderPort()
	leader := common.NodeAddrInet4{Addr: [4]byte{leaderIp[0], leaderIp[1], leaderIp[2], leaderIp[3]}, Port: int(leaderPort)}
	if msg.GetStatus() != RaftReplyOk {
		log.Errorf("Rejected: getServerConfig, GetConfig, reply=%v, leader=%v:%v", msg.GetStatus(), leaderIp, leaderPort)
		if msg.GetStatus() == RaftReplyNotLeader && randomAddr != leader {
			return leader, nil, msg.GetStatus()
		}
		return leader, nil, msg.GetStatus()
	}
	if len(msg.GetConfigBytes()) == 0 {
		log.Errorf("Failed: GetServerConfig, GetConfig does not return config")
		return randomAddr, nil, RaftReplyFail
	}
	c, err := common.NewConfigFromByteArray(msg.GetConfigBytes())
	if err != nil {
		log.Errorf("Failed: GetServerConfig, NewConfigFromByteArray, err=%v", err)
		return randomAddr, nil, RaftReplyFail
	}
	log.Debugf("Success: getServerConfig, leader=%v", leader)
	return randomAddr, &c, RaftReplyOk
}

func GetServerConfig(args *common.ObjcacheCmdlineArgs, timeout time.Duration) (common.ObjcacheConfig, error) {
	c := args.GetObjcacheConfig()
	if args.HeadWorkerIp == "" {
		return c, nil
	}
	self, err := GetAddrInet4FromString(args.ExternalIp, args.ApiPort)
	if err != nil {
		log.Errorf("Failed: GetServerConfig, GetAddrInet4FromString, args.ExternalIp=%v, args.ApiPort=%v", args.ExternalIp, args.ApiPort)
		return common.ObjcacheConfig{}, unix.EINVAL
	}
	randomAddr, err := GetAddrInet4FromString(args.HeadWorkerIp, args.HeadWorkerPort)
	if err != nil {
		log.Errorf("Failed: GetServerConfig, GetAddrInet4FromString, args.HeadWorkerIp=%v, args.HeadworkerPort=%v, err=%v", args.HeadWorkerIp, args.HeadWorkerPort, err)
		return common.ObjcacheConfig{}, err
	}
	if self == randomAddr {
		return c, nil
	}
	for {
		nextNode, config, reply := getServerConfig(randomAddr, timeout)
		if reply != RaftReplyOk {
			log.Errorf("Failed: GetServerConfig, getServerConfig, err=%v", reply)
			if reply == RaftReplyNotLeader {
				randomAddr = nextNode
			}
			if needRetry(reply) {
				time.Sleep(time.Millisecond * 10)
				continue
			}
			return common.ObjcacheConfig{}, ReplyToFuseErr(reply)
		}
		log.Infof("Success: GetServerConfig, tracker=%v", randomAddr)
		return c.MergeServerConfig(config), nil
	}
}

func (n *NodeServer) GetApiPort(leader RaftNode) (apiPort int, reply int32) {
	ret, r := n.rpcMgr.CallRpc(NewGetApiIpAndPortOp(leader).GetCaller(n))
	if r.reply != RaftReplyOk {
		return -1, r.reply
	}
	return ret.ext.(int), RaftReplyOk
}

func GetAddrInet4FromString(headWorkerAddr string, headWorkerPort int) (common.NodeAddrInet4, error) {
	ip := net.ParseIP(headWorkerAddr)
	if ip == nil {
		var ips []net.IP
		for i := 0; ; i++ {
			var err error
			ips, err = net.LookupIP(headWorkerAddr)
			if err != nil {
				log.Errorf("Failed: GetAddrInet4FromString, headWorkerAddr=%v, headWorkerPort=%v, attempt=%v, err=%v", headWorkerAddr, headWorkerPort, i, err)
				time.Sleep(time.Second)
				continue
			}
			break
		}
		var found = false
		for _, cip := range ips {
			if cipv4 := cip.To4(); cipv4 != nil {
				ip = cip
				found = true
				break
			}
		}
		if !found {
			log.Errorf("Failed: GetAddrInet4FromString, headWorkerAddr=%v, headWorkerPort=%v", headWorkerAddr, headWorkerPort)
			return common.NodeAddrInet4{}, unix.EINVAL
		}
	}
	ipv4 := ip.To4()
	randomAddr := common.NodeAddrInet4{Addr: [4]byte{ipv4[0], ipv4[1], ipv4[2], ipv4[3]}, Port: headWorkerPort}
	return randomAddr, nil
}

func (n *NodeServer) RequestJoinLocal(headWorkerAddr string, headWorkerPort int) error {
	if n.restarted {
		log.Infof("Skip RequestJoin because this node started without cleanup")
		return nil
	}
	randomAddr, err := GetAddrInet4FromString(headWorkerAddr, headWorkerPort)
	if err != nil {
		log.Errorf("Failed: RequestJoinLocal, err=%v", err)
		return err
	}
	if n.selfNode.addr.Addr == randomAddr.Addr && n.args.ApiPort == randomAddr.Port {
		log.Infof("RequestJoinLocal, selfAddr==randomAddr=%v:%v, ignore this request.", n.selfNode.addr, n.args.ApiPort)
		return nil
	}
	trackerNode := RaftNode{addr: randomAddr}
	var r RaftBasicReply
	var nodeListVer uint64
	id := n.raft.GenerateCoordinatorId()
	for {
		if atomic.LoadInt32(&n.shutdown) != 0 {
			log.Errorf("Failed: RequestJoin, shutdown is requested")
			return unix.EIO
		}
		nextNode, nodeGroups, nlv, reply := n.getTrackerNodeRemote(trackerNode.addr)
		if reply != RaftReplyOk {
			log.Errorf("Failed: RequestJoinLocal, getTrackerNodeRemote, err=%v", reply)
			if reply == RaftReplyNotLeader {
				port, reply2 := n.GetApiPort(nextNode)
				if reply2 == RaftReplyOk {
					nextNode.addr.Port = port
					trackerNode = nextNode
				} else {
					return ReplyToFuseErr(reply2)
				}
			}
			if needRetry(reply) {
				time.Sleep(time.Second * 10)
				continue
			}
			return ReplyToFuseErr(reply)
		}
		nodeListVer = nlv
		groupAddr := map[string]bool{}
		for _, ng := range nodeGroups {
			groupAddr[ng.GetGroupId()] = true
		}
		fn := NewCoordinatorUpdateNodeListOp(id, nextNode, true, n.selfNode, groupAddr, nodeListVer)
		if nextNode.nodeId == n.raft.selfId {
			if r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer); r.reply == RaftReplyOk {
				tm := NewTxIdMgr(fn.GetTxId())
				if _, r.reply = fn.local(n, tm, n.raftGroup.GetNodeListLocal()); r.reply != RaftReplyOk {
					n.rpcMgr.AbortAll(tm, n.flags.CoordinatorTimeoutDuration, true)
				}
			}
		} else {
			_, r = fn.remote(n, nextNode.addr, nodeListVer)
		}
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: RequestJoinLocal, NewCoordinatorUpdateNodeListOp, nextNode=%v, reply=%v", nextNode, r.reply)
			if newTracker, found := fixupRpcErr(n, nextNode, r); found {
				port, reply2 := n.GetApiPort(newTracker)
				if reply2 == RaftReplyOk {
					newTracker.addr.Port = port
					trackerNode = newTracker
				} else {
					return ReplyToFuseErr(reply2)
				}
			}
			if needRetry(r.reply) {
				continue
			}
			return ReplyToFuseErr(r.reply)
		}
		trackerNode = nextNode
		break
	}
	log.Infof("Success: RequestJoinLocal, selfAddr=%v, trackerNode=%v", n.selfNode.addr, trackerNode)
	return nil
}

func (n *NodeServer) UpdateNodeListAsClient() error {
	if !n.args.ClientMode {
		log.Errorf("Failed: UpdateNodeListAsClient, the node is not client mode")
		return unix.EINVAL
	}
	randomAddr, err := GetAddrInet4FromString(n.args.HeadWorkerIp, n.args.HeadWorkerPort)
	if err != nil {
		log.Errorf("Failed: UpdateNodeListAsClient, err=%v", err)
		return err
	}
	for i := 0; i < n.flags.MaxRetry; i++ {
		n.LockNode()
		nextNode, nodeGroups, nodeListVer, reply := n.getTrackerNodeRemote(randomAddr)
		if reply != RaftReplyOk {
			n.UnlockNode()
			log.Errorf("Failed: UpdateNodeListAsClient, getTrackerNodeRemote, addr=%v, i=%v, err=%v", randomAddr, i, reply)
			if reply == RaftReplyNotLeader {
				// we retry only if the leader has changed
				port, reply2 := n.GetApiPort(nextNode)
				if reply2 == RaftReplyOk {
					nextNode.addr.Port = port
					randomAddr = nextNode.addr
				} else {
					return ReplyToFuseErr(reply2)
				}
				time.Sleep(time.Millisecond * 10)
				continue
			}
			return ReplyToFuseErr(reply)
		}
		n.raftGroup.SetNodeListDirect(nodeGroups, nodeListVer)
		n.UnlockNode()
		log.Infof("Success: UpdateNodeListAsClient, tracker=%v, len(nodeGroups)=%v, nodeListVer=%v", randomAddr, len(nodeGroups), nodeListVer)
		return nil
	}
	return unix.ETIMEDOUT
}

func (n *NodeServer) GetApiIpAndPort(RpcMsg) *common.GetApiIpAndPortRet {
	r := n.raft.IsLeader()
	ret := &common.GetApiIpAndPortRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	if r.reply != RaftReplyOk {
		log.Errorf("Rejected: GetApiIpAndPort, IsLeader, reply=%v", r.reply)
		return ret
	}
	ret.ApiAddr = n.args.ExternalIp
	ret.ApiPort = int32(n.args.ApiPort)
	return ret
}

func (n *NodeServer) GetApiIpAndPortRemote(node RaftNode) (apiAddr string, apiPort int, reply int32) {
	if node == n.selfNode {
		return n.args.ExternalIp, n.args.ApiPort, RaftReplyOk
	}
	for {
		msg := &common.GetApiIpAndPortRet{}
		reply := n.rpcClient.CallObjcacheRpc(RpcGetApiIpAndPortCmdId, &api.Void{}, node.addr, n.flags.RpcTimeoutDuration, n.raft.extLogger, nil, nil, msg)
		if reply != RaftReplyOk {
			log.Errorf("Failed: GetApiIpAndPortRemote, CallObjcacheRpc, node=%v, reply=%v", node, reply)
			return "", 0, reply
		}
		r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: GetApiIpAndPortRemote, node=%v, r=%v", node, r)
			if r.reply == RaftReplyNotLeader {
				node = RaftNode{addr: r.leaderAddr}
			}
			if needRetry(r.reply) {
				time.Sleep(time.Millisecond * 10)
				continue
			}
			return "", 0, r.reply
		}
		return msg.GetApiAddr(), int(msg.GetApiPort()), r.reply
	}
}

func (n *NodeServer) RequestRemoveNode(_ context.Context, args *api.RequestLeaveArgs) (*api.ApiRet, error) {
	leavingAddr := common.NodeAddrInet4{Addr: [4]byte{args.Ip[0], args.Ip[1], args.Ip[2], args.Ip[3]}, Port: int(args.GetPort())}
	trackerNode := RaftNode{nodeId: n.raft.selfId, addr: n.selfNode.addr, groupId: n.raftGroup.selfGroup}
	if leavingAddr == n.selfNode.addr {
		log.Infof("RequestRemoveNode, start shutdown")
		_ = unix.Kill(unix.Getpid(), unix.SIGTERM)
		return &api.ApiRet{Status: RaftReplyOk}, nil
	}
	var r RaftBasicReply
	node := RaftNode{addr: common.NodeAddrInet4{Port: int(args.GetPort()), Addr: [4]byte{}}, nodeId: args.GetNodeId()}
	copy(node.addr.Addr[:], args.GetIp())
	id := n.raft.GenerateCoordinatorId()
	for {
		nextNode, nodeGroups, nodeListVer, reply := n.getTrackerNodeRemote(trackerNode.addr)
		if reply != RaftReplyOk {
			log.Errorf("Failed: RequestRemoveNode, getTrackerNodeRemote, err=%v", reply)
			if reply == RaftReplyNotLeader {
				port, reply2 := n.GetApiPort(nextNode)
				if reply2 == RaftReplyOk {
					nextNode.addr.Port = port
					trackerNode = nextNode
				} else {
					reply = reply2
				}
			}
			if needRetry(reply) {
				continue
			}
			return &api.ApiRet{Status: reply}, nil
		}
		groupAddr := map[string]bool{}
		var found = false
		for _, ng := range nodeGroups {
			groupAddr[ng.GetGroupId()] = true
			if ng.Addr[0] == leavingAddr.Addr[0] && ng.Addr[1] == leavingAddr.Addr[1] && ng.Addr[2] == leavingAddr.Addr[2] && ng.Addr[3] == leavingAddr.Addr[3] && ng.GetPort() == int32(leavingAddr.Port) {
				node.groupId = ng.GetGroupId()
				found = true
			}
		}
		if !found {
			log.Infof("Success: RequestRemoveNode, not found: leavingAddr=%v", leavingAddr)
			return &api.ApiRet{Status: RaftReplyOk}, nil
		}
		fn := NewCoordinatorUpdateNodeListOp(id, nextNode, false, node, groupAddr, nodeListVer)
		if nextNode.nodeId == n.raft.selfId {
			if r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer); r.reply == RaftReplyOk {
				tm := NewTxIdMgr(fn.GetTxId())
				if _, r.reply = fn.local(n, tm, n.raftGroup.GetNodeListLocal()); r.reply != RaftReplyOk {
					n.rpcMgr.AbortAll(tm, n.flags.CoordinatorTimeoutDuration, true)
				}
			}
		} else {
			_, r = fn.remote(n, nextNode.addr, nodeListVer)
		}
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: RequestRemoveNode, NewCoordinatorUpdateNodeListOp, leavingNode=%v, nextNode=%v, r=%v", node, nextNode, r)
			if newTracker, ok := fixupRpcErr(n, nextNode, r); ok {
				port, reply2 := n.GetApiPort(newTracker)
				if reply2 == RaftReplyOk {
					newTracker.addr.Port = port
					trackerNode = newTracker
				} else {
					r.reply = reply2
				}
			}
			if needRetry(r.reply) {
				continue
			}
			return &api.ApiRet{Status: r.reply}, nil
		}
		trackerNode = nextNode
		break
	}
	log.Infof("Success: RequestRemoveNode, leavingNode=%v, trackerNode=%v", node, trackerNode)
	return &api.ApiRet{Status: RaftReplyOk}, nil
}

func (n *NodeServer) sendDirtyMetasForNodeJoin(migrationId MigrationId, nodeList *RaftNodeList, newRing *hashring.HashRing, target RaftNode) (reply int32) {
	dirtyInodes := n.dirtyMgr.GetDirtyMetasForNodeJoin(migrationId, nodeList, newRing, n.raftGroup.selfGroup, target.groupId)
	snapshot := n.inodeMgr.GetMetaForNodeJoin(migrationId, n.raftGroup.selfGroup, target.groupId, nodeList, newRing, dirtyInodes)
	if len(snapshot.metas)+len(snapshot.dirents)+len(snapshot.files) > 0 {
		_, r := n.rpcMgr.CallRpc(NewRestoreDirtyMetaOp(snapshot, target).GetCaller(n))
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: sendDirtyMetasForNodeJoin, CallRemote, target=%v, len(dirtyInodes)=%v, r=%v", target, len(dirtyInodes), r)
			return r.reply
		}
		if reply = n.raft.AppendExtendedLogEntry(NewRecordMigratedRemoveMetaCommand(migrationId, dirtyInodes, snapshot.dirents)); reply != RaftReplyOk {
			log.Errorf("Failed: sendDirtyMetasForNodeJoin, AppendExtendedLogEntry, migrationId=%v, len(dirInodes)=%v, reply=%v",
				migrationId, len(dirtyInodes), reply)
			return reply
		}
	}
	return RaftReplyOk
}

func (n *NodeServer) sendDirtyChunksForNodeJoin(migrationId MigrationId, nodeList *RaftNodeList, newRing *hashring.HashRing, target RaftNode) (reply int32) {
	chunks := make([]*common.ChunkRemoveDirtyMsg, 0)
	for inodeKey, chunkInfo := range n.dirtyMgr.GetDirtyChunkForNodeJoin(migrationId, nodeList, newRing, n.raftGroup.selfGroup, target.groupId) {
		for offset, version := range chunkInfo.OffsetVersions {
			reply = n.sendDirtyChunkAny(migrationId, nodeList.version, target, inodeKey, offset, version, chunkInfo.chunkSize, chunkInfo.objectSize, n.flags.RpcChunkSizeBytes)
			if reply != RaftReplyOk {
				log.Errorf("Failed: sendDirtyChunkAny, inodeKey=%v, offset=%v, version=%v, reply=%v", inodeKey, offset, version, reply)
				return reply
			}
			chunks = append(chunks, &common.ChunkRemoveDirtyMsg{InodeKey: uint64(inodeKey), Offset: offset, Version: version})
		}
	}
	if reply = n.raft.AppendExtendedLogEntry(NewRecordMigratedRemoveChunkCommand(migrationId, chunks)); reply != RaftReplyOk {
		log.Errorf("Failed: sendDirtyMetasForNodeJoin, AppendExtendedLogEntry, migrationId=%v, len(chunks)=%v, reply=%v",
			migrationId, len(chunks), reply)
		return reply
	}
	return RaftReplyOk
}

/*************************************
 * methods to remove (shutdown) a Node
 *************************************/

func (n *NodeServer) sendDirtyMetasForNodeLeave(migrationId MigrationId, nodeList *RaftNodeList) (reply int32) {
	dirtyInodes, dirtyMetas := n.dirtyMgr.GetDirtyMetaForNodeLeave(nodeList)
	snapshots := n.inodeMgr.GetMetaForNodeLeave(migrationId, nodeList, dirtyMetas)
	for groupId, snapshot := range snapshots {
		leader, ok := n.raftGroup.GetGroupLeader(groupId, nodeList)
		if !ok {
			log.Errorf("Failed: sendDirtyMetasForNodeLeave, GetLeaderNode, groupId=%v", groupId)
			return RaftReplyNoGroup
		}
		_, r := n.rpcMgr.CallRpc(NewRestoreDirtyMetaOp(snapshot, leader).GetCaller(n))
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: sendDirtyMetasForNodeLeave, CallRemote, target=%v, len(dirtyInodes)=%v, r=%v", leader, len(dirtyInodes), r)
			return r.reply
		}
	}
	if reply = n.raft.AppendExtendedLogEntry(NewRecordMigratedRemoveMetaCommand(migrationId, dirtyInodes, nil)); reply != RaftReplyOk {
		log.Errorf("Failed: sendDirtyMetasForNodeJoin, AppendExtendedLogEntry, migrationId=%v, len(dirtyInodes)=%v, reply=%v",
			migrationId, len(dirtyInodes), reply)
		return reply
	}
	return RaftReplyOk
}

func (n *NodeServer) sendDirtyChunksForNodeLeave(migrationId MigrationId, nodeList *RaftNodeList) (reply int32) {
	chunks := make([]*common.ChunkRemoveDirtyMsg, 0)
	for inodeKey, chunkInfo := range n.dirtyMgr.GetDirtyChunkAll() {
		for offset, version := range chunkInfo.OffsetVersions {
			groupId, ok := GetGroupForChunk(nodeList.ring, inodeKey, offset, chunkInfo.chunkSize)
			if !ok {
				log.Errorf("Failed: sendDirtyChunksForNodeLeave, GetNode, inodeKey=%v, offset=%v", inodeKey, offset)
				return RaftReplyNoGroup
			}
			leader, ok := n.raftGroup.GetGroupLeader(groupId, nodeList)
			if !ok {
				log.Errorf("Failed: sendDirtyChunksForNodeLeave, GetGroupLeader, inodeKey=%v, offset=%v", inodeKey, offset)
				return RaftReplyNoGroup
			}
			reply = n.sendDirtyChunkAny(migrationId, nodeList.version, leader, inodeKey, offset, version, chunkInfo.chunkSize, chunkInfo.objectSize, n.flags.RpcChunkSizeBytes)
			if reply != RaftReplyOk {
				log.Errorf("Failed: sendDirtyChunksForNodeLeave, sendDirtyChunkAny, leader=%v, inodeKey=%v, offset=%v, r=%v", leader, inodeKey, offset, reply)
				return reply
			}
			chunks = append(chunks, &common.ChunkRemoveDirtyMsg{InodeKey: uint64(inodeKey), Offset: offset, Version: version})
		}
	}
	if reply = n.raft.AppendExtendedLogEntry(NewRecordMigratedRemoveChunkCommand(migrationId, chunks)); reply != RaftReplyOk {
		log.Errorf("Failed: sendDirtyMetasForNodeJoin, AppendExtendedLogEntry, migrationId=%v, len(msg.RemoveDirtyChunks)=%v, reply=%v",
			migrationId, len(chunks), reply)
	}
	return reply
}

func (n *NodeServer) RequestLeave() int32 {
	id := n.raft.GenerateCoordinatorId()
	var nodeList *RaftNodeList = nil
	var trackerNode RaftNode
	for i := 0; ; i++ {
		n.WaitNodeUnlocked()
		l := n.raftGroup.GetNodeListLocal()
		if nodeList == nil || nodeList.version != l.version {
			var reply int32
			trackerNode, nodeList, reply = n.raftGroup.getMetaOwnerNodeLocal(TrackerNodeKey)
			if reply != RaftReplyOk {
				log.Errorf("Failed: NodeServer.RequestLeave, getChunkOwnerNodeLocal, reply=%v", reply)
				return reply
			}
		}
		groupAddr := map[string]bool{}
		for gId := range nodeList.groupNode {
			groupAddr[gId] = true
		}
		ret, r := n.rpcMgr.CallRpcOneShot(NewCoordinatorUpdateNodeListOp(id, trackerNode, false, n.selfNode, groupAddr, nodeList.version).GetCaller(n), nodeList)
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: NodeServer.RequestLeave, NewCoordinatorUpdateNodeListOp, trackerNode=%v, nodeListVer=%v, i=%v, reply=%v", trackerNode, nodeList.version, i, r.reply)
			if needRetry(r.reply) {
				continue
			}
			return r.reply
		} else if i > 0 {
			n.raftGroup.UpdateLeader(ret.node)
		}
		// delete all nodes/groups this node maintains
		if reply := n.raft.RemoveAllServerIds(); reply != RaftReplyOk {
			log.Errorf("Failed (ignore): RequestLeave, Raft.RemoveAllServerIds, server=%v, reply=%v", n.raft.selfId, reply)
		}
		log.Infof("Success: RequestLeave, selfAddr=%v, trackerNode=%v", n.selfNode.addr, trackerNode)
		break
	}
	return RaftReplyOk
}

func (n *NodeServer) PersistAllDirty() {
	r := n.raft.IsLeader()
	if r.reply != RaftReplyOk {
		return
	}
	// Pre-Copy: reduce cluster-level data Migration by flushing dirty objects
	log.Infof("PersistAllDirty: Flush dirty metadata on this Node")
	dirtyKeys := n.dirtyMgr.CopyAllPrimaryDirtyMeta()
	for _, inodeKey := range dirtyKeys {
		_, r := n.rpcMgr.CallRpc(NewCoordinatorPersistOp(n.raft.GenerateCoordinatorId(), inodeKey, 1000).GetCaller(n))
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: PersistAllDirty, persistMetaAny, inodeKey=%v, r=%v", inodeKey, r)
		} else {
			log.Infof("Success: PersistAllDirty, persistMetaAny, inodeKey=%v", inodeKey)
		}
	}
	log.Infof("PersistAllDirty: Flush deleted keys on this Node")
	deletedKeys := n.dirtyMgr.CopyAllPrimaryDeletedKeys()
	for key, inodeKey := range deletedKeys {
		_, r := n.rpcMgr.CallRpc(NewCoordinatorDeletePersistOp(n.raft.GenerateCoordinatorId(), key, inodeKey, 1000).GetCaller(n))
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: PersistAllDirty, persistDeleteMetaAny, key=%v, inodeKey=%v, r=%v", key, inodeKey, r)
		} else {
			log.Infof("Success: PersistAllDirty, persistDeleteMetaAny, key=%v, inodeKey=%v", key, inodeKey)
		}
	}
	// NOTE: we track chunks that may be dirty to ensure uploading dirty chunks at leaving nodes.
	// When an inode is deleted after updates, we see false positives, i.e., request non-dirty objects (rejected by metadata owner).
	log.Infof("PersistAllDirty: Flush likely dirty chunks on this Node")
	dirtyKeys = n.dirtyMgr.GetLikelyDirtyChunkInodeIds()
	falsePositives := make([]uint64, 0)
	for _, inodeKey := range dirtyKeys {
		ret, r := n.rpcMgr.CallRpc(NewCoordinatorPersistOp(n.raft.GenerateCoordinatorId(), inodeKey, 1000).GetCaller(n))
		if r.reply == RaftReplyOk {
			if ret.ext.(*CoordinatorRet).extReply == ObjCacheIsNotDirty {
				falsePositives = append(falsePositives, uint64(inodeKey))
			} else {
				log.Infof("Success: PersistAllDirty, persistMetaAny, inodeKey=%v", inodeKey)
			}
		} else {
			log.Warnf("Failed: PersistAllDirty, persistMetaAny, inodeKey=%v, reply=%v", inodeKey, r.reply)
		}
	}
	if reply := n.dirtyMgr.AppendRemoveNonDirtyChunksLog(n.raft, falsePositives); reply != RaftReplyOk {
		log.Errorf("Failed (ignore): PersistAllDirty, AppendRemoveNonDirtyChunksLog, falsePositives=%v", falsePositives)
	}
}

// Shutdown
// NOTE: Shutdown grace period is often very short (e.g., 30 sec in k8s). So, heavy tasks must be avoided so as not to be inconsistent.
func (n *NodeServer) Shutdown(deleteStateFile bool) bool {
	if !atomic.CompareAndSwapInt32(&n.shutdown, 0, 1) {
		log.Infof("Shutdown already started")
		return false
	}
	log.Infof("=========== shutdown requested ===========")
	log.Infof("Shutdown: Check and wait if this Node is joining")
	n.WaitNodeUnlocked()
	if !n.args.ClientMode {
		n.PersistAllDirty()
		if reply := n.RequestLeave(); reply != RaftReplyOk {
			log.Errorf("Failed: Shutdown, RequestLeave, reply=%v", reply)
			return false
		}
		n.dropChunkMetaCache()
		n.raft.Shutdown()
	}
	n.replyClient.Close()
	n.rpcClient.Close()
	log.Infof("Shutdown: GracefulStop of gRPC server...")
	n.s.Stop()

	log.Infof("Shutdown: Cleanup state file...")
	if deleteStateFile {
		_ = os.Remove(n.getStateFile())
	} else {
		_ = common.MarkExit(n.getStateFile())
	}
	log.Infof("=========== Finish shutdown ===========")
	return true
}

func (n *NodeServer) CheckReset() (ok bool) {
	ok = true
	if ok2 := n.inodeMgr.CheckReset(); !ok2 {
		log.Errorf("Failed: NodeServer.CheckReset, inodeMgr")
		ok = false
	}
	if ok2 := n.dirtyMgr.CheckReset(); !ok2 {
		log.Errorf("Failed: NodeServer.CheckReset, dirtyMgr")
		ok = false
	}
	if ok2 := n.localHistory.CheckReset(); !ok2 {
		log.Errorf("Failed: NodeServer.CheckReset, localHistory")
		ok = false
	}
	if ok2 := n.raft.CheckReset(); !ok2 {
		log.Errorf("Failed: NodeServer.CheckReset, raft")
		ok = false
	}
	if ok2 := n.rpcClient.CheckReset(); !ok2 {
		log.Errorf("Failed: NodeServer.CheckReset, rpcClient")
		ok = false
	}
	if ok2 := n.replyClient.CheckReset(); !ok2 {
		log.Errorf("Failed: NodeServer.CheckReset, replyClient")
		ok = false
	}
	if ok2 := n.rpcMgr.CheckReset(); !ok2 {
		log.Errorf("Failed: NodeServer.CheckReset, rpcMgr")
		ok = false
	}
	if ok2 := n.raftGroup.CheckReset(); !ok2 {
		log.Errorf("Failed: NodeServer.CheckReset, raftGroup")
		ok = false
	}
	if ok2 := n.remoteCache.CheckReset(); !ok2 {
		log.Errorf("Failed: NodeServer.CheckReset, remoteCache")
		ok = false
	}
	if n.fs != nil {
		if ok2 := n.fs.CheckReset(); !ok2 {
			log.Errorf("Failed: NodeServer.CheckReset, fs")
			ok = false
		}
	}
	return
}

func (n *NodeServer) ForceStop() {
	log.Warn("ForceStop")
	atomic.StoreInt32(&n.shutdown, 1)
	n.raft.Shutdown()
	n.s.Stop()
	time.Sleep(time.Second * 10)
}

func (n *NodeServer) TerminateThread() {
	if n.Shutdown(false) {
		if n.args.MountPoint != "" {
			if err := fuse.Unmount(n.args.MountPoint); err != nil {
				log.Printf("Failed: Unmount %s, err=%v", n.args.MountPoint, err)
			} else {
				log.Printf("Success: Unmount %s,", n.args.MountPoint)
			}
		}
	}
}

func (n *NodeServer) Terminate(_ context.Context, _ *api.Void) (*api.Void, error) {
	go n.TerminateThread()
	return &api.Void{}, nil
}

func (n *NodeServer) Panic(_ context.Context, _ *api.Void) (*api.Void, error) {
	panic("Panic is triggered by a GRPC client")
}

func (n *NodeServer) CoreDump(_ context.Context, _ *api.Void) (*api.Void, error) {
	pid, _, _ := syscall.Syscall(syscall.SYS_FORK, 0, 0, 0)
	if pid == 0 {
		syscall.Kill(syscall.Getpid(), syscall.SIGABRT)
		syscall.Exit(0)
	}
	var status syscall.WaitStatus
	wpid, err := syscall.Wait4(-1, &status, syscall.WUNTRACED, nil)
	if err != nil {
		log.Errorf("Failed: CoreDump, Wait4, err=%v", err)
	} else {
		log.Infof("Success: CoreDump at a forked process, pid=%v", wpid)
	}
	return &api.Void{}, nil
}

func (n *NodeServer) Rejuvenate(_ context.Context, _ *api.Void) (*api.ApiRet, error) {
	if n == nil {
		log.Errorf("Failed: Rejuvenate, not started yet")
		return &api.ApiRet{Status: ErrnoToReply(unix.EBUSY)}, nil
	}
	// shutdown to drop all states including memory cache, raft logs, and network states
	trackerNode, nodeList, reply := n.raftGroup.getMetaOwnerNodeLocal(TrackerNodeKey)
	if reply != RaftReplyOk {
		log.Errorf("Failed: Rejuvenate, getMetaOwnerNodeLocal for trackerNode, reply=%v", reply)
		return &api.ApiRet{Status: reply}, nil
	}
	nodes := make([]RaftNode, 0)
	if trackerNode.nodeId != n.raft.selfId {
		nodes = append(nodes, trackerNode)
	}
	if ns, ok := nodeList.groupNode[trackerNode.groupId]; ok {
		for _, node := range ns {
			if node.nodeId != n.raft.selfId && node.nodeId != trackerNode.nodeId {
				nodes = append(nodes, node)
			}
		}
	}
	for groupId, ns := range nodeList.groupNode {
		if groupId != trackerNode.groupId {
			for _, node := range ns {
				if node.nodeId != n.raft.selfId {
					nodes = append(nodes, node)
				}
			}
		}
	}
	if n.fs != nil {
		if err := n.fs.Reset(false); err != nil {
			log.Errorf("Failed: Rejuvenate, fs.Reset, err=%v", err)
			return &api.ApiRet{Status: ErrnoToReply(err)}, nil
		}
	}
	go func() {
		if n.Shutdown(true) {
			if !n.CheckReset() {
				log.Errorf("Failed: Rejuvenate, CheckReset failure")
				return
			}
			log.Infof("Rejuvenate: confirmed all states are reset. restart a new session of node server...")
			err := n.Init(n.inodeMgr.back)
			if err != nil {
				log.Fatalf("Failed: Rejuvenate, Init, err=%v", err)
				return
			}
			if len(nodes) == 0 {
				n.fs.EndReset() // unblock FS operations
				log.Infof("Success: Rejuvenate, skip RequestJoinLocal since we have no other nodes in a cluster.")
				return
			}
			var i = 0
			for _, node := range nodes {
				// the node list does not contain API IP/port for nodes. we need to look up it through raft IP/port.
				apiAddr, apiPort, reply := n.GetApiIpAndPortRemote(node)
				if reply != RaftReplyOk {
					log.Errorf("Failed: Rejuvenate, GetApiIpAndPortRemote, node=%v, attempt=%v, reply=%v", node, i, reply)
					i += 1
					continue
				}
				err := n.RequestJoinLocal(apiAddr, apiPort)
				if err != nil {
					log.Errorf("Failed: Rejuvenate, RequestJoinLocal, apiAddr=%v:%v, attempt=%v, err=%v", apiAddr, apiPort, i, err)
					i += 1
				} else {
					log.Infof("Success: Rejuvenate, RequestJoinLocal, apiAddr=%v:%v, attempt=%v", apiAddr, apiPort, i)
					if n.fs != nil {
						n.fs.EndReset() // unblock FS operations
					}
					return
				}
			}
			log.Fatalf("Failed: Rejuvenate, RequestJoinLocal, cannot re-join the cluster")
		} else if n.fs != nil {
			n.fs.EndReset() // unblock FS operations
		}
	}()
	return &api.ApiRet{Status: RaftReplyOk}, nil
}

func (n *NodeServer) RequestJoin(_ context.Context, args *api.RequestJoinArgs) (*api.ApiRet, error) {
	err := n.RequestJoinLocal(args.GetTargetAddr(), int(args.GetTargetPort()))
	return &api.ApiRet{Status: ErrnoToReply(err)}, nil
}
