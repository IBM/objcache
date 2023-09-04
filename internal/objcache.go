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
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/takeshi-yoshimura/fuse/fuseops"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/serialx/hashring"
	"github.com/takeshi-yoshimura/fuse"
	"github.ibm.com/TYOS/objcache/api"
	"github.ibm.com/TYOS/objcache/common"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const TrackerNodeKey = InodeKeyType(math.MaxUint64)

type NodeServer struct {
	args      *common.ObjcacheCmdlineArgs
	flags     *common.ObjcacheConfig
	restarted bool
	shutdown  int32

	selfNode RaftNode

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

var serverInstance *NodeServer = nil

/******************************
 * Methods to initialize a Node
 ******************************/

func (n *NodeServer) StartGrpcServer() {
	listenAddr := fmt.Sprintf("%s:%d", n.args.ListenIp, n.args.ApiPort)
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen %v: %v", listenAddr, err)
	}
	n.s = grpc.NewServer()
	api.RegisterObjcacheApiServer(n.s, n)
	if err = n.s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve at %v: %v", listenAddr, err)
	}
}

func ProfilerThread(blockProfileRate int, mutexProfileRate int, listenIp string, profilePort int) {
	runtime.SetBlockProfileRate(blockProfileRate)
	runtime.SetMutexProfileFraction(mutexProfileRate)
	common.StdouterrLogger.Println(http.ListenAndServe(fmt.Sprintf("%s:%d", listenIp, profilePort), nil))
}

func NewNodeServer(back *ObjCacheBackend, args *common.ObjcacheCmdlineArgs, flags *common.ObjcacheConfig) *NodeServer {
	if serverInstance != nil {
		return serverInstance
	}
	if args.ProfilePort > 0 {
		go ProfilerThread(flags.BlockProfileRate, flags.MutexProfileRate, args.ListenIp, args.ProfilePort)
	}
	if args.ClientMode {
		args.ServerId = uint(math.MaxUint) - 1 // math.MaxUint is used for invalid Id in raft logic. so, decrement it for fake client ID
		args.RaftGroupId = "client"
	}
	server := NodeServer{args: args, flags: flags}
	server.Init(back)
	return &server
}

var SectorSize = 512

func (n *NodeServer) Init(back *ObjCacheBackend) {
	InitMemoryPool()
	InitAccessLinkHead()

	SectorSize = int(PageSize) // difficult to get precise size (using df --output=source . and /sys/block/*/queue/hw_sector is too simple for LVM setting)

	n.remoteCache = NewRemotePageBufferCache(n.flags.RemoteCacheSizeBytes)
	n.localHistory = NewLocalBufferCacheHistory()
	rpcClient, err := NewRpcClientV2(n.flags.IfName)
	if err != nil {
		log.Fatalf("Failed: NodeServer.Init, NewRpcClientV2, err=%v", err)
	}
	replyClient, err := NewRpcReplyClient()
	if err != nil {
		log.Fatalf("Failed: NodeServer.Init, NewRpcReplyClient, err=%v", err)
	}
	n.rpcClient = rpcClient
	n.replyClient = replyClient
	var logLength uint64
	n.raft, logLength = NewRaftInstance(n)
	if n.raft == nil {
		log.Fatal("Failed: NodeServer.Init, NewRaftInstance")
	}
	n.raftGroup = NewRaftGroupMgr(n.args.RaftGroupId, 1)
	n.txMgr = NewTxMgr()
	n.rpcMgr = NewRpcManager(n)
	n.selfNode = RaftNode{nodeId: n.raft.selfId, addr: n.raft.selfAddr, groupId: n.raftGroup.selfGroup}
	n.inodeMgr = NewInodeMgr(back, n.raft, n.flags)
	n.dirtyMgr = NewDirtyMgr()
	n.raft.Init(n.args.PassiveRaftInit)
	n.restarted = logLength > 1

	if !n.args.ClientMode {
		if !n.restarted {
			if !n.args.PassiveRaftInit {
				log.Infof("NodeServer.Init, this is the very first initialization.")
				if reply := n.raft.AppendBootstrapLogs(n.raftGroup.selfGroup); reply != RaftReplyOk {
					log.Fatalf("Failed: NodeServer.Init, AppendBootstrapLogs, reply=%v", reply)
				}
			}
		} else {
			log.Infof("NodeServer.Init, log is detected at disk. restart from Index=%v", logLength-1)
			if reply := n.raft.ReplayAll(); reply != RaftReplyOk {
				log.Fatalf("Failed: NodeServer.Init, ReplayAll, reply=%v", reply)
			}
		}
		cacheCapacityBytes, err := resource.ParseQuantity(n.args.CacheCapacity)
		if err != nil {
			log.Fatalf("Failed: NodeServer.Init, FromHumanSize, CacheCapacity=%v, err=%v", n.args.CacheCapacity, err)
		}
		serverInstance = n
		go n.FlusherThread()
		go n.EvictionThread(cacheCapacityBytes.Value())
	} else {
		serverInstance = n
	}
	go n.StartGrpcServer()
	maxCpus := runtime.NumCPU()
	maxProcesses := runtime.GOMAXPROCS(0)
	log.Infof("Success: NodeServer.Init, addr=%v, maxCpus=%v, GOMAXPROCS=%v", n.selfNode, maxCpus, maxProcesses)
	n.shutdown = 0
}

func (n *NodeServer) SetFs(fs *ObjcacheFileSystem) {
	n.fs = fs
}

func (n *NodeServer) IsReady(_ context.Context, _ *api.Void) (*api.ApiRet, error) {
	if serverInstance == nil {
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

/*******************************************
 * methods to read meta
 * files/inodes can be on the cluster or COS
 *******************************************/

func (n *NodeServer) GetMetaRpc(msg RpcMsg) *common.GetWorkingMetaRet {
	args := &common.GetMetaAnyArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: GetMetaRpc, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.GetWorkingMetaRet{Status: reply}
	}
	r := n.raftGroup.BeginRaftRead(n.raft, args.GetNodeListVer())
	ret := &common.GetWorkingMetaRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: GetMetaRpc, BeginRaftRead, r=%v", r)
		return ret
	}
	parent := MetaAttributes{inodeKey: InodeKeyType(args.GetParentInodeKey()), mode: args.GetParentMode()}
	meta, reply := n.inodeMgr.GetOrFetchWorkingMeta(InodeKeyType(args.GetInodeKey()), args.GetKey(), args.GetChunkSize(), args.GetExpireMs(), parent)
	if reply != RaftReplyOk {
		log.Errorf("Failed: GetMetaRpc, getMetaLocal, name=%v, reply=%v", args.GetKey(), reply)
		ret.Status = reply
		return ret
	}
	ret.Meta = meta.toMsg()
	return ret
}

func (n *NodeServer) getMetaRemote(nodeList *RaftNodeList, node RaftNode, inodeKey InodeKeyType, key string, chunkSize int64, expireMs int32, parent MetaAttributes) (meta *WorkingMeta, r RaftBasicReply) {
	args := &common.GetMetaAnyArgs{
		InodeKey: uint64(inodeKey), Key: key, ChunkSize: chunkSize, ExpireMs: expireMs,
		ParentInodeKey: uint64(parent.inodeKey), ParentMode: parent.mode, NodeListVer: nodeList.version,
	}
	res := &common.GetWorkingMetaRet{}
	r.reply = n.rpcClient.CallObjcacheRpc(RpcGetMetaCmdId, args, node.addr, time.Duration(0), n.raft.files, nil, nil, res)
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: getMetaRemote, CallObjcacheRpc, Node=%v, name=%v, reply=%v", node, key, r.reply)
		return
	}
	r = NewRaftBasicReply(res.GetStatus(), res.GetLeader())
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: getMetaRemote, Node=%v, name=%v, reply=%v", node, key, r)
		return
	}
	return NewWorkingMetaFromMsg(res.GetMeta()), r
}

func (n *NodeServer) GetMetaFromClusterOrCOS(inodeKey InodeKeyType, key string, parent MetaAttributes) (*WorkingMeta, error) {
	var nodeList *RaftNodeList
	var leader RaftNode
	var meta *WorkingMeta
	for i := 0; i < n.flags.MaxRetry; i++ {
		l := n.raftGroup.GetNodeListLocal()
		if nodeList == nil || nodeList.version != l.version {
			var reply int32
			leader, nodeList, reply = n.raftGroup.getMetaOwnerNodeLocal(inodeKey)
			if reply != RaftReplyOk {
				log.Errorf("Failed: GetMetaFromClusterOrCOS, getMetaOwnerNodeLocal, not found, inodeKey=%v, nodeListVer=%v, reply=%v", inodeKey, nodeList.version, reply)
				return nil, ReplyToFuseErr(reply)
			}
		} else {
			nodeList = l
		}
		var r RaftBasicReply
		if leader.nodeId == n.raft.selfId {
			r = n.raftGroup.BeginRaftRead(n.raft, nodeList.version)
			if r.reply == RaftReplyOk {
				meta, r.reply = n.inodeMgr.GetOrFetchWorkingMeta(inodeKey, key, n.flags.ChunkSizeBytes, n.flags.DirtyExpireIntervalMs, parent)
			}
		} else {
			meta, r = n.getMetaRemote(nodeList, leader, inodeKey, key, n.flags.ChunkSizeBytes, n.flags.DirtyExpireIntervalMs, parent)
		}
		if r.reply != RaftReplyOk || meta == nil {
			log.Infof("Failed: GetMetaFromClusterOrCOS, Node=%v, key=%v, reply=%v", leader, key, r.reply)
			if r.reply != RaftReplyOk {
				if newLeader, found := fixupRpcErr(n, leader, r); found {
					leader = newLeader
				}
			}
			if needRetry(r.reply) {
				continue
			}
			return nil, ReplyToFuseErr(r.reply)
		} else if i > 0 {
			n.raftGroup.UpdateLeader(leader)
		}
		log.Debugf("Success: GetMetaFromClusterOrCOS, meta=%v", *meta)
		break
	}
	if meta == nil {
		log.Errorf("Failed: GetMetaFromClusterOrCOS, getLatestWorkingMetaAny does not progress, key=%v", key)
		return nil, unix.ETIMEDOUT
	}
	return meta, nil
}

/************************
 * methods to read chunks
 ************************/

func (n *NodeServer) DownloadChunkViaRemote(msg RpcMsg) (*common.DownloadChunkRet, []SlicedPageBuffer) {
	begin := time.Now()
	r := n.raft.SyncBeforeClientQuery()
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: DownloadChunkViaRemote, SyncBeforeClientQuery, r=%v", r)
		return &common.DownloadChunkRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	raft_sync := time.Now()
	args := &common.DownloadChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: DownloadChunkViaRemote, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.DownloadChunkRet{Status: reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	deserialize := time.Now()
	h := NewMetaRWHandlerFromMsg(args.GetHandler())
	length := args.GetLength()
	bufs, count, reply := n.inodeMgr.VectorReadChunk(h, args.GetKey(), args.GetOffset(), int(length), true)
	if reply != RaftReplyOk {
		return &common.DownloadChunkRet{Status: reply, Leader: r.GetLeaderNodeMsg(), Count: 0}, nil
	}
	endAll := time.Now()
	log.Debugf("Success: DownloadChunkViaRemote, h=%v, offset=%v, len=%v, count=%v, sync=%v, deser=%v, read=%v",
		h, args.GetOffset(), args.GetLength(), count, raft_sync.Sub(begin), deserialize.Sub(raft_sync), endAll.Sub(deserialize))
	return &common.DownloadChunkRet{Status: RaftReplyOk, Leader: r.GetLeaderNodeMsg(), Count: int32(count)}, bufs
}

func CalculateBufferLengthForDownload(h MetaRWHandler, offset int64) int {
	length := h.size - offset
	remaining := h.chunkSize - offset%h.chunkSize
	if length > remaining {
		length = remaining
	}
	if length > int64(math.MaxInt64) {
		length = math.MaxInt32
	}
	return int(length)
}

func (n *NodeServer) readChunkRemoteNoCache(node RaftNode, h MetaRWHandler, key string, offset int64, size int, blocking bool) (SlicedPageBuffer, int, RaftBasicReply) {
	begin := time.Now()
	length := int64(CalculateBufferLengthForDownload(h, offset))
	if int(length) > size {
		length = int64(size)
	}
	alignedSize := length
	if length%PageSize != 0 {
		alignedSize = length - length%PageSize + PageSize
	}
	remoteBuf, err := GetPageBuffer(alignedSize)
	if err != nil {
		log.Errorf("Failed: readChunkRemoteNoCache, GetPageBuffer, chunkSize=%v, err=%v", h.chunkSize, err)
		return SlicedPageBuffer{}, 0, RaftBasicReply{reply: ErrnoToReply(err)}
	}
	var r = RaftBasicReply{reply: RaftReplyOk}
	var c = 0
	var rpcTime time.Duration
	for c < int(length) {
		args := &common.DownloadChunkArgs{
			Handler: h.toMsg(), Key: key, Offset: offset + int64(c), Length: int32(int(length) - c),
		}
		msg := &common.DownloadChunkRet{}
		rpcArgs := &RpcSeqNumArgs{data: remoteBuf.Buf[c:length]}
		rpcBegin := time.Now()
		r.reply = n.rpcClient.CallObjcacheRpc(RpcDownloadChunkViaRemoteCmdId, args, node.addr, time.Duration(0), n.raft.files, nil, rpcArgs, msg)
		rpcTime += time.Since(rpcBegin)
		if r.reply != RaftReplyOk {
			ReturnPageBuffer(remoteBuf)
			log.Errorf("Failed: readChunkRemoteNoCache, CallObjcacheRpc, Node=%v, meta=%v, offset=%v, reply=%v", node, h, offset, r.reply)
			return SlicedPageBuffer{}, 0, r
		}
		r = NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		if r.reply != RaftReplyOk {
			ReturnPageBuffer(remoteBuf)
			log.Errorf("Failed: readChunkRemoteNoCache, CallObjcacheRpc, Node=%v, meta=%v, offset=%v, reply=%v", node, h, offset, r.reply)
			return SlicedPageBuffer{}, 0, r
		}
		c += int(msg.GetCount())
	}
	remoteBuf.Buf = remoteBuf.Buf[:c]
	remoteBuf.Up()
	slice := remoteBuf.AsSlice()
	log.Debugf("Success: readChunkRemoteNoCache, node=%v, h=%v, offset=%v, len=%v, count=%v, rpcTime=%v, elapsed=%v", node, h, offset, length, c, rpcTime, time.Since(begin))
	return slice, c, r
}

func (n *NodeServer) readChunkRemote(node RaftNode, h MetaRWHandler, key string, offset int64, size int, blocking bool) (SlicedPageBuffer, int, RaftBasicReply) {
	if n.flags.DisableLocalWritebackCaching {
		return n.readChunkRemoteNoCache(node, h, key, offset, size, blocking)
	}
	begin := time.Now()
	length := CalculateBufferLengthForDownload(h, offset)
	if blocking {
		if slice, ok := n.remoteCache.GetCacheWithFillWait(h.inodeKey, offset, length); ok {
			log.Debugf("Success: readChunkRemote (cached), h=%v, offset=%v, size=%v", h, offset, size)
			return slice, len(slice.Buf), RaftBasicReply{reply: RaftReplyOk}
		}
	} else {
		p, beginFill := n.remoteCache.GetCacheOrBeginFill(h.inodeKey, offset)
		if p != nil {
			log.Debugf("Success: readChunkRemote (cached), h=%v, offset=%v, size=%v", h, offset, size)
			slice := p.AsSlice(n.remoteCache.dec)
			return slice, len(slice.Buf), RaftBasicReply{reply: RaftReplyOk}
		} else if !beginFill {
			return SlicedPageBuffer{}, 0, RaftBasicReply{reply: ErrnoToReply(unix.EAGAIN)}
		}
	}

	remoteBuf, err := n.remoteCache.GetRemotePageBuffer(h.inodeKey, offset, h.chunkSize, blocking)
	if err != nil {
		if blocking || (!blocking && err != unix.EAGAIN) {
			log.Errorf("Failed: readChunkRemote, GetPageBuffer, chunkSize=%v, err=%v", h.chunkSize, err)
		}
		n.remoteCache.EndFill(h.inodeKey, offset)
		return SlicedPageBuffer{}, 0, RaftBasicReply{reply: ErrnoToReply(err)}
	}
	var r = RaftBasicReply{reply: RaftReplyOk}
	var c = 0
	var rpcTime time.Duration
	for c < length {
		args := &common.DownloadChunkArgs{
			Handler: h.toMsg(), Key: key, Offset: offset + int64(c), Length: int32(length - c),
		}
		msg := &common.DownloadChunkRet{}
		rpcArgs := &RpcSeqNumArgs{data: remoteBuf.buf.Buf[c:length]}
		rpcBegin := time.Now()
		r.reply = n.rpcClient.CallObjcacheRpc(RpcDownloadChunkViaRemoteCmdId, args, node.addr, time.Duration(0), n.raft.files, nil, rpcArgs, msg)
		rpcTime += time.Since(rpcBegin)
		if r.reply != RaftReplyOk {
			n.remoteCache.ReleaseInFlightBuffer(remoteBuf)
			n.remoteCache.EndFill(h.inodeKey, offset)
			log.Errorf("Failed: readChunkRemote, CallObjcacheRpc, Node=%v, meta=%v, offset=%v, reply=%v", node, h, offset, r.reply)
			return SlicedPageBuffer{}, 0, r
		}
		r = NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		if r.reply != RaftReplyOk {
			n.remoteCache.ReleaseInFlightBuffer(remoteBuf)
			n.remoteCache.EndFill(h.inodeKey, offset)
			log.Errorf("Failed: readChunkRemote, CallObjcacheRpc, Node=%v, meta=%v, offset=%v, reply=%v", node, h, offset, r.reply)
			return SlicedPageBuffer{}, 0, r
		}
		c += int(msg.GetCount())
	}
	remoteBuf.buf.Buf = remoteBuf.buf.Buf[:c]
	remoteBuf.buf.Up()
	slice := remoteBuf.AsSlice(n.remoteCache.dec)
	n.remoteCache.EndFillWithPut(remoteBuf)
	log.Debugf("Success: readChunkRemote, node=%v, h=%v, offset=%v, len=%v, count=%v, rpcTime=%v, elapsed=%v", node, h, offset, length, c, rpcTime, time.Since(begin))
	return slice, c, r
}

/*
	func (n *NodeServer) ReadChunk(h MetaRWHandler, key string, offset int64, data []byte) (buf *PageBuffer, count int, err error) {
		if h.size == 0 {
			return nil, 0, nil
		}
		begin := time.Now()
		var nodeList *RaftNodeList
		var leader RaftNode
		for i := 0; i < n.flags.MaxRetry; i++ {
			l := n.raftGroup.GetNodeListLocal()
			if nodeList == nil || nodeList.version != l.version {
				var reply int32
				leader, nodeList, reply = n.raftGroup.getChunkOwnerNodeLocal(h.inodeKey, offset, h.chunkSize)
				if reply != RaftReplyOk {
					log.Errorf("Failed: ReadChunk, getKeyOwnerNodeAny, inodeKey=%v, offset=%v, reply=%v", h.inodeKey, offset, reply)
					return nil, 0, ReplyToFuseErr(reply)
				}
			}
			var r RaftBasicReply
			if leader.nodeId == n.raft.selfId {
				if r = n.raftGroup.BeginRaftRead(n.raft, nodeList.version); r.reply == RaftReplyOk {
					buf, count, r.reply = n.inodeMgr.ReadChunk(h, key, offset, data)
				}
			} else {
				buf, count, r = n.readChunkRemote(leader, h, key, offset, data, len(data))
			}
			if r.reply != RaftReplyOk {
				log.Infof("Failed: ReadChunk, attempt=%v, leader=%v, meta=%v, offset=%v, r=%v", i, leader, h, offset, r)
				if r.reply != RaftReplyOk {
					if newLeader, found := fixupRpcErr(n, leader, r); found {
						leader = newLeader
					}
				}
				if needRetry(r.reply) {
					continue
				}
				err = ReplyToFuseErr(r.reply)
			} else if i > 0 {
				n.raftGroup.UpdateLeader(leader)
			}
			break
		}
		endAll := time.Now()
		log.Debugf("Success: ReadChunk, local=%v, inode=%v, key=%v, offset=%v, count=%v, elapsed=%v",
			leader.nodeId == n.raft.selfId, h.inodeKey, key, offset, count, endAll.Sub(begin))
		return
	}
*/
func (n *NodeServer) ReadAheadChunk(h MetaRWHandler, key string, offset int64, size int64) {
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
		leader, nodeList, reply := n.raftGroup.getChunkOwnerNodeLocal(h.inodeKey, alignedOffset, h.chunkSize)
		if reply != RaftReplyOk {
			log.Errorf("Failed: ReadAheadChunk, getKeyOwnerNodeAny, inodeKey=%v, alignedOffset=%v, reply=%v", h.inodeKey, alignedOffset, reply)
			return
		}
		if leader.nodeId == n.raft.selfId {
			if cachedLength, ok := n.localHistory.Has(h.inodeKey, alignedOffset); ok && cachedLength == length {
				off = alignedOffset + int64(length)
				continue
			}
			go func(alignedOffset int64, length int) {
				if r := n.raftGroup.BeginRaftRead(n.raft, nodeList.version); r.reply != RaftReplyOk {
					return
				}
				bufs, c, reply := n.inodeMgr.VectorReadChunk(h, key, alignedOffset, length, false)
				if reply != RaftReplyOk {
					return
				}
				n.localHistory.Add(h.inodeKey, alignedOffset, c)
				for _, buf := range bufs {
					buf.SetEvictable()
				}
			}(alignedOffset, length)
		} else {
			if n.remoteCache.Has(h.inodeKey, alignedOffset) {
				off = alignedOffset + int64(length)
				continue
			}
			go func(alignedOffset int64, length int) {
				slice, _, r := n.readChunkRemote(leader, h, key, alignedOffset, length, false)
				if r.reply != RaftReplyOk && r.reply != ErrnoToReply(unix.EAGAIN) {
					log.Errorf("Failed: ReadAheadChunk, leader=%v, meta=%v, alignedOffset=%v, r=%v", leader, h, alignedOffset, r)
					if r.reply != RaftReplyOk {
						if newLeader, found := fixupRpcErr(n, leader, r); found {
							leader = newLeader
						}
					}
				}
				if r.reply == RaftReplyOk {
					slice.SetEvictable()
				}
			}(alignedOffset, length)
		}
		off = alignedOffset + int64(length)
	}
}

func (n *NodeServer) VectorReadFastPath(h MetaRWHandler, key string, offset int64, size int, blocking bool) (bufs []SlicedPageBuffer, count int, err error) {
	var r RaftBasicReply
	if _, ok := n.localHistory.Has(h.inodeKey, offset); ok {
		bufs, count, r.reply = n.inodeMgr.VectorReadChunk(h, key, offset, size, blocking)
	} else if slice, ok := n.remoteCache.GetCache(h.inodeKey, offset); ok {
		length := CalculateBufferLengthForDownload(h, offset)
		if slice.orig != nil && length == len(slice.Buf) {
			count = length
			bufs = append(bufs, slice)
			r.reply = RaftReplyOk
		} else {
			slice.SetEvictable()
		}
	}
	if r.reply == RaftReplyOk {
		return
	}
	err = unix.EIO
	return
}

func (n *NodeServer) VectorReadChunk(h MetaRWHandler, key string, offset int64, size int, blocking bool) (bufs []SlicedPageBuffer, count int, err error) {
	if h.size == 0 {
		return nil, 0, nil
	}
	if !n.flags.DisableLocalWritebackCaching {
		if bufs, count, err = n.VectorReadFastPath(h, key, offset, size, blocking); err == nil {
			return
		}
	}
	err = nil
	begin := time.Now()
	var nodeList *RaftNodeList
	var leader RaftNode
	i := 0
	for ; i < n.flags.MaxRetry; i++ {
		l := n.raftGroup.GetNodeListLocal()
		if nodeList == nil || nodeList.version != l.version {
			var reply int32
			leader, nodeList, reply = n.raftGroup.getChunkOwnerNodeLocal(h.inodeKey, offset, h.chunkSize)
			if reply != RaftReplyOk {
				log.Errorf("Failed: VectorReadChunk, getKeyOwnerNodeAny, inodeKey=%v, offset=%v, reply=%v", h.inodeKey, offset, reply)
				return nil, 0, ReplyToFuseErr(reply)
			}
		}
		var r RaftBasicReply
		if leader.nodeId == n.raft.selfId {
			if r = n.raftGroup.BeginRaftRead(n.raft, nodeList.version); r.reply == RaftReplyOk {
				bufs, count, r.reply = n.inodeMgr.VectorReadChunk(h, key, offset, size, blocking)
				if r.reply == RaftReplyOk {
					n.localHistory.Add(h.inodeKey, offset, count)
				}
			}
		} else {
			var slice SlicedPageBuffer
			slice, count, r = n.readChunkRemote(leader, h, key, offset, size, blocking)
			if r.reply == RaftReplyOk {
				bufs = append(bufs, slice)
			}
		}
		if !blocking && r.reply == ErrnoToReply(unix.EAGAIN) {
			err = unix.EAGAIN
			return
		}
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: VectorReadChunk, attempt=%v, leader=%v, meta=%v, offset=%v, r=%v", i, leader, h, offset, r)
			if r.reply != RaftReplyOk {
				if newLeader, found := fixupRpcErr(n, leader, r); found {
					leader = newLeader
				}
			}
			if needRetry(r.reply) {
				continue
			}
			err = ReplyToFuseErr(r.reply)
		} else if i > 0 {
			n.raftGroup.UpdateLeader(leader)
		}
		break
	}
	endAll := time.Now()
	if i >= 1 {
		log.Infof("Success: VectorReadChunk, inode=%v, key=%v, offset=%v, count=%v, retried=%v, elapsed=%v",
			h.inodeKey, key, offset, count, i, endAll.Sub(begin))
	} else {
		log.Debugf("Success: VectorReadChunk, inode=%v, key=%v, offset=%v, count=%v, elapsed=%v",
			h.inodeKey, key, offset, count, endAll.Sub(begin))
	}
	return
}

/*func (n *NodeServer) downloadChunkLocalFd(h MetaRWHandler, key string, offset int64, fd int) (int, int32) {
	if n.checkJoining() {
		log.Errorf("TryAgain: downloadChunkLocalFd, Node is joining, h=%v, offset=%v", h, offset)
		return 0, ObjCacheReplyJoining
	}

	working, reply := n.inodeMgr.GetChunkOrSetAtRemote(h, key, offset, n.txMgr.GetFileId())
	if reply != RaftReplyOk {
		log.Errorf("Failed: downloadChunkLocalFd, GetChunkOrSetAtRemote, inodeKey=%v, offset=%v, chunkSize=%v, reply=%v", h.inodeKey, offset, h.chunkSize, reply)
		return 0, reply
	}
	reader := working.GetReader(h.chunkSize, h.size, offset, n.raft)
	length, err2 := reader.GetLen()
	if err2 != nil {
		_ = reader.Close()
		log.Errorf("Failed: downloadChunkLocalFd, GetLen, err=%v", err2)
		return 0, ErrnoToReply(err2)
	}
	if length > int64(math.MaxInt32) {
		length = int64(math.MaxInt32)
	}
	if length <= 0 {
		_ = reader.Close()
		return 0, RaftReplyOk
	}
	if err := unix.Ftruncate(fd, length); err != nil {
		_ = reader.Close()
		log.Errorf("Failed: downloadChunkLocalFd, Ftruncate, length=%v, err=%v", length, err)
		return 0, ErrnoToReply(err)
	}
	buf, errno := unix.Mmap(fd, 0, int(length), unix.PROT_WRITE|unix.PROT_READ, unix.MAP_SHARED|unix.MAP_POPULATE)
	if errno != nil {
		_ = reader.Close()
		log.Errorf("Failed: Mmap at mmapFileHandle, fd=%v, length=%v, errno=%v", fd, length, errno)
		return 0, ErrnoToReply(errno)
	}
	c, err := reader.Read(buf)
	if err != nil && err != io.EOF {
		log.Errorf("Failed: downloadChunkLocalFd, Read, offset=%v, err=%v", offset%h.chunkSize, err)
	}
	_ = reader.Close()
	if err2 := unix.Munmap(buf); err2 != nil {
		log.Errorf("Failed (ignore): downloadChunkLocalFd, Munmap, err=%v", err2)
	}
	if err != nil {
		log.Errorf("Failed: downloadChunkLocalFd, inodeKey=%v, err=%v", h.inodeKey, err)
		return c, ErrnoToReply(err)
	}
	return c, RaftReplyOk
}

func (n *NodeServer) downloadChunkRemoteFd(node RaftNode, h MetaRWHandler, key string, offset int64, fd int) (c int, r RaftBasicReply) {
	length := CalculateBufferLengthForDownload(h, offset)
	for c < length {
		args := &DownloadChunkArgs{
			Handler: h.toMsg(), Key: key, Offset: offset + int64(c), Length: int32(length - c),
		}
		msg := &DownloadChunkRet{}
		rpcArgs := &RpcSeqNumArgs{fd: fd, fileOffset: (offset + int64(c)) % h.chunkSize}
		r.reply = n.rpcClient.CallObjcacheRpc(RpcDownloadChunkViaRemoteCmdId, args, node.addr, n.flags.ChunkRpcTimeoutDuration, n.raft.files, nil, rpcArgs, msg)
		if r.reply != RaftReplyOk {
			fixupRpcErr(n, node, r)
			log.Debugf("Failed: downloadChunkRemoteFd, CallObjcacheRpc, Node=%v, h=%v, offset=%v, reply=%v", node, h, offset, r.reply)
			return 0, r
		}
		r = NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: downloadChunkRemoteFd, CallObjcacheRpc, r=%v", r)
			return 0, r
		}
		c += int(msg.GetCount())
	}
	log.Debugf("Success: downloadChunkRemoteFd, node=%v, h=%v, offset=%v, len=%v, count=%v", node, h, offset, length, c)
	return c, r
}

func (n *NodeServer) DownloadChunkAnyFd(h MetaRWHandler, key string, offset int64, fd int) (c int, err error) {
	for i := 0; i < n.flags.MaxRetry; i++ {
		leader, nodeList, reply := n.raftGroup.getChunkOwnerNodeLocal(h.inodeKey, offset, h.chunkSize)
		if reply != RaftReplyOk {
			log.Errorf("Failed: DownloadChunkAnyFd, getKeyOwnerNodeAny, inodeKey=%v, offset=%v, reply=%v", h.inodeKey, offset, reply)
			return -1, ReplyToFuseErr(reply)
		}
		var r RaftBasicReply
		if leader.nodeId == n.raft.selfId {
			r = n.raftGroup.BeginRaftRead(n.raft, nodeList.version)
			if r.reply == RaftReplyOk {
				c, r.reply = n.downloadChunkLocalFd(h, key, offset, fd)
			}
			if r.reply != RaftReplyOk {
				fixupLocalErr(n, leader.groupId, r)
			}
		} else {
			c, r = n.downloadChunkRemoteFd(leader, h, key, offset, fd)
		}
		if r.reply != RaftReplyOk {
			log.Infof("Failed: DownloadChunkAnyFd, attempt=%v, leader=%v, h=%v, offset=%v, r=%v", i, leader, h, offset, r)
			//latestMeta = <-metaChan
			if needRetry(r.reply) {
				continue
			}
			err = ReplyToFuseErr(r.reply)
		}
		break
	}
	if c == 0 && err == nil {
		err = io.EOF
	}
	return
}*/

func (n *NodeServer) PrefetchChunk(msg RpcMsg) *common.Ack {
	args := &common.PrefetchChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: PrefetchChunk, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	r := n.raftGroup.BeginRaftRead(n.raft, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: PrefetchChunk, BeginRaftRead, r=%v", r)
		return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	h := NewMetaRWHandlerFromMsg(args.GetHandler())
	offset := args.GetOffset()
	length := args.GetLength()
	for off := offset; off < offset+length && off < h.size; off += h.chunkSize {
		leader, _, reply := n.raftGroup.getChunkOwnerNodeLocal(h.inodeKey, off, h.chunkSize)
		if reply != RaftReplyOk {
			log.Errorf("Failed: PrefetchChunk, getKeyOwnerNodeAny, inodeKey=%v, off=%v, reply=%v", h.inodeKey, off, reply)
			break
		}
		if leader.nodeId == n.raft.selfId {
			go n.inodeMgr.PrefetchChunkThread(h, args.GetKey(), off)
		}
	}
	return &common.Ack{Status: RaftReplyOk, Leader: r.GetLeaderNodeMsg()}
}

func (n *NodeServer) prefetchChunkThread(h MetaRWHandler, args *common.PrefetchChunkArgs, leader RaftNode, nodeListVer uint64) {
	var r RaftBasicReply
	if leader.nodeId == n.raft.selfId {
		r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer)
		if r.reply == RaftReplyOk {
			for off := args.Offset; off < args.Offset+args.Length && off < h.size; off += h.chunkSize {
				l, _, reply := n.raftGroup.getChunkOwnerNodeLocal(h.inodeKey, off, h.chunkSize)
				if reply != RaftReplyOk {
					log.Errorf("Failed: prefetchChunkThread, getKeyOwnerNodeAny, inodeKey=%v, off=%v, reply=%v", h.inodeKey, off, reply)
					break
				}
				if l.nodeId == n.raft.selfId {
					go n.inodeMgr.PrefetchChunkThread(h, args.Key, off)
				}
			}
		}
	} else {
		msg := &common.Ack{}
		r.reply = n.rpcClient.CallObjcacheRpc(RpcPrefetchChunkCmdId, args, leader.addr, n.flags.ChunkRpcTimeoutDuration, n.raft.files, nil, nil, msg)
		if r.reply == RaftReplyOk {
			r = NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		}
		if r.reply != RaftReplyOk {
			if newLeader, found := fixupRpcErr(n, leader, r); found {
				leader = newLeader
			}
		}
	}
}

func (n *NodeServer) prefetchChunkAny(h MetaRWHandler, key string, offset int64, length int64) {
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
			log.Errorf("Failed: prefetchChunkAny, GetGroupForChunk, l.version=%v, inodeKey=%v, off=%v", l.version, h.inodeKey, off)
			return
		}
		groups[groupId] = true
		//log.Debugf("prefetchChunkAny, need inodeKey=%v, off=%v", h.inodeKey, off)
	}
	if len(groups) == 0 {
		return
	}
	args := &common.PrefetchChunkArgs{
		Handler:     h.toMsg(),
		Key:         key,
		Offset:      offset,
		Length:      length,
		NodeListVer: l.version,
	}
	for groupId := range groups {
		leader, ok := n.raftGroup.GetGroupLeader(groupId, l)
		if ok {
			go n.prefetchChunkThread(h, args, leader, l.version)
		} else {
			log.Errorf("Failed: prefetchChunkAny, GetGroupLeader, l.version=%v, inodeKey=%v, groupId=%v", l.version, h.inodeKey, groupId)
		}
	}
}

/*************************
 * methods to write chunks
 *************************/

func (n *NodeServer) SpliceChunk(fd int, pipeFds [2]int, state *ReadRpcMsgState, inodeKey InodeKeyType, dataLength uint32, offset int64, chunkSize int64) (fileId FileIdType, fileOffset int64, reply int32) {
	var reserveTime time.Duration
	begin := time.Now()
	fileId = NewFileIdTypeFromInodeKey(inodeKey, offset, chunkSize)
	if state.optComplete == 1 {
		fileOffset = n.raft.files.ReserveRange(fileId, int64(dataLength))
		state.fileOffsetForSplice = fileOffset
		reserveTime = time.Since(begin)
	}
	beginSplice := time.Now()
	err := n.raft.files.Splice(pipeFds, fileId, fd, state.fileOffsetForSplice, int32(dataLength), &state.bufOffForSplice)
	if err != nil {
		log.Errorf("Failed: SpliceChunk, Splice, err=%v", err)
		reply = ErrnoToReply(err)
		return
	}
	endAll := time.Now()
	state.completed = uint32(state.bufOffForSplice) >= dataLength
	log.Debugf("Success: SpliceChunk, dataLength=%v, readLength=%v, completed=%v, reserveTime=%v, splice=%v, elapsed=%v", dataLength, state.bufOffForSplice, state.completed, reserveTime, endAll.Sub(beginSplice), time.Since(begin))
	return fileId, state.fileOffsetForSplice, RaftReplyOk
}

func (n *NodeServer) UpdateChunkTopHalf(msg RpcMsg, dataLength uint32, fd int, pipeFds [2]int, state *ReadRpcMsgState) (m proto.Message, fileId FileIdType, fileOffset int64, r RaftBasicReply) {
	args := &common.UpdateChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: UpdateChunkTopHalf, ParseExecProtoBufMessage, reply=%v", reply)
		r = RaftBasicReply{reply: reply}
		state.completed = true
		return
	}
	r = n.raftGroup.BeginRaftRead(n.raft, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: UpdateChunkTopHalf, BeginRaftRead, r=%v", r)
		state.completed = true
		return
	}
	fileId, fileOffset, r.reply = n.SpliceChunk(fd, pipeFds, state, InodeKeyType(args.GetInodeKey()), dataLength, args.GetOffset(), args.GetChunkSize())
	m = args
	return
}

func (n *NodeServer) updateChunksAny(inodeKey InodeKeyType, chunkSize int64, offset int64, size int64, buf []byte) ([]*common.UpdateChunkRecordMsg, int32) {
	begin := time.Now()
	id := n.CreateCoordinatorId()
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
		fn := NewUpdateChunkOp(txId, inodeKey, chunkSize, off, buf[bufOff:bufOff+dataLen])
		op, r := n.rpcMgr.CallAny(fn, txId, n.flags.ChunkRpcTimeoutDuration)
		if op.node != nil {
			if _, ok := groupRets[op.node.GroupId]; !ok {
				groupRets[op.node.GroupId] = make([]*common.StagingChunkMsg, 0)
			}
			if op.ext != nil {
				groupRets[op.node.GroupId] = append(groupRets[op.node.GroupId], op.ext.(*common.StagingChunkMsg))
			}
		}
		if r.reply != RaftReplyOk {
			ret := make([]*common.UpdateChunkRecordMsg, 0)
			for groupId, stags := range groupRets {
				ret = append(ret, &common.UpdateChunkRecordMsg{GroupId: groupId, Stags: stags})
			}
			log.Debugf("Failed: UpdateChunksAny, inodeKey=%v, offset=%v, size=%v, elapsed=%v, r=%v", inodeKey, offset, size, time.Since(begin), r)
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
	log.Debugf("Success: UpdateChunksAny, inodeKey=%v, offset=%v, size=%v, i=%v, elapsed=%v", inodeKey, offset, size, i, time.Since(begin))
	return ret, RaftReplyOk
}

/*********************************************************
 * methods to persist cached files to cloud object storage
 *********************************************************/

func (n *NodeServer) persistMultipleChunksMeta(ret *common.TwoPCPersistRecordMsg, txId TxId, metaKeys []string, meta *WorkingMeta, priority int) (nextTxId TxId, keyUploadIds map[string]string, reply int32) {
	ret.PrimaryMeta.Chunks = make([]*common.PersistedChunkInfoMsg, 0)
	keyUploadIds = make(map[string]string)
	begin := time.Now()
	uploadIds := make([]string, 0)
	var beginMsg *common.MpuBeginMsg
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
	beginMsg = &common.MpuBeginMsg{TxId: txId.toMsg(), Keys: metaKeys, UploadIds: uploadIds}
	if lastErrReply = n.raft.AppendExtendedLogEntry(AppendEntryBeginPersistCmdId, beginMsg); lastErrReply != RaftReplyOk {
		log.Errorf("Failed: persistMultipleChunksMeta, AppendBeginPersistLog, keys=%v, reply=%v", metaKeys, lastErrReply)
		goto mpuAbort
	}
	txId = txId.GetNext()
	startMpu = time.Now()

	keyETags = make(map[string][]string)
	ret.PrimaryMeta.Chunks = make([]*common.PersistedChunkInfoMsg, 0)
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
				op := NewMpuAddOp(node, nodeList.version, txId, metaKey, meta, offs, uploadId, priority)
				if node.nodeId == n.raft.selfId {
					localOps = append(localOps, op)
				} else {
					ctx, r := op.remoteAsync(n)
					if r == RaftReplyOk {
						ctxs = append(ctxs, ctx)
					}
				}
				txId = txId.GetNext()
				delete(offsets, node)
			}
		}
		for node, offsets := range offsets {
			op := NewMpuAddOp(node, nodeList.version, txId, metaKey, meta, offsets, uploadId, priority)
			if node.nodeId == n.raft.selfId {
				localOps = append(localOps, op)
			} else {
				ctx, r := op.remoteAsync(n)
				if r == RaftReplyOk {
					ctxs = append(ctxs, ctx)
				}
			}
			txId = txId.GetNext()
		}
		lastErrReply = RaftReplyOk
		for _, localOp := range localOps {
			rpcRet, r := n.rpcMgr.__execParticipant(localOp, localOp.txId, localOp.nodeListVer, false, false)
			lastErrReply = r.reply
			if r.reply == RaftReplyOk {
				outs := rpcRet.ext.([]MpuAddOut)
				c := &common.PersistedChunkInfoMsg{
					TxSeqNum: localOp.txId.TxSeqNum, GroupId: n.raftGroup.selfGroup,
					Offsets: make([]int64, len(outs)), CVers: make([]uint32, len(outs)),
				}
				for i, out := range outs {
					keyETags[metaKey][out.idx] = out.etag
					c.Offsets[i] = int64(out.idx) * meta.chunkSize
					c.CVers[i] = out.cVer
				}
				ret.PrimaryMeta.Chunks = append(ret.PrimaryMeta.Chunks, c)
			}
		}
		for _, ctx := range ctxs {
			outs, r := ctx.WaitRet(n)
			lastErrReply = r.reply
			if r.reply == RaftReplyOk {
				c := &common.PersistedChunkInfoMsg{
					TxSeqNum: ctx.txId.TxSeqNum, GroupId: ctx.node.groupId,
					Offsets: make([]int64, len(outs)), CVers: make([]uint32, len(outs)),
				}
				for i, out := range outs {
					keyETags[metaKey][out.idx] = out.etag
					c.Offsets[i] = int64(out.idx) * meta.chunkSize
					c.CVers[i] = out.cVer
				}
				ret.PrimaryMeta.Chunks = append(ret.PrimaryMeta.Chunks, c)
			}
		}
		if lastErrReply != RaftReplyOk {
			goto mpuAbort
		}
	}
	mpuAdd = time.Now()

	for metaKey, uploadId := range keyUploadIds {
		ret.PrimaryMeta.Ts, lastErrReply = n.inodeMgr.MpuCommit(metaKey, uploadId, keyETags[metaKey])
		if lastErrReply != RaftReplyOk {
			log.Errorf("Failed: persistMultipleChunksMeta, MpuCommit, reply=%v", lastErrReply)
			goto mpuAbort
		}
		commit = time.Now()
		log.Debugf("Success: persistMultipleChunksMeta, metaKey=%v, inodeKey=%v, length=%v, mpubegin=%v, mpuadd=%v, commit=%v",
			metaKey, meta.inodeKey, meta.size, startMpu.Sub(begin), mpuAdd.Sub(startMpu), commit.Sub(mpuAdd))
	}
	return txId, keyUploadIds, RaftReplyOk
mpuAbort:
	for _, chunk := range ret.PrimaryMeta.Chunks {
		txId2 := txId.GetVariant(chunk.GetTxSeqNum())
		n.rpcMgr.CallRpcAnyNoFail(NewAbortParticipantOp(txId2, []*common.TxIdMsg{}, chunk.GetGroupId()), txId2, n.flags.RpcTimeoutDuration)
	}
	for metaKey, uploadId := range keyUploadIds {
		n.inodeMgr.MpuAbort(metaKey, uploadId)
	}
	return txId, nil, lastErrReply
}

func (n *NodeServer) flushExpired() {
	if r := n.raft.IsLeader(); r.reply != RaftReplyOk {
		return
	}
	dirtyKeys := n.dirtyMgr.CopyAllExpiredPrimaryDirtyMeta()
	for _, inodeKey := range dirtyKeys {
		log.Debugf("flushExpired: Start flushing inodeKey=%v", inodeKey)
		_, reply := n.CallCoordinator(NewCoordinatorPersistOp(inodeKey, 0))
		if reply != RaftReplyOk {
			break
		}
	}
	deletedKeys := n.dirtyMgr.CopyAllExpiredPrimaryDeletedDirtyMeta()
	for key, inodeKey := range deletedKeys {
		log.Debugf("flushExpired: Start flushing deleted inodeKey=%v", inodeKey)
		_, reply := n.CallCoordinator(NewCoordinatorDeletePersistOp(key, inodeKey, 0))
		if reply != RaftReplyOk {
			break
		}
	}
}

func (n *NodeServer) dropChunkMetaCache() int32 {
	n.localHistory.DropAll()
	n.remoteCache.DropAll()
	n.inodeMgr.Clean(n.dirtyMgr)
	current := n.raft.files.GetDiskUsage()
	n.inodeMgr.chunkMapLock.RLock()
	chunks := make([]*Chunk, 0)
	msg := &common.DropLRUChunksArgs{InodeKeys: make([]uint64, 0), Offsets: make([]int64, 0)}
	for inodeKey, offs := range n.inodeMgr.chunkMap {
		for off, chunk := range offs {
			if n.dirtyMgr.IsDirtyChunk(chunk) {
				log.Warnf("dropChunkMetaCache, chunk is dirty, inodeKey=%v, offset=%v", inodeKey, off)
				continue
			}
			chunks = append(chunks, chunk)
			msg.InodeKeys = append(msg.InodeKeys, uint64(inodeKey))
			msg.Offsets = append(msg.Offsets, off)
			chunk.lock.Lock()
		}
	}
	n.inodeMgr.chunkMapLock.RUnlock()
	if len(chunks) > 0 {
		reply := n.raft.AppendExtendedLogEntry(AppendEntryDropLRUChunksCmdId, msg)
		if reply != RaftReplyOk {
			log.Errorf("Failed: dropChunkMetaCache, AppendExtendedLogEntry, msg=%v, reply=%v", msg, reply)
			return reply
		}
		for _, c := range chunks {
			c.lock.Unlock()
		}
	}
	n.raft.files.Clear()
	updated := n.raft.files.GetDiskUsage()
	log.Infof("Success: dropChunkMetaCache, prev=%v, now=%v, size=%v", current, updated, current-updated)
	return RaftReplyOk
}

func (n *NodeServer) DropCache(_ context.Context, _ *api.Void) (*api.ApiRet, error) {
	r := n.raft.IsLeader()
	r.leaderAddr.Port = n.args.ApiPort
	if r.reply != RaftReplyOk {
		return &api.ApiRet{Status: r.reply}, nil
	}
	n.inodeMgr.waitNodeResume()
	n.inodeMgr.SuspendNode()
	if n.fs != nil {
		if err := n.fs.Reset(); err != nil {
			log.Errorf("Failed: DropCache, fs.Reset, err=%v", err)
			return &api.ApiRet{Status: ErrnoToReply(err)}, nil
		}
	}
	if r.reply = n.dirtyMgr.AppendForgetAllDirtyLog(n.raft); r.reply != RaftReplyOk {
		log.Errorf("Failed: DropCache, AppendForgetAllDirtyLog, reply=%v", r.reply)
		n.inodeMgr.ResumeNode()
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
	n.inodeMgr.ResumeNode()
	return &api.ApiRet{Status: reply}, nil
}

func (n *NodeServer) dropLRUCacheOnDisk(cacheCapacityBytes int64) {
	if r := n.raft.IsLeader(); r.reply != RaftReplyOk {
		return
	}
	current := n.raft.files.GetDiskUsage()
	if current <= cacheCapacityBytes {
		return
	}
	opened := make(map[InodeKeyType]bool)
	if n.fs != nil {
		opened = n.fs.GetOpenInodes()
	}
	targetDropSize := (current - cacheCapacityBytes) * 2
	msg := CollectLRUChunks(n.dirtyMgr, n.raft, targetDropSize)
	if len(msg.InodeKeys) == 0 {
		keys := CollectLRUDirtyKeys(n.dirtyMgr, n.raft, targetDropSize)
		falsePositives := make([]uint64, 0)
		for inodeKey := range keys {
			if _, ok := opened[inodeKey]; ok {
				log.Debugf("dropLRUCacheOnDisk: skip flushing opening inodeKey=%v", inodeKey)
				continue
			}
			log.Debugf("dropLRUCacheOnDisk: Start flushing inodeKey=%v", inodeKey)
			ret, reply := n.CallCoordinator(NewCoordinatorPersistOp(inodeKey, 0))
			if reply == RaftReplyOk {
				if ret.extReply == ObjCacheIsNotDirty {
					falsePositives = append(falsePositives, uint64(inodeKey))
				}
			} else {
				break
			}
		}
		if reply := n.dirtyMgr.AppendRemoveNonDirtyChunksLog(n.raft, falsePositives); reply != RaftReplyOk {
			log.Errorf("Failed (ignore): dropLRUCacheOnDisk, AppendRemoveNonDirtyChunksLog, falsePositives=%v", falsePositives)
		}
		msg = CollectLRUChunks(n.dirtyMgr, n.raft, targetDropSize)
		if len(msg.InodeKeys) == 0 {
			return
		}
	}
	chunks := make([]*Chunk, 0)
	n.inodeMgr.chunkMapLock.RLock()
	for _, key := range msg.InodeKeys {
		inodeKey := InodeKeyType(key)
		cs, ok := n.inodeMgr.chunkMap[inodeKey]
		if !ok {
			continue
		}
		for _, c := range cs {
			if c.lock.TryLock() {
				chunks = append(chunks, c)
			}
		}
	}
	n.inodeMgr.chunkMapLock.RUnlock()
	reply := n.raft.AppendExtendedLogEntry(AppendEntryDropLRUChunksCmdId, msg)
	if reply != RaftReplyOk {
		log.Errorf("Failed: dropLRUCacheOnDisk, AppendExtendedLogEntry, msg=%v, reply=%v", msg, reply)
		return
	}
	for _, c := range chunks {
		c.lock.Unlock()
	}
	updated := n.raft.files.GetDiskUsage()
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
	switch rpcId {
	case RpcCommitParticipantCmdId:
		ret = n.rpcMgr.CommitParticipant(msg)
	case RpcCommitMigrationParticipantCmdId:
		ret = n.rpcMgr.CommitMigrationParticipant(msg)
	case RpcAbortParticipantCmdId:
		ret = n.rpcMgr.AbortParticipant(msg)
	case RpcCreateMetaCmdId:
		ret = n.rpcMgr.CreateMeta(msg)
	case RpcLinkMetaCmdId:
		ret = n.rpcMgr.LinkMeta(msg)
	case RpcTruncateMetaCmdId:
		ret = n.rpcMgr.TruncateMeta(msg)
	case RpcUpdateMetaSizeCmdId:
		ret = n.rpcMgr.UpdateMetaSize(msg)
	case RpcDeleteMetaCmdId:
		ret = n.rpcMgr.DeleteMeta(msg)
	case RpcUnlinkMetaCmdId:
		ret = n.rpcMgr.UnlinkMeta(msg)
	case RpcRenameMetaCmdId:
		ret = n.rpcMgr.RenameMeta(msg)
	case RpcCommitUpdateChunkCmdId:
		ret = n.rpcMgr.CommitUpdateChunk(msg)
	case RpcCommitDeleteChunkCmdId:
		ret = n.rpcMgr.CommitDeleteChunk(msg)
	case RpcCommitExpandChunkCmdId:
		ret = n.rpcMgr.CommitExpandChunk(msg)
	case RpcCommitPersistChunkCmdId:
		ret = n.rpcMgr.CommitPersistChunk(msg)
	case RpcUpdateNodeListCmdId:
		ret = n.rpcMgr.UpdateNodeList(msg)
	case RpcInitNodeListCmdId:
		ret = n.rpcMgr.InitNodeList(msg)
	case RpcCreateChildMetaCmdId:
		ret = n.rpcMgr.CreateChildMeta(msg)
	case RpcCoordinatorUpdateNodeListCmdId:
		ret = n.rpcMgr.CoordinatorUpdateNodeList(msg)
	//case RpcCoordinatorAbortTxCmdId:
	//	ret = n.rpcMgr.CoordinatorAbort(msg)
	case RpcMpuAddCmdId:
		ret = n.rpcMgr.MpuAdd(msg)
	case RpcJoinMigrationCmdId:
		ret = n.rpcMgr.JoinMigration(msg)
	case RpcLeaveMigrationCmdId:
		ret = n.rpcMgr.LeaveMigration(msg)
	case RpcCoordinatorFlushObjectCmdId:
		ret = n.rpcMgr.CoordinatorFlushObject(msg)
	case RpcCoordinatorTruncateObjectCmdId:
		ret = n.rpcMgr.CoordinatorTruncateObject(msg)
	case RpcCoordinatorDeleteObjectCmdId:
		ret = n.rpcMgr.CoordinatorDeleteObject(msg)
	case RpcCoordinatorHardLinkObjectCmdId:
		ret = n.rpcMgr.CoordinatorHardLinkObject(msg)
	case RpcCoordinatorRenameObjectCmdId:
		ret = n.rpcMgr.CoordinatorRenameObject(msg)
	case RpcCoordinatorCreateObjectCmdId:
		ret = n.rpcMgr.CoordinatorCreateObject(msg)
	case RpcCoordinatorPersistCmdId:
		ret = n.rpcMgr.CoordinatorPersist(msg)
	case RpcRestoreDirtyMetasCmdId:
		ret = n.rpcMgr.RestoreDirtyMeta(msg)
	case RpcGetApiIpAndPortCmdId:
		ret = n.GetApiIpAndPort(msg)
	case RpcPrefetchChunkCmdId:
		ret = n.PrefetchChunk(msg)
	case RpcDownloadChunkViaRemoteCmdId:
		ret, buffer = n.DownloadChunkViaRemote(msg)
	case RpcGetMetaCmdId:
		ret = n.GetMetaRpc(msg)
	case RpcCoordinatorDeletePersistCmdId:
		ret = n.rpcMgr.CoordinatorDeletePersist(msg)
	case RpcUpdateMetaKeyCmdId:
		ret = n.rpcMgr.UpdateMetaKey(msg)
	case RpcUpdateMetaAttrCmdId:
		ret = n.rpcMgr.UpdateMetaAttr(msg)
	case RpcGetApiPortCmdId:
		ret = n.rpcMgr.GetApiPort(msg)
	case RpcRestoreDirtyChunksCmdId:
		fallthrough
	case RpcUpdateChunkCmdId:
		log.Errorf("BUG (ignore): ExecRpc, rpcId=%v should be handled at ExecDataRpc", rpcId)
		return
	default:
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

func (n *NodeServer) ExecDataRpc(msg RpcMsg, sa common.NodeAddrInet4, fd int, pipeFds [2]int, state *ReadRpcMsgState) (noData bool) {
	payload := msg.GetCmdPayload()
	rpcId := msg.GetExecProtoBufRpcId(payload)
	seqNum := msg.GetExecProtoBufRpcSeqNum(payload)
	dataLength, _ := msg.GetOptControlHeader()
	noData = rpcId&DataCmdIdBit == 0
	if noData {
		state.completed = true
		return
	}
	var m proto.Message
	var fileId FileIdType
	var fileOffset int64
	var r RaftBasicReply
	switch rpcId {
	case RpcUpdateChunkCmdId:
		m, fileId, fileOffset, r = n.UpdateChunkTopHalf(msg, dataLength, fd, pipeFds, state)
	case RpcRestoreDirtyChunksCmdId:
		m, fileId, fileOffset, r = n.RestoreDirtyChunksTopHalf(msg, dataLength, fd, pipeFds, state)
	default:
		log.Errorf("Failed (ignore): unknown rpcId, rpcId=%v", rpcId)
		noData = true
		state.completed = true
		return
	}
	if !state.completed {
		return
	}
	var ret proto.Message
	switch rpcId {
	case RpcUpdateChunkCmdId:
		ret = n.rpcMgr.UpdateChunkBottomHalf(m, fileId, fileOffset, dataLength, r)
	case RpcRestoreDirtyChunksCmdId:
		ret = n.rpcMgr.RestoreDirtyChunksBottomHalf(m, fileId, fileOffset, dataLength, r)
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
	//log.Debugf("Success: ExecDataRpc, RpcUpdateChunk, seqNum=%v, optHeaderLength=%v, dataLength=%v", seqNum, h.optHeaderLength, dataLength)
	return
}

/**********************************************************
 * methods to start a coordinator for inode/file operations
 **********************************************************/

func (n *NodeServer) CreateCoordinatorId() CoordinatorId {
	return n.raft.GenerateCoordinatorId()
}

func (n *NodeServer) CallCoordinator(fn CoordinatorOpBase) (ret CoordinatorRet, reply int32) {
	id := n.CreateCoordinatorId()
	var nodeList *RaftNodeList = nil
	var leader RaftNode
	for i := 0; i < n.flags.MaxRetry; i++ {
		l := n.raftGroup.GetNodeListLocal()
		if nodeList == nil || nodeList.version != l.version {
			nodeList = l
			var ok bool
			leader, ok = fn.GetLeader(n, nodeList)
			if !ok {
				log.Errorf("Failed: CallCoordinator (%v), GetLeader", fn.name())
				return CoordinatorRet{}, RaftReplyNoGroup
			}
		}
		var r RaftBasicReply
		if leader.nodeId == n.raft.selfId {
			if r = n.raftGroup.BeginRaftRead(n.raft, nodeList.version); r.reply == RaftReplyOk {
				ret, r.reply = fn.local(n, id, nodeList)
			}
		} else {
			ret, r = fn.remote(n, leader.addr, id, nodeList.version)
		}
		reply = r.reply
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: CallCoordinator (%v), i=%v, id=%v, leader=%v, nodeListVer=%v, r=%v", fn.name(), i, id, leader, nodeList.version, r)
			if r.reply != RaftReplyOk {
				if newLeader, found := fixupRpcErr(n, leader, r); found {
					leader = newLeader
				}
			}
			if needRetry(r.reply) {
				continue
			}
		} else if i > 0 {
			n.raftGroup.UpdateLeader(leader)
		}
		return
	}
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

func (n *NodeServer) UpdateObjectAttr(attr MetaAttributes, ts int64) (meta *WorkingMeta, err error) {
	coordinatorId := n.raft.GenerateCoordinatorId()
	txId := coordinatorId.toCoordinatorTxId()
	ret, r := n.rpcMgr.CallAny(NewUpdateMetaAttrOp(txId, attr, ts), txId, n.flags.RpcTimeoutDuration)
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: UpdateObjectAttr, CallAny, attr=%v, reply=%v", attr, r.reply)
	} else {
		log.Debugf("Success: UpdateObjectAttr, attr=%v", attr)
	}
	return ret.ext.(*WorkingMeta), ReplyToFuseErr(r.reply)
}

func (n *NodeServer) TruncateObject(inodeId fuseops.InodeID, size int64) (meta *WorkingMeta, err error) {
	inodeKey := InodeKeyType(inodeId)
	ret, reply := n.CallCoordinator(NewCoordinatorTruncateObjectOp(inodeKey, size))
	if reply != RaftReplyOk {
		log.Errorf("Failed: TruncateObject, CallCoordinator, inodeKey=%v, size=%v, reply=%v", inodeKey, size, reply)
	} else {
		log.Debugf("Success: TruncateObject, inodeKey=%v, size=%v", inodeKey, size)
	}
	return ret.meta, ReplyToFuseErr(reply)
}

func (n *NodeServer) UnlinkObject(parentFullPath string, parentId fuseops.InodeID, name string, childKey InodeKeyType) (err error) {
	// NOTE: childId and childIsDir are required to make coordinator idempotent (== "declare that an inode is now deleted (do not care if the inode exists right now)")
	parentKey := InodeKeyType(parentId)
	_, reply := n.CallCoordinator(NewCoordinatorDeleteObjectOp(parentFullPath, parentKey, name, childKey))
	if reply != RaftReplyOk {
		log.Errorf("Failed: UnlinkObject, CallCoordinator, parentKey=%v, name=%v, reply=%v", parentKey, name, reply)
	} else {
		log.Debugf("Success: UnlinkObject, parentKey=%v, name=%v", parentKey, name)
	}
	return ReplyToFuseErr(reply)
}

func (n *NodeServer) HardLinkObject(srcInodeId fuseops.InodeID, srcParent MetaAttributes, dstParentKey string, dstParentInodeId fuseops.InodeID, dstName string, childAttr MetaAttributes) (meta *WorkingMeta, err error) {
	// NOTE: childId and childIsDir are required to make coordinator idempotent
	srcInodeKey := InodeKeyType(srcInodeId)
	dstParentInodeKey := InodeKeyType(dstParentInodeId)
	ret, reply := n.CallCoordinator(NewCoordinatorHardLinkOp(srcInodeKey, srcParent, dstParentKey, dstParentInodeKey, dstName, childAttr))
	if reply != RaftReplyOk {
		log.Errorf("Failed: HardLinkObject, CallCoordinator, srcInodeId=%v, dstParentInodeId=%v, dstName=%v, reply=%v", srcInodeKey, dstParentInodeKey, dstName, reply)
	} else {
		log.Errorf("Success: HardLinkObject, srcInodeId=%v, dstParentInodeId=%v, dstName=%v", srcInodeKey, dstParentInodeKey, dstName)
	}
	return ret.meta, ReplyToFuseErr(reply)
}

func (n *NodeServer) RenameObject(srcParentKey string, srcParent MetaAttributes, dstParentKey string, dstParentId fuseops.InodeID, srcName string, dstName string, childAttr MetaAttributes) (err error) {
	// NOTE: childId and childIsDir are required to make coordinator idempotent
	dstParentInodeKey := InodeKeyType(dstParentId)
	_, reply := n.CallCoordinator(NewCoordinatorRenameObjectOp(srcParentKey, srcParent, srcName, dstParentKey, dstParentInodeKey, dstName, childAttr))
	if reply != RaftReplyOk {
		log.Errorf("Failed: RenameObject, CallCoordinator, srcParentKey=%v, srcName=%v, dstParentKey=%v, dstName=%v, reply=%v",
			srcParentKey, srcName, dstParentKey, dstName, reply)
	} else {
		log.Debugf("Success: RenameObject, srcParentKey=%v, srcName=%v, dstParentKey=%v, dstName=%v",
			srcParentKey, srcName, dstParentKey, dstName)
	}
	return ReplyToFuseErr(reply)
}

func (n *NodeServer) CreateObject(parentKey string, parentAttr MetaAttributes, name string, childAttr MetaAttributes) (meta *WorkingMeta, err error) {
	ret, reply := n.CallCoordinator(NewCoordinatorCreateObjectOp(parentKey, parentAttr, name, childAttr, n.flags.ChunkSizeBytes, n.flags.DirtyExpireIntervalMs))
	if reply != RaftReplyOk {
		log.Errorf("Failed: CreateObject, CallCoordinator, parentKey=%v, name=%v, reply=%v", parentAttr.inodeKey, name, reply)
	} else {
		log.Debugf("Success: CreateObject, parentKey=%v, name=%v, inodeKey=%v", parentAttr.inodeKey, name, childAttr.inodeKey)
	}
	return ret.meta, ReplyToFuseErr(reply)
}

func (n *NodeServer) FlushObject(inodeKey InodeKeyType, records []*common.UpdateChunkRecordMsg, mTime int64, mode uint32) (meta *WorkingMeta, err error) {
	begin := time.Now()
	ret, reply := n.CallCoordinator(NewCoordinatorFlushObjectOp(inodeKey, records, mTime, mode))
	if reply != RaftReplyOk {
		log.Errorf("Failed: FlushObject, CallCoordinator, inodeKey=%v, reply=%v", inodeKey, reply)
	} else {
		log.Debugf("Success: FlushObject, inodeKey=%v, len(records)=%v, elapsed=%v", inodeKey, len(records), time.Since(begin))
	}
	return ret.meta, ReplyToFuseErr(reply)
}

func (n *NodeServer) AbortWriteObject(recordMap map[string][]*common.StagingChunkMsg) {
	// Abort is directly requested from client. This means failures at client loose target TxIds to be aborted.
	// We cannot reclaim disk space for the non-aborted staging chunks, but no h corruption in the filesystem.
	// Note that client roles are taken by some participant servers in a cluster, so, our end-users, filesystem
	// applications in this case, cannot intentionally cause disk leakages by exploiting this procedure.
	id := n.CreateCoordinatorId()
	txId := id.toTxId(1)
	for groupId, stags := range recordMap {
		txIds := make([]*common.TxIdMsg, 0)
		for _, stag := range stags {
			txIds = append(txIds, stag.GetTxId())
		}
		ret := n.rpcMgr.CallRpcAnyNoFail(NewAbortParticipantOp(txId, txIds, groupId), txId, n.flags.RpcTimeoutDuration)
		txId = txId.GetNext()
		if ret.unlock != nil {
			ret.unlock(n)
		}
	}
}

func (n *NodeServer) PersistObject(inodeId fuseops.InodeID) (meta *WorkingMeta, err error) {
	inodeKey := InodeKeyType(inodeId)
	ret, reply := n.CallCoordinator(NewCoordinatorPersistOp(inodeKey, 1000))
	if reply != RaftReplyOk {
		log.Errorf("Failed: PersistObject, inodeKey=%v, reply=%v", inodeKey, reply)
	} else {
		log.Debugf("Success: PersistObject, inodeKey=%v", inodeKey)
	}
	return ret.meta, ReplyToFuseErr(reply)
}

/**************************************************
 * methods to migrate data at node addition/removal
 **************************************************/

func (n *NodeServer) RestoreDirtyChunksTopHalf(msg RpcMsg, dataLength uint32, fd int, pipeFds [2]int, state *ReadRpcMsgState) (m proto.Message, fileId FileIdType, fileOffset int64, r RaftBasicReply) {
	args := &common.RestoreDirtyChunksArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: RestoreDirtyChunksTopHalf, ParseExecProtoBufMessage, reply=%v", reply)
		r = RaftBasicReply{reply: reply}
		state.completed = true
		return
	}
	r = n.raft.SyncBeforeClientQuery()
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: RestoreDirtyChunksTopHalf, SyncBeforeClientQuery, r=%v", r)
		state.completed = true
		return
	}
	fileId, _, r.reply = n.SpliceChunk(fd, pipeFds, state, InodeKeyType(args.GetInodeKey()), dataLength, args.GetOffset(), args.GetChunkSize())
	return args, fileId, state.fileOffsetForSplice, r
}

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
		bs, c, err := reader.GetBufferDirect(int(length))
		reader.Close()
		if err != nil {
			log.Errorf("Failed: sendDirtyChunkAny, GetBufferDirect, err=%v", err)
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
	fn := NewRestoreDirtyChunkOp(migrationId, target, inodeKey, chunkSize, offset, objectSize, working.chunkVer, bufs)
	ok := n.rpcMgr.CallRemote(fn, n.flags.ChunkRpcTimeoutDuration)
	for _, slice := range slices {
		slice.SetEvictable()
	}
	if !ok {
		log.Error("Failed: sendDirtyChunkAny, CallRemote(RestoreDirtyChunksBottomHalf)")
		return RaftReplyFail
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
	n.inodeMgr.SuspendNode()
	defer n.inodeMgr.ResumeNode()
	trackerNode := RaftNode{addr: randomAddr}
	var r RaftBasicReply
	var nodeListVer uint64
	id := n.CreateCoordinatorId()
	for {
		nextNode, nodeGroups, nlv, reply := n.getTrackerNodeRemote(trackerNode.addr)
		if reply != RaftReplyOk {
			log.Errorf("Failed: RequestJoinLocal, getTrackerNodeRemote, err=%v", reply)
			if reply == RaftReplyNotLeader {
				port, reply2 := CallGetApiPort(n, nextNode.addr)
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
		fn := NewCoordinatorUpdateNodeListOp(true, n.selfNode, groupAddr, nodeListVer)
		if nextNode.nodeId == n.raft.selfId {
			if r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer); r.reply == RaftReplyOk {
				_, r.reply = fn.local(n, id, n.raftGroup.GetNodeListLocal())
			}
		} else {
			_, r = fn.remote(n, nextNode.addr, id, nodeListVer)
		}
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: RequestJoinLocal, NewCoordinatorUpdateNodeListOp, nextNode=%v, reply=%v", nextNode, r.reply)
			if newTracker, found := fixupRpcErr(n, nextNode, r); found {
				port, reply2 := CallGetApiPort(n, newTracker.addr)
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
		log.Fatalf("UpdateNodeListAsClient is called but the node is not client mode")
	}
	randomAddr, err := GetAddrInet4FromString(n.args.HeadWorkerIp, n.args.HeadWorkerPort)
	if err != nil {
		log.Errorf("Failed: UpdateNodeListAsClient, err=%v", err)
		return err
	}
	for i := 0; i < n.flags.MaxRetry; i++ {
		n.inodeMgr.SuspendNode()
		nextNode, nodeGroups, nodeListVer, reply := n.getTrackerNodeRemote(randomAddr)
		if reply != RaftReplyOk {
			n.inodeMgr.ResumeNode()
			log.Errorf("Failed: UpdateNodeListAsClient, getTrackerNodeRemote, addr=%v, i=%v, err=%v", randomAddr, i, reply)
			if reply == RaftReplyNotLeader {
				// we retry only if the leader has changed
				port, reply2 := CallGetApiPort(n, nextNode.addr)
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
		n.inodeMgr.ResumeNode()
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
		reply := n.rpcClient.CallObjcacheRpc(RpcGetApiIpAndPortCmdId, &api.Void{}, node.addr, n.flags.RpcTimeoutDuration, n.raft.files, nil, nil, msg)
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
	var nodeListReply *common.MembershipListMsg
	var r RaftBasicReply
	node := RaftNode{addr: common.NodeAddrInet4{Port: int(args.GetPort()), Addr: [4]byte{}}, nodeId: args.GetNodeId()}
	copy(node.addr.Addr[:], args.GetIp())
	id := n.CreateCoordinatorId()
	for {
		nextNode, nodeGroups, nodeListVer, reply := n.getTrackerNodeRemote(trackerNode.addr)
		if reply != RaftReplyOk {
			log.Errorf("Failed: RequestRemoveNode, getTrackerNodeRemote, err=%v", reply)
			if reply == RaftReplyNotLeader {
				port, reply2 := CallGetApiPort(n, nextNode.addr)
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
		fn := NewCoordinatorUpdateNodeListOp(false, node, groupAddr, nodeListVer)
		if nextNode.nodeId == n.raft.selfId {
			if r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer); r.reply == RaftReplyOk {
				nodeListReply, r.reply = fn.local(n, id, n.raftGroup.GetNodeListLocal())
			}
		} else {
			nodeListReply, r = fn.remote(n, nextNode.addr, id, nodeListVer)
		}
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: RequestRemoveNode, NewCoordinatorUpdateNodeListOp, leavingNode=%v, nextNode=%v, r=%v", node, nextNode, r)
			if newTracker, ok := fixupRpcErr(n, nextNode, r); ok {
				port, reply2 := CallGetApiPort(n, newTracker.addr)
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
	log.Infof("Success: RequestRemoveNode, leavingNode=%v, trackerNode=%v, nodeListReply=%v", node, trackerNode, nodeListReply)
	return &api.ApiRet{Status: RaftReplyOk}, nil
}

func (n *NodeServer) sendDirtyMetasForNodeJoin(migrationId MigrationId, nodeList *RaftNodeList, newRing *hashring.HashRing, target RaftNode) (reply int32) {
	keys := n.dirtyMgr.GetDirtyMetasForNodeJoin(migrationId, nodeList, newRing, n.raftGroup.selfGroup, target.groupId)
	dirKeys := n.dirtyMgr.GetDirMetasForNodeJoin(n.inodeMgr.GetAllDirectoryMeta(), migrationId, nodeList, newRing, n.raftGroup.selfGroup, target.groupId)
	if len(keys)+len(dirKeys) > 0 {
		metas, files := n.inodeMgr.GetAllMeta(keys)
		dirMetas, dirFiles := n.inodeMgr.GetAllMeta(dirKeys)
		ok := n.rpcMgr.CallRemote(NewRestoreDirtyMetaOp(migrationId, metas, files, dirMetas, dirFiles, target), n.flags.ChunkRpcTimeoutDuration)
		if !ok {
			log.Errorf("Failed: sendDirtyMetasForNodeJoin, CallRemote, target=%v, len(keys)=%v", target, len(keys))
			return RaftReplyFail
		}
		n.dirtyMgr.RecordMigratedRemoveMetas(migrationId, keys...)
		n.dirtyMgr.RecordMigratedRemoveDirMetas(migrationId, dirKeys...)
		for _, meta := range metas {
			log.Infof("Success: sendDirtyMetasForNodeJoin, metadata map: inodeKey=%v, target=%v", meta.GetInodeKey(), target)
		}
		for _, file := range files {
			log.Infof("Success: sendDirtyMetasForNodeJoin, inode to key map: inodeKey=%v, key=%v, target=%v", file.GetInodeKey(), file.GetFilePath(), target)
		}
		for _, meta := range dirMetas {
			log.Infof("Success: sendDirtyMetasForNodeJoin, metadata map (Dir): inodeKey=%v, target=%v", meta.GetInodeKey(), target)
		}
		for _, file := range dirFiles {
			log.Infof("Success: sendDirtyMetasForNodeJoin, inode to key map (Dir): inodeKey=%v, key=%v, target=%v", file.GetInodeKey(), file.GetFilePath(), target)
		}
	}
	return RaftReplyOk
}

func (n *NodeServer) sendDirtyChunksForNodeJoin(migrationId MigrationId, nodeList *RaftNodeList, newRing *hashring.HashRing, target RaftNode) (reply int32) {
	for inodeKey, chunkInfo := range n.dirtyMgr.GetDirtyChunkForNodeJoin(migrationId, nodeList, newRing, n.raftGroup.selfGroup, target.groupId) {
		for offset, version := range chunkInfo.OffsetVersions {
			reply = n.sendDirtyChunkAny(migrationId, nodeList.version, target, inodeKey, offset, version, chunkInfo.chunkSize, chunkInfo.objectSize, n.flags.RpcChunkSizeBytes)
			if reply != RaftReplyOk {
				log.Errorf("Failed: sendDirtyChunkAny, inodeKey=%v, offset=%v, version=%v, reply=%v", inodeKey, offset, version, reply)
				return reply
			}
			n.dirtyMgr.RecordMigratedRemoveChunks(migrationId, inodeKey, offset, version)
		}
	}
	return RaftReplyOk
}

/*************************************
 * methods to remove (shutdown) a Node
 *************************************/

func (n *NodeServer) sendDirtyMetasForNodeLeave(migrationId MigrationId, nodeList *RaftNodeList) (reply int32) {
	for groupId, inodeKeys := range n.dirtyMgr.GetDirtyMetaForNodeLeave(nodeList) {
		leader, ok := n.raftGroup.GetGroupLeader(groupId, nodeList)
		if !ok {
			log.Errorf("Failed: sendDirtyMetasForNodeLeave, GetLeaderNode, groupId=%v", groupId)
			return RaftReplyNoGroup
		}
		metas, files := n.inodeMgr.GetAllMeta(inodeKeys)
		ok = n.rpcMgr.CallRemote(NewRestoreDirtyMetaOp(migrationId, metas, files, nil, nil, leader), n.flags.ChunkRpcTimeoutDuration)
		if !ok {
			log.Errorf("Failed: sendDirtyMetasForNodeLeave, CallRemote, target=%v, len(keys)=%v", leader, len(inodeKeys))
			return RaftReplyFail
		}
		n.dirtyMgr.RecordMigratedRemoveMetas(migrationId, inodeKeys...)
		for _, meta := range metas {
			log.Infof("Success: sendDirtyMetasForNodeLeave, node=%v, inodeKey=%v", leader, meta.GetInodeKey())
		}
		for _, file := range files {
			log.Infof("Success: sendDirtyMetasForNodeLeave, node=%v, inodeKey=%v, key=%v", leader, file.GetInodeKey(), file.GetFilePath())
		}
	}
	return RaftReplyOk
}

func (n *NodeServer) sendDirMetasForNodeLeave(migrationId MigrationId, nodeList *RaftNodeList) (reply int32) {
	for groupId, inodeKeys := range n.dirtyMgr.GetDirMetaForNodeLeave(n.inodeMgr.GetAllDirectoryMeta(), nodeList) {
		leader, ok := n.raftGroup.GetGroupLeader(groupId, nodeList)
		if !ok {
			log.Errorf("Failed: sendDirMetasForNodeLeave, GetLeaderNode, groupId=%v", groupId)
			return RaftReplyNoGroup
		}
		metas, files := n.inodeMgr.GetAllMeta(inodeKeys)
		ok = n.rpcMgr.CallRemote(NewRestoreDirtyMetaOp(migrationId, nil, nil, metas, files, leader), n.flags.ChunkRpcTimeoutDuration)
		if !ok {
			log.Errorf("Failed: sendDirMetasForNodeLeave, CallRemote, target=%v, len(keys)=%v", leader, len(inodeKeys))
			return RaftReplyFail
		}
		n.dirtyMgr.RecordMigratedRemoveMetas(migrationId, inodeKeys...)
		for _, meta := range metas {
			log.Infof("Success: sendDirMetasForNodeLeave, node=%v, inodeKey=%v", leader, meta.GetInodeKey())
		}
		for _, file := range files {
			log.Infof("Success: sendDirMetasForNodeLeave, node=%v, inodeKey=%v, key=%v", leader, file.GetInodeKey(), file.GetFilePath())
		}
	}
	return RaftReplyOk
}

func (n *NodeServer) sendDirtyChunksForNodeLeave(migrationId MigrationId, nodeList *RaftNodeList) (reply int32) {
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
			n.dirtyMgr.RecordMigratedRemoveChunks(migrationId, inodeKey, offset, version)
			log.Infof("Success: sendDirtyChunkAny, node=%v, migrationId=%v, inodeKey=%v, offset=%v, version=%v", leader, migrationId, inodeKey, offset, version)
		}
	}
	return RaftReplyOk
}

func (n *NodeServer) RequestLeave() int32 {
	log.Infof("RequestLeave: Make this Node readonly")
	n.inodeMgr.SuspendNode()
	id := n.CreateCoordinatorId()
	var nodeList *RaftNodeList = nil
	var trackerNode RaftNode
	for i := 0; i < n.flags.MaxRetry; i++ {
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
		fn := NewCoordinatorUpdateNodeListOp(false, n.selfNode, groupAddr, nodeList.version)
		var r RaftBasicReply
		var nodeListReply *common.MembershipListMsg
		if trackerNode.nodeId == n.raft.selfId {
			if r = n.raftGroup.BeginRaftRead(n.raft, nodeList.version); r.reply == RaftReplyOk {
				nodeListReply, r.reply = fn.local(n, id, n.raftGroup.GetNodeListLocal())
			}
		} else {
			nodeListReply, r = fn.remote(n, trackerNode.addr, id, nodeList.version)
		}
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: NodeServer.RequestLeave, NewCoordinatorUpdateNodeListOp, trackerNode=%v, nodeListVer=%v, reply=%v", trackerNode, nodeList.version, r.reply)
			if newTracker, found := fixupRpcErr(n, trackerNode, r); found {
				trackerNode = newTracker
			}
			if needRetry(r.reply) {
				continue
			}
			return r.reply
		} // else {
		//n.raftGroup.UpdateLeader(trackerNode) // we don't need leader info now
		//}
		// delete all nodes/groups this node maintains
		txId := n.raft.GenerateCoordinatorId()
		txId2 := txId.toCoordinatorTxId()
		if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateNodeListLocalCmdId, &common.UpdateNodeListMsg{Nodes: nodeListReply, TxId: txId2.toMsg(), IsAdd: false}); reply != RaftReplyOk {
			log.Errorf("Failed (ignore): RequestLeave, AppendExtendedLogEntry, nodeListReply=%v, reply=%v", nodeListReply, reply)
		}
		if reply := n.raft.RemoveAllServerIds(); reply != RaftReplyOk {
			log.Errorf("Failed (ignore): RequestLeave, Raft.RemoveServerLocal, server=%v, reply=%v", n.raft.selfId, reply)
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
		_, reply := n.CallCoordinator(NewCoordinatorPersistOp(inodeKey, 1000))
		if reply != RaftReplyOk {
			log.Errorf("Failed: PersistAllDirty, persistMetaAny, inodeKey=%v, reply=%v", inodeKey, reply)
		} else {
			log.Infof("Success: PersistAllDirty, persistMetaAny, inodeKey=%v", inodeKey)
		}
	}
	log.Infof("PersistAllDirty: Flush deleted keys on this Node")
	deletedKeys := n.dirtyMgr.CopyAllPrimaryDeletedKeys()
	for key, inodeKey := range deletedKeys {
		_, reply := n.CallCoordinator(NewCoordinatorDeletePersistOp(key, inodeKey, 1000))
		if reply != RaftReplyOk {
			log.Errorf("Failed: PersistAllDirty, persistDeleteMetaAny, key=%v, inodeKey=%v, reply=%v", key, inodeKey, reply)
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
		ret, reply := n.CallCoordinator(NewCoordinatorPersistOp(inodeKey, 1000))
		if reply == RaftReplyOk {
			if ret.extReply == ObjCacheIsNotDirty {
				falsePositives = append(falsePositives, uint64(inodeKey))
			} else {
				log.Infof("Success: PersistAllDirty, persistMetaAny, inodeKey=%v", inodeKey)
			}
		} else {
			log.Warnf("Failed: PersistAllDirty, persistMetaAny, inodeKey=%v, reply=%v", inodeKey, reply)
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
	n.inodeMgr.waitNodeResume()
	if !n.args.ClientMode {
		n.PersistAllDirty()
		n.RequestLeave()
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
	if serverInstance == nil {
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
		if err := n.fs.Reset(); err != nil {
			log.Errorf("Failed: Rejuvenate, fs.Reset, err=%v", err)
			return &api.ApiRet{Status: ErrnoToReply(err)}, nil
		}
	}
	go func() {
		if n.Shutdown(true) {
			if !n.CheckReset() {
				log.Fatalf("Failed: Rejuvenate, CheckReset failure")
			}
			log.Infof("Rejuvenate: confirmed all states are reset. restart a new session of node server...")
			n.Init(n.inodeMgr.back)
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
