/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"hash/crc32"
	"math"
	"time"

	"github.com/IBM/objcache/api"
	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

type SingleShotOp interface {
	name() string
	GetLeader(*NodeServer, *RaftNodeList) (RaftNode, bool)
	local(*NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32)
	remote(*NodeServer, common.NodeAddrInet4, uint64, time.Duration) (interface{}, RaftBasicReply)
	RetToMsg(interface{}, RaftBasicReply) (proto.Message, []SlicedPageBuffer)
	GetCaller(*NodeServer) RpcCaller
}

type GetApiIpAndPortOp struct {
	leader RaftNode
}

func NewGetApiIpAndPortOp(leader RaftNode) GetApiIpAndPortOp {
	return GetApiIpAndPortOp{leader: leader}
}

func NewGetApiIpAndPortOpFromMsg(msg RpcMsg) (fn SingleShotOp, nodeListVer uint64, reply int32) {
	args := &api.Void{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewGetApiIpAndPortOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return GetApiIpAndPortOp{}, uint64(math.MaxUint64), reply
	}
	return GetApiIpAndPortOp{}, uint64(math.MaxUint64), RaftReplyOk
}

func (o GetApiIpAndPortOp) name() string {
	return "GetApiIpAndPort"
}

func (o GetApiIpAndPortOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	return n.args.ApiPort, nil, RaftReplyOk
}

func (o GetApiIpAndPortOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	msg := &common.GetApiPortRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	if r.reply != RaftReplyOk {
		return msg, nil
	}
	msg.ApiPort = ret.(int32)
	return msg, nil
}

func (o GetApiIpAndPortOp) GetCaller(*NodeServer) RpcCaller {
	return NewSingleShotRpcCaller(o, time.Duration(0), true)
}

func (o GetApiIpAndPortOp) remote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64, to time.Duration) (interface{}, RaftBasicReply) {
	msg := &common.GetApiPortRet{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateNodeListCmdId, &common.GetApiPortArgs{}, addr, n.flags.RpcTimeoutDuration, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: CallGetApiPort, CallObjcacheRpc, addr=%v, reply=%v", addr, reply)
		return int32(-1), RaftBasicReply{reply: reply}
	}
	return msg.GetApiPort(), NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o GetApiIpAndPortOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return o.leader, true
}

type GetWorkingMetaRet struct {
	meta     *WorkingMeta
	children map[string]InodeKeyType
}

func NewGetWrokingMetaRetFromMsg(msg *common.GetWorkingMetaRetMsg) *GetWorkingMetaRet {
	children := make(map[string]InodeKeyType)
	for _, childMsg := range msg.GetChildren() {
		children[childMsg.GetName()] = InodeKeyType(childMsg.GetInodeKey())
	}
	return &GetWorkingMetaRet{meta: NewWorkingMetaFromMsg(msg.GetMeta()), children: children}
}

func (u *GetWorkingMetaRet) GetChildrenMsg() []*common.CopiedMetaChildMsg {
	children := make([]*common.CopiedMetaChildMsg, 0)
	for key, value := range u.children {
		children = append(children, &common.CopiedMetaChildMsg{Name: key, InodeKey: uint64(value)})
	}
	return children
}

type GetMetaOp struct {
	inodeKey  InodeKeyType
	key       string
	chunkSize int64
	expireMs  int32
	parent    InodeKeyType
}

func NewGetMetaOp(inodeKey InodeKeyType, key string, chunkSize int64, expireMs int32, parent InodeKeyType) GetMetaOp {
	return GetMetaOp{inodeKey, key, chunkSize, expireMs, parent}
}

func NewGetMetaOpFromMsg(msg RpcMsg) (fn SingleShotOp, nodeListVer uint64, reply int32) {
	args := &common.GetMetaAnyArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewGetMetaOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return GetMetaOp{}, uint64(math.MaxUint64), reply
	}
	parent := InodeKeyType(args.GetParentInodeKey())
	return NewGetMetaOp(InodeKeyType(args.GetInodeKey()), args.GetKey(), args.GetChunkSize(), args.GetExpireMs(), parent), args.GetNodeListVer(), RaftReplyOk
}

func (o GetMetaOp) name() string {
	return "GetMeta"
}

func (o GetMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	meta, children, reply := n.inodeMgr.GetOrFetchWorkingMeta(o.inodeKey, o.key, o.chunkSize, o.expireMs, o.parent)
	if reply != RaftReplyOk {
		log.Errorf("Failed: %v, GetOrFetchWorkingMeta, name=%v, reply=%v", o.name(), o.key, reply)
		return nil, nil, reply
	}
	return &GetWorkingMetaRet{meta: meta, children: children}, nil, RaftReplyOk
}

func (o GetMetaOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	if r.reply != RaftReplyOk {
		return &common.GetWorkingMetaRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	ret2 := ret.(*GetWorkingMetaRet)
	return &common.GetWorkingMetaRetMsg{Meta: ret2.meta.toMsg(), Children: ret2.GetChildrenMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o GetMetaOp) GetCaller(*NodeServer) RpcCaller {
	return NewSingleShotRpcCaller(o, time.Duration(0), false)
}

func (o GetMetaOp) remote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64, to time.Duration) (interface{}, RaftBasicReply) {
	args := &common.GetMetaAnyArgs{
		InodeKey: uint64(o.inodeKey), Key: o.key, ChunkSize: o.chunkSize, ExpireMs: o.expireMs,
		ParentInodeKey: uint64(o.parent), NodeListVer: nodeListVer,
	}
	msg := &common.GetWorkingMetaRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcGetMetaCmdId, args, addr, to, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, addr=%v, name=%v, reply=%v", o.name(), addr, o.key, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	meta := NewWorkingMetaFromMsg(msg.GetMeta())
	var children map[string]InodeKeyType = nil
	if len(msg.GetChildren()) > 0 {
		children = make(map[string]InodeKeyType)
		for _, childMsg := range msg.GetChildren() {
			children[childMsg.GetName()] = InodeKeyType(childMsg.GetInodeKey())
		}
	}
	return &GetWorkingMetaRet{meta: meta, children: children}, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o GetMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

type UpdateMetaAttrOp struct {
	txId     TxId
	inodeKey InodeKeyType
	mode     uint32
	ts       int64
}

func NewUpdateMetaAttrOp(txId TxId, inodeKey InodeKeyType, mode uint32, ts int64) UpdateMetaAttrOp {
	return UpdateMetaAttrOp{txId: txId, inodeKey: inodeKey, mode: mode, ts: ts}
}

func NewUpdateMetaAttrOpFromMsg(msg RpcMsg) (SingleShotOp, uint64, int32) {
	args := &common.UpdateMetaAttrRpcMsg{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewUpdateMetaAttrOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := UpdateMetaAttrOp{
		txId: NewTxIdFromMsg(args.GetTxId()), ts: args.GetTs(),
		inodeKey: InodeKeyType(args.GetInodeKey()), mode: args.GetMode(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o UpdateMetaAttrOp) name() string {
	return "UpdateMetaAttr"
}

func (o UpdateMetaAttrOp) GetTxId() TxId {
	return o.txId
}

func (o UpdateMetaAttrOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	var working *WorkingMeta
	working, reply = n.inodeMgr.UpdateMetaAttr(o.inodeKey, o.mode, o.ts)
	return working, nil, reply
}

func (o UpdateMetaAttrOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.UpdateMetaAttrRpcMsg{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), Mode: o.mode, Ts: o.ts,
	}
	msg := &common.MetaTxMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateMetaAttrCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if msg.GetStatus() != RaftReplyOk {
		return nil, r
	}
	meta := NewWorkingMetaFromMsg(msg.GetNewMeta())
	return meta, r
}

func (o UpdateMetaAttrOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	if r.reply != RaftReplyOk {
		return &common.MetaTxMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	return &common.MetaTxMsg{NewMeta: ret.(*WorkingMeta).toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o UpdateMetaAttrOp) GetCaller(n *NodeServer) RpcCaller {
	return NewSingleShotRpcCaller(o, n.flags.RpcTimeoutDuration, false)
}

func (o UpdateMetaAttrOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

type PrefetchChunkOp struct {
	leader      RaftNode
	offset      int64
	length      int64
	h           MetaRWHandler
	nodeListVer uint64
}

func NewPrefetchChunkOp(leader RaftNode, offset int64, length int64, h MetaRWHandler, nodeListVer uint64) PrefetchChunkOp {
	return PrefetchChunkOp{leader, offset, length, h, nodeListVer}
}

func NewPrefetchChunkOpFromMsg(msg RpcMsg) (fn SingleShotOp, nodeListVer uint64, reply int32) {
	args := &common.PrefetchChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewPrefetchChunkOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return PrefetchChunkOp{}, uint64(math.MaxUint64), reply
	}
	return NewPrefetchChunkOp(RaftNode{}, args.GetOffset(), args.GetLength(), NewMetaRWHandlerFromMsg(args.GetHandler()), 0), args.GetNodeListVer(), RaftReplyOk
}

func (o PrefetchChunkOp) name() string {
	return "PrefetchChunk"
}

func (o PrefetchChunkOp) local(n *NodeServer) (ret interface{}, unused func(*NodeServer), reply int32) {
	for off := o.offset; off < o.offset+o.length && off < o.h.size; off += o.h.chunkSize {
		leader, _, reply := n.raftGroup.getChunkOwnerNodeLocal(o.h.inodeKey, off, o.h.chunkSize)
		if reply != RaftReplyOk {
			log.Errorf("Failed: %v, getKeyOwnerNodeAny, inodeKey=%v, off=%v, reply=%v", o.name(), o.h.inodeKey, off, reply)
			break
		}
		if leader.nodeId == n.raft.selfId {
			go n.inodeMgr.PrefetchChunkThread(o.h, off)
		}
	}
	return nil, nil, RaftReplyOk
}

func (o PrefetchChunkOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o PrefetchChunkOp) GetCaller(*NodeServer) RpcCaller {
	return NewSingleShotRpcCaller(o, time.Duration(0), false)
}

func (o PrefetchChunkOp) remote(n *NodeServer, addr common.NodeAddrInet4, _ uint64, to time.Duration) (interface{}, RaftBasicReply) {
	args := &common.PrefetchChunkArgs{
		Handler:     o.h.toMsg(),
		Offset:      o.offset,
		Length:      o.length,
		NodeListVer: o.nodeListVer,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcPrefetchChunkCmdId, args, addr, to, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, addr=%v, reply=%v", o.name(), addr, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o PrefetchChunkOp) GetLeader(*NodeServer, *RaftNodeList) (RaftNode, bool) {
	return o.leader, true
}

type ReadChunkOp struct {
	h         MetaRWHandler
	offset    int64
	size      int
	blocking  bool
	readAhead bool
}

type ReadChunkOpRet struct {
	bufs  []SlicedPageBuffer
	count int
}

func NewReadChunkOp(h MetaRWHandler, offset int64, size int, blocking bool, readAhead bool) ReadChunkOp {
	return ReadChunkOp{h, offset, size, blocking, readAhead}
}

func NewReadChunkOpFromMsg(msg RpcMsg) (fn SingleShotOp, nodeListVer uint64, reply int32) {
	args := &common.ReadChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewReadChunkOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return ReadChunkOp{}, uint64(math.MaxUint64), reply
	}
	h := NewMetaRWHandlerFromMsg(args.GetHandler())
	return NewReadChunkOp(h, args.GetOffset(), int(args.GetLength()), args.GetBlocking(), args.GetReadAhead()), uint64(math.MaxUint64), RaftReplyOk
}

func (o ReadChunkOp) name() string {
	return "ReadChunk"
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

func (n *NodeServer) VectorReadFastPath(h MetaRWHandler, offset int64, size int, blocking bool) (bufs []SlicedPageBuffer, count int, err error) {
	var r RaftBasicReply
	if _, ok := n.localHistory.Has(h.inodeKey, offset); ok {
		bufs, count, r.reply = n.inodeMgr.VectorReadChunk(h, offset, size, blocking)
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

func (o ReadChunkOp) local(n *NodeServer) (ret interface{}, unused func(*NodeServer), reply int32) {
	if o.readAhead {
		if cachedLength, ok := n.localHistory.Has(o.h.inodeKey, o.offset); ok && cachedLength == o.size {
			return ReadChunkOpRet{}, nil, RaftReplyOk
		}
		go func() {
			bufs, count, reply := n.inodeMgr.VectorReadChunk(o.h, o.offset, o.size, o.blocking)
			if reply == RaftReplyOk {
				n.localHistory.Add(o.h.inodeKey, o.offset, count)
				if o.readAhead {
					for _, buf := range bufs {
						buf.SetEvictable()
					}
				}
			}
		}()
		return ReadChunkOpRet{}, nil, reply
	}
	if !n.flags.DisableLocalWritebackCaching {
		if bufs, count, err := n.VectorReadFastPath(o.h, o.offset, o.size, o.blocking); err == nil {
			return ReadChunkOpRet{bufs: bufs, count: count}, nil, RaftReplyOk
		}
	}
	bufs, count, reply := n.inodeMgr.VectorReadChunk(o.h, o.offset, o.size, o.blocking)
	if reply == RaftReplyOk {
		n.localHistory.Add(o.h.inodeKey, o.offset, count)
	}
	return ReadChunkOpRet{bufs: bufs, count: count}, nil, reply
}

func (o ReadChunkOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	ret2 := ret.(ReadChunkOpRet)
	return &common.ReadChunkRet{Status: RaftReplyOk, Leader: r.GetLeaderNodeMsg(), Count: int32(ret2.count)}, ret2.bufs
}

func (o ReadChunkOp) GetCaller(*NodeServer) RpcCaller {
	return NewSingleShotRpcCaller(o, time.Duration(0), false)
}

func (o ReadChunkOp) __remoteNoCache(n *NodeServer, addr common.NodeAddrInet4) (SlicedPageBuffer, int, RaftBasicReply) {
	begin := time.Now()
	length := int64(CalculateBufferLengthForDownload(o.h, o.offset))
	if int(length) > o.size {
		length = int64(o.size)
	}
	alignedSize := length
	if length%PageSize != 0 {
		alignedSize = length - length%PageSize + PageSize
	}
	remoteBuf, err := GetPageBuffer(alignedSize)
	if err != nil {
		log.Errorf("Failed: %v, GetPageBuffer, chunkSize=%v, err=%v", o.name(), o.h.chunkSize, err)
		return SlicedPageBuffer{}, 0, RaftBasicReply{reply: ErrnoToReply(err)}
	}
	var r = RaftBasicReply{reply: RaftReplyOk}
	var c = 0
	var rpcTime time.Duration
	for c < int(length) {
		args := &common.ReadChunkArgs{
			Handler: o.h.toMsg(), Offset: o.offset + int64(c), Length: int32(int(length) - c), Blocking: o.blocking, ReadAhead: o.readAhead,
		}
		msg := &common.ReadChunkRet{}
		rpcArgs := &RpcSeqNumArgs{data: remoteBuf.Buf[c:length]}
		rpcBegin := time.Now()
		r.reply = n.rpcClient.CallObjcacheRpc(RpcReadChunkCmdId, args, addr, time.Duration(0), n.raft.extLogger, nil, rpcArgs, msg)
		rpcTime += time.Since(rpcBegin)
		if r.reply != RaftReplyOk {
			ReturnPageBuffer(remoteBuf)
			log.Errorf("Failed: %v, CallObjcacheRpc, Node=%v, meta=%v, offset=%v, reply=%v", o.name(), addr, o.h, o.offset, r.reply)
			return SlicedPageBuffer{}, 0, r
		}
		r = NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		if r.reply != RaftReplyOk {
			ReturnPageBuffer(remoteBuf)
			log.Errorf("Failed: %v, CallObjcacheRpc, Node=%v, meta=%v, offset=%v, reply=%v", o.name(), addr, o.h, o.offset, r.reply)
			return SlicedPageBuffer{}, 0, r
		}
		c += int(msg.GetCount())
	}
	remoteBuf.Buf = remoteBuf.Buf[:c]
	remoteBuf.Up()
	slice := remoteBuf.AsSlice()
	log.Debugf("Success: %v, node=%v, h=%v, offset=%v, len=%v, count=%v, rpcTime=%v, elapsed=%v", o.name(), addr, o.h, o.offset, length, c, rpcTime, time.Since(begin))
	return slice, c, r
}

func (o ReadChunkOp) __remote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64, to time.Duration) (SlicedPageBuffer, int, RaftBasicReply) {
	if n.flags.DisableLocalWritebackCaching {
		return o.__remoteNoCache(n, addr)
	}
	begin := time.Now()
	length := CalculateBufferLengthForDownload(o.h, o.offset)
	if o.blocking {
		if slice, ok := n.remoteCache.GetCacheWithFillWait(o.h.inodeKey, o.offset, length); ok {
			log.Debugf("Success: %v.remote (cached), h=%v, offset=%v, size=%v", o.name(), o.h, o.offset, o.size)
			return slice, len(slice.Buf), RaftBasicReply{reply: RaftReplyOk}
		}
	} else {
		p, beginFill := n.remoteCache.GetCacheOrBeginFill(o.h.inodeKey, o.offset)
		if p != nil {
			log.Debugf("Success: %v.remote (cached), h=%v, offset=%v, size=%v", o.name(), o.h, o.offset, o.size)
			slice := p.AsSlice(n.remoteCache.dec)
			return slice, len(slice.Buf), RaftBasicReply{reply: RaftReplyOk}
		} else if !beginFill {
			return SlicedPageBuffer{}, 0, RaftBasicReply{reply: ErrnoToReply(unix.EAGAIN)}
		}
	}

	remoteBuf, err := n.remoteCache.GetRemotePageBuffer(o.h.inodeKey, o.offset, o.h.chunkSize, o.blocking)
	if err != nil {
		if o.blocking || (!o.blocking && err != unix.EAGAIN) {
			log.Errorf("Failed: %v.remote, GetPageBuffer, chunkSize=%v, err=%v", o.name(), o.h.chunkSize, err)
		}
		n.remoteCache.EndFill(o.h.inodeKey, o.offset)
		return SlicedPageBuffer{}, 0, RaftBasicReply{reply: ErrnoToReply(err)}
	}
	var r = RaftBasicReply{reply: RaftReplyOk}
	var c = 0
	var rpcTime time.Duration
	for c < length {
		args := &common.ReadChunkArgs{
			Handler: o.h.toMsg(), Offset: o.offset + int64(c), Length: int32(length - c),
		}
		msg := &common.ReadChunkRet{}
		rpcArgs := &RpcSeqNumArgs{data: remoteBuf.buf.Buf[c:length]}
		rpcBegin := time.Now()
		r.reply = n.rpcClient.CallObjcacheRpc(RpcReadChunkCmdId, args, addr, time.Duration(0), n.raft.extLogger, nil, rpcArgs, msg)
		rpcTime += time.Since(rpcBegin)
		if r.reply != RaftReplyOk {
			n.remoteCache.ReleaseInFlightBuffer(remoteBuf)
			n.remoteCache.EndFill(o.h.inodeKey, o.offset)
			log.Errorf("Failed: %v.remote, CallObjcacheRpc, addr=%v, meta=%v, offset=%v, reply=%v", o.name(), addr, o.h, o.offset, r.reply)
			return SlicedPageBuffer{}, 0, r
		}
		r = NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		if r.reply != RaftReplyOk {
			n.remoteCache.ReleaseInFlightBuffer(remoteBuf)
			n.remoteCache.EndFill(o.h.inodeKey, o.offset)
			log.Errorf("Failed: %v.remote, CallObjcacheRpc, addr=%v, meta=%v, offset=%v, reply=%v", o.name(), addr, o.h, o.offset, r.reply)
			return SlicedPageBuffer{}, 0, r
		}
		c += int(msg.GetCount())
	}
	remoteBuf.buf.Buf = remoteBuf.buf.Buf[:c]
	remoteBuf.buf.Up()
	slice := remoteBuf.AsSlice(n.remoteCache.dec)
	n.remoteCache.EndFillWithPut(remoteBuf)
	if o.readAhead {
		slice.SetEvictable()
		return SlicedPageBuffer{}, 0, r
	}
	log.Debugf("Success: %v.remote, addr=%v, h=%v, offset=%v, len=%v, count=%v, rpcTime=%v, elapsed=%v", o.name(), addr, o.h, o.offset, length, c, rpcTime, time.Since(begin))
	return slice, c, r
}

func (o ReadChunkOp) remote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64, to time.Duration) (interface{}, RaftBasicReply) {
	if o.readAhead {
		if ok := n.remoteCache.Has(o.h.inodeKey, o.offset); ok {
			return ReadChunkOpRet{}, RaftBasicReply{reply: RaftReplyOk}
		}
		go o.__remote(n, addr, nodeListVer, to)
		return ReadChunkOpRet{}, RaftBasicReply{reply: RaftReplyOk}
	}

	if !n.flags.DisableLocalWritebackCaching {
		if bufs, count, err := n.VectorReadFastPath(o.h, o.offset, o.size, o.blocking); err == nil {
			return ReadChunkOpRet{bufs: bufs, count: count}, RaftBasicReply{reply: RaftReplyOk}
		}
	}
	slice, c, r := o.__remote(n, addr, nodeListVer, to)
	if r.reply != RaftReplyOk {
		return ReadChunkOpRet{}, r
	}
	return ReadChunkOpRet{bufs: []SlicedPageBuffer{slice}, count: c}, r
}

func (o ReadChunkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getChunkOwnerNodeLocalNew(l, o.h.inodeKey, o.offset, o.h.chunkSize)
}

///////////////////////////////////////////////////////////////////

type RestoreDirtyMetaOp struct {
	target RaftNode
	s      *Snapshot
}

func NewRestoreDirtyMetaOp(s *Snapshot, target RaftNode) RestoreDirtyMetaOp {
	return RestoreDirtyMetaOp{target: target, s: s}
}

func NewRestoreDirtyMetaOpFromMsg(msg RpcMsg) (fn SingleShotOp, nodeListVer uint64, reply int32) {
	args := &common.SnapshotMsg{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewRestoreDirtyMetaOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return RestoreDirtyMetaOp{}, uint64(math.MaxUint64), reply
	}
	return RestoreDirtyMetaOp{s: NewSnapshotFromMsg(args)}, uint64(math.MaxUint64), RaftReplyOk
}

func (o RestoreDirtyMetaOp) name() string {
	return "RestoreDirtyMeta"
}

func (o RestoreDirtyMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	if reply = n.raft.AppendExtendedLogEntry(NewRecordMigratedAddMetaCommand(o.s)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, migrationId=%v, reply=%v", o.name(), o.s.migrationId, reply)
	}
	return nil, nil, reply
}

func (o RestoreDirtyMetaOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o RestoreDirtyMetaOp) GetCaller(*NodeServer) RpcCaller {
	return NewSingleShotRpcCaller(o, time.Duration(0), true)
}

func (o RestoreDirtyMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	lastReply := RaftBasicReply{reply: RaftReplyOk}
	for {
		next := o.s.GetNext()
		if next == nil {
			break
		}
		msg := &common.Ack{}
		if reply := n.rpcClient.CallObjcacheRpc(RpcRestoreDirtyMetasCmdId, next.toMsg(), sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
			log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
			return nil, RaftBasicReply{reply: reply}
		}
		lastReply = NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		if lastReply.reply != RaftReplyOk {
			break
		}
	}
	log.Infof("Success: %v, target=%v, migrationId=%v, len(metas)=%v, len(files)=%v, len(dirents)=%v",
		o.name(), o.target, o.s.migrationId, len(o.s.metas), len(o.s.files), len(o.s.dirents))
	return nil, lastReply
}

func (o RestoreDirtyMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return o.target, true
}

///////////////////////////////////////////////////////////////////

type RestoreDirtyChunkOp struct {
	migrationId MigrationId
	target      RaftNode
	inodeKey    InodeKeyType
	chunkSize   int64
	offset      int64
	objectSize  int64
	chunkVer    uint32
	bufs        [][]byte

	logId        LogIdType
	logOffset    int64
	dataLength   uint32
	extBufChksum [4]byte
}

func NewRestoreDirtyChunkOp(migrationId MigrationId, target RaftNode, inodeKey InodeKeyType, chunkSize int64, offset int64, objectSize int64, chunkVer uint32, bufs [][]byte) RestoreDirtyChunkOp {
	ret := RestoreDirtyChunkOp{
		migrationId: migrationId, target: target, inodeKey: inodeKey,
		chunkSize: chunkSize, offset: offset, objectSize: objectSize, chunkVer: chunkVer, bufs: bufs,
	}
	chksum := crc32.NewIEEE()
	for _, buf := range bufs {
		_, _ = chksum.Write(buf)
	}
	copy(ret.extBufChksum[:], chksum.Sum(nil))
	return ret
}

func NewRestoreDirtyChunkOpFromProtoMsg(m proto.Message, logId LogIdType, logOffset int64, dataLength uint32) (fn SingleShotOp, nodeListVer uint64) {
	args := m.(*common.RestoreDirtyChunksArgs)
	orig := RestoreDirtyChunkOp{
		migrationId: NewMigrationIdFromMsg(args.GetMigrationId()), inodeKey: InodeKeyType(args.GetInodeKey()), chunkSize: args.GetChunkSize(),
		offset: args.GetOffset(), chunkVer: args.GetChunkVer(), objectSize: args.GetObjectSize(),
		logId: logId, logOffset: logOffset, dataLength: dataLength,
	}
	copy(orig.extBufChksum[:], args.GetExtBufChksum())
	return orig, uint64(math.MaxUint64)
}

func (o RestoreDirtyChunkOp) name() string {
	return "RestoreDirtyChunk"
}

func (o RestoreDirtyChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	reply = n.inodeMgr.AppendStagingChunkLog(o.inodeKey, o.offset, o.logId, o.logOffset, o.dataLength, o.extBufChksum)
	if reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendStagingChunkLog, inodeKey=%v, offset=%v, logId=%v, logOffset=%v, dataLength=%v, reply=%v",
			o.name(), o.inodeKey, o.offset, o.logId, o.logOffset, o.dataLength, reply)
		return nil, nil, reply
	}
	// we do not need fetchKey here since all the content is already fetched by the sender and random writes later will need it neither
	chunk := n.inodeMgr.GetChunk(o.inodeKey, o.offset, o.chunkSize)
	chunk.lock.RLock()
	working, _ := chunk.GetWorkingChunk(o.chunkVer, false)
	if working == nil {
		working = chunk.NewWorkingChunk(o.chunkVer)
	}
	chunk.lock.RUnlock()
	working.AddStag(&StagingChunk{
		filled: 1, slop: o.offset % o.chunkSize, length: int64(o.dataLength),
		updateType: StagingChunkData, logOffset: o.logOffset,
	})
	msg := &common.AppendCommitUpdateChunksMsg{
		InodeKey: uint64(o.inodeKey), ChunkSize: o.chunkSize, Version: working.chunkVer, ObjectSize: o.objectSize,
		Chunks: []*common.WorkingChunkAddMsg{{Offset: o.offset, Stagings: working.toStagingChunkAddMsg()}},
	}
	if reply = n.raft.AppendExtendedLogEntry(NewRecordMigratedAddChunkCommand(o.migrationId, msg)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, migrationId=%v, reply=%v", o.name(), o.migrationId, reply)
	} else {
		log.Infof("Success: %v, target=%v, migrationId=%v, inodeKey=%v, offset=%v, dataLength=%v",
			o.name(), o.target, o.migrationId, o.inodeKey, o.offset, o.dataLength)
	}
	return nil, nil, reply
}

func (o RestoreDirtyChunkOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o RestoreDirtyChunkOp) GetCaller(*NodeServer) RpcCaller {
	return NewSingleShotRpcCaller(o, time.Duration(0), true)
}

func (o RestoreDirtyChunkOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.RestoreDirtyChunksArgs{
		MigrationId: o.migrationId.toMsg(), InodeKey: uint64(o.inodeKey),
		ChunkSize: o.chunkSize, ChunkVer: o.chunkVer, Offset: o.offset, ObjectSize: o.objectSize,
	}
	copy(args.ExtBufChksum, o.extBufChksum[:])
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcRestoreDirtyChunksCmdId, args, sa, timeout, n.raft.extLogger, o.bufs, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o RestoreDirtyChunkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return o.target, true
}

///////////////////////////////////////////////////////////////////

var SingleShotOpMap = map[uint16]func(RpcMsg) (SingleShotOp, uint64, int32){
	RpcGetMetaCmdId:           NewGetMetaOpFromMsg,
	RpcReadChunkCmdId:         NewReadChunkOpFromMsg,
	RpcPrefetchChunkCmdId:     NewPrefetchChunkOpFromMsg,
	RpcRestoreDirtyMetasCmdId: NewRestoreDirtyMetaOpFromMsg,
	RpcGetApiIpAndPortCmdId:   NewGetApiIpAndPortOpFromMsg,
	RpcUpdateMetaAttrCmdId:    NewUpdateMetaAttrOpFromMsg,
}

func (o *RpcMgr) RestoreDirtyChunksBottomHalf(m proto.Message, logId LogIdType, logOffset int64, dataLength uint32, reply int32) (proto.Message, []SlicedPageBuffer) {
	if reply != RaftReplyOk {
		return &common.Ack{Status: reply}, nil
	}
	fn, nodeListVer := NewRestoreDirtyChunkOpFromProtoMsg(m, logId, logOffset, dataLength)
	ret, r := fn.GetCaller(o.n).ExecLocalInRpc(o.n, nodeListVer)
	return fn.RetToMsg(ret.ext, r)
}
