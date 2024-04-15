/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"hash/crc32"
	"math"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/IBM/objcache/common"
)

type ParticipantOp interface {
	name() string
	GetTxId() TxId
	GetLeader(*NodeServer, *RaftNodeList) (RaftNode, bool)
	local(*NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32)
	writeLog(*NodeServer, interface{}) int32
	remote(*NodeServer, common.NodeAddrInet4, uint64, time.Duration) (interface{}, RaftBasicReply)
	RetToMsg(interface{}, RaftBasicReply) (proto.Message, []SlicedPageBuffer)
	GetCaller(*NodeServer) RpcCaller
}

///////////////////////////////////////////////////////////////////
//////////////////// Commit/Abort Operations //////////////////////
///////////////////////////////////////////////////////////////////

type AbortParticipantOp struct {
	rpcSeqNum  TxId
	abortTxIds []*common.TxIdMsg
	groupId    string
	nodeLock   bool
}

func NewAbortParticipantOp(rpcSeqNum TxId, abortTxIds []*common.TxIdMsg, groupId string, nodeLock bool) AbortParticipantOp {
	return AbortParticipantOp{rpcSeqNum: rpcSeqNum, abortTxIds: abortTxIds, groupId: groupId, nodeLock: nodeLock}
}

func NewAbortParticipantOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.AbortCommitArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewAbortParticipantOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	return AbortParticipantOp{
		rpcSeqNum: NewTxIdFromMsg(args.GetRpcSeqNum()), abortTxIds: args.GetAbortTxIds(), nodeLock: args.GetNodeLock(),
	}, args.GetNodeListVer(), RaftReplyOk
}

func (o AbortParticipantOp) name() string {
	return "AbortParticipant"
}

func (o AbortParticipantOp) GetTxId() TxId {
	return o.rpcSeqNum
}

func (o AbortParticipantOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	old := make([]RpcRet, 0)
	txIds := make([]TxId, 0)
	for _, msg := range o.abortTxIds {
		txId := NewTxIdFromMsg(msg)
		op, ok := n.rpcMgr.DeleteAndGet(txId)
		if !ok {
			log.Infof("%v, skip inactive tx, txId=%v", o.name(), txId)
			continue
		}
		txIds = append(txIds, txId)
		old = append(old, op.ret)
	}
	// do not roll back at failures around networking (roll back for disk failures, though)
	// retried abort/commit will notice resolution
	if len(txIds) == 0 {
		log.Infof("%v, nothing to do", o.name())
		return nil, nil, RaftReplyOk
	}
	if reply = n.raft.AppendExtendedLogEntry(NewAbortTxCommand(txIds)); reply != RaftReplyOk {
		for i := 0; i < len(txIds); i++ {
			n.rpcMgr.Record(txIds[i], old[i])
		}
		log.Errorf("Failed: %v, AppendExtendedLogEntry, abortTxIds=%v, reply=%v", o.name(), o.abortTxIds, reply)
		return nil, nil, reply
	} else {
		log.Infof("%v, recorded an abort log, txIds=%v", o.name(), o.abortTxIds)
	}
	unlock = func(n *NodeServer) {
		for i := len(txIds) - 1; i >= 0; i-- {
			if old[i].unlock != nil {
				old[i].unlock(n)
			}
		}
	}
	return nil, unlock, RaftReplyOk
}

func (o AbortParticipantOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o AbortParticipantOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	msg := &common.Ack{}
	args := &common.AbortCommitArgs{
		RpcSeqNum: o.rpcSeqNum.toMsg(), AbortTxIds: o.abortTxIds, NodeListVer: nodeListVer, NodeLock: o.nodeLock,
	}
	if reply := n.rpcClient.CallObjcacheRpc(RpcAbortParticipantCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o AbortParticipantOp) RetToMsg(_ interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o AbortParticipantOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCommitAbortRpcCaller(o, n.flags.CommitRpcTimeoutDuration, o.nodeLock)
}

func (o AbortParticipantOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.GetGroupLeader(o.groupId, l)
}

///////////////////////////////////////////////////////////////////

type CommitParticipantOp struct {
	rpcSeqNum  TxId
	commitTxId TxId
	groupId    string
	node       RaftNode
	nodeLock   bool
}

func NewCommitParticipantOp(rpcSeqNum TxId, commitTxId TxId, groupId string) CommitParticipantOp {
	return CommitParticipantOp{rpcSeqNum: rpcSeqNum, commitTxId: commitTxId, groupId: groupId, nodeLock: false}
}

func NewCommitParticipantOpForUpdateNode(rpcSeqNum TxId, commitTxId TxId, node RaftNode) CommitParticipantOp {
	return CommitParticipantOp{rpcSeqNum: rpcSeqNum, commitTxId: commitTxId, node: node, groupId: node.groupId, nodeLock: true}
}

func NewCommitParticipantOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.CommitArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCommitParticipantOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	return CommitParticipantOp{
		rpcSeqNum: NewTxIdFromMsg(args.GetRpcSeqNum()), commitTxId: NewTxIdFromMsg(args.GetCommitTxId()), nodeLock: args.GetNodeLock(),
	}, args.GetNodeListVer(), RaftReplyOk
}

func (o CommitParticipantOp) name() string {
	return "CommitParticipant"
}

func (o CommitParticipantOp) GetTxId() TxId {
	return o.rpcSeqNum
}

func (o CommitParticipantOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	if o.node.groupId != "" {
		return o.node, true
	}
	return n.raftGroup.GetGroupLeader(o.groupId, l)
}

func (o CommitParticipantOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	op, ok := n.rpcMgr.DeleteAndGet(o.commitTxId)
	if !ok {
		log.Infof("%v, skip inactive tx, txId=%v", o.name(), o.commitTxId)
		return nil, nil, RaftReplyOk
	}
	// do not roll back at failures around networking (roll back for disk failures, though)
	// retried abort/commit will notice resolution
	if _, _, reply = n.raft.AppendEntriesLocal(NewCommitCommand(o.commitTxId), nil); reply != RaftReplyOk {
		n.rpcMgr.Record(o.commitTxId, op.ret)
		log.Errorf("Failed: %v, AppendEntriesLocal, commitTxId=%v, reply=%v", o.name(), o.commitTxId, reply)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, commitTxId=%v, unlocked=%v", o.name(), o.commitTxId, op.ret.unlock != nil)
	return nil, op.ret.unlock, RaftReplyOk
}

func (o CommitParticipantOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o CommitParticipantOp) RetToMsg(_ interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o CommitParticipantOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCommitAbortRpcCaller(o, n.flags.RpcTimeoutDuration, o.nodeLock)
}

func (o CommitParticipantOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.CommitArgs{
		RpcSeqNum: o.rpcSeqNum.toMsg(), CommitTxId: o.commitTxId.toMsg(), NodeListVer: nodeListVer, NodeLock: o.nodeLock,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitParticipantCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

///////////////////////////////////////////////////////////////////
/////////////////// Prepare Meta Operations ///////////////////////
///////////////////////////////////////////////////////////////////

type CreateMetaOp struct {
	txId           TxId
	inodeKey       InodeKeyType
	parentInodeKey InodeKeyType
	newKey         string
	chunkSize      int64
	expireMs       int32
	mode           uint32
}

func NewCreateMetaOp(txId TxId, inodeKey InodeKeyType, parentInodeKey InodeKeyType, newKey string, mode uint32, chunkSize int64, expireMs int32) CreateMetaOp {
	return CreateMetaOp{txId: txId, inodeKey: inodeKey, parentInodeKey: parentInodeKey, newKey: newKey, mode: mode, chunkSize: chunkSize, expireMs: expireMs}
}

func NewCreateMetaOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.BeginCreateMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCreateMetaOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CreateMetaOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()),
		parentInodeKey: InodeKeyType(args.GetParentInodeKey()), newKey: args.GetNewKey(), chunkSize: args.GetChunkSize(),
		mode: args.GetMode(), expireMs: args.GetExpireMs(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CreateMetaOp) name() string {
	return "CreateMeta"
}

func (o CreateMetaOp) GetTxId() TxId {
	return o.txId
}

func (o CreateMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	stag, unlock, reply := n.inodeMgr.PrepareCreateMeta(o.inodeKey, o.chunkSize, o.expireMs, o.mode)
	if reply != RaftReplyOk {
		log.Errorf("Failed: %v, prepareCreateMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, inodeKey=%v, txId=%v", o.name(), o.inodeKey, o.txId)
	return stag, unlock, RaftReplyOk
}

func (o CreateMetaOp) writeLog(n *NodeServer, ext interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(NewCreateMetaCommand(o.txId, ext.(*WorkingMeta), o.newKey, o.parentInodeKey)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o CreateMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginCreateMetaArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey),
		ParentInodeKey: uint64(o.parentInodeKey), NewKey: o.newKey, ChunkSize: o.chunkSize, Mode: o.mode, ExpireMs: o.expireMs,
	}
	msg := &common.MetaTxMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCreateMetaCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
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

func (o CreateMetaOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	if r.reply != RaftReplyOk {
		return &common.MetaTxMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	return &common.MetaTxMsg{NewMeta: ret.(*WorkingMeta).toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o CreateMetaOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, false)
}

func (o CreateMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type CreateChildMetaOp struct {
	txId          TxId
	inodeKey      InodeKeyType
	key           string
	childName     string
	childInodeKey InodeKeyType
	childIsDir    bool
}

func NewCreateChildMetaOp(txId TxId, inodeKey InodeKeyType, key string, childName string, childInodeKey InodeKeyType, childIsDir bool) CreateChildMetaOp {
	return CreateChildMetaOp{txId: txId, inodeKey: inodeKey, key: key, childName: childName, childInodeKey: childInodeKey, childIsDir: childIsDir}
}

func NewCreateChildMetaOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.BeginCreateChildMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCreateChildMetaOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CreateChildMetaOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()),
		childName: args.GetChildName(), childInodeKey: InodeKeyType(args.GetChildInodeKey()), childIsDir: args.GetChildIsDir(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CreateChildMetaOp) name() string {
	return "CreateChildMeta"
}

func (o CreateChildMetaOp) GetTxId() TxId {
	return o.txId
}

func (o CreateChildMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	meta, children, unlock, reply := n.inodeMgr.PrepareUpdateParent(o.inodeKey, false)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, PrepareUpdateParent, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	old, ok := children[o.childName]
	if ok && old != o.inodeKey {
		unlock(n)
		//race condition. abort. we can check retried transaction by checking the inode key
		log.Errorf("Failed: %v, child is already exist. inodeKey=%v (requested: %v)", o.name(), old, o.inodeKey)
		return nil, nil, ErrnoToReply(unix.EEXIST)
	}
	r := UpdateParentRet{info: NewUpdateParentInfo(meta.inodeKey, o.key, o.childInodeKey, o.childName, "", o.childIsDir)}
	log.Debugf("Success: %v, inodeKey=%v, childName=%v, newInodeId=%v, txId=%v", o.name(), o.inodeKey, o.childName, o.inodeKey, o.txId)
	return r, unlock, RaftReplyOk
}

func (o CreateChildMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	r := ret.(UpdateParentRet)
	if reply := n.raft.AppendExtendedLogEntry(NewUpdateParentMetaCommand(o.txId, r.info)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o CreateChildMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginCreateChildMetaArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey),
		ChildInodeKey: uint64(o.childInodeKey), ChildName: o.childName, ChildIsDir: o.childIsDir,
	}
	msg := &common.UpdateParentRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCreateChildMetaCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return NewUpdateParentRetFromMsg(msg)
}

func (o CreateChildMetaOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return ret.(UpdateParentRet).toMsg(r), nil
}

func (o CreateChildMetaOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, false)
}

func (o CreateChildMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type TruncateMetaOp struct {
	txId     TxId
	inodeKey InodeKeyType
	newSize  int64
}

func NewTruncateMetaOp(txId TxId, inodeKey InodeKeyType, newSize int64) TruncateMetaOp {
	return TruncateMetaOp{txId: txId, inodeKey: inodeKey, newSize: newSize}
}

func NewTruncateMetaOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.BeginTruncateMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewTruncateMetaOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := TruncateMetaOp{txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), newSize: args.GetNewObjectSize()}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o TruncateMetaOp) name() string {
	return "TruncateMeta"
}

func (o TruncateMetaOp) GetTxId() TxId {
	return o.txId
}

func (o TruncateMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey, false)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareCommitMetaLocal, inodeKey=%v, newSize=%v, reply=%v", o.name(), o.inodeKey, o.newSize, reply)
		return nil, nil, reply
	}
	meta.size = o.newSize
	log.Debugf("Success: %v, inodeKey=%v, size=%v->%v", o.name(), o.inodeKey, meta.prevVer.size, o.newSize)
	return meta, unlock, RaftReplyOk
}

func (o TruncateMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(NewUpdateMetaCommand(o.txId, ret.(*WorkingMeta))); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o TruncateMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginTruncateMetaArgs{NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), NewObjectSize: o.newSize}
	msg := &common.MetaTxWithPrevMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcTruncateMetaCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	meta := NewWorkingMetaFromMsg(msg.GetNewMeta())
	meta.prevVer = NewWorkingMetaFromMsg(msg.GetPrev())
	return meta, r
}

func (o TruncateMetaOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	if r.reply != RaftReplyOk {
		return &common.MetaTxWithPrevMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	meta := ret.(*WorkingMeta)
	return &common.MetaTxWithPrevMsg{NewMeta: meta.toMsg(), Prev: meta.prevVer.toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o TruncateMetaOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, false)
}

func (o TruncateMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type UpdateMetaSizeOp struct {
	txId     TxId
	inodeKey InodeKeyType
	newSize  int64
	mTime    int64
	mode     uint32
}

func NewUpdateMetaSizeOp(txId TxId, inodeKey InodeKeyType, newSize int64, mTime int64, mode uint32) UpdateMetaSizeOp {
	return UpdateMetaSizeOp{txId: txId, inodeKey: inodeKey, newSize: newSize, mTime: mTime, mode: mode}
}

func NewUpdateMetaSizeOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.BeginUpdateMetaSizeArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewUpdateMetaSizeOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := UpdateMetaSizeOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), newSize: args.GetNewSize(), mTime: args.GetMTime(), mode: args.GetMode(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o UpdateMetaSizeOp) name() string {
	return "UpdateMetaSize"
}

func (o UpdateMetaSizeOp) GetTxId() TxId {
	return o.txId
}

func (o UpdateMetaSizeOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey, false)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v prepareCommitMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	prev := meta.prevVer
	if prev.IsDir() {
		unlock(n)
		log.Errorf("Failed: %v, cannot update a directory, inodeKey=%v", o.name(), o.inodeKey)
		return nil, nil, ErrnoToReply(unix.EINVAL)
	}
	if prev.size < o.newSize {
		meta.size = o.newSize
	}
	meta.mTime = o.mTime
	meta.mode = o.mode
	log.Debugf("Success: %v, inodeKey=%v, size=%v->%v, mTime=%v->%v, mode=%v->%v, txId=%v",
		o.name(), o.inodeKey, prev.size, meta.size, prev.mTime, meta.mTime, prev.mode, meta.mode, o.txId)
	return meta, unlock, RaftReplyOk
}

func (o UpdateMetaSizeOp) writeLog(n *NodeServer, ret interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(NewUpdateMetaCommand(o.txId, ret.(*WorkingMeta))); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o UpdateMetaSizeOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	msg := &common.MetaTxWithPrevMsg{}
	args := &common.BeginUpdateMetaSizeArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), NewSize: o.newSize, MTime: o.mTime, Mode: o.mode,
	}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateMetaSizeCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	meta := NewWorkingMetaFromMsg(msg.GetNewMeta())
	meta.prevVer = NewWorkingMetaFromMsg(msg.GetPrev())
	return meta, r
}

func (o UpdateMetaSizeOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	if r.reply != RaftReplyOk {
		return &common.MetaTxWithPrevMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	meta := ret.(*WorkingMeta)
	return &common.MetaTxWithPrevMsg{NewMeta: meta.toMsg(), Prev: meta.prevVer.toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o UpdateMetaSizeOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, false)
}

func (o UpdateMetaSizeOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type DeleteMetaOp struct {
	txId       TxId
	inodeKey   InodeKeyType
	removedKey string
}

func NewDeleteMetaOp(txId TxId, inodeKey InodeKeyType, removedKey string) DeleteMetaOp {
	return DeleteMetaOp{txId: txId, inodeKey: inodeKey, removedKey: removedKey}
}

func NewDeleteMetaOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.BeginDeleteMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewDeleteMetaOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := DeleteMetaOp{txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), removedKey: args.GetRemovedKey()}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o DeleteMetaOp) name() string {
	return "DeleteMeta"
}

func (o DeleteMetaOp) GetTxId() TxId {
	return o.txId
}

func (o DeleteMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey, true)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareCommitMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	// do not care if removedKey does exist or not here to make this idempotent
	meta.size = 0
	log.Debugf("Success: %v, inodeKey=%v, txId=%v", o.name(), o.inodeKey, o.txId)
	return meta, unlock, RaftReplyOk
}

func (o DeleteMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(NewDeleteMetaCommand(o.txId, ret.(*WorkingMeta), o.removedKey)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o DeleteMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginDeleteMetaArgs{NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), RemovedKey: o.removedKey}
	msg := &common.MetaTxWithPrevMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcDeleteMetaCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	meta := NewWorkingMetaFromMsg(msg.GetNewMeta())
	meta.prevVer = NewWorkingMetaFromMsg(msg.GetPrev())
	return meta, r
}

func (o DeleteMetaOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	if r.reply != RaftReplyOk {
		return &common.MetaTxWithPrevMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	meta := ret.(*WorkingMeta)
	return &common.MetaTxWithPrevMsg{NewMeta: meta.toMsg(), Prev: meta.prevVer.toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o DeleteMetaOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, false)
}

func (o DeleteMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type UpdateParentRet struct {
	info          *UpdateParentInfo
	mayErrorReply int32
}

func NewUpdateParentRetFromMsg(msg *common.UpdateParentRetMsg) (UpdateParentRet, RaftBasicReply) {
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return UpdateParentRet{}, r
	}
	return UpdateParentRet{
		info: NewUpdateParentInfoFromMsg(msg.GetInfo()), mayErrorReply: msg.GetMayErrorReply(),
	}, r
}

func (d UpdateParentRet) toMsg(r RaftBasicReply) *common.UpdateParentRetMsg {
	if r.reply != RaftReplyOk {
		return &common.UpdateParentRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg(), MayErrorReply: d.mayErrorReply}
	}
	return &common.UpdateParentRetMsg{
		Status: r.reply, Leader: r.GetLeaderNodeMsg(), Info: d.info.toMsg(), MayErrorReply: d.mayErrorReply,
	}
}

type UnlinkMetaOp struct {
	txId       TxId
	inodeKey   InodeKeyType
	parentKey  string
	childName  string
	childIsDir bool
}

func NewUnlinkMetaOp(txId TxId, inodeKey InodeKeyType, parentKey string, childName string, childIsDir bool) UnlinkMetaOp {
	return UnlinkMetaOp{txId: txId, inodeKey: inodeKey, parentKey: parentKey, childName: childName, childIsDir: childIsDir}
}

func NewUnlinkMetaOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.BeginUnlinkMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewUnlinkMetaOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := UnlinkMetaOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()),
		parentKey: args.GetParentKey(), childName: args.GetChildName(), childIsDir: args.GetChildIsDir(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o UnlinkMetaOp) name() string {
	return "UnlinkMeta"
}

func (o UnlinkMetaOp) GetTxId() TxId {
	return o.txId
}

func (o UnlinkMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	meta, children, unlock, reply := n.inodeMgr.PrepareUpdateParent(o.inodeKey, false)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, PrepareUpdateParent, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, unlock, reply
	}
	// To make rename transactions idempotent, ENOENT is ignored here but reported since only coordinator can check results of two separated link and unlink operations.
	// The coordinator can abort rename operation (delete coordinator just ignores this warning)
	r := UpdateParentRet{mayErrorReply: RaftReplyOk}
	_, ok := children[o.childName]
	if !ok {
		log.Warnf("%v, inode does not exist. inodeKey=%v, child=%v, txId=%v", o.name(), o.inodeKey, o.childName, o.txId)
		r.mayErrorReply = ErrnoToReply(unix.ENOENT)
	}
	//delete(meta.childAttrs, o.childName) //delete is done at Commit
	r.info = NewUpdateParentInfo(meta.inodeKey, o.parentKey, 0, "", o.childName, o.childIsDir)
	log.Debugf("Success: %v, inodeKey=%v, child=%v, txId=%v", o.name(), o.inodeKey, o.childName, o.txId)
	return r, unlock, RaftReplyOk
}

func (o UnlinkMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	r := ret.(UpdateParentRet)
	if reply := n.raft.AppendExtendedLogEntry(NewUpdateParentMetaCommand(o.txId, r.info)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o UnlinkMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginUnlinkMetaArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey),
		ParentKey: o.parentKey, ChildName: o.childName, ChildIsDir: o.childIsDir,
	}
	msg := &common.UpdateParentRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUnlinkMetaCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return NewUpdateParentRetFromMsg(msg)
}

func (o UnlinkMetaOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return ret.(UpdateParentRet).toMsg(r), nil
}

func (o UnlinkMetaOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, false)
}

func (o UnlinkMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type RenameMetaOp struct {
	txId          TxId
	inodeKey      InodeKeyType
	key           string
	srcName       string
	dstName       string
	childInodekey InodeKeyType
}

func NewRenameMetaOp(txId TxId, inodeKey InodeKeyType, key string, srcName string, dstName string, childInodekey InodeKeyType) RenameMetaOp {
	return RenameMetaOp{txId: txId, inodeKey: inodeKey, key: key, srcName: srcName, dstName: dstName, childInodekey: childInodekey}
}

func NewRenameMetaOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.BeginRenameMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewRenameMetaOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := RenameMetaOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), srcName: args.GetSrcName(), dstName: args.GetDstName(),
		childInodekey: InodeKeyType(args.GetChildInodeKey()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o RenameMetaOp) name() string {
	return "RenameMeta"
}

func (o RenameMetaOp) GetTxId() TxId {
	return o.txId
}

func (o RenameMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	meta, children, unlock, reply := n.inodeMgr.PrepareUpdateParent(o.inodeKey, false)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, PrepareUpdateParent, inodeKey=%v, r=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	_, srcExist := children[o.srcName]
	_, dstExist := children[o.dstName]
	// we allow entering rename transaction if src exists and dst doesn't or if dst exists and src doesn't exist to ensure idempotency (i.e., allow retries).
	// other patterns can be simple mistakes or racy conditions.
	if !srcExist && !dstExist {
		unlock(n)
		log.Errorf("Failed: %v, srcName and dstName do not exist, inodeKey=%v, srcName=%v, dstName=%v", o.name(), o.inodeKey, o.srcName, o.dstName)
		return nil, nil, ErrnoToReply(unix.ENOENT)
	}
	// delete(meta.childAttrs, o.srcName)
	// meta.childAttrs[o.dstName] = o.childInodekey

	// childIsDir is not necessary for this case. we set false for either cases.
	r := UpdateParentRet{info: NewUpdateParentInfo(meta.inodeKey, o.key, o.childInodekey, o.dstName, o.srcName, false), mayErrorReply: RaftReplyOk}
	log.Debugf("Success: %v, inodeKey=%v, name=%v->%v, txId=%v", o.name(), o.inodeKey, o.srcName, o.dstName, o.txId)
	return r, unlock, RaftReplyOk
}

func (o RenameMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	r := ret.(UpdateParentRet)
	if reply := n.raft.AppendExtendedLogEntry(NewUpdateParentMetaCommand(o.txId, r.info)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o RenameMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginRenameMetaArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), SrcName: o.srcName, DstName: o.dstName, ChildInodeKey: uint64(o.childInodekey),
	}
	msg := &common.UpdateParentRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcRenameMetaCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return NewUpdateParentRetFromMsg(msg)
}

func (o RenameMetaOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return ret.(UpdateParentRet).toMsg(r), nil
}

func (o RenameMetaOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, false)
}

func (o RenameMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type UpdateMetaKeyOp struct {
	txId     TxId
	inodeKey InodeKeyType
	parent   InodeKeyType
	oldKey   string
	newKey   string
}

func NewUpdateMetaKeyOp(txId TxId, inodeKey InodeKeyType, oldKey string, newKey string, parent InodeKeyType) UpdateMetaKeyOp {
	return UpdateMetaKeyOp{txId: txId, inodeKey: inodeKey, oldKey: oldKey, newKey: newKey, parent: parent}
}

func NewUpdateMetaKeyOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.BeginUpdateMetaKeyArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewUpdateMetaKeyOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := UpdateMetaKeyOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()),
		oldKey: args.GetOldKey(), newKey: args.GetNewKey(), parent: InodeKeyType(args.GetParent()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o UpdateMetaKeyOp) name() string {
	return "UpdateMetaKey"
}

func (o UpdateMetaKeyOp) GetTxId() TxId {
	return o.txId
}

func (o UpdateMetaKeyOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	working, children, unlock, reply := n.inodeMgr.PrepareUpdateMetaKey(o.inodeKey, o.oldKey, o.parent, n.flags.ChunkSizeBytes, n.flags.DirtyExpireIntervalMs)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareMutateMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, inodeKey=%v", o.name(), o.inodeKey)
	return &GetWorkingMetaRet{meta: working, children: children}, unlock, RaftReplyOk
}

func (o UpdateMetaKeyOp) writeLog(n *NodeServer, ret interface{}) int32 {
	r := ret.(*GetWorkingMetaRet)
	if reply := n.raft.AppendExtendedLogEntry(NewUpdateMetaKeyCommand(o.txId, r.meta, r.children, o.oldKey, o.newKey)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o UpdateMetaKeyOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginUpdateMetaKeyArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey),
		OldKey: o.oldKey, NewKey: o.newKey, Parent: uint64(o.parent),
	}
	msg := &common.GetWorkingMetaRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateMetaKeyCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewGetWrokingMetaRetFromMsg(msg), r
}

func (o UpdateMetaKeyOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	if r.reply != RaftReplyOk {
		return &common.GetWorkingMetaRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	ret2 := ret.(*GetWorkingMetaRet)
	return &common.GetWorkingMetaRetMsg{Meta: ret2.meta.toMsg(), Children: ret2.GetChildrenMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o UpdateMetaKeyOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, false)
}

func (o UpdateMetaKeyOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////
//////////////////////// Chunk Operations /////////////////////////
///////////////////////////////////////////////////////////////////

func NewAppendCommitUpdateChunkMsg(meta *WorkingMeta, chunks map[int64]*WorkingChunk, isDelete bool) *common.AppendCommitUpdateChunksMsg {
	msg := &common.AppendCommitUpdateChunksMsg{
		InodeKey: uint64(meta.inodeKey), Version: meta.version, ChunkSize: meta.chunkSize,
		ObjectSize: meta.size, IsDelete: isDelete, Chunks: make([]*common.WorkingChunkAddMsg, 0),
	}
	for offset, chunk := range chunks {
		msg.Chunks = append(msg.Chunks, &common.WorkingChunkAddMsg{Offset: offset, Stagings: chunk.toStagingChunkAddMsg()})
	}
	return msg
}

type CommitUpdateChunkOp struct {
	txId    TxId
	newMeta *WorkingMeta
	record  *common.UpdateChunkRecordMsg
}

func NewCommitUpdateChunkOp(txId TxId, record *common.UpdateChunkRecordMsg, newMeta *WorkingMeta) CommitUpdateChunkOp {
	return CommitUpdateChunkOp{txId: txId, newMeta: newMeta, record: record}
}

func NewCommitUpdateChunkOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.CommitUpdateChunksArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCommitUpdateChunkOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	newMeta := NewWorkingMetaFromMsg(args.GetMeta())
	newMeta.prevVer = NewWorkingMetaFromMsg(args.GetPrev())
	fn := CommitUpdateChunkOp{txId: NewTxIdFromMsg(args.GetTxId()), newMeta: newMeta, record: args.GetChunk()}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CommitUpdateChunkOp) name() string {
	return "CommitUpdateChunk"
}

func (o CommitUpdateChunkOp) GetTxId() TxId {
	return o.txId
}

func PrepareCommitUpdateChunkBody(inodeMgr *InodeMgr, offStags map[int64][]*common.StagingChunkMsg, newMeta *WorkingMeta) (chunks map[int64]*WorkingChunk, unlocks []func()) {
	chunks = make(map[int64]*WorkingChunk)
	unlocks = make([]func(), 0)
	for off, stags := range offStags {
		beginAdd := time.Now()
		chunk, working := inodeMgr.PrepareUpdateChunk(newMeta, off)
		for _, stag := range stags {
			working.AddStag(&StagingChunk{
				slop: stag.GetOffset() % newMeta.chunkSize, filled: 1, length: stag.GetLength(),
				updateType: StagingChunkData, logOffset: stag.GetLogOffset(),
			})
		}
		chunks[off] = working
		unlocks = append(unlocks, func() { chunk.lock.Unlock() })
		endAll := time.Now()
		log.Debugf("Success: PrepareCommitUpdateChunkBody, inodeKey=%v, offset=%v, add=%v", newMeta.inodeKey, off, endAll.Sub(beginAdd))
	}
	return chunks, unlocks
}

func (o CommitUpdateChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	offStags := map[int64][]*common.StagingChunkMsg{}
	old := make(map[TxId]RpcRet)
	for _, stags := range o.record.GetStags() {
		txId := NewTxIdFromMsg(stags.GetTxId())
		op, ok := n.rpcMgr.DeleteAndGet(txId)
		if !ok {
			// duplicated/aborted request
			log.Infof("%v, skip inactive tx, txId=%v", o.name(), txId)
		} else {
			align := stags.GetOffset() - stags.GetOffset()%o.newMeta.chunkSize
			if _, ok = offStags[align]; !ok {
				offStags[align] = make([]*common.StagingChunkMsg, 0)
			}
			offStags[align] = append(offStags[align], stags)
			old[txId] = op.ret
		}
	}
	if len(old) == 0 {
		log.Infof("%v, nothing to do", o.name())
		return nil, nil, RaftReplyOk
	}
	chunks, unlocks := PrepareCommitUpdateChunkBody(n.inodeMgr, offStags, o.newMeta)
	unlock = func(n *NodeServer) {
		for _, u := range unlocks {
			u()
		}
	}
	if reply = n.raft.AppendExtendedLogEntry(NewCommitChunkCommand(o.newMeta, chunks, false)); reply != RaftReplyOk {
		// accident happens. roll back to enable retries
		// NOTE: do not use writeLog to handle this situation
		// NOTE2: do not care network failures after this method since retried commit will notice this.
		unlock(n)
		for txId, op := range old {
			n.rpcMgr.Record(txId, op)
		}
		log.Errorf("Failed: %v, AppendExtendedLogEntry, len(chunks)=%v, reply=%v", o.name(), len(chunks), reply)
		return ret, nil, reply
	}
	return ret, unlock, RaftReplyOk
}

func (o CommitUpdateChunkOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o CommitUpdateChunkOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.CommitUpdateChunksArgs{
		Meta: o.newMeta.toMsg(), Prev: o.newMeta.prevVer.toMsg(), Chunk: o.record,
		TxId: o.txId.toMsg(), NodeListVer: nodeListVer,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitUpdateChunkCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, txId=%v, reply=%v", o.name(), sa, o.txId, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o CommitUpdateChunkOp) RetToMsg(_ interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o CommitUpdateChunkOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCommitChunkRpcCaller(o, n.flags.CommitRpcTimeoutDuration)
}

func (o CommitUpdateChunkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.GetGroupLeader(o.record.GetGroupId(), l)
}

///////////////////////////////////////////////////////////////////

type CommitDeleteChunkOp struct {
	txId      TxId
	newMeta   *WorkingMeta
	offset    int64
	deleteLen int64
}

func NewCommitDeleteChunkOp(txId TxId, newMeta *WorkingMeta, offset int64, deleteLen int64) CommitDeleteChunkOp {
	return CommitDeleteChunkOp{txId: txId, newMeta: newMeta, offset: offset, deleteLen: deleteLen}
}

func NewCommitDeleteChunkOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.CommitDeleteChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CommitDeleteChunk, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	newMeta := NewWorkingMetaFromMsg(args.GetMeta())
	newMeta.prevVer = NewWorkingMetaFromMsg(args.GetPrev())
	fn := CommitDeleteChunkOp{txId: txId, newMeta: newMeta, offset: args.GetOffset(), deleteLen: args.GetDeleteLength()}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CommitDeleteChunkOp) name() string {
	return "CommitDeleteChunk"
}

func (o CommitDeleteChunkOp) GetTxId() TxId {
	return o.txId
}

func (o CommitDeleteChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	alignedOffset := o.offset - o.offset%o.newMeta.chunkSize
	chunk, working := n.inodeMgr.PrepareUpdateChunk(o.newMeta, alignedOffset)
	unlock = func(*NodeServer) {
		chunk.lock.Unlock()
	}
	working.AddStag(&StagingChunk{
		filled:     1,
		length:     o.deleteLen,
		slop:       o.offset - alignedOffset,
		updateType: StagingChunkDelete,
	})
	if reply = n.raft.AppendExtendedLogEntry(NewCommitChunkCommand(o.newMeta, map[int64]*WorkingChunk{alignedOffset: working}, true)); reply != RaftReplyOk {
		unlock(n)
		log.Errorf("Failed: %v, AppendExtendedLogEntry, chunk=%v, reply=%v", o.name(), *working, reply)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, inodeKey=%v", o.name(), o.newMeta.inodeKey)
	return working, unlock, RaftReplyOk
}

func (o CommitDeleteChunkOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o CommitDeleteChunkOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.CommitDeleteChunkArgs{
		TxId:         o.txId.toMsg(),
		Meta:         o.newMeta.toMsg(),
		Prev:         o.newMeta.prevVer.toMsg(),
		Offset:       o.offset,
		DeleteLength: o.deleteLen,
		NodeListVer:  nodeListVer,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitDeleteChunkCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o CommitDeleteChunkOp) RetToMsg(_ interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o CommitDeleteChunkOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCommitChunkRpcCaller(o, n.flags.CommitRpcTimeoutDuration)
}

func (o CommitDeleteChunkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getChunkOwnerNodeLocalNew(l, o.newMeta.inodeKey, o.offset, o.newMeta.chunkSize)
}

///////////////////////////////////////////////////////////////////

type CommitExpandChunkOp struct {
	txId      TxId
	newMeta   *WorkingMeta
	offset    int64
	expandLen int64
}

func NewCommitExpandChunkOp(txId TxId, newMeta *WorkingMeta, offset int64, expandLen int64) CommitExpandChunkOp {
	return CommitExpandChunkOp{txId: txId, newMeta: newMeta, offset: offset, expandLen: expandLen}
}

func NewCommitExpandChunkOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.CommitExpandChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCommitExpandChunkOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	newMeta := NewWorkingMetaFromMsg(args.GetMeta())
	newMeta.prevVer = NewWorkingMetaFromMsg(args.GetPrev())
	fn := CommitExpandChunkOp{txId: txId, newMeta: newMeta, offset: args.GetOffset(), expandLen: args.GetExpandLength()}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CommitExpandChunkOp) name() string {
	return "CommitExpandChunk"
}

func (o CommitExpandChunkOp) GetTxId() TxId {
	return o.txId
}

func (o CommitExpandChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	alignedOffset := o.offset - o.offset%o.newMeta.chunkSize
	chunk, working := n.inodeMgr.PrepareUpdateChunk(o.newMeta, alignedOffset)
	working.AddStag(&StagingChunk{
		filled:     1,
		length:     o.expandLen,
		slop:       o.offset - alignedOffset,
		updateType: StagingChunkBlank,
	})
	unlock = func(n *NodeServer) {
		chunk.lock.Unlock()
	}
	if reply = n.raft.AppendExtendedLogEntry(NewCommitChunkCommand(o.newMeta, map[int64]*WorkingChunk{alignedOffset: working}, false)); reply != RaftReplyOk {
		unlock(n)
		log.Errorf("Failed: %v, AppendExtendedLogEntry, chunk=%v, reply=%v", o.name(), *working, reply)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, inodeKey=%v", o.name(), o.newMeta.inodeKey)
	return working, unlock, RaftReplyOk
}

func (o CommitExpandChunkOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o CommitExpandChunkOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.CommitExpandChunkArgs{
		TxId:         o.txId.toMsg(),
		Meta:         o.newMeta.toMsg(),
		Prev:         o.newMeta.prevVer.toMsg(),
		Offset:       o.offset,
		ExpandLength: o.expandLen,
		NodeListVer:  nodeListVer,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitExpandChunkCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o CommitExpandChunkOp) RetToMsg(_ interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o CommitExpandChunkOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCommitChunkRpcCaller(o, n.flags.CommitRpcTimeoutDuration)
}

func (o CommitExpandChunkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getChunkOwnerNodeLocalNew(l, o.newMeta.inodeKey, o.offset, o.newMeta.chunkSize)
}

///////////////////////////////////////////////////////////////////

type UpdateChunkOp struct {
	txId      TxId
	inodeKey  InodeKeyType
	chunkSize int64
	offset    int64
	buf       []byte

	logId        LogIdType
	logOffset    int64
	dataLength   uint32
	extBufChksum [4]byte
}

func NewUpdateChunkOp(txId TxId, inodeKey InodeKeyType, chunkSize int64, offset int64, buf []byte) UpdateChunkOp {
	chksum := crc32.NewIEEE()
	_, _ = chksum.Write(buf)
	ret := UpdateChunkOp{txId: txId, inodeKey: inodeKey, chunkSize: chunkSize, offset: offset, buf: buf}
	copy(ret.extBufChksum[:], chksum.Sum(nil))
	return ret
}

func NewUpdateChunkOpFromProtoMsg(m proto.Message, logId LogIdType, logOffset int64, dataLength uint32) (ParticipantOp, uint64) {
	args := m.(*common.UpdateChunkArgs)
	fn := UpdateChunkOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), chunkSize: args.GetChunkSize(),
		offset: args.GetOffset(), buf: nil, logId: logId, logOffset: logOffset, dataLength: dataLength,
	}
	copy(fn.extBufChksum[:], args.GetExtBufChksum())
	return fn, args.GetNodeListVer()
}

func (o UpdateChunkOp) name() string {
	return "UpdateChunk"
}

func (o UpdateChunkOp) GetTxId() TxId {
	return o.txId
}

func (o UpdateChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	if o.buf != nil {
		o.dataLength = uint32(len(o.buf))
		o.logOffset, reply = n.inodeMgr.AppendStagingChunkBuffer(o.inodeKey, o.offset, o.chunkSize, o.buf)
	} else {
		reply = n.inodeMgr.AppendStagingChunkLog(o.inodeKey, o.offset, o.logId, o.logOffset, o.dataLength, o.extBufChksum)
	}
	if reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendStagingChunk, logId=%v, reply=%v", o.name(), o.logId, reply)
		return &common.StagingChunkMsg{TxId: o.txId.toMsg()}, nil, reply
	}
	//log.Debugf("Success: %v, txId=%v, inodeKey=%v, offset=%v, length=%v", o.txId, o.inodeKey, o.offset, o.dataLength)
	stag := &common.StagingChunkMsg{TxId: o.txId.toMsg(), Offset: o.offset, Length: int64(o.dataLength), LogOffset: o.logOffset}
	return stag, nil, RaftReplyOk
}

func (o UpdateChunkOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o UpdateChunkOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.UpdateChunkArgs{
		InodeKey:    uint64(o.inodeKey),
		ChunkSize:   o.chunkSize,
		Offset:      o.offset,
		TxId:        o.txId.toMsg(),
		NodeListVer: nodeListVer,
	}
	copy(args.ExtBufChksum, o.extBufChksum[:])
	msg := &common.UpdateChunkRet{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateChunkCmdId, args, sa, timeout, n.raft.extLogger, [][]byte{o.buf}, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v.remote, inodeKey=%v, offset=%v", o.name(), o.inodeKey, o.offset)
		return &common.StagingChunkMsg{TxId: o.txId.toMsg()}, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if msg.GetStatus() != RaftReplyOk {
		log.Errorf("Failed: %v, r=%v", o.name(), r)
		return msg.GetStag(), r
	}
	log.Debugf("Success: %v.remote, inodeKey=%v, offset=%v, dataLen=%v", o.name(), o.inodeKey, o.offset, len(o.buf))
	return msg.GetStag(), r
}

func (o UpdateChunkOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	if r.reply != RaftReplyOk {
		return &common.UpdateChunkRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	return &common.UpdateChunkRet{Status: r.reply, Leader: r.GetLeaderNodeMsg(), Stag: ret.(*common.StagingChunkMsg)}, nil
}

func (o UpdateChunkOp) GetCaller(n *NodeServer) RpcCaller {
	return NewUpadteChunkRpcCaller(o, n.flags.ChunkRpcTimeoutDuration)
}

func (o UpdateChunkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getChunkOwnerNodeLocalNew(l, o.inodeKey, o.offset, o.chunkSize)
}

///////////////////////////////////////////////////////////////////
///////////////// Persist Chunk Operations ////////////////////////
///////////////////////////////////////////////////////////////////

type CommitPersistChunkOp struct {
	txId       TxId
	commitTxId TxId
	groupId    string
	inodeKey   InodeKeyType
	offsets    []int64
	cVers      []uint32
}

func NewCommitPersistChunkOp(txId TxId, commitTxId TxId, groupId string, offsets []int64, cVers []uint32, inodeKey InodeKeyType) CommitPersistChunkOp {
	return CommitPersistChunkOp{
		txId: txId, commitTxId: commitTxId, groupId: groupId, inodeKey: inodeKey, offsets: offsets, cVers: cVers,
	}
}

func NewCommitPersistChunkOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.CommitPersistChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCommitPersistChunkOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CommitPersistChunkOp{
		txId: NewTxIdFromMsg(args.GetTxId()), commitTxId: NewTxIdFromMsg(args.GetCommitTxId()),
		groupId: "", inodeKey: InodeKeyType(args.GetInodeKey()), offsets: args.GetOffsets(), cVers: args.GetCVers(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CommitPersistChunkOp) name() string {
	return "CommitPersistChunk"
}

func (o CommitPersistChunkOp) GetTxId() TxId {
	return o.txId
}

func (o CommitPersistChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	op, ok := n.rpcMgr.DeleteAndGet(o.commitTxId)
	if !ok {
		log.Infof("%v, skip inactive tx, txId=%v", o.name(), o.commitTxId)
		return nil, nil, RaftReplyOk
	}
	if reply = n.raft.AppendExtendedLogEntry(NewPersistChunkCommand(o.inodeKey, o.offsets, o.cVers)); reply != RaftReplyOk {
		n.rpcMgr.Record(o.commitTxId, op.ret)
		log.Errorf("Failed: %v, AppendExtendedLogEntry, metaKey=%v, offsets=%v, txId=%v, commitTxId=%v, reply=%v",
			o.name(), o.inodeKey, o.offsets, o.txId, o.commitTxId, reply)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, inodeKey=%v, offsets=%v", o.name(), o.inodeKey, o.offsets)
	return nil, op.ret.unlock, RaftReplyOk
}

func (o CommitPersistChunkOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o CommitPersistChunkOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.CommitPersistChunkArgs{
		TxId:        o.txId.toMsg(),
		CommitTxId:  o.commitTxId.toMsg(),
		InodeKey:    uint64(o.inodeKey),
		Offsets:     o.offsets,
		CVers:       o.cVers,
		NodeListVer: nodeListVer,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitPersistChunkCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o CommitPersistChunkOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o CommitPersistChunkOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCommitAbortRpcCaller(o, n.flags.CommitRpcTimeoutDuration, false)
}

func (o CommitPersistChunkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.GetGroupLeader(o.groupId, l)
}

///////////////////////////////////////////////////////////////////

type MpuAddOp struct {
	leader      RaftNode
	nodeListVer uint64
	txId        TxId
	metaKey     string
	meta        *WorkingMeta
	offsets     []int64
	uploadId    string
	priority    int
}

func NewMpuAddOp(leader RaftNode, nodeListVer uint64, txId TxId, metaKey string, meta *WorkingMeta, offsets []int64, uploadId string, priority int) MpuAddOp {
	return MpuAddOp{leader: leader, nodeListVer: nodeListVer, txId: txId, metaKey: metaKey, meta: meta, offsets: offsets, uploadId: uploadId, priority: priority}
}

func NewMpuAddOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.MpuAddArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewMpuAddOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := MpuAddOp{
		leader: RaftNode{}, nodeListVer: args.GetNodeListVer(),
		txId: NewTxIdFromMsg(args.GetTxId()), metaKey: args.GetMetaKey(), meta: NewWorkingMetaFromMsg(args.GetMeta()),
		offsets: args.GetOffsets(), uploadId: args.GetUploadId(), priority: int(args.GetPriority()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o MpuAddOp) name() string {
	return "MpuAdd"
}

func (o MpuAddOp) GetTxId() TxId {
	return o.txId
}

func (o MpuAddOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	var wg sync.WaitGroup
	outs := make([]MpuAddOut, len(o.offsets))
	unlocks := make([]func(*NodeServer), len(o.offsets))
	reply = RaftReplyOk
	for i, offset := range o.offsets {
		n.inodeMgr.uploadSem <- struct{}{}
		wg.Add(1)
		go func(i int, offset int64) {
			var r int32
			outs[i], unlocks[i], r = n.inodeMgr.MpuAdd(o.metaKey, o.meta, offset, o.uploadId, o.priority, n.flags.DirtyFlusherIntervalDuration)
			if r != RaftReplyOk {
				reply = r
			}
			wg.Done()
			<-n.inodeMgr.uploadSem
		}(i, offset)
	}
	wg.Wait()
	unlock = func(n *NodeServer) {
		for _, u := range unlocks {
			if u != nil {
				u(n)
			}
		}
	}
	return outs, unlock, reply
}

func (o MpuAddOp) writeLog(_ *NodeServer, _ interface{}) int32 { return RaftReplyOk }

func (o MpuAddOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	return nil, RaftBasicReply{reply: RaftReplyFail}
}

type MpuContext struct {
	txId   TxId
	con    *RpcClientConnectionV2
	seqNum uint64
	node   RaftNode
}

func (o MpuAddOp) remoteAsync(n *NodeServer) (c MpuContext, reply int32) {
	args := &common.MpuAddArgs{
		TxId:        o.txId.toMsg(),
		MetaKey:     o.metaKey,
		Meta:        o.meta.toMsg(),
		Offsets:     o.offsets,
		UploadId:    o.uploadId,
		Priority:    int32(o.priority),
		NodeListVer: o.nodeListVer,
	}
	c.node = o.leader
	c.txId = o.txId
	c.con, c.seqNum, reply = n.rpcClient.AsyncObjcacheRpc(RpcMpuAddCmdId, args, o.leader.addr, n.raft.extLogger, nil, nil)
	if reply != RaftReplyOk {
		log.Errorf("Failed: %v, AsyncObjcacheRpc, sa=%v, reply=%v", o.name(), o.leader.addr, reply)
	}
	return
}

func (c *MpuContext) WaitRet(n *NodeServer) (ret []MpuAddOut, r RaftBasicReply) {
	msg := &common.MpuAddRet{}
	reply := n.rpcClient.WaitAsyncObjcacheRpc(c.con, c.seqNum, msg)
	if reply != RaftReplyOk {
		log.Errorf("Failed: WaitAsyncObjcacheRpc, sa=%v, reply=%v", c.con.sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r = NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	ret = make([]MpuAddOut, 0)
	for _, outMsg := range msg.GetOuts() {
		ret = append(ret, MpuAddOut{idx: outMsg.GetIndex(), etag: outMsg.GetEtag(), cVer: outMsg.GetCVer()})
	}
	return ret, r
}

func (o MpuAddOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	if r.reply != RaftReplyOk {
		return &common.MpuAddRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
	}
	outs := ret.([]MpuAddOut)
	outMsg := make([]*common.MpuAddRetIndex, 0)
	for _, out := range outs {
		outMsg = append(outMsg, &common.MpuAddRetIndex{Index: out.idx, Etag: out.etag, CVer: out.cVer})
	}
	return &common.MpuAddRet{Outs: outMsg, Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o MpuAddOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.ChunkRpcTimeoutDuration, false)
}

func (o MpuAddOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	if l.version != o.nodeListVer {
		log.Errorf("Failed: %v.GetLeader, nodeListVer is updated, current=%v, expected=%v", o.name(), l.version, o.nodeListVer)
		return RaftNode{}, false
	}
	return o.leader, true
}

type MpuAbortOp struct {
	txId      TxId
	keys      []string
	uploadIds []string
}

func NewMpuAbortOp(txId TxId, keys []string, uploadIds []string) MpuAbortOp {
	return MpuAbortOp{txId: txId, keys: keys, uploadIds: uploadIds}
}

func (o MpuAbortOp) name() string {
	return "MpuAbort"
}

func (o MpuAbortOp) GetTxId() TxId {
	return o.txId
}

func (o MpuAbortOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	for i := 0; i < len(o.keys); i++ {
		if reply := n.inodeMgr.MpuAbort(o.keys[i], o.uploadIds[i]); reply != RaftReplyOk {
			log.Errorf("Failed: %v, MpuAbort, txId=%v, reply=%v", o.name(), o.txId, reply)
			return nil, nil, reply
		}
	}
	reply = n.raft.AppendExtendedLogEntry(NewAbortTxCommand([]TxId{o.txId}))
	if reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
	}
	return nil, nil, RaftReplyOk
}

func (o MpuAbortOp) RetToMsg(interface{}, RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return nil, nil
}

func (o MpuAbortOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCommitAbortRpcCaller(o, n.flags.PersistTimeoutDuration, false)
}

func (o MpuAbortOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o MpuAbortOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	log.Errorf("BUG: %v must be called in local", o.name())
	return nil, NewRaftBasicReply(RaftReplyFail, &common.LeaderNodeMsg{})
}

func (o MpuAbortOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.selfNode, true
}

///////////////////////////////////////////////////////////////////
///////////////////// Node List Operations ////////////////////////

type UpdateNodeListOp struct {
	txId          TxId
	isAdd         bool
	added         RaftNode
	leaderGroupId string
	target        *common.NodeAddrInet4
	migrationId   MigrationId
}

func NewUpdateNodeListOp(txId TxId, added RaftNode, leaderGroupId string, isAdd bool, target *common.NodeAddrInet4, migrationId MigrationId) UpdateNodeListOp {
	return UpdateNodeListOp{txId: txId, isAdd: isAdd, added: added, leaderGroupId: leaderGroupId, target: target, migrationId: migrationId}
}

func NewUpdateNodeListOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.UpdateNodeListArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewUpdateNodeListOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	fn := UpdateNodeListOp{
		txId: txId, isAdd: args.GetIsAdd(), added: NewRaftNodeFromMsg(args.GetNode()),
		leaderGroupId: "", target: nil, migrationId: NewMigrationIdFromMsg(args.GetMigrationId()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o UpdateNodeListOp) name() string {
	return "UpdateNodeList"
}

func (o UpdateNodeListOp) GetTxId() TxId {
	return o.txId
}

func (o UpdateNodeListOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	if o.added.groupId == n.raftGroup.selfGroup {
		if o.isAdd {
			reply = n.raft.AddServerLocal(o.added.addr, o.added.nodeId)
		} else {
			reply = n.raft.RemoveServerLocal(o.added.nodeId)
		}
		if reply != RaftReplyOk {
			unlock(n)
			log.Errorf("Failed: %v, Raft.Add/RemoveServerLocal, IsAdd=%v, nodeId=%v, reply=%v", o.name(), o.isAdd, o.added.nodeId, reply)
			return true, nil, reply
		}
	}
	log.Debugf("Success: %v, added=%v, isAdd=%v", o.name(), o.added.nodeId, o.isAdd)
	return true, unlock, RaftReplyOk
}

func (o UpdateNodeListOp) writeLog(n *NodeServer, _ interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(NewUpdateNodeListCommand(o.txId, o.migrationId, o.isAdd, true, []RaftNode{o.added}, 0)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, reply, r=%v", o.name(), reply)
		return reply
	}
	return RaftReplyOk
}

func (o UpdateNodeListOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.UpdateNodeListArgs{
		TxId:        o.txId.toMsg(),
		Node:        o.added.toMsg(),
		IsAdd:       o.isAdd,
		MigrationId: o.migrationId.toMsg(),
		NodeListVer: nodeListVer,
	}
	msg := &common.UpdateNodeListRet{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateNodeListCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return msg.GetNeedRestore(), NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o UpdateNodeListOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	var needRestore = false
	if r.reply == RaftReplyOk {
		needRestore = ret.(bool)
	}
	return &common.UpdateNodeListRet{Status: r.reply, Leader: r.GetLeaderNodeMsg(), NeedRestore: needRestore}, nil
}

func (o UpdateNodeListOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, true)
}

func (o UpdateNodeListOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	if o.target != nil {
		return RaftNode{addr: *o.target}, true
	}
	return n.raftGroup.GetGroupLeader(o.leaderGroupId, l)
}

///////////////////////////////////////////////////////////////////

type FillNodeListOp struct {
	txId           TxId
	migrationId    MigrationId
	nodes          []RaftNode
	newNodeListVer uint64
	target         RaftNode
}

func NewFillNodeListOp(txId TxId, nodeList *RaftNodeList, target RaftNode, migrationId MigrationId) FillNodeListOp {
	nodes := make([]RaftNode, 0)
	for _, ns := range nodeList.groupNode {
		for _, node := range ns {
			nodes = append(nodes, node)
		}
	}
	return FillNodeListOp{txId: txId, migrationId: migrationId, nodes: nodes, newNodeListVer: nodeList.version + 1, target: target}
}

func NewFillNodeListOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.InitNodeListArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewFillNodeListOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	nodes := make([]RaftNode, 0)
	for _, node := range args.GetNodes() {
		nodes = append(nodes, NewRaftNodeFromMsg(node))
	}
	fn := FillNodeListOp{txId: txId, nodes: nodes, newNodeListVer: args.GetNewNodeListVer(), migrationId: NewMigrationIdFromMsg(args.GetMigrationId())}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o FillNodeListOp) name() string {
	return "FillNodeList"
}

func (o FillNodeListOp) GetTxId() TxId {
	return o.txId
}

func (o FillNodeListOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	if reply = n.raft.AppendExtendedLogEntry(NewUpdateNodeListCommand(o.txId, o.migrationId, true, true, o.nodes, o.newNodeListVer)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, reply, r=%v", o.name(), reply)
	}
	return nil, nil, reply
}

func (o FillNodeListOp) writeLog(n *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o FillNodeListOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	nodes := make([]*common.NodeMsg, 0)
	for _, node := range o.nodes {
		nodes = append(nodes, node.toMsg())
	}
	args := &common.InitNodeListArgs{
		Nodes:          nodes,
		TxId:           o.txId.toMsg(),
		NodeListVer:    nodeListVer,
		NewNodeListVer: o.newNodeListVer,
		MigrationId:    o.migrationId.toMsg(),
	}
	msg := &common.UpdateNodeListRet{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcFillNodeListCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o FillNodeListOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.UpdateNodeListRet{Status: r.reply, Leader: r.GetLeaderNodeMsg(), NeedRestore: false}, nil
}

func (o FillNodeListOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.RpcTimeoutDuration, true)
}

func (o FillNodeListOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return o.target, true
}

///////////////////////////////////////////////////////////////////

type JoinMigrationOp struct {
	txId          TxId
	target        RaftNode
	leaderGroupId string
	migrationId   MigrationId
}

func NewJoinMigrationOp(txId TxId, target RaftNode, leaderGroupId string, migrationId MigrationId) JoinMigrationOp {
	return JoinMigrationOp{target: target, leaderGroupId: leaderGroupId, txId: txId, migrationId: migrationId}
}

func NewJoinMigrationOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.MigrationArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewJoinMigrationOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	fn := JoinMigrationOp{
		target: NewRaftNodeFromMsg(args.GetNode()), leaderGroupId: "", txId: txId,
		migrationId: NewMigrationIdFromMsg(args.GetMigrationId()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o JoinMigrationOp) name() string {
	return "JoinMigration"
}

func (o JoinMigrationOp) GetTxId() TxId {
	return o.txId
}

func (o JoinMigrationOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	l := n.raftGroup.GetNodeListLocal()
	newRing := l.ring.AddWeightedNode(o.target.groupId, 1)
	unlock = func(n *NodeServer) {
		n.dirtyMgr.DropMigratingData(o.migrationId)
	}
	if reply = n.sendDirtyMetasForNodeJoin(o.migrationId, l, newRing, o.target); reply != RaftReplyOk {
		unlock(n)
		return nil, nil, reply
	}
	if reply = n.sendDirtyChunksForNodeJoin(o.migrationId, l, newRing, o.target); reply != RaftReplyOk {
		unlock(n)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, target=%v", o.name(), o.target)
	return nil, unlock, RaftReplyOk
}

func (o JoinMigrationOp) writeLog(n *NodeServer, _ interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(NewUpdateNodeListCommand(o.txId, o.migrationId, true, true, []RaftNode{o.target}, 0)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, reply, r=%v", o.name(), reply)
		return reply
	}
	return RaftReplyOk
}

func (o JoinMigrationOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.MigrationArgs{
		TxId:        o.txId.toMsg(),
		Node:        o.target.toMsg(),
		NodeListVer: nodeListVer,
		MigrationId: o.migrationId.toMsg(),
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcJoinMigrationCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o JoinMigrationOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o JoinMigrationOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.ChunkRpcTimeoutDuration, true)
}

func (o JoinMigrationOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.GetGroupLeader(o.leaderGroupId, l)
}

/////////////////////////////////////////////////////////////////////////////////////

type LeaveMigrationOp struct {
	txId          TxId
	target        RaftNode
	leaderGroupId string
	migrationId   MigrationId
}

func NewLeaveMigrationOp(txId TxId, target RaftNode, leaderGroupId string, migrationId MigrationId) LeaveMigrationOp {
	return LeaveMigrationOp{txId: txId, target: target, leaderGroupId: leaderGroupId, migrationId: migrationId}
}

func NewLeaveMigrationOpFromMsg(msg RpcMsg) (ParticipantOp, uint64, int32) {
	args := &common.MigrationArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewLeaveMigrationOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	fn := LeaveMigrationOp{
		txId: txId, target: NewRaftNodeFromMsg(args.GetNode()), leaderGroupId: "", migrationId: NewMigrationIdFromMsg(args.GetMigrationId()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o LeaveMigrationOp) name() string {
	return "LeaveMigration"
}

func (o LeaveMigrationOp) GetTxId() TxId {
	return o.txId
}

func (o LeaveMigrationOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	if o.target.nodeId == n.raft.selfId {
		nodeList := n.raftGroup.GetRemovedNodeListLocal(o.target)
		unlock = func(n *NodeServer) {
			n.dirtyMgr.DropMigratingData(o.migrationId)
		}
		log.Infof("Shutdown: Migrate dirty metas")
		if reply = n.sendDirtyMetasForNodeLeave(o.migrationId, nodeList); reply != RaftReplyOk {
			unlock(n)
			log.Errorf("Failed: %v, sendDirtyMetasForNodeLeave, reply=%v", o.name(), reply)
			return nil, nil, reply
		}
		log.Infof("Shutdown: Migrate dirty chunks")
		if reply = n.sendDirtyChunksForNodeLeave(o.migrationId, nodeList); reply != RaftReplyOk {
			unlock(n)
			log.Errorf("Failed: %v, sendDirtyChunksForNodeLeave, reply=%v", o.name(), reply)
			return nil, nil, reply
		}
	} else {
		unlock = nil
		// do nothing
	}
	return nil, unlock, RaftReplyOk
}

func (o LeaveMigrationOp) writeLog(n *NodeServer, _ interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(NewUpdateNodeListCommand(o.txId, o.migrationId, false, false, []RaftNode{o.target}, 0)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, reply, r=%v", o.name(), reply)
		return reply
	}
	return RaftReplyOk
}

func (o LeaveMigrationOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.MigrationArgs{
		TxId:        o.txId.toMsg(),
		Node:        o.target.toMsg(),
		NodeListVer: nodeListVer,
		MigrationId: o.migrationId.toMsg(),
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcLeaveMigrationCmdId, args, sa, timeout, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o LeaveMigrationOp) RetToMsg(ret interface{}, r RaftBasicReply) (proto.Message, []SlicedPageBuffer) {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}, nil
}

func (o LeaveMigrationOp) GetCaller(n *NodeServer) RpcCaller {
	return NewPrepareRpcCaller(o, n.flags.ChunkRpcTimeoutDuration, true)
}

func (o LeaveMigrationOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.GetGroupLeader(o.leaderGroupId, l)
}

var ParticipantOpMap = map[uint16]func(RpcMsg) (ParticipantOp, uint64, int32){
	RpcCommitParticipantCmdId:  NewCommitParticipantOpFromMsg,
	RpcAbortParticipantCmdId:   NewAbortParticipantOpFromMsg,
	RpcCreateMetaCmdId:         NewCreateMetaOpFromMsg,
	RpcTruncateMetaCmdId:       NewTruncateMetaOpFromMsg,
	RpcUpdateMetaSizeCmdId:     NewUpdateMetaSizeOpFromMsg,
	RpcDeleteMetaCmdId:         NewDeleteMetaOpFromMsg,
	RpcUnlinkMetaCmdId:         NewUnlinkMetaOpFromMsg,
	RpcRenameMetaCmdId:         NewRenameMetaOpFromMsg,
	RpcCommitUpdateChunkCmdId:  NewCommitUpdateChunkOpFromMsg,
	RpcCommitDeleteChunkCmdId:  NewCommitDeleteChunkOpFromMsg,
	RpcCommitExpandChunkCmdId:  NewCommitExpandChunkOpFromMsg,
	RpcCommitPersistChunkCmdId: NewCommitPersistChunkOpFromMsg,
	RpcUpdateNodeListCmdId:     NewUpdateNodeListOpFromMsg,
	RpcFillNodeListCmdId:       NewFillNodeListOpFromMsg,
	RpcMpuAddCmdId:             NewMpuAddOpFromMsg,
	RpcJoinMigrationCmdId:      NewJoinMigrationOpFromMsg,
	RpcLeaveMigrationCmdId:     NewLeaveMigrationOpFromMsg,
	RpcCreateChildMetaCmdId:    NewCreateChildMetaOpFromMsg,
	RpcUpdateMetaKeyCmdId:      NewUpdateMetaKeyOpFromMsg,
}

func (o *RpcMgr) UpdateChunkBottomHalf(m proto.Message, logId LogIdType, logOffset int64, dataLength uint32, reply int32) proto.Message {
	if reply != RaftReplyOk {
		return &common.UpdateChunkRet{Status: reply}
	}
	fn, nodeListVer := NewUpdateChunkOpFromProtoMsg(m, logId, logOffset, dataLength)
	ret, r := fn.GetCaller(o.n).ExecLocalInRpc(o.n, nodeListVer)
	ret2, _ := fn.RetToMsg(ret.ext, r)
	return ret2
}
