/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"math"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/IBM/objcache/common"
)

type ParticipantOp interface {
	name() string
	GetLeader(*NodeServer, *RaftNodeList) (RaftNode, bool)
	local(*NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32)
	writeLog(*NodeServer, interface{}) int32
	remote(*NodeServer, common.NodeAddrInet4, uint64, time.Duration) (interface{}, RaftBasicReply)
}

///////////////////////////////////////////////////////////////////
//////////////////// Commit/Abort Operations //////////////////////
///////////////////////////////////////////////////////////////////

type AbortParticipantOp struct {
	rpcSeqNum  TxId
	abortTxIds []*common.TxIdMsg
	groupId    string
}

func NewAbortParticipantOp(rpcSeqNum TxId, abortTxIds []*common.TxIdMsg, groupId string) AbortParticipantOp {
	return AbortParticipantOp{rpcSeqNum: rpcSeqNum, abortTxIds: abortTxIds, groupId: groupId}
}

func (o AbortParticipantOp) name() string {
	return "AbortParticipant"
}

func (o AbortParticipantOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	old := make([]RpcRet, 0)
	txIds := make([]*common.TxIdMsg, 0)
	for _, msg := range o.abortTxIds {
		txId := NewTxIdFromMsg(msg)
		op, ok := n.rpcMgr.DeleteAndGet(txId)
		if !ok {
			log.Infof("%v, skip inactive tx, txId=%v", o.name(), txId)
			continue
		}
		txIds = append(txIds, msg)
		old = append(old, op.ret)
	}
	// do not roll back at failures around networking (roll back for disk failures, though)
	// retried abort/commit will notice resolution
	if len(txIds) == 0 {
		log.Infof("%v, nothing to do", o.name())
		return nil, nil, RaftReplyOk
	}
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryAbortTxCmdId, &common.AbortCommitArgs{AbortTxIds: txIds}); reply != RaftReplyOk {
		for i := 0; i < len(txIds); i++ {
			n.rpcMgr.Record(NewTxIdFromMsg(txIds[i]), old[i])
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
	args := &common.AbortCommitArgs{RpcSeqNum: o.rpcSeqNum.toMsg(), AbortTxIds: o.abortTxIds, NodeListVer: nodeListVer}
	if reply := n.rpcClient.CallObjcacheRpc(RpcAbortParticipantCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) AbortParticipant(msg RpcMsg) *common.Ack {
	args := &common.AbortCommitArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: AbortParticipant, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	op := AbortParticipantOp{rpcSeqNum: NewTxIdFromMsg(args.GetRpcSeqNum()), abortTxIds: args.GetAbortTxIds()}
	_, r := o.ExecCommitAbort(op, op.rpcSeqNum, args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o AbortParticipantOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.GetGroupLeader(o.groupId, l)
}

///////////////////////////////////////////////////////////////////

type CommitParticipantOp struct {
	rpcSeqNum  TxId
	commitTxId TxId
	groupId    string
}

func NewCommitParticipantOp(rpcSeqNum TxId, commitTxId TxId, groupId string) CommitParticipantOp {
	return CommitParticipantOp{rpcSeqNum: rpcSeqNum, commitTxId: commitTxId, groupId: groupId}
}

func (o CommitParticipantOp) name() string {
	return "CommitParticipant"
}

func (o CommitParticipantOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
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
	if _, _, reply = n.raft.AppendEntriesLocal(NewAppendEntryCommitTxCommand(n.raft.currentTerm.Get(), o.commitTxId.toMsg())); reply != RaftReplyOk {
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

func (o *RpcMgr) CommitParticipant(msg RpcMsg) *common.Ack {
	args := &common.CommitArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CommitParticipant, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	op := CommitParticipantOp{rpcSeqNum: NewTxIdFromMsg(args.GetRpcSeqNum()), commitTxId: NewTxIdFromMsg(args.GetCommitTxId())}
	_, r := o.ExecCommitAbort(op, op.rpcSeqNum, args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o CommitParticipantOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.CommitArgs{RpcSeqNum: o.rpcSeqNum.toMsg(), CommitTxId: o.commitTxId.toMsg(), NodeListVer: nodeListVer}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitParticipantCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

///////////////////////////////////////////////////////////////////

type CommitMigrationParticipantOp struct {
	rpcSeqNum   TxId
	commitTxId  TxId
	groupId     string
	nodeListVer uint64
	addr        *common.NodeAddrInet4
	migrationId MigrationId
}

func NewCommitMigrationParticipantOp(rpcSeqNum TxId, commitTxId TxId, groupId string, nodeListVer uint64, addr *common.NodeAddrInet4, migrationId MigrationId) CommitMigrationParticipantOp {
	return CommitMigrationParticipantOp{rpcSeqNum: rpcSeqNum, commitTxId: commitTxId, groupId: groupId, nodeListVer: nodeListVer, addr: addr, migrationId: migrationId}
}

func (o CommitMigrationParticipantOp) name() string {
	return "CommitMigrationParticipant"
}

func (o CommitMigrationParticipantOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	if o.addr != nil {
		return RaftNode{addr: *o.addr}, true
	}
	return n.raftGroup.GetGroupLeader(o.groupId, l)
}

func (o CommitMigrationParticipantOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	op, ok := n.rpcMgr.DeleteAndGet(o.commitTxId)
	if !ok {
		log.Infof("%v, skip inactive tx, txId=%v", o.name(), o.commitTxId)
		return nil, nil, RaftReplyOk
	}
	if reply = n.dirtyMgr.AppendCommitMigrationLog(n.raft, o.commitTxId, o.migrationId); reply != RaftReplyOk {
		n.rpcMgr.Record(o.commitTxId, op.ret)
		log.Errorf("Failed: %v, AppendCommitMigrationLog, mTxId=%v, reply=%v", o.name(), o.migrationId, reply)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, commitTxId=%v, unlocked=%v", o.name(), o.commitTxId, op.ret.unlock != nil)
	return nil, op.ret.unlock, RaftReplyOk
}

func (o CommitMigrationParticipantOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o *RpcMgr) CommitMigrationParticipant(msg RpcMsg) *common.Ack {
	args := &common.CommitMigrationArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CommitParticipant, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	op := CommitMigrationParticipantOp{
		rpcSeqNum: NewTxIdFromMsg(args.GetRpcSeqNum()), commitTxId: NewTxIdFromMsg(args.GetCommitTxId()), migrationId: NewMigrationIdFromMsg(args.GetMigrationId()),
	}
	_, r := o.ExecCommitAbort(op, op.rpcSeqNum, args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o CommitMigrationParticipantOp) remote(n *NodeServer, sa common.NodeAddrInet4, _ uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.CommitMigrationArgs{
		RpcSeqNum: o.rpcSeqNum.toMsg(), CommitTxId: o.commitTxId.toMsg(), MigrationId: o.migrationId.toMsg(),
		NodeListVer: o.nodeListVer,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitMigrationParticipantCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

///////////////////////////////////////////////////////////////////
//////////// Meta Operations without transactions /////////////////
///////////////////////////////////////////////////////////////////

type UpdateMetaAttrOp struct {
	txId TxId
	attr MetaAttributes
	ts   int64
}

func NewUpdateMetaAttrOp(txId TxId, attr MetaAttributes, ts int64) UpdateMetaAttrOp {
	return UpdateMetaAttrOp{txId: txId, attr: attr, ts: ts}
}

func (o UpdateMetaAttrOp) name() string {
	return "UpdateMetaAttr"
}

func (o UpdateMetaAttrOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	var working *WorkingMeta
	working, reply = n.inodeMgr.UpdateMetaAttr(o.attr, o.ts)
	return working, nil, reply
}

func (o UpdateMetaAttrOp) writeLog(_ *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o UpdateMetaAttrOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.UpdateMetaAttrRpcMsg{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.attr.inodeKey), Mode: o.attr.mode, Ts: o.ts,
	}
	msg := &common.MetaTxMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateMetaAttrCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
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

func (o *RpcMgr) UpdateMetaAttr(msg RpcMsg) *common.MetaTxMsg {
	args := &common.UpdateMetaAttrRpcMsg{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: UpdateMetaAttr, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.MetaTxMsg{Status: reply}
	}
	op := UpdateMetaAttrOp{
		txId: NewTxIdFromMsg(args.GetTxId()), attr: MetaAttributes{inodeKey: InodeKeyType(args.GetInodeKey()), mode: args.GetMode()}, ts: args.GetTs(),
	}
	ret, r := o.ExecCommitAbort(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.MetaTxMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return &common.MetaTxMsg{NewMeta: ret.ext.(*WorkingMeta).toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o UpdateMetaAttrOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.attr.inodeKey)
}

///////////////////////////////////////////////////////////////////
/////////////////// Prepare Meta Operations ///////////////////////
///////////////////////////////////////////////////////////////////

type CreateMetaOp struct {
	txId            TxId
	inodeKey        InodeKeyType
	parentInodeAttr MetaAttributes
	newKey          string
	chunkSize       int64
	expireMs        int32
	mode            uint32
}

func NewCreateMetaOp(txId TxId, inodeKey InodeKeyType, parentInodeAttr MetaAttributes, newKey string, mode uint32, chunkSize int64, expireMs int32) CreateMetaOp {
	return CreateMetaOp{txId: txId, inodeKey: inodeKey, parentInodeAttr: parentInodeAttr, newKey: newKey, mode: mode, chunkSize: chunkSize, expireMs: expireMs}
}

func (o CreateMetaOp) name() string {
	return "CreateMeta"
}

func (o CreateMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	stag, unlock, reply := n.inodeMgr.PrepareCreateMeta(o.inodeKey, o.parentInodeAttr, o.chunkSize, o.expireMs, o.mode)
	if reply != RaftReplyOk {
		log.Errorf("Failed: %v, prepareCreateMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, inodeKey=%v, txId=%v", o.name(), o.inodeKey, o.txId)
	return stag, unlock, RaftReplyOk
}

func (o CreateMetaOp) writeLog(n *NodeServer, ext interface{}) int32 {
	msg := &common.CreateMetaMsg{TxId: o.txId.toMsg(), Meta: ext.(*WorkingMeta).toMsg(), NewKey: o.newKey}
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryCreateMetaCmdId, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o CreateMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginCreateMetaArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey),
		ParentInodeAttr: o.parentInodeAttr.toMsg(""), NewKey: o.newKey, ChunkSize: o.chunkSize, Mode: o.mode, ExpireMs: o.expireMs,
	}
	msg := &common.MetaTxMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCreateMetaCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
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

func (o *RpcMgr) CreateMeta(msg RpcMsg) *common.MetaTxMsg {
	args := &common.BeginCreateMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CreateMeta, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.MetaTxMsg{Status: reply}
	}
	op := CreateMetaOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()),
		parentInodeAttr: NewMetaAttributesFromMsg(args.GetParentInodeAttr()), newKey: args.GetNewKey(), chunkSize: args.GetChunkSize(), mode: args.GetMode(),
		expireMs: args.GetExpireMs(),
	}
	ret, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.MetaTxMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return &common.MetaTxMsg{NewMeta: ret.ext.(*WorkingMeta).toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o CreateMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type LinkMetaOp struct {
	txId      TxId
	inodeKey  InodeKeyType
	childName string
	childAttr MetaAttributes
}

func NewLinkMetaOp(txId TxId, inodeKey InodeKeyType, childName string, childAttr MetaAttributes) LinkMetaOp {
	return LinkMetaOp{txId: txId, inodeKey: inodeKey, childName: childName, childAttr: childAttr}
}

func (o LinkMetaOp) name() string {
	return "LinkMeta"
}

func (o LinkMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	_, meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareCommitMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	// To make rename transactions idempotent, EEXIST is ignored here but reported since only coordinator can check results of two separated link and unlink operations.
	// The coordinator can abort rename operation (hardlink coordinator just ignores this warning)
	r := RenameRet{mayErrorReply: RaftReplyOk}
	if _, ok := meta.childAttrs[o.childName]; ok {
		log.Warnf("%v, file already exists, inodeKey=%v, childName=%v", o.name(), o.inodeKey, o.childName)
		r.mayErrorReply = ErrnoToReply(unix.EEXIST)
	}
	meta.childAttrs[o.childName] = o.childAttr
	r.meta = meta
	log.Debugf("Success: %v, inodeKey=%v, child=%v, txId=%v", o.name(), o.inodeKey, o.childName, o.txId)
	return r, unlock, RaftReplyOk
}

func (o LinkMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateMetaCmdId, &common.UpdateMetaMsg{TxId: o.txId.toMsg(), Meta: ret.(RenameRet).meta.toMsg()}); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o LinkMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginLinkMetaArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey),
		ChildAttr: o.childAttr.toMsg(o.childName),
	}
	msg := &common.RenameRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcLinkMetaCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return NewRenameRetFromMsg(msg)
}

func (o *RpcMgr) LinkMeta(msg RpcMsg) *common.RenameRetMsg {
	args := &common.BeginLinkMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: LinkMeta, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.RenameRetMsg{Status: reply}
	}
	op := LinkMetaOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), childName: args.GetChildAttr().GetName(),
		childAttr: NewMetaAttributesFromMsg(args.GetChildAttr()),
	}
	ret, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.RenameRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.ext.(RenameRet).toMsg(r)
}

func (o LinkMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type CreateChildMetaOp struct {
	txId      TxId
	inodeKey  InodeKeyType
	childName string
	childAttr MetaAttributes
}

func NewCreateChildMetaOp(txId TxId, inodeKey InodeKeyType, childName string, childAttr MetaAttributes) CreateChildMetaOp {
	return CreateChildMetaOp{txId: txId, inodeKey: inodeKey, childName: childName, childAttr: childAttr}
}

func (o CreateChildMetaOp) name() string {
	return "CreateChildMeta"
}

func (o CreateChildMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	_, meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareCommitMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	old, ok := meta.childAttrs[o.childName]
	if ok && old.inodeKey != o.inodeKey {
		unlock(n)
		//race condition. abort. we can check retried transaction by checking the inode key
		log.Errorf("Failed: %v, child is already exist. inodeKey=%v (requested: %v)", old.inodeKey, old.inodeKey, o.inodeKey)
		return nil, nil, ErrnoToReply(unix.EEXIST)
	}
	meta.childAttrs[o.childName] = o.childAttr
	log.Debugf("Success: %v, inodeKey=%v, childName=%v, newInodeId=%v, txId=%v", o.name(), o.inodeKey, o.childName, o.inodeKey, o.txId)
	return meta, unlock, RaftReplyOk
}

func (o CreateChildMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateParentMetaCmdId, &common.UpdateMetaMsg{TxId: o.txId.toMsg(), Meta: ret.(*WorkingMeta).toMsg()}); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o CreateChildMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginCreateChildMetaArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), ChildAttr: o.childAttr.toMsg(o.childName),
	}
	msg := &common.MetaTxMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCreateChildMetaCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewWorkingMetaFromMsg(msg.GetNewMeta()), r
}

func (o *RpcMgr) CreateChildMeta(msg RpcMsg) *common.MetaTxMsg {
	args := &common.BeginCreateChildMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CreateChildMeta, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.MetaTxMsg{Status: reply}
	}
	op := CreateChildMetaOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()),
		childName: args.GetChildAttr().GetName(), childAttr: NewMetaAttributesFromMsg(args.GetChildAttr()),
	}
	ret, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.MetaTxMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return &common.MetaTxMsg{NewMeta: ret.ext.(*WorkingMeta).toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

func (o TruncateMetaOp) name() string {
	return "TruncateMeta"
}

func (o TruncateMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	_, meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareCommitMetaLocal, inodeKey=%v, newSize=%v, reply=%v", o.name(), o.inodeKey, o.newSize, reply)
		return nil, nil, reply
	}
	meta.size = o.newSize
	log.Debugf("Success: %v, inodeKey=%v, size=%v->%v", o.name(), o.inodeKey, meta.prevVer.size, o.newSize)
	return meta, unlock, RaftReplyOk
}

func (o TruncateMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateMetaCmdId, &common.UpdateMetaMsg{TxId: o.txId.toMsg(), Meta: ret.(*WorkingMeta).toMsg()}); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o TruncateMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginTruncateMetaArgs{NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), NewObjectSize: o.newSize}
	msg := &common.MetaTxWithPrevMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcTruncateMetaCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
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

func (o *RpcMgr) TruncateMeta(msg RpcMsg) *common.MetaTxWithPrevMsg {
	args := &common.BeginTruncateMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: TruncateMeta, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.MetaTxWithPrevMsg{Status: reply}
	}
	op := TruncateMetaOp{txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), newSize: args.GetNewObjectSize()}
	ret, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.MetaTxWithPrevMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	meta := ret.ext.(*WorkingMeta)
	return &common.MetaTxWithPrevMsg{NewMeta: meta.toMsg(), Prev: meta.prevVer.toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

func (o UpdateMetaSizeOp) name() string {
	return "UpdateMetaSize"
}

func (o UpdateMetaSizeOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	_, meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey)
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
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateMetaCmdId, &common.UpdateMetaMsg{TxId: o.txId.toMsg(), Meta: ret.(*WorkingMeta).toMsg()}); reply != RaftReplyOk {
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
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateMetaSizeCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
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

func (o *RpcMgr) UpdateMetaSize(msg RpcMsg) *common.MetaTxWithPrevMsg {
	args := &common.BeginUpdateMetaSizeArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: UpdateMetaSize, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.MetaTxWithPrevMsg{Status: reply}
	}
	op := UpdateMetaSizeOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), newSize: args.GetNewSize(), mTime: args.GetMTime(), mode: args.GetMode(),
	}
	ret, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.MetaTxWithPrevMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	meta := ret.ext.(*WorkingMeta)
	return &common.MetaTxWithPrevMsg{NewMeta: meta.toMsg(), Prev: meta.prevVer.toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

func (o DeleteMetaOp) name() string {
	return "DeleteMeta"
}

func (o DeleteMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	_, meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareCommitMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	if meta.IsDir() && len(meta.childAttrs) > 2 {
		unlock(n)
		return nil, nil, ErrnoToReply(unix.ENOTEMPTY)
	}
	// do not care if removedKey does exist or not here to make this idempotent
	meta.size = 0
	log.Debugf("Success: %v, inodeKey=%v, txId=%v", o.name(), o.inodeKey, o.txId)
	return meta, unlock, RaftReplyOk
}

func (o DeleteMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryDeleteMetaCmdId, &common.DeleteMetaMsg{TxId: o.txId.toMsg(), Meta: ret.(*WorkingMeta).toMsg(), Key: o.removedKey}); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o DeleteMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginDeleteMetaArgs{NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), RemovedKey: o.removedKey}
	msg := &common.MetaTxWithPrevMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcDeleteMetaCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
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

func (o *RpcMgr) DeleteMeta(msg RpcMsg) *common.MetaTxWithPrevMsg {
	args := &common.BeginDeleteMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: DeleteMeta, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.MetaTxWithPrevMsg{Status: reply}
	}
	op := DeleteMetaOp{txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), removedKey: args.GetRemovedKey()}
	ret, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.MetaTxWithPrevMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	meta := ret.ext.(*WorkingMeta)
	return &common.MetaTxWithPrevMsg{NewMeta: meta.toMsg(), Prev: meta.prevVer.toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o DeleteMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type RenameRet struct {
	meta          *WorkingMeta
	mayErrorReply int32
}

func NewRenameRetFromMsg(msg *common.RenameRetMsg) (*RenameRet, RaftBasicReply) {
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return &RenameRet{
		meta: NewWorkingMetaFromMsg(msg.GetParent()), mayErrorReply: msg.GetMayErrorReply(),
	}, r
}

func (d RenameRet) toMsg(r RaftBasicReply) *common.RenameRetMsg {
	return &common.RenameRetMsg{
		Status: r.reply, Leader: r.GetLeaderNodeMsg(), Parent: d.meta.toMsg(), MayErrorReply: d.mayErrorReply,
	}
}

type UnlinkMetaOp struct {
	txId      TxId
	inodeKey  InodeKeyType
	childName string
}

func NewUnlinkMetaOp(txId TxId, inodeKey InodeKeyType, childName string) UnlinkMetaOp {
	return UnlinkMetaOp{txId: txId, inodeKey: inodeKey, childName: childName}
}

func (o UnlinkMetaOp) name() string {
	return "UnlinkMeta"
}

func (o UnlinkMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	_, meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareCommitMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, unlock, reply
	}
	// To make rename transactions idempotent, ENOENT is ignored here but reported since only coordinator can check results of two separated link and unlink operations.
	// The coordinator can abort rename operation (delete coordinator just ignores this warning)
	r := RenameRet{mayErrorReply: RaftReplyOk}
	if _, ok := meta.childAttrs[o.childName]; !ok {
		log.Warnf("%v, inode does not exist. inodeKey=%v, child=%v, txId=%v", o.name(), o.inodeKey, o.childName, o.txId)
		r.mayErrorReply = ErrnoToReply(unix.ENOENT)
	}
	delete(meta.childAttrs, o.childName)
	r.meta = meta
	log.Debugf("Success: %v, inodeKey=%v, child=%v, txId=%v", o.name(), o.inodeKey, o.childName, o.txId)
	return r, unlock, RaftReplyOk
}

func (o UnlinkMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	r := ret.(RenameRet)
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateMetaCmdId, &common.UpdateMetaMsg{TxId: o.txId.toMsg(), Meta: r.meta.toMsg()}); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o UnlinkMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginUnlinkMetaArgs{NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), ChildName: o.childName}
	msg := &common.RenameRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUnlinkMetaCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewRenameRetFromMsg(msg)
}

func (o *RpcMgr) UnlinkMeta(msg RpcMsg) *common.RenameRetMsg {
	args := &common.BeginUnlinkMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: UnlinkMeta, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.RenameRetMsg{Status: reply}
	}
	op := UnlinkMetaOp{txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), childName: args.GetChildName()}
	opRet, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.RenameRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return opRet.ext.(RenameRet).toMsg(r)
}

func (o UnlinkMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type RenameMetaOp struct {
	txId      TxId
	inodeKey  InodeKeyType
	srcName   string
	dstName   string
	childAttr MetaAttributes
}

func NewRenameMetaOp(txId TxId, inodeKey InodeKeyType, srcName string, dstName string, childAttr MetaAttributes) RenameMetaOp {
	return RenameMetaOp{txId: txId, inodeKey: inodeKey, srcName: srcName, dstName: dstName, childAttr: childAttr}
}

func (o RenameMetaOp) name() string {
	return "RenameMeta"
}

func (o RenameMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	_, meta, unlock, reply := n.inodeMgr.PrepareUpdateMeta(o.inodeKey)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareCommitMetaLocal, inodeKey=%v, r=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	_, srcExist := meta.childAttrs[o.srcName]
	_, dstExist := meta.childAttrs[o.dstName]
	// we allow entering rename transaction if src exists and dst doesn't or if dst exists and src doesn't exist to ensure idempotency (i.e., allow retries).
	// other patterns can be simple mistakes or racy conditions.
	if !srcExist && !dstExist {
		unlock(n)
		log.Errorf("Failed: %v, srcName and dstName do not exist, inodeKey=%v, srcName=%v, dstName=%v", o.name(), o.inodeKey, o.srcName, o.dstName)
		return nil, nil, ErrnoToReply(unix.ENOENT)
	}
	delete(meta.childAttrs, o.srcName)
	meta.childAttrs[o.dstName] = o.childAttr
	log.Debugf("Success: %v, inodeKey=%v, name=%v->%v, txId=%v", o.name(), o.inodeKey, o.srcName, o.dstName, o.txId)
	return meta, unlock, RaftReplyOk
}

func (o RenameMetaOp) writeLog(n *NodeServer, ret interface{}) int32 {
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateParentMetaCmdId, &common.UpdateMetaMsg{TxId: o.txId.toMsg(), Meta: ret.(*WorkingMeta).toMsg()}); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o RenameMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginRenameMetaArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey), SrcName: o.srcName, DstName: o.dstName, ChildAttr: o.childAttr.toMsg(""),
	}
	msg := &common.MetaTxMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcRenameMetaCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewWorkingMetaFromMsg(msg.GetNewMeta()), r
}

func (o *RpcMgr) RenameMeta(msg RpcMsg) *common.MetaTxMsg {
	args := &common.BeginRenameMetaArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: RenameMeta, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.MetaTxMsg{Status: reply}
	}
	op := RenameMetaOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), srcName: args.GetSrcName(), dstName: args.GetDstName(),
		childAttr: NewMetaAttributesFromMsg(args.GetChildAttr()),
	}
	opRet, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.MetaTxMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return &common.MetaTxMsg{NewMeta: opRet.ext.(*WorkingMeta).toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o RenameMetaOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

///////////////////////////////////////////////////////////////////

type UpdateMetaKeyOp struct {
	txId     TxId
	inodeKey InodeKeyType
	parent   MetaAttributes
	oldKey   string
	newKey   string
}

func NewUpdateMetaKeyOp(txId TxId, inodeKey InodeKeyType, oldKey string, newKey string, parent MetaAttributes) UpdateMetaKeyOp {
	return UpdateMetaKeyOp{txId: txId, inodeKey: inodeKey, oldKey: oldKey, newKey: newKey, parent: parent}
}

func (o UpdateMetaKeyOp) name() string {
	return "UpdateMetaKey"
}

func (o UpdateMetaKeyOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	_, working, unlock, reply := n.inodeMgr.PrepareUpdateMetaKey(o.inodeKey, o.oldKey, o.parent, n.flags.ChunkSizeBytes, n.flags.DirtyExpireIntervalMs)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, prepareMutateMetaLocal, inodeKey=%v, reply=%v", o.name(), o.inodeKey, reply)
		return nil, nil, reply
	}
	log.Debugf("Success: %v, inodeKey=%v", o.name(), o.inodeKey)
	return working, unlock, RaftReplyOk
}

func (o UpdateMetaKeyOp) writeLog(n *NodeServer, ret interface{}) int32 {
	msg := &common.UpdateMetaKeyMsg{TxId: o.txId.toMsg(), Meta: ret.(*WorkingMeta).toMsg(), OldKey: o.oldKey, NewKey: o.newKey}
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateMetaKeyCmdId, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), o.txId, reply)
		return reply
	}
	return RaftReplyOk
}

func (o UpdateMetaKeyOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.BeginUpdateMetaKeyArgs{
		NodeListVer: nodeListVer, TxId: o.txId.toMsg(), InodeKey: uint64(o.inodeKey),
		OldKey: o.oldKey, NewKey: o.newKey, Parent: o.parent.toMsg(""),
	}
	msg := &common.MetaTxMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateMetaKeyCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewWorkingMetaFromMsg(msg.GetNewMeta()), r
}

func (o *RpcMgr) UpdateMetaKey(msg RpcMsg) *common.MetaTxMsg {
	args := &common.BeginUpdateMetaKeyArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: UpdateMetaKey, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.MetaTxMsg{Status: reply}
	}
	op := UpdateMetaKeyOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()),
		oldKey: args.GetOldKey(), newKey: args.GetNewKey(), parent: NewMetaAttributesFromMsg(args.GetParent()),
	}
	ret, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.MetaTxMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return &common.MetaTxMsg{NewMeta: ret.ext.(*WorkingMeta).toMsg(), Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

func (o CommitUpdateChunkOp) name() string {
	return "CommitUpdateChunk"
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
				updateType: StagingChunkData, fileOffset: stag.GetFileOffset(),
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
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryCommitChunkCmdId, NewAppendCommitUpdateChunkMsg(o.newMeta, chunks, false)); reply != RaftReplyOk {
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
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitUpdateChunkCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, txId=%v, reply=%v", o.name(), sa, o.txId, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) CommitUpdateChunk(msg RpcMsg) *common.Ack {
	args := &common.CommitUpdateChunksArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CommitUpdateChunk, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	newMeta := NewWorkingMetaFromMsg(args.GetMeta())
	newMeta.prevVer = NewWorkingMetaFromMsg(args.GetPrev())
	op := CommitUpdateChunkOp{txId: NewTxIdFromMsg(args.GetTxId()), newMeta: newMeta, record: args.GetChunk()}
	_, r := o.ExecCommitAbort(op, op.txId, args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

func (o CommitDeleteChunkOp) name() string {
	return "CommitDeleteChunk"
}

func (o CommitDeleteChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	n.inodeMgr.nodeLock.RLock()
	alignedOffset := o.offset - o.offset%o.newMeta.chunkSize
	chunk, working := n.inodeMgr.PrepareUpdateChunk(o.newMeta, alignedOffset)
	unlock = func(*NodeServer) {
		chunk.lock.Unlock()
		n.inodeMgr.nodeLock.RUnlock()
	}
	working.AddStag(&StagingChunk{
		filled:     1,
		length:     o.deleteLen,
		slop:       o.offset - alignedOffset,
		updateType: StagingChunkDelete,
	})
	msg := NewAppendCommitUpdateChunkMsg(o.newMeta, map[int64]*WorkingChunk{alignedOffset: working}, true)
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryCommitChunkCmdId, msg); reply != RaftReplyOk {
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
		Meta:         o.newMeta.toMsg(),
		Prev:         o.newMeta.prevVer.toMsg(),
		Offset:       o.offset,
		DeleteLength: o.deleteLen,
		NodeListVer:  nodeListVer,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitDeleteChunkCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) CommitDeleteChunk(msg RpcMsg) *common.Ack {
	args := &common.CommitDeleteChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CommitDeleteChunk, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	newMeta := NewWorkingMetaFromMsg(args.GetMeta())
	newMeta.prevVer = NewWorkingMetaFromMsg(args.GetPrev())
	op := CommitDeleteChunkOp{txId: txId, newMeta: newMeta, offset: args.GetOffset(), deleteLen: args.GetDeleteLength()}
	_, r := o.ExecCommitAbort(op, txId, args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

func (o CommitExpandChunkOp) name() string {
	return "CommitExpandChunk"
}

func (o CommitExpandChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	n.inodeMgr.nodeLock.RLock()
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
		n.inodeMgr.nodeLock.RUnlock()
	}
	msg := NewAppendCommitUpdateChunkMsg(o.newMeta, map[int64]*WorkingChunk{alignedOffset: working}, false)
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryCommitChunkCmdId, msg); reply != RaftReplyOk {
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
		Meta:         o.newMeta.toMsg(),
		Prev:         o.newMeta.prevVer.toMsg(),
		Offset:       o.offset,
		ExpandLength: o.expandLen,
		NodeListVer:  nodeListVer,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitExpandChunkCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) CommitExpandChunk(msg RpcMsg) *common.Ack {
	args := &common.CommitExpandChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CommitExpandChunk, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	newMeta := NewWorkingMetaFromMsg(args.GetMeta())
	newMeta.prevVer = NewWorkingMetaFromMsg(args.GetPrev())
	op := CommitExpandChunkOp{txId: txId, newMeta: newMeta, offset: args.GetOffset(), expandLen: args.GetExpandLength()}
	_, r := o.ExecCommitAbort(op, txId, args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

	fileId     FileIdType
	fileOffset int64
	dataLength uint32
}

func NewUpdateChunkOp(txId TxId, inodeKey InodeKeyType, chunkSize int64, offset int64, buf []byte) UpdateChunkOp {
	return UpdateChunkOp{txId: txId, inodeKey: inodeKey, chunkSize: chunkSize, offset: offset, buf: buf}
}

func (o UpdateChunkOp) name() string {
	return "UpdateChunk"
}

func (o UpdateChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	if o.buf != nil {
		o.dataLength = uint32(len(o.buf))
		o.fileOffset, reply = n.inodeMgr.AppendStagingChunkBuffer(o.inodeKey, o.offset, o.chunkSize, o.buf)
	} else {
		reply = n.inodeMgr.AppendStagingChunkFile(o.inodeKey, o.offset, o.fileId, o.fileOffset, o.dataLength)
	}
	if reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendStagingChunk, fileId=%v, reply=%v", o.name(), o.fileId, reply)
		return &common.StagingChunkMsg{TxId: o.txId.toMsg()}, nil, reply
	}
	//log.Debugf("Success: %v, txId=%v, inodeKey=%v, offset=%v, length=%v", o.txId, o.inodeKey, o.offset, o.dataLength)
	stag := &common.StagingChunkMsg{TxId: o.txId.toMsg(), Offset: o.offset, Length: int64(o.dataLength), FileOffset: o.fileOffset}
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
	msg := &common.UpdateChunkRet{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateChunkCmdId, args, sa, timeout, n.raft.files, [][]byte{o.buf}, nil, msg); reply != RaftReplyOk {
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

func (o *RpcMgr) UpdateChunkBottomHalf(m proto.Message, fileId FileIdType, fileOffset int64, dataLength uint32, r RaftBasicReply) *common.UpdateChunkRet {
	if r.reply != RaftReplyOk {
		return &common.UpdateChunkRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	args := m.(*common.UpdateChunkArgs)
	op := UpdateChunkOp{
		txId: NewTxIdFromMsg(args.GetTxId()), inodeKey: InodeKeyType(args.GetInodeKey()), chunkSize: args.GetChunkSize(),
		offset: args.GetOffset(), buf: nil, fileId: fileId, fileOffset: fileOffset, dataLength: dataLength,
	}
	var ret RpcRet
	ret, r.reply = o.__execSingle(op, op.txId)
	if r.reply != RaftReplyOk {
		return &common.UpdateChunkRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return &common.UpdateChunkRet{Status: r.reply, Leader: r.GetLeaderNodeMsg(), Stag: ret.ext.(*common.StagingChunkMsg)}
}

func (o UpdateChunkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getChunkOwnerNodeLocalNew(l, o.inodeKey, o.offset, o.chunkSize)
}

///////////////////////////////////////////////////////////////////
///////////////// Persist Chunk Operations ////////////////////////
///////////////////////////////////////////////////////////////////

type CommitPersistChunkOp struct {
	txId        TxId
	commitTxId  TxId
	groupId     string
	inodeKey    InodeKeyType
	offsets     []int64
	cVers       []uint32
	newFetchKey string
}

func NewCommitPersistChunkOp(txId TxId, commitTxId TxId, groupId string, offsets []int64, cVers []uint32, inodeKey InodeKeyType, newFetchKey string) CommitPersistChunkOp {
	return CommitPersistChunkOp{
		txId: txId, commitTxId: commitTxId, groupId: groupId, inodeKey: inodeKey, offsets: offsets, cVers: cVers, newFetchKey: newFetchKey,
	}
}

func (o CommitPersistChunkOp) name() string {
	return "CommitPersistChunk"
}

func (o CommitPersistChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	op, ok := n.rpcMgr.DeleteAndGet(o.commitTxId)
	if !ok {
		log.Infof("%v, skip inactive tx, txId=%v", o.name(), o.commitTxId)
		return nil, nil, RaftReplyOk
	}
	msg := &common.PersistedChunkInfoMsg{InodeKey: uint64(o.inodeKey), Offsets: o.offsets, CVers: o.cVers, NewFetchKey: o.newFetchKey}
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryPersistChunkCmdId, msg); reply != RaftReplyOk {
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
		NewFetchKey: o.newFetchKey,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCommitPersistChunkCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) CommitPersistChunk(msg RpcMsg) *common.Ack {
	args := &common.CommitPersistChunkArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CommitPersistChunk, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	op := CommitPersistChunkOp{
		txId: NewTxIdFromMsg(args.GetTxId()), commitTxId: NewTxIdFromMsg(args.GetCommitTxId()),
		groupId: "", inodeKey: InodeKeyType(args.GetInodeKey()), offsets: args.GetOffsets(), cVers: args.GetCVers(), newFetchKey: args.GetNewFetchKey(),
	}
	_, r := o.ExecCommitAbort(op, op.txId, args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

func (o MpuAddOp) name() string {
	return "MpuAdd"
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
	c.con, c.seqNum, reply = n.rpcClient.AsyncObjcacheRpc(RpcMpuAddCmdId, args, o.leader.addr, n.raft.files, nil, nil)
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

func (o *RpcMgr) MpuAdd(msg RpcMsg) *common.MpuAddRet {
	args := &common.MpuAddArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: MpuAdd, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.MpuAddRet{Status: reply}
	}
	op := MpuAddOp{
		leader: o.n.selfNode, nodeListVer: args.GetNodeListVer(),
		txId: NewTxIdFromMsg(args.GetTxId()), metaKey: args.GetMetaKey(), meta: NewWorkingMetaFromMsg(args.GetMeta()),
		offsets: args.GetOffsets(), uploadId: args.GetUploadId(), priority: int(args.GetPriority()),
	}
	ret, r := o.ExecPrepare(op, op.txId, args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.MpuAddRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	outs := ret.ext.([]MpuAddOut)
	outMsg := make([]*common.MpuAddRetIndex, 0)
	for _, out := range outs {
		outMsg = append(outMsg, &common.MpuAddRetIndex{Index: out.idx, Etag: out.etag, CVer: out.cVer})
	}
	return &common.MpuAddRet{Outs: outMsg, Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o MpuAddOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	if l.version != o.nodeListVer {
		log.Errorf("Failed: %v.GetLeader, nodeListVer is updated, current=%v, expected=%v", o.name(), l.version, o.nodeListVer)
		return RaftNode{}, false
	}
	return o.leader, true
}

///////////////////////////////////////////////////////////////////
///////////////////// Node List Operations ////////////////////////

func CallGetApiPort(n *NodeServer, sa common.NodeAddrInet4) (apiPort int, reply int32) {
	for {
		msg := &common.GetApiPortRet{}
		if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateNodeListCmdId, &common.GetApiPortArgs{}, sa, n.flags.RpcTimeoutDuration, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
			log.Errorf("Failed: CallGetApiPort, CallObjcacheRpc, sa=%v, reply=%v", sa, reply)
			return -1, reply
		}
		r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		if r.reply == RaftReplyOk {
			return int(msg.GetApiPort()), RaftReplyOk
		}
		if r.reply == RaftReplyNotLeader {
			sa = r.leaderAddr
		}
		if needRetry(r.reply) {
			continue
		}
		return -1, r.reply
	}
}

func (o *RpcMgr) GetApiPort(msg RpcMsg) *common.GetApiPortRet {
	args := &common.UpdateNodeListArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: GetApiPort, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.GetApiPortRet{Status: reply}
	}
	r := o.n.raft.IsLeader()
	ret := &common.GetApiPortRet{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	if r.reply != RaftReplyOk {
		return ret
	}
	ret.ApiPort = int32(o.n.args.ApiPort)
	return ret
}

///////////////////////////////////////////////////////////////////

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

func (o UpdateNodeListOp) name() string {
	return "UpdateNodeList"
}

func (o UpdateNodeListOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	n.inodeMgr.SuspendNode()
	unlock = func(n *NodeServer) {
		n.inodeMgr.ResumeNode()
	}
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
	msg := &common.UpdateNodeListMsg{
		TxId: o.txId.toMsg(), MigrationId: o.migrationId.toMsg(),
		Nodes: &common.MembershipListMsg{}, IsAdd: o.isAdd, NeedRestore: true,
	}
	msg.Nodes.Servers = append(msg.Nodes.Servers, o.added.toMsg())
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateNodeListCmdId, msg); reply != RaftReplyOk {
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
	if reply := n.rpcClient.CallObjcacheRpc(RpcUpdateNodeListCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return msg.GetNeedRestore(), NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) UpdateNodeList(msg RpcMsg) *common.UpdateNodeListRet {
	args := &common.UpdateNodeListArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: UpdateNodeList, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.UpdateNodeListRet{Status: reply}
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	op := UpdateNodeListOp{
		txId: txId, isAdd: args.GetIsAdd(), added: NewRaftNodeFromMsg(args.GetNode()),
		leaderGroupId: "", target: nil, migrationId: NewMigrationIdFromMsg(args.GetMigrationId()),
	}
	ret, r := o.ExecPrepare(op, txId, args.GetNodeListVer())
	var needRestore = false
	if r.reply == RaftReplyOk {
		needRestore = ret.ext.(bool)
	}
	return &common.UpdateNodeListRet{Status: r.reply, Leader: r.GetLeaderNodeMsg(), NeedRestore: needRestore}
}
func (o UpdateNodeListOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	if o.target != nil {
		return RaftNode{addr: *o.target}, true
	}
	return n.raftGroup.GetGroupLeader(o.leaderGroupId, l)
}

///////////////////////////////////////////////////////////////////

type InitNodeListOp struct {
	txId           TxId
	migrationId    MigrationId
	nodes          []*common.NodeMsg
	newNodeListVer uint64
	target         RaftNode
}

func NewInitNodeListOp(txId TxId, nodeList *RaftNodeList, target RaftNode, migrationId MigrationId) InitNodeListOp {
	nodes := make([]*common.NodeMsg, 0)
	for _, ns := range nodeList.groupNode {
		for _, node := range ns {
			nodes = append(nodes, node.toMsg())
		}
	}
	return InitNodeListOp{txId: txId, migrationId: migrationId, nodes: nodes, newNodeListVer: nodeList.version + 1, target: target}
}

func (o InitNodeListOp) name() string {
	return "InitNodeList"
}

func (o InitNodeListOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	msg := &common.UpdateNodeListMsg{
		TxId: o.txId.toMsg(), MigrationId: o.migrationId.toMsg(),
		Nodes: &common.MembershipListMsg{Servers: o.nodes}, IsAdd: true, NodeListVer: o.newNodeListVer,
	}
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryUpdateNodeListCmdId, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, reply, r=%v", o.name(), reply)
	}
	return nil, nil, reply
}

func (o InitNodeListOp) writeLog(n *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o InitNodeListOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.InitNodeListArgs{
		Nodes:          o.nodes,
		TxId:           o.txId.toMsg(),
		NodeListVer:    nodeListVer,
		NewNodeListVer: o.newNodeListVer,
		MigrationId:    o.migrationId.toMsg(),
	}
	msg := &common.UpdateNodeListRet{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcInitNodeListCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) InitNodeList(msg RpcMsg) *common.UpdateNodeListRet {
	args := &common.InitNodeListArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: InitNodeList, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.UpdateNodeListRet{Status: reply}
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	op := InitNodeListOp{txId: txId, nodes: args.GetNodes(), newNodeListVer: args.GetNewNodeListVer(), migrationId: NewMigrationIdFromMsg(args.GetMigrationId())}
	_, r := o.ExecPrepare(op, txId, uint64(math.MaxUint64))
	return &common.UpdateNodeListRet{Status: r.reply, Leader: r.GetLeaderNodeMsg(), NeedRestore: false}
}
func (o InitNodeListOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return o.target, true
}

///////////////////////////////////////////////////////////////////

type RestoreDirtyMetaOp struct {
	target      RaftNode
	metas       []*common.CopiedMetaMsg
	files       []*common.InodeToFileMsg
	dirMetas    []*common.CopiedMetaMsg
	dirFiles    []*common.InodeToFileMsg
	migrationId MigrationId
}

func NewRestoreDirtyMetaOp(migrationId MigrationId, metas []*common.CopiedMetaMsg, files []*common.InodeToFileMsg, dirMetas []*common.CopiedMetaMsg, dirFiles []*common.InodeToFileMsg, target RaftNode) RestoreDirtyMetaOp {
	return RestoreDirtyMetaOp{target: target, metas: metas, files: files, dirMetas: dirMetas, dirFiles: dirFiles, migrationId: migrationId}
}

func (o RestoreDirtyMetaOp) name() string {
	return "RestoreDirtyMeta"
}

func (o RestoreDirtyMetaOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	n.dirtyMgr.RecordMigratedAddMetas(o.migrationId, o.metas, o.files)
	for _, m := range o.metas {
		log.Debugf("Success: %v, metadata map: inodeKey=%v", o.name(), m.GetInodeKey())
	}
	for _, f := range o.files {
		log.Debugf("Success: %v, inode to file map: inodeKey=%v, key=%v", o.name(), f.GetInodeKey(), f.GetFilePath())
	}
	n.dirtyMgr.RecordMigratedDirMetas(o.migrationId, o.dirMetas, o.dirFiles)
	for _, m := range o.dirMetas {
		log.Debugf("Success: %v, metadata map (Dir): inodeKey=%v", o.name(), m.GetInodeKey())
	}
	for _, f := range o.dirFiles {
		log.Debugf("Success: %v, inode to file map (Dir): inodeKey=%v, key=%v", o.name(), f.GetInodeKey(), f.GetFilePath())
	}
	return nil, nil, RaftReplyOk
}

func (o RestoreDirtyMetaOp) writeLog(n *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o RestoreDirtyMetaOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	mc := 0
	fc := 0
	dmc := 0
	dfc := 0
	lastReply := RaftBasicReply{reply: RaftReplyOk}
	for {
		args := &common.RestoreDirtyMetasArgs{MigrationId: o.migrationId.toMsg()}
		var send = false
		for {
			if len(o.metas) > mc+64 {
				args.Metas = o.metas[mc : mc+64]
				mc += 64
				send = true
				break
			} else if mc < len(o.metas) {
				args.Metas = o.metas[mc:]
				mc = len(o.metas)
				send = true
			}
			if len(o.files) > fc+64 {
				args.Files = o.files[fc : fc+64]
				fc += 64
				send = true
				break
			} else if fc < len(o.files) {
				args.Files = o.files[fc:]
				fc = len(o.files)
				send = true
			}
			if len(o.dirFiles) > dfc+64 {
				args.DirFiles = o.dirFiles[dfc : dfc+64]
				dfc += 64
				send = true
				break
			} else if dfc < len(o.dirFiles) {
				args.DirFiles = o.dirFiles[dfc:]
				dfc = len(o.dirFiles)
				send = true
			}
			if dmc < len(o.dirMetas) {
				total := 0
				args.DirMetas = make([]*common.CopiedMetaMsg, 0)
				dirMetas := o.dirMetas[dmc:]
				for i, dirMeta := range dirMetas {
					args.DirMetas = append(args.DirMetas, dirMeta)
					dmc += 1
					send = true
					total += len(dirMeta.Children)
					if i >= 64 || total >= 128 {
						break
					}
				}
			}
			break
		}
		if !send {
			break
		}
		msg := &common.Ack{}
		if reply := n.rpcClient.CallObjcacheRpc(RpcRestoreDirtyMetasCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
			log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
			return nil, RaftBasicReply{reply: reply}
		}
		lastReply = NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
		if lastReply.reply != RaftReplyOk {
			break
		}
	}
	log.Infof("Success: %v, target=%v, migrationId=%v, len(metas)=%v, len(files)=%v, len(dirMetas)=%v, len(dirFiles)=%v",
		o.name(), o.target, o.migrationId, len(o.metas), len(o.files), len(o.dirMetas), len(o.dirFiles))
	return nil, lastReply
}

func (o *RpcMgr) RestoreDirtyMeta(msg RpcMsg) *common.Ack {
	args := &common.RestoreDirtyMetasArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: RestoreDirtyMeta, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	r := o.n.raft.SyncBeforeClientQuery()
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: RestoreDirtyMeta, SyncBeforeClientQuery, r=%v", r)
		return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	op := RestoreDirtyMetaOp{
		target: o.n.selfNode, metas: args.GetMetas(), files: args.GetFiles(),
		dirMetas: args.GetDirMetas(), dirFiles: args.GetDirFiles(),
		migrationId: NewMigrationIdFromMsg(args.GetMigrationId()),
	}
	_, _, r.reply = op.local(o.n)
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

	fileId     FileIdType
	fileOffset int64
	dataLength uint32
}

func NewRestoreDirtyChunkOp(migrationId MigrationId, target RaftNode, inodeKey InodeKeyType, chunkSize int64, offset int64, objectSize int64, chunkVer uint32, bufs [][]byte) RestoreDirtyChunkOp {
	return RestoreDirtyChunkOp{
		migrationId: migrationId, target: target, inodeKey: inodeKey,
		chunkSize: chunkSize, offset: offset, objectSize: objectSize, chunkVer: chunkVer, bufs: bufs,
	}
}

func (o RestoreDirtyChunkOp) name() string {
	return "RestoreDirtyChunk"
}

func (o RestoreDirtyChunkOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	reply = n.inodeMgr.AppendStagingChunkFile(o.inodeKey, o.offset, o.fileId, o.fileOffset, o.dataLength)
	if reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendStagingChunkFile, inodeKey=%v, offset=%v, fileId=%v, fileOffset=%v, dataLength=%v, reply=%v",
			o.name(), o.inodeKey, o.offset, o.fileId, o.fileOffset, o.dataLength, reply)
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
		updateType: StagingChunkData, fileOffset: o.fileOffset,
	})
	n.dirtyMgr.RecordMigratedAddChunks(o.migrationId, chunk.inodeKey, o.chunkSize, working, o.offset, o.objectSize)
	log.Infof("Success: %v, target=%v, migrationId=%v, inodeKey=%v, offset=%v, dataLength=%v",
		o.name(), o.target, o.migrationId, o.inodeKey, o.offset, o.dataLength)
	return nil, nil, reply
}

func (o RestoreDirtyChunkOp) writeLog(n *NodeServer, _ interface{}) int32 {
	return RaftReplyOk
}

func (o RestoreDirtyChunkOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64, timeout time.Duration) (interface{}, RaftBasicReply) {
	args := &common.RestoreDirtyChunksArgs{
		MigrationId: o.migrationId.toMsg(), InodeKey: uint64(o.inodeKey),
		ChunkSize: o.chunkSize, ChunkVer: o.chunkVer, Offset: o.offset, ObjectSize: o.objectSize,
	}
	msg := &common.Ack{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcRestoreDirtyChunksCmdId, args, sa, timeout, n.raft.files, o.bufs, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) RestoreDirtyChunksBottomHalf(m proto.Message, fileId FileIdType, fileOffset int64, dataLength uint32, r RaftBasicReply) *common.Ack {
	if r.reply != RaftReplyOk {
		return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	args := m.(*common.RestoreDirtyChunksArgs)
	fn := RestoreDirtyChunkOp{
		migrationId: NewMigrationIdFromMsg(args.GetMigrationId()), inodeKey: InodeKeyType(args.GetInodeKey()), chunkSize: args.GetChunkSize(),
		offset: args.GetOffset(), chunkVer: args.GetChunkVer(), objectSize: args.GetObjectSize(),
		fileId: fileId, fileOffset: fileOffset, dataLength: dataLength,
	}
	_, _, r.reply = fn.local(o.n) // do not need to deduplicate requests
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o RestoreDirtyChunkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
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

func (o JoinMigrationOp) name() string {
	return "JoinMigration"
}

func (o JoinMigrationOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	l := n.raftGroup.GetNodeListLocal()
	newRing := l.ring.AddWeightedNode(o.target.groupId, 1)
	// all writes on this Node is blocked during this period
	n.inodeMgr.SuspendNode()
	unlock = func(n *NodeServer) {
		n.inodeMgr.ResumeNode()
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
	msg := &common.UpdateNodeListMsg{
		TxId: o.txId.toMsg(), MigrationId: o.migrationId.toMsg(),
		Nodes: &common.MembershipListMsg{Servers: []*common.NodeMsg{o.target.toMsg()}}, IsAdd: true, NeedRestore: true,
	}
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateNodeListCmdId, msg); reply != RaftReplyOk {
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
	if reply := n.rpcClient.CallObjcacheRpc(RpcJoinMigrationCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) JoinMigration(msg RpcMsg) *common.Ack {
	args := &common.MigrationArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: JoinMigration, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	op := JoinMigrationOp{target: NewRaftNodeFromMsg(args.GetNode()), leaderGroupId: "", txId: txId, migrationId: NewMigrationIdFromMsg(args.GetMigrationId())}
	_, r := o.ExecPrepare(op, txId, args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
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

func (o LeaveMigrationOp) name() string {
	return "LeaveMigration"
}

func (o LeaveMigrationOp) local(n *NodeServer) (ret interface{}, unlock func(*NodeServer), reply int32) {
	if o.target.nodeId == n.raft.selfId {
		nodeList := n.raftGroup.GetRemovedNodeListLocal(o.target)
		n.inodeMgr.SuspendNode()
		unlock = func(n *NodeServer) {
			n.inodeMgr.ResumeNode()
			n.dirtyMgr.DropMigratingData(o.migrationId)
		}
		log.Infof("Shutdown: Migrate dirty metas")
		if reply = n.sendDirtyMetasForNodeLeave(o.migrationId, nodeList); reply != RaftReplyOk {
			unlock(n)
			log.Errorf("Failed: %v, sendDirtyMetasForNodeLeave, reply=%v", o.name(), reply)
			return nil, nil, reply
		}
		log.Infof("Shutdown: Migrate directory metas")
		if reply = n.sendDirMetasForNodeLeave(o.migrationId, nodeList); reply != RaftReplyOk {
			unlock(n)
			log.Errorf("Failed: %v, sendDirMetasForNodeLeave, reply=%v", o.name(), reply)
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
	msg := &common.UpdateNodeListMsg{
		TxId: o.txId.toMsg(), MigrationId: o.migrationId.toMsg(),
		Nodes: &common.MembershipListMsg{Servers: []*common.NodeMsg{o.target.toMsg()}}, IsAdd: false, NeedRestore: false,
	}
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateNodeListCmdId, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, reply, r=%v", o.name(), reply)
		return reply
	}
	log.Debugf("Success: %v, target=%v", o.name(), o.target)
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
	if reply := n.rpcClient.CallObjcacheRpc(RpcLeaveMigrationCmdId, args, sa, timeout, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return nil, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o *RpcMgr) LeaveMigration(msg RpcMsg) *common.Ack {
	args := &common.MigrationArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: LeaveMigration, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	txId := NewTxIdFromMsg(args.GetTxId())
	op := LeaveMigrationOp{
		txId: txId, target: NewRaftNodeFromMsg(args.GetNode()), leaderGroupId: "", migrationId: NewMigrationIdFromMsg(args.GetMigrationId()),
	}
	_, r := o.ExecPrepare(op, txId, args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o LeaveMigrationOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.GetGroupLeader(o.leaderGroupId, l)
}
