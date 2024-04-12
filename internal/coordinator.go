/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"math"
	"path/filepath"
	"sort"
	"time"

	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

/**
## Overview and Terminology of objcache transaction protocol
When a filesystem user requests a file/inode operation (e.g., open, write, read), objcache FUSE receives it and needs to redirect it to cluster nodes.
The client translates the request as a *distributed transaction* to achieve atomic file/inode operations over distributed, internal data strucutre (i.e., meta/chunks).
For describing our transaction protocol and server roles, we use same terminology as common distributed transaction protocols: client, coordinator, and participants.

Client: a server thread that requests a coordinator for file/inode operations
Coordinator: a server thread that acomplishes atomic file/inode opeartions by requesting a series of RPCs that transaciton participants execute.
Participants: servers that executes meta/chunks operations. It can be further classified as prepare, commit, or abort participants.

Prepare participants start with a locking on the target data structure and stage the update in a Raft log entry.
Commit participants promotes the staged data to the actual working data and unlock it, while abort participants discard the staged data and unlock it.
This typical two-phase commit protocol is required if more than two nodes need to udpate meta/chunks for an inode/file update in atomic.
This prevents from race condition under parallel distributed file/inode operations.

We can simply draw an RPC chain as client -> coordinator -> prepare/commit/abort participants
Client and coordinator maintains timeout for RPCs.
Coordinator starts abort participants at timeout of prepare (and usual errors like ENOENT) and reports it to client.
Client restarts coordinator in this case.

However, client also needs to independently maintain timeout for coordinator in case of coordinator failures.
This means that client may try to restart a alive but very slow coordinator, which make things quite complex since it may cause deadlocks or something bad at duplicated participants.
Unfortunately, this is not rare since we may have heavy disk I/O with I/O waits when requests reach the maximum disk throughput.
But we can avoid such situations with a trick of TxId as described later.

## Key data structures to understand objcache transactions
1. RaftNodeList
All the objcache servers keep the latest RaftNodeList.
Every node leave/join broadcasts the latest cluster nodes and Raft replication groups with a version.
The list is used to determine the destination server for meta/chunk operations.
Every RPC checks client's version of RaftNodeList to ensure always selecting a single server for an operations on the same node list version.
It enables strict, but high-performance consistency of file/inode operations by pessimistic locking even among different nodes.

Every coordinator/participant must be a Raft group leader.
When the Raft protocol elects a follower to be a leader, it resumes incomplete commits.
So, a client/coordinator can eventually complete its requests by retrying one of servers in a group even if there are server failures.
if a coordinator/participant detects itself a follower at the beginning of its RPC, it rejects a request with a likely leader's server ID.

1. TxId = <ClientId, SeqNum, TxSeqNum>
ClientId = the unique ID of a client server
SeqNum   = a monotonic, local clock at the client server.
TxSeqNum = a fixed sequence number that each coordinator maintains.

A client executes a coordinator RPC according to user's file operation.
It passes the ClientId and SeqNum and a coordinator geneerates TxSeqNum for its operations locally or remotely.
For usual operations, we do not have to care about this, but things get quite complex if some errors or timeouts happen.
With TxId, the client can restart the coordinator and participants with the exactly same TxId for the same operation.
As a result, participants can determine duplicated requests with the passed TxId.
This is useful when a parcitipant is very slow but succeed its operation although client restarts a coordinator due to client-side timeouts.

## Pseudo implementation and detailed documents (comments) for a coordinator
local() {
record := &TwoPcPrepareRecordMsg{TxId: coordinatorTxId}
op[0], reply[0] := CallPrepareRpc(NewTxOp(txId[0]), nodeList, timeout) // exec local or remote via RPC
if reply[0] != RaftReplyOk {
    return CoordinatorRet{}, reply[0]
}
record.Body[0] = &RecordBodyMsg{TxId: txId[0].toMsg(), Result: op[0].GetResult(), GroupId: op[0].node.GroupId}
op[1], reply[1] := CallPrepareRpc(NewTxOp2(txId[1]), nodeList, timeout) // exec local or remote via RPC
if reply[1] != RaftReplyOk {
    CallOpNoFail(NewAbortParticipantOp(txId[2], []*common.TxIdMsg{txId[0].toMsg}, op[0].node.GroupId), retryInterval) // abort with retries
    return CoordinatorRet{}, reply[1]
}
record.Body[1] = &RecordBodyMsg{TxId: txId[1].toMsg(), Result: op[1].GetResult(), GroupId: op[1].node.GroupId}
// record prepare logs
if reply[2] := raft.AppendExtendedLogEntry(CoordinatorTypeId, record, ...); reply[2] != RaftReplyOk {
    CallOpNoFail(NewAbortParticipantOp(txId[2], []*common.TxIdMsg{txId[0].toMsg}, op[0].node.GroupId), retryInterval) // abort with retries
    CallOpNoFail(NewAbortParticipantOp(txId[3], []*common.TxIdMsg{txId[1].toMsg}, op[1].node.GroupId), retryInterval) // abort with retries
    return CoordinatorRet{}, reply[2]
}
if hasRemoteOps(record) {
    // commit and unlock remote operations
    for _, body := range record.Body {
        if body.GroupId != raft.selfId {
            CallRemote(NewCommitParticipantOp(txId4, body.TxId, body.GroupId))
        }
    }
    // record a commit for local operations
    if _, _, reply[3] := raft.AppendEntriesLocal(NewAppendEntryCommitTxCommand(raft.currentTerm.Get(), coordinatorTxId)); reply[3] != RaftReplyOk {
        return CoordinatorRet{}, reply[3]
    }
} else {
    // AppendExtendedLogEntry executes a quick commit if no remote operations (logging a commit instead of prepare)
    // here we can avoid the second raft logging
}
// unlock local operations (remote ones are ignored). locking is done outside the Raft h machine, so we need this here (remote commit RPCs also do this)
rpcMgr.DeleteAndUnlockLocalAll(txId[0], txId[1])
// complete. often returns a primary meta for tracking the latest inode h
return CoordinatorRet{meta: op[0].GetResult().GetMetaRpc()}, RaftReplyOk
}

## Special transaction architecture for write()
In naive implementation, write may be a single transaction with updating chunks and meta (to update file size).
However, it can be very slow when we write apps with many write() with few bytes of buffers since 1) it requires locking the entire file for meta operations,
and 2) transfering data between client-coordinator and coordinator-participants.

To optimize this, Updating chunks is treated as a special transaction.
At write(), client directly transfers chunk updates to a participant and keep the updated info without updating meta.
Then, When FUSE informs a size update (SetInodeAttributes or Flush), client starts a transaction to commit updating chunks and the new size of meta.
Chunk updates are recorded as StagingChunk and multiple StagingChunks are commited to be a new version of WorkingChunk.
Client failures may loose information of StagingChunks, but this does not break consistency of any files (but becomes an orphan chunks, which recovery procedure like fsck should reclaim).

## Summary of erroneous procedures of coordinators
1. if a participant reports an error, abort all the succeeded operations for other participants and propagate it to the client
2. if a participant timeouts, abort all the succeeded operations for other participants and notify retry to the client
The failed participant may be very slow (often disk I/O stall) and succeed in background. In that case,
the retried request to the participant will report its success since the exactly same txId is passed to the participant.
if the retried request is still incomplete, rpcMgr will block the request and wait for the completion of the first attempt (see RpcMgr.__execParticipant).
For both cases, it can realize the duplicated request and immediately return the completed result.

3. if the coordinator fails during recording its prepare, abort all the succeeded operations for participants and notify retry to the client
4. if the coordinator node fails after recording its prepare before commit, the commit is restarted during the log replay after the restart
see ResumeCoordinatorCommit
*/

type CoordinatorId struct {
	clientId uint32
	seqNum   uint32
}

func (t *CoordinatorId) toMsg() *common.CoordinatorIdMsg {
	return &common.CoordinatorIdMsg{ClientId: t.clientId, SeqNum: t.seqNum}
}

func (t *CoordinatorId) toPrepareTxId() TxId {
	return TxId{ClientId: t.clientId, SeqNum: t.seqNum, TxSeqNum: 1}
}

func (t *CoordinatorId) toTxId(txSeqNum uint64) TxId {
	return TxId{ClientId: t.clientId, SeqNum: t.seqNum, TxSeqNum: txSeqNum}
}

func (t *CoordinatorId) toCoordinatorTxId() TxId {
	return TxId{ClientId: t.clientId, SeqNum: t.seqNum, TxSeqNum: 0}
}

func (t *CoordinatorId) toMigrationId() MigrationId {
	return MigrationId{ClientId: t.clientId, SeqNum: t.seqNum}
}

func (t *CoordinatorId) toCoordinatorTxIdMsg() *common.TxIdMsg {
	return &common.TxIdMsg{ClientId: t.clientId, SeqNum: t.seqNum, TxSeqNum: 0}
}

func NewCoordinatorIdFromMsg(msg *common.CoordinatorIdMsg) CoordinatorId {
	return CoordinatorId{clientId: msg.GetClientId(), seqNum: msg.GetSeqNum()}
}

type CoordinatorRet struct {
	meta     *WorkingMeta
	parent   *WorkingMeta
	extReply int32 // to notify false positives when persisting objects
}

func NewCoordinatorRetFromMsg(msg *common.CoordinatorRetMsg) CoordinatorRet {
	p := msg.GetParent()
	ret := CoordinatorRet{meta: NewWorkingMetaFromMsg(msg.GetMeta()), extReply: msg.GetExtReply()}
	if len(p) > 0 {
		ret.parent = NewWorkingMetaFromMsg(p[0])
	}
	return ret
}

func (c *CoordinatorRet) toMsg(r RaftBasicReply) *common.CoordinatorRetMsg {
	ret := &common.CoordinatorRetMsg{Meta: c.meta.toMsg(), Parent: nil, Status: r.reply, Leader: r.GetLeaderNodeMsg(), ExtReply: c.extReply}
	if c.parent != nil {
		ret.Parent = make([]*common.CopiedMetaMsg, 1)
		ret.Parent[0] = c.parent.toMsg()
	}
	return ret
}

/////////////////////////////////////////////////////////////////////////////////////

type CoordinatorOpBase interface {
	name() string
	GetLeader(*NodeServer, *RaftNodeList) (RaftNode, bool)
	local(*NodeServer, CoordinatorId, *RaftNodeList) (CoordinatorRet, int32)
	remote(n *NodeServer, sa common.NodeAddrInet4, txId CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply)
}

type CoordinatorFlushObjectOp struct {
	inodeKey InodeKeyType
	records  []*common.UpdateChunkRecordMsg
	mTime    int64
	mode     uint32
}

func NewCoordinatorFlushObjectOp(inodeKey InodeKeyType, records []*common.UpdateChunkRecordMsg, mTime int64, mode uint32) CoordinatorFlushObjectOp {
	return CoordinatorFlushObjectOp{inodeKey: inodeKey, records: records, mTime: mTime, mode: mode}
}

func (o CoordinatorFlushObjectOp) name() string {
	return "CoordinatorFlushObject"
}

func (o CoordinatorFlushObjectOp) local(n *NodeServer, id CoordinatorId, _ *RaftNodeList) (CoordinatorRet, int32) {
	begin := time.Now()
	txId := id.toPrepareTxId()
	var updateSize = int64(0)
	for _, record := range o.records {
		for _, stag := range record.GetStags() {
			lastOffset := stag.GetOffset() + stag.GetLength()
			if updateSize < lastOffset {
				updateSize = lastOffset
			}
		}
	}
	txIds := make([]TxId, 0)
	groups := make([]string, 0)
	op, reply := n.rpcMgr.CallPrepareAny(NewUpdateMetaSizeOp(txId, o.inodeKey, updateSize, o.mTime, o.mode), txId, n.flags.RpcTimeoutDuration)
	if reply != RaftReplyOk {
		return CoordinatorRet{}, reply
	}
	txIds = append(txIds, txId)
	groups = append(groups, op.node.GroupId)
	ret := NewTwoPCUpdateCommitLog(n, txId, op, o.records, TxUpdateCoordinator)
	endUpdateMetaSize := time.Now()
	endLinkMeta := time.Now()
	if reply = ret.CommitCoordinator(n, n.flags.CommitRpcTimeoutDuration); reply != RaftReplyOk {
		n.rpcMgr.AbortAll(txId.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
		log.Errorf("Failed: %v, CommitCoordinator, txId=%v, reply=%v", o.name(), ret.txId, reply)
		return CoordinatorRet{}, reply
	}
	for _, chunk := range o.records {
		if chunk.GroupId == n.raftGroup.selfGroup {
			for _, stag := range chunk.GetStags() {
				txIds = append(txIds, NewTxIdFromMsg(stag.GetTxId()))
			}
		}
	}
	n.rpcMgr.DeleteAndUnlockLocalAll(txIds...)
	endAll := time.Now()
	log.Debugf("Success: %v, inodeKey=%v, updateSize=%v, link=%v, commit=%v",
		o.name(), o.inodeKey, endUpdateMetaSize.Sub(begin), endLinkMeta.Sub(endUpdateMetaSize), endAll.Sub(endLinkMeta))
	return CoordinatorRet{meta: ret.primaryMeta}, reply
}

func (o CoordinatorFlushObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, id CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply) {
	args := &common.FlushObjectArgs{
		Id:          id.toMsg(),
		NodeListVer: nodeListVer,
		InodeKey:    uint64(o.inodeKey),
		MTime:       o.mTime,
		Mode:        o.mode,
		Records:     o.records,
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorFlushObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return CoordinatorRet{}, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return CoordinatorRet{}, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o *RpcMgr) CoordinatorFlushObject(msg RpcMsg) *common.CoordinatorRetMsg {
	args := &common.FlushObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorFlushObject, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.CoordinatorRetMsg{Status: reply}
	}
	op := CoordinatorFlushObjectOp{inodeKey: InodeKeyType(args.GetInodeKey()), records: args.GetRecords(), mTime: args.GetMTime(), mode: args.GetMode()}
	ret, r := o.CallCoordinatorMetaInRPC(op, NewCoordinatorIdFromMsg(args.GetId()), args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.toMsg(r)
}

func (o CoordinatorFlushObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.inodeKey})
}

/////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorTruncateObjectOp struct {
	inodeKey InodeKeyType
	newSize  int64
}

func NewCoordinatorTruncateObjectOp(inodeKey InodeKeyType, newSize int64) CoordinatorTruncateObjectOp {
	return CoordinatorTruncateObjectOp{inodeKey: inodeKey, newSize: newSize}
}

func (o CoordinatorTruncateObjectOp) name() string {
	return "CoordinatorTruncateObject"
}

func (o CoordinatorTruncateObjectOp) local(n *NodeServer, id CoordinatorId, _ *RaftNodeList) (CoordinatorRet, int32) {
	txId := id.toPrepareTxId()
	op, reply := n.rpcMgr.CallPrepareAny(NewTruncateMetaOp(txId, o.inodeKey, o.newSize), txId, n.flags.RpcTimeoutDuration)
	if reply != RaftReplyOk {
		return CoordinatorRet{}, reply
	}
	ret := NewTwoPCUpdateCommitLog(n, txId, op, nil, TxTruncateCoordinator)
	if reply = ret.CommitCoordinator(n, n.flags.CommitRpcTimeoutDuration); reply != RaftReplyOk {
		n.rpcMgr.AbortAll(txId.GetNext(), []string{op.node.GetGroupId()}, []TxId{txId}, n.flags.RpcTimeoutDuration)
		log.Errorf("Failed: %v, CommitCoordinator, txId=%v, reply=%v", o.name(), ret.txId, reply)
		return CoordinatorRet{}, reply
	}
	n.rpcMgr.DeleteAndUnlockLocalAll(txId)
	return CoordinatorRet{meta: ret.primaryMeta}, reply
}

func (o CoordinatorTruncateObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, id CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply) {
	args := &common.TruncateObjectArgs{
		Id:          id.toMsg(),
		NodeListVer: nodeListVer,
		InodeKey:    uint64(o.inodeKey),
		NewSize:     o.newSize,
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorTruncateObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return CoordinatorRet{}, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return CoordinatorRet{}, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o *RpcMgr) CoordinatorTruncateObject(msg RpcMsg) *common.CoordinatorRetMsg {
	args := &common.TruncateObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorTruncateObject, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.CoordinatorRetMsg{Status: reply}
	}
	op := CoordinatorTruncateObjectOp{inodeKey: InodeKeyType(args.GetInodeKey()), newSize: args.GetNewSize()}
	ret, r := o.CallCoordinatorMetaInRPC(op, NewCoordinatorIdFromMsg(args.GetId()), args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.toMsg(r)
}

func (o CoordinatorTruncateObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.inodeKey})
}

///////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorDeleteObjectOp struct {
	parentFullPath string
	parentInodeKey InodeKeyType
	childName      string
	childInodeKey  InodeKeyType
}

func NewCoordinatorDeleteObjectOp(parentFullPath string, parentInodeKey InodeKeyType, childName string, childInodeKey InodeKeyType) CoordinatorDeleteObjectOp {
	return CoordinatorDeleteObjectOp{parentFullPath: parentFullPath, parentInodeKey: parentInodeKey, childName: childName, childInodeKey: childInodeKey}
}

func (o CoordinatorDeleteObjectOp) name() string {
	return "CoordinatorDeleteObject"
}

func (o CoordinatorDeleteObjectOp) local(n *NodeServer, id CoordinatorId, _ *RaftNodeList) (CoordinatorRet, int32) {
	record := &common.TwoPCDeleteMetaCommitRecordMsg{TxId: id.toCoordinatorTxIdMsg()}
	txId := id.toPrepareTxId()
	removedKey := filepath.Join(o.parentFullPath, o.childName)
	op, reply := n.rpcMgr.CallPrepareAny(NewDeleteMetaOp(txId, o.childInodeKey, removedKey), txId, n.flags.RpcTimeoutDuration)
	if reply != RaftReplyOk {
		return CoordinatorRet{}, reply
	}
	remote := make([]*common.MetaCommitRecordMsg, 0)
	childMeta := op.asWorkingMeta()
	if op.node.GetNodeId() == n.raft.selfId {
		record.Meta = childMeta.toMsg()
		record.Prev = childMeta.prevVer.toMsg()
		record.Key = removedKey
	} else {
		record.RemoteMeta = &common.MetaCommitRecordMsg{TxSeqNum: txId.TxSeqNum, GroupId: op.node.GetGroupId()}
		remote = append(remote, record.RemoteMeta)
	}
	txId2 := txId.GetNext()
	record.NextTxSeqNum = txId2.TxSeqNum + 1
	op2, reply2 := n.rpcMgr.CallPrepareAny(NewUnlinkMetaOp(txId2, o.parentInodeKey, o.childName), txId2, n.flags.RpcTimeoutDuration)
	if reply2 != RaftReplyOk {
		n.rpcMgr.AbortAll(txId.GetNext(), []string{op.node.GetGroupId()}, []TxId{txId}, n.flags.RpcTimeoutDuration)
		return CoordinatorRet{}, reply2
	}
	parent := op2.ext.(RenameRet).meta
	if op2.node.GetNodeId() == n.raft.selfId {
		record.Parent = parent.toMsg()
	} else {
		record.RemoteParent = &common.MetaCommitRecordMsg{TxSeqNum: txId.TxSeqNum, GroupId: op2.node.GetGroupId()}
		remote = append(remote, record.RemoteParent)
	}
	if reply = n.CommitRecord(record.TxId, record.NextTxSeqNum, AppendEntryDeleteMetaCoordinatorCmdId, record, n.flags.CommitRpcTimeoutDuration, remote); reply != RaftReplyOk {
		n.rpcMgr.AbortAll(txId2.GetNext(), []string{op.node.GetGroupId(), op2.node.GetGroupId()}, []TxId{txId, txId2}, n.flags.RpcTimeoutDuration)
		log.Errorf("Failed: %v, CommitRecord, txId=%v, reply=%v", o.name(), record.TxId, reply)
		return CoordinatorRet{}, reply
	}
	n.rpcMgr.DeleteAndUnlockLocalAll(txId2, txId)
	log.Debugf("Success: %v, parentInodeId=%v, childName=%v", o.name(), o.parentInodeKey, o.childName)
	return CoordinatorRet{meta: parent}, reply
}

func (o CoordinatorDeleteObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, id CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply) {
	args := &common.DeleteObjectArgs{
		Id:             id.toMsg(),
		NodeListVer:    nodeListVer,
		ParentInodeKey: uint64(o.parentInodeKey),
		ParentFullPath: o.parentFullPath,
		ChildName:      o.childName,
		ChildInodeKey:  uint64(o.childInodeKey),
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorDeleteObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return CoordinatorRet{}, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return CoordinatorRet{}, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o *RpcMgr) CoordinatorDeleteObject(msg RpcMsg) *common.CoordinatorRetMsg {
	args := &common.DeleteObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorDeleteObject, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.CoordinatorRetMsg{Status: reply}
	}
	op := CoordinatorDeleteObjectOp{
		parentInodeKey: InodeKeyType(args.GetParentInodeKey()), parentFullPath: args.GetParentFullPath(),
		childName: args.GetChildName(), childInodeKey: InodeKeyType(args.GetChildInodeKey()),
	}
	ret, r := o.CallCoordinatorMetaInRPC(op, NewCoordinatorIdFromMsg(args.GetId()), args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.toMsg(r)
}

func (o CoordinatorDeleteObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.parentInodeKey})
}

////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorHardLinkOp struct {
	srcInodeKey       InodeKeyType
	srcParent         MetaAttributes
	dstParentKey      string
	dstParentInodeKey InodeKeyType
	dstName           string
	childAttr         MetaAttributes
}

func NewCoordinatorHardLinkOp(srcInodeKey InodeKeyType, srcParent MetaAttributes, dstParentKey string, dstParentInodeKey InodeKeyType, dstName string, childAttr MetaAttributes) CoordinatorHardLinkOp {
	return CoordinatorHardLinkOp{
		srcInodeKey: srcInodeKey, srcParent: srcParent, dstParentKey: dstParentKey, dstParentInodeKey: dstParentInodeKey, dstName: dstName, childAttr: childAttr,
	}
}

func (o CoordinatorHardLinkOp) name() string {
	return "CoordinatorHardLink"
}

func (o CoordinatorHardLinkOp) local(n *NodeServer, id CoordinatorId, _ *RaftNodeList) (CoordinatorRet, int32) {
	txId := id.toPrepareTxId()
	op, reply := n.rpcMgr.CallPrepareAny(NewUpdateMetaKeyOp(txId, o.childAttr.inodeKey, "", filepath.Join(o.dstParentKey, o.dstName), o.srcParent), txId, n.flags.RpcTimeoutDuration)
	if reply != RaftReplyOk {
		return CoordinatorRet{}, reply
	}
	txId2 := txId.GetNext()
	op2, reply2 := n.rpcMgr.CallPrepareAny(NewLinkMetaOp(txId, o.dstParentInodeKey, o.dstName, o.childAttr), txId, n.flags.RpcTimeoutDuration)
	if reply2 != RaftReplyOk {
		n.rpcMgr.AbortAll(txId2.GetNext(), []string{op.node.GetGroupId()}, []TxId{txId}, n.flags.RpcTimeoutDuration)
		return CoordinatorRet{}, reply2
	}
	ret := NewTwoPCUpdateCommitLog(n, id.toCoordinatorTxId(), op, nil, TxUpdateCoordinator)
	meta := op2.ext.(*WorkingMeta)
	if op2.node.NodeId == n.raft.selfId {
		ret.localMetas = append(ret.localMetas, meta.toMsg())
	} else {
		ret.remoteMetas = append(ret.remoteMetas, &common.MetaCommitRecordMsg{TxSeqNum: txId2.TxSeqNum, GroupId: op2.node.GetGroupId()})
	}
	if reply = ret.CommitCoordinator(n, n.flags.CommitRpcTimeoutDuration); reply != RaftReplyOk {
		n.rpcMgr.AbortAll(txId2.GetNext(), []string{op.node.GetGroupId()}, []TxId{txId}, n.flags.RpcTimeoutDuration)
		log.Errorf("Failed: %v, CommitCoordinator, txId=%v, reply=%v", o.name(), ret.txId, reply)
		return CoordinatorRet{}, reply
	}
	n.rpcMgr.DeleteAndUnlockLocalAll(txId)
	return CoordinatorRet{meta: meta}, reply
}

func (o CoordinatorHardLinkOp) remote(n *NodeServer, sa common.NodeAddrInet4, id CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply) {
	args := &common.HardLinkObjectArgs{
		Id:                id.toMsg(),
		NodeListVer:       nodeListVer,
		SrcParent:         o.srcParent.toMsg(""),
		SrcInodeKey:       uint64(o.srcInodeKey),
		DstParentKey:      o.dstParentKey,
		DstParentInodeKey: uint64(o.dstParentInodeKey),
		ChildAttr:         o.childAttr.toMsg(o.dstName),
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorHardLinkObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return CoordinatorRet{}, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return CoordinatorRet{}, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o *RpcMgr) CoordinatorHardLinkObject(msg RpcMsg) *common.CoordinatorRetMsg {
	args := &common.HardLinkObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorHardLinkObject, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.CoordinatorRetMsg{Status: reply}
	}
	op := CoordinatorHardLinkOp{
		srcInodeKey: InodeKeyType(args.GetSrcInodeKey()), srcParent: NewMetaAttributesFromMsg(args.GetSrcParent()),
		dstParentInodeKey: InodeKeyType(args.GetDstParentInodeKey()), dstName: args.GetChildAttr().GetName(),
		childAttr: NewMetaAttributesFromMsg(args.GetChildAttr()),
	}
	ret, r := o.CallCoordinatorMetaInRPC(op, NewCoordinatorIdFromMsg(args.GetId()), args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.toMsg(r)
}

func (o CoordinatorHardLinkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.dstParentInodeKey})
}

////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorRenameObjectOp struct {
	srcParentKey      string
	srcParent         MetaAttributes
	srcName           string
	dstParentKey      string
	dstParentInodeKey InodeKeyType
	dstName           string
	childAttr         MetaAttributes
	parent            MetaAttributes
}

func NewCoordinatorRenameObjectOp(srcParentKey string, srcParent MetaAttributes, srcName string, dstParentKey string, dstParentInodeKey InodeKeyType, dstName string, childAttr MetaAttributes) CoordinatorRenameObjectOp {
	return CoordinatorRenameObjectOp{
		srcParentKey: srcParentKey, dstParentKey: dstParentKey,
		srcParent: srcParent, srcName: srcName, dstParentInodeKey: dstParentInodeKey, dstName: dstName, childAttr: childAttr,
	}
}

func (o CoordinatorRenameObjectOp) name() string {
	return "CoordinatorRenameObject"
}

func (n *NodeServer) CommitRecord(txIdMsg *common.TxIdMsg, nextSeqNum uint64, txType uint16, record proto.Message, retryInterval time.Duration, remotes []*common.MetaCommitRecordMsg) int32 {
	begin := time.Now()
	if reply := n.raft.AppendExtendedLogEntry(txType, record); reply != RaftReplyOk {
		log.Errorf("Failed: CommitRecord, AppendExtendedLogEntry, reply=%v", reply)
		return reply
	}
	txId := NewTxIdFromMsg(txIdMsg)
	prepare := time.Now()
	var remoteCommit = false
	for _, remote := range remotes {
		if remote != nil {
			remoteCommit = true
			txId2 := txId.GetVariant(nextSeqNum)
			nextSeqNum += 1
			n.rpcMgr.CallRemote(NewCommitParticipantOp(txId2, txId.GetVariant(remote.GetTxSeqNum()), remote.GetGroupId()), retryInterval)
		}
	}
	commitRemote := time.Now()
	if remoteCommit {
		if _, _, reply := n.raft.AppendEntriesLocal(NewAppendEntryCommitTxCommand(n.raft.currentTerm.Get(), txIdMsg)); reply != RaftReplyOk {
			// TODO: must not be here
			log.Errorf("Failed: CommitRecord, AppendEntriesLocal, txId=%v, reply=%v", txId, reply)
			return reply
		}
	}
	endAll := time.Now()
	log.Debugf("Success: CommitRecord, txId=%v, prepare=%v, commitRemote=%v, commit=%v",
		txId, prepare.Sub(begin), commitRemote.Sub(prepare), endAll.Sub(commitRemote))
	return RaftReplyOk
}

func (o CoordinatorRenameObjectOp) CheckError(unlink RenameRet, link RenameRet) int32 {
	// TODO: rename ignores overwrite. is this okay?
	if (unlink.mayErrorReply == RaftReplyOk && link.mayErrorReply == RaftReplyOk) ||
		link.mayErrorReply == ErrnoToReply(unix.EEXIST) {
		return RaftReplyOk
	} else if unlink.mayErrorReply != RaftReplyOk {
		return unlink.mayErrorReply
	}
	return link.mayErrorReply
}

func (o CoordinatorRenameObjectOp) local(n *NodeServer, id CoordinatorId, _ *RaftNodeList) (CoordinatorRet, int32) {
	var parent *WorkingMeta
	removedKey := filepath.Join(o.srcParentKey, o.srcName)
	newKey := filepath.Join(o.dstParentKey, o.dstName)
	record := &common.TwoPCRenameCommitRecordMsg{TxId: id.toCoordinatorTxIdMsg(), LocalUpdateParents: make([]*common.CommitUpdateParentMetaKeyArgs, 0), RemoteOps: make([]*common.MetaCommitRecordMsg, 0)}
	txId := id.toPrepareTxId()
	txIds := make([]TxId, 0)
	groups := make([]string, 0)
	record.NextTxSeqNum = txId.TxSeqNum + 1

	op, reply := n.rpcMgr.CallPrepareAny(NewUpdateMetaKeyOp(txId, o.childAttr.inodeKey, removedKey, newKey, o.parent), txId, n.flags.RpcTimeoutDuration)
	if reply != RaftReplyOk {
		return CoordinatorRet{}, reply
	}
	childMeta := op.asWorkingMeta()
	if op.node.GetNodeId() != n.raft.selfId {
		record.RemoteOps = append(record.RemoteOps, &common.MetaCommitRecordMsg{TxSeqNum: txId.TxSeqNum, GroupId: op.node.GetGroupId()})
	} else {
		record.LocalUpdateMetaKeys = append(record.LocalUpdateMetaKeys, &common.CommitUpdateMetaKeyArgs{RemovedKey: removedKey, NewKey: newKey, Meta: childMeta.toMsg()})
	}
	txIds = append(txIds, txId)
	groups = append(groups, op.node.GetGroupId())
	txId2 := txId.GetNext()
	record.NextTxSeqNum = txId2.TxSeqNum + 1

	if childMeta.IsDir() {
		dirOldToNewPath := make(map[string]string)
		dirs := make(map[string]*WorkingMeta)
		nextDirOldToNewPath := make(map[string]string)
		nextDirs := make(map[string]*WorkingMeta)
		dirOldToNewPath[removedKey] = newKey
		dirs[removedKey] = childMeta
		for {
			for dirRemovedKey, working := range dirs {
				dirNewKey := dirOldToNewPath[dirRemovedKey]
				dirAttr := working.toMetaAttr()
				for name, attr := range working.childAttrs {
					if name == "." || name == ".." {
						continue
					}
					childRemovedKey := filepath.Join(dirRemovedKey, name)
					childNewKey := filepath.Join(dirNewKey, name)
					op2, reply2 := n.rpcMgr.CallPrepareAny(NewUpdateMetaKeyOp(txId2, attr.inodeKey, childRemovedKey, childNewKey, dirAttr), txId2, n.flags.RpcTimeoutDuration)
					if reply2 != RaftReplyOk {
						n.rpcMgr.AbortAll(txId2.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
						return CoordinatorRet{}, reply2
					}
					newWorking := op2.asWorkingMeta()
					if op2.node.GetNodeId() != n.raft.selfId {
						record.RemoteOps = append(record.RemoteOps, &common.MetaCommitRecordMsg{TxSeqNum: txId2.TxSeqNum, GroupId: op2.node.GetGroupId()})
					} else {
						record.LocalUpdateMetaKeys = append(record.LocalUpdateMetaKeys, &common.CommitUpdateMetaKeyArgs{RemovedKey: childRemovedKey, NewKey: childNewKey, Meta: newWorking.toMsg()})
					}
					txIds = append(txIds, txId2)
					groups = append(groups, op2.node.GetGroupId())
					txId2 = txId2.GetNext()
					record.NextTxSeqNum = txId2.TxSeqNum + 1
					if newWorking.IsDir() {
						nextDirs[childRemovedKey] = newWorking
						nextDirOldToNewPath[childRemovedKey] = childNewKey
					}
				}
			}
			if len(nextDirs) == 0 {
				break
			}
			dirs = nextDirs
			dirOldToNewPath = nextDirOldToNewPath
			nextDirOldToNewPath = make(map[string]string)
			nextDirs = make(map[string]*WorkingMeta)
		}
	}

	if o.dstParentInodeKey == o.srcParent.inodeKey {
		// TODO: rename ignores overwrite. overwritten inodes become inaccessible but exists
		op2, reply2 := n.rpcMgr.CallPrepareAny(NewRenameMetaOp(txId2, o.srcParent.inodeKey, o.srcName, o.dstName, o.childAttr), txId2, n.flags.RpcTimeoutDuration)
		if reply2 != RaftReplyOk {
			n.rpcMgr.AbortAll(txId2.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
			return CoordinatorRet{}, reply2
		}
		parent = op2.ext.(*WorkingMeta)
		if op2.node.GetNodeId() != n.raft.selfId {
			record.RemoteOps = append(record.RemoteOps, &common.MetaCommitRecordMsg{TxSeqNum: txId2.TxSeqNum, GroupId: op2.node.GetGroupId()})
		} else {
			record.LocalUpdateParents = append(record.LocalUpdateParents, &common.CommitUpdateParentMetaKeyArgs{Meta: parent.toMsg(), Key: o.srcParentKey})
		}
		txIds = append(txIds, txId2)
		groups = append(groups, op2.node.GetGroupId())
		txId = txId2
	} else if o.srcParent.inodeKey < o.dstParentInodeKey { // ensure lock ordering becomes globally consistent to avoid deadlocks
		op2, reply2 := n.rpcMgr.CallPrepareAny(NewUnlinkMetaOp(txId2, o.srcParent.inodeKey, o.srcName), txId2, n.flags.RpcTimeoutDuration)
		if reply2 != RaftReplyOk {
			n.rpcMgr.AbortAll(txId2.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
			return CoordinatorRet{}, reply2
		}
		parent = op2.ext.(RenameRet).meta
		if op2.node.GetNodeId() != n.raft.selfId {
			record.RemoteOps = append(record.RemoteOps, &common.MetaCommitRecordMsg{TxSeqNum: txId2.TxSeqNum, GroupId: op2.node.GetGroupId()})
		} else {
			record.LocalUpdateParents = append(record.LocalUpdateParents, &common.CommitUpdateParentMetaKeyArgs{Meta: parent.toMsg(), Key: o.srcParentKey})
		}
		txIds = append(txIds, txId2)
		groups = append(groups, op2.node.GetGroupId())

		txId3 := txId2.GetNext()
		record.NextTxSeqNum = txId3.TxSeqNum + 1
		op3, reply3 := n.rpcMgr.CallPrepareAny(NewLinkMetaOp(txId3, o.dstParentInodeKey, o.dstName, o.childAttr), txId3, n.flags.RpcTimeoutDuration)
		if reply3 != RaftReplyOk {
			n.rpcMgr.AbortAll(txId3.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
			return CoordinatorRet{}, reply3
		}
		if op3.node.GetNodeId() != n.raft.selfId {
			record.RemoteOps = append(record.RemoteOps, &common.MetaCommitRecordMsg{TxSeqNum: txId3.TxSeqNum, GroupId: op3.node.GetGroupId()})
		} else {
			record.LocalUpdateParents = append(record.LocalUpdateParents, &common.CommitUpdateParentMetaKeyArgs{Meta: op3.ext.(*RenameRet).meta.toMsg(), Key: o.dstParentKey})
		}
		txIds = append(txIds, txId3)
		groups = append(groups, op3.node.GetGroupId())
		txId = txId3
		if reply4 := o.CheckError(op2.ext.(RenameRet), op3.ext.(RenameRet)); reply4 != RaftReplyOk {
			n.rpcMgr.AbortAll(txId3.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
			return CoordinatorRet{}, reply4
		}
	} else {
		op2, reply2 := n.rpcMgr.CallPrepareAny(NewLinkMetaOp(txId2, o.dstParentInodeKey, o.dstName, o.childAttr), txId2, n.flags.RpcTimeoutDuration)
		if reply2 != RaftReplyOk {
			n.rpcMgr.AbortAll(txId2.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
			return CoordinatorRet{}, reply2
		}
		if op2.node.GetNodeId() != n.raft.selfId {
			record.RemoteOps = append(record.RemoteOps, &common.MetaCommitRecordMsg{TxSeqNum: txId2.TxSeqNum, GroupId: op2.node.GetGroupId()})
		} else {
			record.LocalUpdateParents = append(record.LocalUpdateParents, &common.CommitUpdateParentMetaKeyArgs{Meta: op2.ext.(RenameRet).meta.toMsg(), Key: o.dstParentKey})
		}
		txIds = append(txIds, txId2)
		groups = append(groups, op2.node.GetGroupId())

		txId3 := txId2.GetNext()
		record.NextTxSeqNum = txId3.TxSeqNum + 1
		op3, reply3 := n.rpcMgr.CallPrepareAny(NewUnlinkMetaOp(txId3, o.srcParent.inodeKey, o.srcName), txId3, n.flags.RpcTimeoutDuration)
		if reply3 != RaftReplyOk {
			n.rpcMgr.AbortAll(txId3.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
			return CoordinatorRet{}, reply3
		}
		parent = op3.ext.(RenameRet).meta
		if op3.node.GetNodeId() != n.raft.selfId {
			record.RemoteOps = append(record.RemoteOps, &common.MetaCommitRecordMsg{TxSeqNum: txId3.TxSeqNum, GroupId: op3.node.GetGroupId()})
		} else {
			record.LocalUpdateParents = append(record.LocalUpdateParents, &common.CommitUpdateParentMetaKeyArgs{Meta: parent.toMsg(), Key: o.srcParentKey})
		}
		txIds = append(txIds, txId3)
		groups = append(groups, op3.node.GetGroupId())
		txId = txId3
		if reply4 := o.CheckError(op3.ext.(RenameRet), op2.ext.(RenameRet)); reply4 != RaftReplyOk {
			n.rpcMgr.AbortAll(txId3.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
			return CoordinatorRet{}, reply4
		}
	}

	if reply4 := n.CommitRecord(record.TxId, record.NextTxSeqNum, AppendEntryRenameCoordinatorCmdId, record, n.flags.CommitRpcTimeoutDuration, record.RemoteOps); reply4 != RaftReplyOk {
		n.rpcMgr.AbortAll(txId.GetNext(), groups, txIds, n.flags.RpcTimeoutDuration)
		log.Errorf("Failed: %v, CommitRecord txId=%v, reply=%v", o.name(), record.TxId, reply4)
		return CoordinatorRet{}, reply4
	}
	n.rpcMgr.DeleteAndUnlockLocalAll(txIds...)
	return CoordinatorRet{meta: childMeta, parent: parent}, RaftReplyOk
}

func (o CoordinatorRenameObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, id CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply) {
	args := &common.RenameObjectArgs{
		Id:                id.toMsg(),
		NodeListVer:       nodeListVer,
		SrcParentKey:      o.srcParentKey,
		SrcParent:         o.srcParent.toMsg(""),
		SrcName:           o.srcName,
		DstParentKey:      o.dstParentKey,
		DstParentInodeKey: uint64(o.dstParentInodeKey),
		ChildAttr:         o.childAttr.toMsg(o.dstName),
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorRenameObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return CoordinatorRet{}, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return CoordinatorRet{}, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o *RpcMgr) CoordinatorRenameObject(msg RpcMsg) *common.CoordinatorRetMsg {
	args := &common.RenameObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorRenameObject, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.CoordinatorRetMsg{Status: reply}
	}
	op := CoordinatorRenameObjectOp{
		srcParentKey: args.GetSrcParentKey(), srcParent: NewMetaAttributesFromMsg(args.GetSrcParent()), srcName: args.GetSrcName(),
		dstParentKey: args.GetDstParentKey(), dstParentInodeKey: InodeKeyType(args.GetDstParentInodeKey()), dstName: args.GetChildAttr().GetName(),
		childAttr: NewMetaAttributesFromMsg(args.GetChildAttr()),
	}
	ret, r := o.CallCoordinatorMetaInRPC(op, NewCoordinatorIdFromMsg(args.GetId()), args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.toMsg(r)
}

func (o CoordinatorRenameObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.dstParentInodeKey, o.srcParent.inodeKey})
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorCreateObjectOp struct {
	parentFullPath string
	parentAttr     MetaAttributes
	childName      string
	childAttr      MetaAttributes
	chunkSize      int64
	expireMs       int32
}

func NewCoordinatorCreateObjectOp(parentFullPath string, parentAttr MetaAttributes, childName string, childAttr MetaAttributes, chunkSize int64, expireMs int32) CoordinatorCreateObjectOp {
	return CoordinatorCreateObjectOp{parentFullPath: parentFullPath, parentAttr: parentAttr, childName: childName, childAttr: childAttr, chunkSize: chunkSize, expireMs: expireMs}
}

func (o CoordinatorCreateObjectOp) name() string {
	return "CoordinatorCreateObject"
}

func (o CoordinatorCreateObjectOp) local(n *NodeServer, id CoordinatorId, _ *RaftNodeList) (CoordinatorRet, int32) {
	record := &common.TwoPCCreateMetaCommitRecordMsg{TxId: id.toCoordinatorTxIdMsg()}
	txId := id.toPrepareTxId()
	record.NextTxSeqNum = txId.TxSeqNum + 1
	newKey := filepath.Join(o.parentFullPath, o.childName)
	op, reply := n.rpcMgr.CallPrepareAny(NewCreateMetaOp(txId, o.childAttr.inodeKey, o.parentAttr, newKey, o.childAttr.mode, o.chunkSize, o.expireMs), txId, n.flags.RpcTimeoutDuration)
	if reply != RaftReplyOk {
		return CoordinatorRet{}, reply
	}
	remote := make([]*common.MetaCommitRecordMsg, 0)
	childMeta := op.ext.(*WorkingMeta)
	if op.node.GetNodeId() != n.raft.selfId {
		record.RemoteMeta = &common.MetaCommitRecordMsg{TxSeqNum: txId.TxSeqNum, GroupId: op.node.GetGroupId()}
		remote = append(remote, record.RemoteMeta)
	} else {
		record.Meta = childMeta.toMsg()
		record.NewKey = newKey
	}

	txId2 := txId.GetNext()
	record.NextTxSeqNum = txId2.TxSeqNum + 1
	op2, reply2 := n.rpcMgr.CallPrepareAny(NewCreateChildMetaOp(txId2, o.parentAttr.inodeKey, o.childName, o.childAttr), txId2, n.flags.RpcTimeoutDuration)
	if reply2 != RaftReplyOk {
		n.rpcMgr.AbortAll(txId2.GetNext(), []string{op.node.GetGroupId()}, []TxId{txId}, n.flags.RpcTimeoutDuration)
		return CoordinatorRet{}, reply2
	}
	parent := op2.ext.(*WorkingMeta)
	if op2.node.GetNodeId() != n.raft.selfId {
		record.RemoteParent = &common.MetaCommitRecordMsg{TxSeqNum: txId.TxSeqNum, GroupId: op2.node.GetGroupId()}
		remote = append(remote, record.RemoteParent)
	} else {
		record.Parent = parent.toMsg()
	}

	if reply3 := n.CommitRecord(record.TxId, record.NextTxSeqNum, AppendEntryCreateMetaCoordinatorCmdId, record, n.flags.CommitRpcTimeoutDuration, remote); reply3 != RaftReplyOk {
		n.rpcMgr.AbortAll(txId2.GetNext(), []string{op.node.GetGroupId(), op2.node.GetGroupId()}, []TxId{txId, txId2}, n.flags.RpcTimeoutDuration)
		return CoordinatorRet{}, reply3
	}
	n.rpcMgr.DeleteAndUnlockLocalAll(txId, txId2)
	return CoordinatorRet{meta: childMeta}, RaftReplyOk
}

func (o CoordinatorCreateObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, id CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply) {
	args := &common.CreateObjectArgs{
		Id:             id.toMsg(),
		NodeListVer:    nodeListVer,
		ParentFullPath: o.parentFullPath,
		ParentAttr:     o.parentAttr.toMsg(""),
		ChildAttr:      o.childAttr.toMsg(o.childName),
		ChunkSize:      o.chunkSize,
		ExpireMs:       o.expireMs,
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorCreateObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return CoordinatorRet{}, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return CoordinatorRet{}, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o *RpcMgr) CoordinatorCreateObject(msg RpcMsg) *common.CoordinatorRetMsg {
	args := &common.CreateObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorCreateObject, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.CoordinatorRetMsg{Status: reply}
	}
	op := CoordinatorCreateObjectOp{
		parentFullPath: args.GetParentFullPath(),
		parentAttr:     NewMetaAttributesFromMsg(args.GetParentAttr()), childName: args.GetChildAttr().GetName(),
		childAttr: NewMetaAttributesFromMsg(args.GetChildAttr()), chunkSize: args.GetChunkSize(), expireMs: args.GetExpireMs(),
	}
	ret, r := o.CallCoordinatorMetaInRPC(op, NewCoordinatorIdFromMsg(args.GetId()), args.GetNodeListVer())
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.toMsg(r)
}

func (o CoordinatorCreateObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.parentAttr.inodeKey})
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorPersistOp struct {
	inodeKey InodeKeyType
	priority int
}

func NewCoordinatorPersistOp(inodeKey InodeKeyType, priority int) CoordinatorPersistOp {
	return CoordinatorPersistOp{inodeKey: inodeKey, priority: priority}
}

func (o CoordinatorPersistOp) name() string {
	return "CoordinatorPersist"
}

func (o CoordinatorPersistOp) local(n *NodeServer, id CoordinatorId, _ *RaftNodeList) (CoordinatorRet, int32) {
	meta, metaKeys, working, reply := n.inodeMgr.PreparePersistMeta(o.inodeKey, n.dirtyMgr)
	if reply == ObjCacheIsNotDirty {
		return CoordinatorRet{meta: working, extReply: reply}, RaftReplyOk
	} else if reply != RaftReplyOk {
		return CoordinatorRet{meta: nil}, reply
	}
	defer n.inodeMgr.UnlockPersistMeta(meta, o.inodeKey)
	txId := id.toCoordinatorTxIdMsg()
	txId2 := id.toPrepareTxId()
	ret := &common.TwoPCPersistRecordMsg{
		TxId: txId, MetaKeys: metaKeys, PrimaryMeta: &common.MetaPersistRecordMsg{InodeKey: uint64(o.inodeKey), Version: working.version, Ts: time.Now().UTC().UnixNano()},
	}
	var keyUploadIds map[string]string
	if working.IsDir() {
		reply = n.inodeMgr.PutDirObject(ret, metaKeys, working, o.priority)
	} else if working.size == 0 {
		reply = n.inodeMgr.PutEmptyObject(ret, metaKeys, working, o.priority)
	} else if working.size < working.chunkSize {
		reply = n.inodeMgr.PutObject(ret, metaKeys, working, o.priority, n.raftGroup.selfGroup, n.flags.DirtyFlusherIntervalDuration)
	} else {
		txId2, keyUploadIds, reply = n.persistMultipleChunksMeta(ret, txId2, metaKeys, working, o.priority)
	}
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, txId=%v, reply=%v", o.name(), ret.TxId, reply)
		return CoordinatorRet{}, reply
	}
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryPersistCmdId, ret); reply != RaftReplyOk {
		log.Errorf("Failed: CommitPersistCoordinator, AppendExtendedLogEntry, txId=%v, reply=%v", ret.GetTxId(), reply)
		if keyUploadIds != nil {
			for _, chunk := range ret.PrimaryMeta.Chunks {
				txId3 := TxId{ClientId: txId.GetClientId(), SeqNum: txId.GetSeqNum(), TxSeqNum: chunk.GetTxSeqNum()}
				n.rpcMgr.CallRpcAnyNoFail(NewAbortParticipantOp(txId3, []*common.TxIdMsg{}, chunk.GetGroupId()), txId3, n.flags.RpcTimeoutDuration)
			}
			for metaKey, uploadId := range keyUploadIds {
				n.inodeMgr.MpuAbort(metaKey, uploadId)
			}
		}
		return CoordinatorRet{}, reply
	}
	txIds := make([]TxId, 0)
	var remote = false
	newFetchKey := ret.GetMetaKeys()[0]
	for _, chunk := range ret.PrimaryMeta.GetChunks() {
		txId3 := TxId{ClientId: txId.ClientId, SeqNum: txId.SeqNum, TxSeqNum: chunk.TxSeqNum}
		if chunk.GetGroupId() != n.raftGroup.selfGroup {
			n.rpcMgr.CallRemote(NewCommitPersistChunkOp(txId2, txId3, chunk.GetGroupId(), chunk.GetOffsets(), chunk.GetCVers(), InodeKeyType(ret.PrimaryMeta.GetInodeKey()), newFetchKey), n.flags.CommitRpcTimeoutDuration)
			txId2 = txId2.GetNext()
			remote = true
		} else {
			txIds = append(txIds, txId3)
		}
	}
	if remote {
		if _, _, reply = n.raft.AppendEntriesLocal(NewAppendEntryCommitTxCommand(n.raft.currentTerm.Get(), ret.GetTxId())); reply != RaftReplyOk {
			log.Errorf("Failed: CommitPersistCoordinator, AppendEntriesLocal, txId=%v, reply=%v", ret.GetTxId(), reply)
			// TODO: must not be here
			return CoordinatorRet{}, reply
		}
	}
	n.rpcMgr.DeleteAndUnlockLocalAll(txIds...)
	log.Debugf("Success: %v, txId=%v, keys=%v, reply=%v", o.name(), txId, metaKeys, reply)
	return CoordinatorRet{meta: working}, reply
}

func (o CoordinatorPersistOp) remote(n *NodeServer, sa common.NodeAddrInet4, id CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply) {
	args := &common.PersistArgs{
		InodeKey:    uint64(o.inodeKey),
		Priority:    int32(o.priority),
		Id:          id.toMsg(),
		NodeListVer: nodeListVer,
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorPersistCmdId, args, sa, time.Duration(0), n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return CoordinatorRet{}, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return CoordinatorRet{}, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o *RpcMgr) CoordinatorPersist(msg RpcMsg) *common.CoordinatorRetMsg {
	args := &common.PersistArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorPersist, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.CoordinatorRetMsg{Status: reply}
	}
	op := CoordinatorPersistOp{inodeKey: InodeKeyType(args.GetInodeKey()), priority: int(args.GetPriority())}
	ret, r := o.CallCoordinatorMetaInRPC(op, NewCoordinatorIdFromMsg(args.GetId()), args.GetNodeListVer())
	retMsg := &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg(), ExtReply: ret.extReply}
	if ret.meta != nil {
		retMsg.Meta = ret.meta.toMsg()
	}
	return retMsg
}

func (o CoordinatorPersistOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

//////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorDeletePersistOp struct {
	key      string
	inodeKey InodeKeyType
	priority int
}

func NewCoordinatorDeletePersistOp(key string, inodeKey InodeKeyType, priority int) CoordinatorDeletePersistOp {
	return CoordinatorDeletePersistOp{key: key, inodeKey: inodeKey, priority: priority}
}

func (o CoordinatorDeletePersistOp) name() string {
	return "CoordinatorDeletePersist"
}

func (o CoordinatorDeletePersistOp) local(n *NodeServer, id CoordinatorId, _ *RaftNodeList) (CoordinatorRet, int32) {
	meta, working, reply := n.inodeMgr.PreparePersistDeleteMeta(o.inodeKey, o.key, n.dirtyMgr)
	if reply == ObjCacheIsNotDirty {
		return CoordinatorRet{meta: working, extReply: reply}, RaftReplyOk
	} else if reply != RaftReplyOk {
		return CoordinatorRet{meta: nil}, reply
	}
	defer n.inodeMgr.UnlockPersistMeta(meta, o.inodeKey)
	txId := id.toCoordinatorTxIdMsg()
	ret := &common.TwoPCPersistRecordMsg{
		TxId: txId, MetaKeys: []string{o.key}, PrimaryMeta: &common.MetaPersistRecordMsg{InodeKey: uint64(working.inodeKey), Version: working.version, Ts: time.Now().UTC().UnixNano()},
	}
	reply = n.inodeMgr.PersistDeleteObject(ret, o.key, working, o.priority)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, PersistDeleteMeta, txId=%v, reply=%v", o.name(), ret.TxId, reply)
		return CoordinatorRet{}, reply
	}
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryDeletePersistCmdId, ret); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, txId=%v, reply=%v", o.name(), ret.GetTxId(), reply)
		// TODO: must not be here
		return CoordinatorRet{}, reply
	}
	log.Debugf("Success: %v, txId=%v, reply=%v", o.name(), txId, reply)
	return CoordinatorRet{meta: working}, reply
}

func (o CoordinatorDeletePersistOp) remote(n *NodeServer, sa common.NodeAddrInet4, id CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply) {
	args := &common.DeletePersistArgs{
		NodeListVer: nodeListVer,
		Id:          id.toMsg(),
		Key:         o.key,
		InodeKey:    uint64(o.inodeKey),
		Priority:    int32(o.priority),
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorDeletePersistCmdId, args, sa, n.flags.PersistTimeoutDuration, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return CoordinatorRet{}, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return CoordinatorRet{}, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o *RpcMgr) CoordinatorDeletePersist(msg RpcMsg) *common.CoordinatorRetMsg {
	args := &common.DeletePersistArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorDeletePersist, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.CoordinatorRetMsg{Status: reply}
	}
	op := CoordinatorDeletePersistOp{inodeKey: InodeKeyType(args.GetInodeKey()), priority: int(args.GetPriority())}
	ret, r := o.CallCoordinatorMetaInRPC(op, NewCoordinatorIdFromMsg(args.GetId()), args.GetNodeListVer())
	retMsg := &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg(), ExtReply: ret.extReply}
	if ret.meta != nil {
		retMsg.Meta = ret.meta.toMsg()
	}
	return retMsg
}

func (o CoordinatorDeletePersistOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

//////////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorUpdateNodeListOp struct {
	isAdd       bool
	target      RaftNode
	nodeListVer uint64
	groupAddr   map[string]bool // for abort
}

func NewCoordinatorUpdateNodeListOp(isAdd bool, target RaftNode, groupAddr map[string]bool, nodeListVer uint64) CoordinatorUpdateNodeListOp {
	return CoordinatorUpdateNodeListOp{isAdd: isAdd, target: target, nodeListVer: nodeListVer, groupAddr: groupAddr}
}

func (o CoordinatorUpdateNodeListOp) name() string {
	return "CoordinatorUpdateNodeList"
}

func (o CoordinatorUpdateNodeListOp) newTwoPCNodeListCommitMsg(txId TxId) *common.TwoPCNodeListCommitRecordMsg {
	cTxId := &common.TxIdMsg{ClientId: txId.ClientId, SeqNum: txId.SeqNum, TxSeqNum: 0}
	return &common.TwoPCNodeListCommitRecordMsg{
		TxId:         cTxId,
		Target:       o.target.toMsg(),
		IsAdd:        o.isAdd,
		Locals:       make([]*common.NodeGroupTxMsg, 0),
		Remotes:      make([]*common.NodeGroupTxMsg, 0),
		NextTxSeqNum: 1,
	}
}

func (o CoordinatorUpdateNodeListOp) addUpdateNodeListOpRet(tpc *common.TwoPCNodeListCommitRecordMsg, txId TxId, op RpcRet, selfId uint32) {
	if op.node.NodeId == selfId {
		tpc.Locals = append(tpc.Locals, &common.NodeGroupTxMsg{TxSeqNum: txId.TxSeqNum, Node: op.node})
	} else {
		tpc.Remotes = append(tpc.Remotes, &common.NodeGroupTxMsg{TxSeqNum: txId.TxSeqNum, Node: op.node})
	}
}

func (o CoordinatorUpdateNodeListOp) abortUpdateNodeList(n *NodeServer, tpc *common.TwoPCNodeListCommitRecordMsg, lTxId TxId, retryInterval time.Duration) {
	for _, remote := range tpc.Remotes {
		abort := lTxId.GetVariant(remote.GetTxSeqNum())
		n.rpcMgr.CallRpcAnyNoFail(NewAbortParticipantOp(lTxId, []*common.TxIdMsg{abort.toMsg()}, remote.Node.GetGroupId()), lTxId, retryInterval)
		lTxId = lTxId.GetNext()
	}
	for _, remote := range tpc.Locals {
		abort := lTxId.GetVariant(remote.GetTxSeqNum())
		n.rpcMgr.CallRpcAnyNoFail(NewAbortParticipantOp(lTxId, []*common.TxIdMsg{abort.toMsg()}, remote.Node.GetGroupId()), lTxId, retryInterval)
		lTxId = lTxId.GetNext()
	}
}

func (o CoordinatorUpdateNodeListOp) local(n *NodeServer, id CoordinatorId, l *RaftNodeList) (*common.MembershipListMsg, int32) {
	if !n.raftGroup.beginUpdateGroup() {
		log.Infof("%v, another Node is joining/leaving... Retry", o.name())
		return nil, RaftReplyRetry
	}
	defer n.raftGroup.finishUpdateGroup()

	if _, ok := l.nodeAddr[o.target.nodeId]; (ok && o.isAdd) || (!ok && !o.isAdd) {
		return &common.MembershipListMsg{}, RaftReplyOk
	}

	gIds := make([]string, 0)
	for gId := range l.groupNode {
		gIds = append(gIds, gId)
	}
	sort.Strings(gIds)

	coordinatorTxId := id.toCoordinatorTxId()
	tpc := o.newTwoPCNodeListCommitMsg(coordinatorTxId)
	migrationId := id.toMigrationId()
	txId := coordinatorTxId.GetNext()

	txIds := make([]TxId, 0)
	n.raftGroup.lock.RLock()
	var noCopyGroup = false
	var reply int32
	if o.isAdd {
		if _, noCopyGroup = l.groupNode[o.target.groupId]; noCopyGroup {
			// migration is not required
			for _, gId := range gIds {
				var ret RpcRet
				ret, reply = n.rpcMgr.CallPrepareAny(NewUpdateNodeListOp(txId, o.target, gId, o.isAdd, nil, migrationId), txId, n.flags.RpcTimeoutDuration)
				if reply != RaftReplyOk {
					log.Errorf("Failed: %v, NewUpdateNodeListOp, reply=%v", o.name(), reply)
					goto abortUnlock
				}
				o.addUpdateNodeListOpRet(tpc, txId, ret, n.raft.selfId)
				txIds = append(txIds, txId)
				txId = txId.GetNext()
			}
		} else {
			// start migration
			for _, gId := range gIds {
				var ret RpcRet
				ret, reply = n.rpcMgr.CallPrepareAny(NewJoinMigrationOp(txId, o.target, gId, migrationId), txId, n.flags.ChunkRpcTimeoutDuration)
				if reply != RaftReplyOk {
					log.Errorf("Failed: %v, NewJoinMigrationOp, reply=%v", o.name(), reply)
					goto abortUnlock
				}
				o.addUpdateNodeListOpRet(tpc, txId, ret, n.raft.selfId)
				txIds = append(txIds, txId)
				txId = txId.GetNext()
			}
		}
		var ret RpcRet
		ret, reply = n.rpcMgr.CallPrepareAny(NewInitNodeListOp(txId, l, o.target, migrationId), txId, n.flags.RpcTimeoutDuration)
		if reply != RaftReplyOk {
			log.Errorf("Failed: %v, NewInitNodeListOp, reply=%v", o.name(), reply)
			goto abortUnlock
		}
		ret.node.GroupId = ""
		o.addUpdateNodeListOpRet(tpc, txId, ret, n.raft.selfId)
		txIds = append(txIds, txId)
		txId = txId.GetNext()
	} else {
		nodes := l.groupNode[o.target.groupId]
		if len(nodes) == 1 {
			// need migration
			for _, gId := range gIds {
				var ret RpcRet
				ret, reply = n.rpcMgr.CallPrepareAny(NewLeaveMigrationOp(txId, o.target, gId, migrationId), txId, n.flags.ChunkRpcTimeoutDuration)
				if reply != RaftReplyOk {
					log.Errorf("Failed: %v, NewLeaveMigrationOp, reply=%v", o.name(), reply)
					goto abortUnlock
				}
				o.addUpdateNodeListOpRet(tpc, txId, ret, n.raft.selfId)
				txIds = append(txIds, txId)
				txId = txId.GetNext()
			}
		} else {
			for _, gId := range gIds {
				var ret RpcRet
				ret, reply := n.rpcMgr.CallPrepareAny(NewUpdateNodeListOp(txId, o.target, gId, false, nil, migrationId), txId, n.flags.RpcTimeoutDuration)
				if reply != RaftReplyOk {
					log.Errorf("Failed: %v, NewUpdateNodeListOp, reply=%v", o.name(), reply)
					goto abortUnlock
				}
				o.addUpdateNodeListOpRet(tpc, txId, ret, n.raft.selfId)
				txIds = append(txIds, txId)
				txId = txId.GetNext()
			}
		}
	}
	n.raftGroup.lock.RUnlock()
	tpc.NextTxSeqNum = uint64(txId.SeqNum)
	if reply := n.raft.AppendExtendedLogEntry(AppendEntryUpdateNodeListCoordinatorCmdId, tpc); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, coordinatorTxId=%v, reply=%v", o.name(), coordinatorTxId, reply)
		goto abort
	}
	for _, node := range tpc.Remotes {
		var addr *common.NodeAddrInet4 = nil
		if node.Node.GetGroupId() == "" {
			a := NewSaFromNodeMsg(node.GetNode())
			addr = &a
		}
		nodeListVer := o.nodeListVer
		if addr != nil && *addr == o.target.addr {
			nodeListVer = uint64(math.MaxUint64)
		}
		n.rpcMgr.CallRemote(NewCommitMigrationParticipantOp(txId, coordinatorTxId.GetVariant(node.TxSeqNum), node.Node.GroupId, nodeListVer, addr, migrationId), n.flags.CommitRpcTimeoutDuration)
		txId = txId.GetNext()
	}
	if _, _, reply := n.raft.AppendEntriesLocal(NewAppendEntryCommitTxCommand(n.raft.currentTerm.Get(), tpc.GetTxId())); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendEntriesLocal, txId=%v, reply=%v", o.name(), tpc.TxId, reply)
		// TODO: must not be here
		return nil, reply
	}
	n.rpcMgr.DeleteAndUnlockLocalAll(txIds...)
	log.Debugf("Success: %v, coordinatorTxId=%v", o.name(), coordinatorTxId)
	return nil, RaftReplyOk
abortUnlock:
	n.raftGroup.lock.RUnlock()
abort:
	o.abortUpdateNodeList(n, tpc, txId, n.flags.RpcTimeoutDuration)
	return nil, reply
}

func (o CoordinatorUpdateNodeListOp) remote(n *NodeServer, sa common.NodeAddrInet4, id CoordinatorId, _ uint64) (*common.MembershipListMsg, RaftBasicReply) {
	args := &common.CoordinatorUpdateNodeListArgs{
		Node:        o.target.toMsg(),
		Id:          id.toMsg(),
		IsAdd:       o.isAdd,
		NodeListVer: o.nodeListVer, // value from tracker Node
	}
	msg := &common.UpdateMembershipRet{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorUpdateNodeListCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.files, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return nil, r
}

func (o *RpcMgr) CoordinatorUpdateNodeList(msg RpcMsg) *common.Ack {
	args := &common.CoordinatorUpdateNodeListArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorUpdateNodeListRpc, ParseExecProtoBufMessage, reply=%v", reply)
		return &common.Ack{Status: reply}
	}
	op := CoordinatorUpdateNodeListOp{
		isAdd:  args.GetIsAdd(),
		target: NewRaftNodeFromMsg(args.GetNode()), nodeListVer: args.GetNodeListVer(),
	}
	_, r := o.CallCoordinatorUpdateNodeListInRPC(op, NewCoordinatorIdFromMsg(args.GetId()), args.GetNodeListVer())
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o CoordinatorUpdateNodeListOp) GetLeader(_ *NodeServer, _ *RaftNodeList) (RaftNode, bool) {
	return RaftNode{}, false // unused
}

////////////////////////////////////////////////////////////////////////////////////////////////

type TwoPCCommitRecord struct {
	txId         TxId
	txType       int
	primaryMeta  *WorkingMeta
	primaryLocal bool
	localMetas   []*common.CopiedMetaMsg
	remoteMetas  []*common.MetaCommitRecordMsg
	nextTxSeqNum uint64

	chunks []*common.UpdateChunkRecordMsg //for update
}

func NewTwoPCUpdateCommitLog(n *NodeServer, txId TxId, pOp RpcRet, chunks []*common.UpdateChunkRecordMsg, txType int) *TwoPCCommitRecord {
	cTxId := TxId{ClientId: txId.ClientId, SeqNum: txId.SeqNum, TxSeqNum: 0}
	isLocal := pOp.node.NodeId == n.raft.selfId
	remoteMetas := make([]*common.MetaCommitRecordMsg, 0)
	if !isLocal {
		remoteMetas = append(remoteMetas, &common.MetaCommitRecordMsg{TxSeqNum: txId.TxSeqNum, GroupId: pOp.node.GroupId})
	} else {
		cTxId.TxSeqNum = txId.TxSeqNum
	}
	meta := pOp.asWorkingMeta()
	return &TwoPCCommitRecord{
		txId:         cTxId,
		txType:       txType,
		primaryMeta:  meta,
		primaryLocal: isLocal,
		localMetas:   make([]*common.CopiedMetaMsg, 0),
		remoteMetas:  remoteMetas,
		chunks:       chunks,
		nextTxSeqNum: txId.TxSeqNum + 1,
	}
}

func NewTwoPCCommitRecordFromMsg(l *common.TwoPCCommitRecordMsg) *TwoPCCommitRecord {
	primary := NewWorkingMetaFromMsg(l.GetPrimaryMeta())
	if len(l.PrimaryPrev) > 0 {
		primary.prevVer = NewWorkingMetaFromMsg(l.GetPrimaryPrev()[0])
	}
	return &TwoPCCommitRecord{
		txId: NewTxIdFromMsg(l.GetTxId()), txType: int(l.GetTxType()), primaryMeta: primary, primaryLocal: l.GetPrimaryLocal(),
		localMetas: l.GetLocalMetas(), remoteMetas: l.GetRemoteMetas(), chunks: l.GetChunks(), nextTxSeqNum: l.GetNextTxSeqNum(),
	}
}

func (o *TwoPCCommitRecord) toMsg() proto.Message {
	prev := make([]*common.CopiedMetaMsg, 0)
	if o.primaryMeta.prevVer != nil {
		prev = append(prev, o.primaryMeta.prevVer.toMsg())
	}
	l := &common.TwoPCCommitRecordMsg{
		TxId: o.txId.toMsg(), TxType: int32(o.txType), PrimaryMeta: o.primaryMeta.toMsg(), PrimaryPrev: prev, PrimaryLocal: o.primaryLocal,
		LocalMetas: o.localMetas, RemoteMetas: o.remoteMetas, Chunks: o.chunks, NextTxSeqNum: o.nextTxSeqNum,
	}
	return l
}

func (o *TwoPCCommitRecord) CommitRemote(n *NodeServer, retryInterval time.Duration) {
	fns := make([]ParticipantOp, 0)
	if o.txType == TxUpdateCoordinator {
		for _, c := range o.chunks {
			if c.GroupId != n.raftGroup.selfGroup {
				txId := o.txId.GetVariant(o.nextTxSeqNum)
				fns = append(fns, NewCommitUpdateChunkOp(txId, c, o.primaryMeta))
				o.nextTxSeqNum += 1
			}
		}
	} else if o.txType == TxTruncateCoordinator {
		// shrink
		for offset := o.primaryMeta.size; offset < o.primaryMeta.prevVer.size; {
			var length = o.primaryMeta.chunkSize
			slop := offset % o.primaryMeta.chunkSize
			if slop > 0 {
				length -= slop
			}
			if offset+length > o.primaryMeta.prevVer.size {
				length = o.primaryMeta.prevVer.size - offset
			}
			txId := o.txId.GetVariant(o.nextTxSeqNum)
			fns = append(fns, NewCommitDeleteChunkOp(txId, o.primaryMeta, offset, length))
			offset += length
			o.nextTxSeqNum += 1
		}
		// expand
		for offset := o.primaryMeta.prevVer.size; offset < o.primaryMeta.size; {
			var length = o.primaryMeta.chunkSize
			slop := offset % o.primaryMeta.chunkSize
			if slop > 0 {
				length -= slop
			}
			if offset+length > o.primaryMeta.size {
				length = o.primaryMeta.size - offset
			}
			txId := o.txId.GetVariant(o.nextTxSeqNum)
			fns = append(fns, NewCommitExpandChunkOp(txId, o.primaryMeta, offset, length))
			offset += length
			o.nextTxSeqNum += 1
		}
	}
	for _, m := range o.remoteMetas {
		txId := o.txId.GetVariant(o.nextTxSeqNum)
		fns = append(fns, NewCommitParticipantOp(txId, o.txId.GetVariant(m.TxSeqNum), m.GroupId))
		o.nextTxSeqNum += 1
	}

	for _, fn := range fns {
		n.rpcMgr.CallRemote(fn, retryInterval)
	}
}

func (o *TwoPCCommitRecord) CountRemote(raftGroup *RaftGroupMgr) (r int) {
	r = len(o.remoteMetas)
	for _, c := range o.chunks {
		if c.GroupId != raftGroup.selfGroup {
			r += 1
		}
	}
	return r
}

func (o *TwoPCCommitRecord) CommitCoordinator(n *NodeServer, retryInterval time.Duration) (reply int32) {
	begin := time.Now()
	if reply = n.raft.AppendExtendedLogEntry(AppendEntryUpdateMetaCoordinatorCmdId, o.toMsg()); reply != RaftReplyOk {
		log.Errorf("Failed: AppendUpdateMetaCoordinatorLog, AppendExtendedLogEntry, reply=%v", reply)
		return reply
	}
	prepare := time.Now()
	var commitRemote time.Time
	if o.CountRemote(n.raftGroup) > 0 {
		o.CommitRemote(n, retryInterval)
		commitRemote = time.Now()
		if _, _, reply = n.raft.AppendEntriesLocal(NewAppendEntryCommitTxCommand(n.raft.currentTerm.Get(), o.txId.toMsg())); reply != RaftReplyOk {
			// TODO: must not be here
			log.Errorf("Failed: TwoPCCommitRecord.CommitCoordinator, AppendEntriesLocal, txId=%v, reply=%v", o.txId, reply)
			return reply
		}
	} else {
		commitRemote = prepare
	}
	endAll := time.Now()
	log.Debugf("Success: TwoPCCommitRecord.CommitCoordinator, txId=%v, prepare=%v, commitRemote=%v, commit=%v",
		o.txId, prepare.Sub(begin), commitRemote.Sub(prepare), endAll.Sub(commitRemote))
	return
}
