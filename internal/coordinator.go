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

/*
*
## Overview and Terminology of objcache transaction protocol
When a FS user requests a file/inode operation (e.g., open, write, read), objcache FUSE receives it and needs to redirect it to cluster nodes.
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

// complete. often returns a primary meta for tracking the latest inode h
return CoordinatorRet{meta: op[0].GetResult().GetMetaRpc()}, RaftReplyOk
}
// unlock local operations (remote ones are ignored). locking is done outside the Raft h machine, so we need this here (remote commit RPCs also do this)

## Special transaction architecture for write()
In naive implementation, write may be a single transaction with updating chunks and meta (to update file size).
However, it can be very slow when we write apps with many write() with few bytes of buffers since 1) it requires locking the entire file for meta operations,
and 2) transfering data between client-coordinator and coordinator-participants.

To optimize this, Updating chunks is treated as a special transaction.
At write(), client directly transfers chunk updates to a participant and keep the updated info without updating meta.
Then, When FUSE informs a size update (SetInodeAttributes or Flush), client starts a transaction to commit updating chunks and the new size of meta.
Chunk updates are recorded as StagingChunk and multiple StagingChunks are commited to be a new version of WorkingChunk.
Client failures may loose information of StagingChunks, but this does not break consistency of any inodes (but becomes an orphan chunks, which recovery procedure like fsck should reclaim).

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

type CoordinatorOpBase interface {
	name() string
	GetTxId() TxId
	GetLeader(*NodeServer, *RaftNodeList) (RaftNode, bool)
	local(*NodeServer, *TxIdMgr, *RaftNodeList) (interface{}, int32)
	remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply)
	RetToMsg(ret interface{}, r RaftBasicReply) proto.Message
	GetCaller(*NodeServer) RpcCaller
}

type CoordinatorId struct {
	clientId uint32
	seqNum   uint32
}

func (t *CoordinatorId) toMsg() *common.CoordinatorIdMsg {
	return &common.CoordinatorIdMsg{ClientId: t.clientId, SeqNum: t.seqNum}
}

func (t *CoordinatorId) toTxId(txSeqNum uint64) TxId {
	return TxId{ClientId: t.clientId, SeqNum: t.seqNum, TxSeqNum: txSeqNum}
}

func (t *CoordinatorId) toCoordinatorTxId() TxId {
	return TxId{ClientId: t.clientId, SeqNum: t.seqNum, TxSeqNum: 0}
}

func NewCoordinatorIdFromMsg(msg *common.CoordinatorIdMsg) CoordinatorId {
	return CoordinatorId{clientId: msg.GetClientId(), seqNum: msg.GetSeqNum()}
}

type CoordinatorRet struct {
	meta     *WorkingMeta
	parent   *WorkingMeta
	extReply int32 // to notify false positives when persisting objects
}

func NewCoordinatorRetFromMsg(msg *common.CoordinatorRetMsg) *CoordinatorRet {
	p := msg.GetParent()
	ret := &CoordinatorRet{meta: NewWorkingMetaFromMsg(msg.GetMeta()), extReply: msg.GetExtReply()}
	if len(p) > 0 {
		ret.parent = NewWorkingMetaFromMsg(p[0])
	}
	return ret
}

func (c *CoordinatorRet) toMsg(r RaftBasicReply) *common.CoordinatorRetMsg {
	ret := &common.CoordinatorRetMsg{Meta: nil, Parent: nil, Status: r.reply, Leader: r.GetLeaderNodeMsg(), ExtReply: c.extReply}
	if c.meta != nil {
		ret.Meta = c.meta.toMsg()
	}
	if c.parent != nil {
		ret.Parent = make([]*common.CopiedMetaMsg, 1)
		ret.Parent[0] = c.parent.toMsg()
	}
	return ret
}

/////////////////////////////////////////////////////////////////////////////////////

type CoordinatorFlushObjectOp struct {
	id       CoordinatorId
	inodeKey InodeKeyType
	records  []*common.UpdateChunkRecordMsg
	mTime    int64
	mode     uint32
}

func NewCoordinatorFlushObjectOp(id CoordinatorId, inodeKey InodeKeyType, records []*common.UpdateChunkRecordMsg, mTime int64, mode uint32) CoordinatorFlushObjectOp {
	return CoordinatorFlushObjectOp{id: id, inodeKey: inodeKey, records: records, mTime: mTime, mode: mode}
}

func NewCoordinatorFlushObjectOpFromMsg(msg RpcMsg) (CoordinatorOpBase, uint64, int32) {
	args := &common.FlushObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorFlushObject, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CoordinatorFlushObjectOp{
		id: NewCoordinatorIdFromMsg(args.GetId()), inodeKey: InodeKeyType(args.GetInodeKey()),
		records: args.GetRecords(), mTime: args.GetMTime(), mode: args.GetMode(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CoordinatorFlushObjectOp) name() string {
	return "CoordinatorFlushObject"
}

func (o CoordinatorFlushObjectOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorFlushObjectOp) local(n *NodeServer, tm *TxIdMgr, _ *RaftNodeList) (interface{}, int32) {
	begin := time.Now()

	txId := tm.GetNextId()
	var updateSize = int64(0)
	for _, record := range o.records {
		for _, stag := range record.GetStags() {
			lastOffset := stag.GetOffset() + stag.GetLength()
			if updateSize < lastOffset {
				updateSize = lastOffset
			}
			tm.AddTx(NewTxIdFromMsg(stag.GetTxId()), RpcRet{node: RaftNode{groupId: record.GetGroupId()}})
		}
	}
	op, r := n.rpcMgr.CallRpc(NewUpdateMetaSizeOp(txId, o.inodeKey, updateSize, o.mTime, o.mode).GetCaller(n))
	if r.reply != RaftReplyOk {
		return nil, r.reply
	}
	metaTx := tm.AddTx(txId, op)

	endUpdateMetaSize := time.Now()
	endLinkMeta := time.Now()
	ccmd := NewFlushCoordinatorCommand(tm.GetCoordinatorTxId(), o.records, metaTx, tm.GetNextId())
	if reply := n.rpcMgr.CommitCoordinator(ccmd, n.flags.CommitRpcTimeoutDuration); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CommitCoordinator, txId=%v, reply=%v", o.name(), txId, reply)
		return nil, reply
	}
	endAll := time.Now()
	log.Debugf("Success: %v, inodeKey=%v, updateSize=%v, link=%v, commit=%v",
		o.name(), o.inodeKey, endUpdateMetaSize.Sub(begin), endLinkMeta.Sub(endUpdateMetaSize), endAll.Sub(endLinkMeta))
	return &CoordinatorRet{meta: op.asWorkingMeta()}, RaftReplyOk
}

func (o CoordinatorFlushObjectOp) RetToMsg(ret interface{}, r RaftBasicReply) proto.Message {
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.(*CoordinatorRet).toMsg(r)
}

func (o CoordinatorFlushObjectOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, false)
}

func (o CoordinatorFlushObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply) {
	args := &common.FlushObjectArgs{
		Id:          o.id.toMsg(),
		NodeListVer: nodeListVer,
		InodeKey:    uint64(o.inodeKey),
		MTime:       o.mTime,
		Mode:        o.mode,
		Records:     o.records,
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorFlushObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o CoordinatorFlushObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.inodeKey})
}

/////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorTruncateObjectOp struct {
	id       CoordinatorId
	inodeKey InodeKeyType
	newSize  int64
}

func NewCoordinatorTruncateObjectOp(id CoordinatorId, inodeKey InodeKeyType, newSize int64) CoordinatorTruncateObjectOp {
	return CoordinatorTruncateObjectOp{id: id, inodeKey: inodeKey, newSize: newSize}
}

func NewCoordinatorTruncateObjectOpFromMsg(msg RpcMsg) (CoordinatorOpBase, uint64, int32) {
	args := &common.TruncateObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCoordinatorTruncateObjectOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CoordinatorTruncateObjectOp{
		id: NewCoordinatorIdFromMsg(args.GetId()), inodeKey: InodeKeyType(args.GetInodeKey()), newSize: args.GetNewSize(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CoordinatorTruncateObjectOp) name() string {
	return "CoordinatorTruncateObject"
}

func (o CoordinatorTruncateObjectOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorTruncateObjectOp) local(n *NodeServer, tm *TxIdMgr, _ *RaftNodeList) (interface{}, int32) {
	txId := tm.GetNextId()
	op, r := n.rpcMgr.CallRpc(NewTruncateMetaOp(txId, o.inodeKey, o.newSize).GetCaller(n))
	if r.reply != RaftReplyOk {
		return nil, r.reply
	}
	metaTx := tm.AddTx(txId, op)
	ccmd := NewTruncateCoordinatorCommand(tm.GetCoordinatorTxId(), metaTx, n.raftGroup, tm.GetNextId())
	if reply := n.rpcMgr.CommitCoordinator(ccmd, n.flags.CommitRpcTimeoutDuration); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CommitCoordinator, txId=%v, reply=%v", o.name(), ccmd.txId, reply)
		return nil, reply
	}
	return &CoordinatorRet{meta: op.asWorkingMeta()}, r.reply
}

func (o CoordinatorTruncateObjectOp) RetToMsg(ret interface{}, r RaftBasicReply) proto.Message {
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.(*CoordinatorRet).toMsg(r)
}

func (o CoordinatorTruncateObjectOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, false)
}

func (o CoordinatorTruncateObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply) {
	args := &common.TruncateObjectArgs{
		Id:          o.id.toMsg(),
		NodeListVer: nodeListVer,
		InodeKey:    uint64(o.inodeKey),
		NewSize:     o.newSize,
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorTruncateObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o CoordinatorTruncateObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.inodeKey})
}

///////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorDeleteObjectOp struct {
	id             CoordinatorId
	parentFullPath string
	parentInodeKey InodeKeyType
	childName      string
	childInodeKey  InodeKeyType
}

func NewCoordinatorDeleteObjectOp(id CoordinatorId, parentFullPath string, parentInodeKey InodeKeyType, childName string, childInodeKey InodeKeyType) CoordinatorDeleteObjectOp {
	return CoordinatorDeleteObjectOp{id: id, parentFullPath: parentFullPath, parentInodeKey: parentInodeKey, childName: childName, childInodeKey: childInodeKey}
}

func NewCoordinatorDeleteObjectOpFromMsg(msg RpcMsg) (CoordinatorOpBase, uint64, int32) {
	args := &common.DeleteObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCoordinatorDeleteObjectOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CoordinatorDeleteObjectOp{
		id:             NewCoordinatorIdFromMsg(args.GetId()),
		parentInodeKey: InodeKeyType(args.GetParentInodeKey()), parentFullPath: args.GetParentFullPath(),
		childName: args.GetChildName(), childInodeKey: InodeKeyType(args.GetChildInodeKey()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CoordinatorDeleteObjectOp) name() string {
	return "CoordinatorDeleteObject"
}

func (o CoordinatorDeleteObjectOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorDeleteObjectOp) local(n *NodeServer, tm *TxIdMgr, _ *RaftNodeList) (interface{}, int32) {
	txId := tm.GetNextId()
	removedKey := filepath.Join(o.parentFullPath, o.childName)
	op, r := n.rpcMgr.CallRpc(NewDeleteMetaOp(txId, o.childInodeKey, removedKey).GetCaller(n))
	if r.reply != RaftReplyOk {
		return nil, r.reply
	}
	prevSize := op.asWorkingMeta().prevVer.size
	metaTx := tm.AddTx(txId, op)
	childMeta := op.ext.(*WorkingMeta)

	txId2 := tm.GetNextId()
	op2, r2 := n.rpcMgr.CallRpc(NewUnlinkMetaOp(txId2, o.parentInodeKey, o.parentFullPath, o.childName, childMeta.IsDir()).GetCaller(n))
	if r2.reply != RaftReplyOk {
		return nil, r2.reply
	}
	parentTx := tm.AddTx(txId2, op2)

	ccmd := NewDeleteCoordinatorCommand(tm.GetCoordinatorTxId(), removedKey, prevSize, metaTx, parentTx, n.raftGroup, tm.GetNextId())
	if reply := n.rpcMgr.CommitCoordinator(ccmd, n.flags.CommitRpcTimeoutDuration); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CommitCoordinator, reply=%v", o.name(), reply)
		return nil, reply
	}
	log.Debugf("Success: %v, parentInodeId=%v, childName=%v", o.name(), o.parentInodeKey, o.childName)
	return &CoordinatorRet{}, RaftReplyOk
}

func (o CoordinatorDeleteObjectOp) RetToMsg(ret interface{}, r RaftBasicReply) proto.Message {
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.(*CoordinatorRet).toMsg(r)
}

func (o CoordinatorDeleteObjectOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, false)
}

func (o CoordinatorDeleteObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply) {
	args := &common.DeleteObjectArgs{
		Id:             o.id.toMsg(),
		NodeListVer:    nodeListVer,
		ParentInodeKey: uint64(o.parentInodeKey),
		ParentFullPath: o.parentFullPath,
		ChildName:      o.childName,
		ChildInodeKey:  uint64(o.childInodeKey),
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorDeleteObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o CoordinatorDeleteObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.childInodeKey})
}

////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorHardLinkOp struct {
	id                CoordinatorId
	srcInodeKey       InodeKeyType
	srcParentInodeKey InodeKeyType
	dstParentKey      string
	dstParentInodeKey InodeKeyType
	dstName           string
	childInodeKey     InodeKeyType
}

func NewCoordinatorHardLinkOp(id CoordinatorId, srcInodeKey InodeKeyType, srcParentInodeKey InodeKeyType, dstParentKey string, dstParentInodeKey InodeKeyType, dstName string, childInodeKey InodeKeyType) CoordinatorHardLinkOp {
	return CoordinatorHardLinkOp{
		id:          id,
		srcInodeKey: srcInodeKey, srcParentInodeKey: srcParentInodeKey, dstParentKey: dstParentKey,
		dstParentInodeKey: dstParentInodeKey, dstName: dstName, childInodeKey: childInodeKey,
	}
}

func NewCoordinatorHardLinkOpFromMsg(msg RpcMsg) (CoordinatorOpBase, uint64, int32) {
	args := &common.HardLinkObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCoordinatorHardLinkOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CoordinatorHardLinkOp{
		id:          NewCoordinatorIdFromMsg(args.GetId()),
		srcInodeKey: InodeKeyType(args.GetSrcInodeKey()), srcParentInodeKey: InodeKeyType(args.GetSrcParentInodeKey()),
		dstParentInodeKey: InodeKeyType(args.GetDstParentInodeKey()), dstName: args.GetChildName(),
		childInodeKey: InodeKeyType(args.GetChildInodeKey()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CoordinatorHardLinkOp) name() string {
	return "CoordinatorHardLink"
}

func (o CoordinatorHardLinkOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorHardLinkOp) local(n *NodeServer, tm *TxIdMgr, _ *RaftNodeList) (interface{}, int32) {
	newKey := filepath.Join(o.dstParentKey, o.dstName)

	txId := tm.GetNextId()
	op, r := n.rpcMgr.CallRpc(NewUpdateMetaKeyOp(txId, o.childInodeKey, "", newKey, o.srcParentInodeKey).GetCaller(n))
	if r.reply != RaftReplyOk {
		return nil, r.reply
	}
	metaTx := tm.AddTx(txId, op)
	childMeta := op.ext.(*GetWorkingMetaRet).meta

	txId2 := tm.GetNextId()
	op2, r2 := n.rpcMgr.CallRpc(NewCreateChildMetaOp(txId, o.dstParentInodeKey, o.dstParentKey, o.dstName, o.childInodeKey, childMeta.IsDir()).GetCaller(n))
	if r2.reply != RaftReplyOk {
		return nil, r2.reply
	}
	parentTx := tm.AddTx(txId2, op2)

	ccmd := NewHardLinkCoordinatorCommand(tm.GetCoordinatorTxId(), n.raftGroup.selfGroup, newKey, metaTx, parentTx, tm.GetNextId())
	if reply := n.rpcMgr.CommitCoordinator(ccmd, n.flags.CommitRpcTimeoutDuration); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CommitCoordinator, txId=%v, reply=%v", o.name(), ccmd.txId, reply)
		return nil, reply
	}
	log.Debugf("Success: %v, srcParent=%v, srcInodeKey=%v, dst=%v/%v", o.name(), o.srcParentInodeKey, o.srcInodeKey, o.dstParentKey, o.dstName)
	return &CoordinatorRet{meta: op.asWorkingMeta()}, RaftReplyOk
}

func (o CoordinatorHardLinkOp) RetToMsg(ret interface{}, r RaftBasicReply) proto.Message {
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.(*CoordinatorRet).toMsg(r)
}

func (o CoordinatorHardLinkOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, false)
}

func (o CoordinatorHardLinkOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply) {
	args := &common.HardLinkObjectArgs{
		Id:                o.id.toMsg(),
		NodeListVer:       nodeListVer,
		SrcParentInodeKey: uint64(o.srcParentInodeKey),
		SrcInodeKey:       uint64(o.srcInodeKey),
		DstParentKey:      o.dstParentKey,
		DstParentInodeKey: uint64(o.dstParentInodeKey),
		ChildName:         o.dstName,
		ChildInodeKey:     uint64(o.childInodeKey),
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorHardLinkObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o CoordinatorHardLinkOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.childInodeKey})
}

////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorRenameObjectOp struct {
	id                CoordinatorId
	srcParentKey      string
	srcParentInodeKey InodeKeyType
	srcName           string
	dstParentKey      string
	dstParentInodeKey InodeKeyType
	dstName           string
	childInodeKey     InodeKeyType
	parent            InodeKeyType
}

func NewCoordinatorRenameObjectOp(id CoordinatorId, srcParentKey string, srcParentInodeKey InodeKeyType, srcName string, dstParentKey string, dstParentInodeKey InodeKeyType, dstName string, childInodeKey InodeKeyType) CoordinatorRenameObjectOp {
	return CoordinatorRenameObjectOp{
		id: id, srcParentKey: srcParentKey, dstParentKey: dstParentKey,
		srcParentInodeKey: srcParentInodeKey, srcName: srcName, dstParentInodeKey: dstParentInodeKey, dstName: dstName, childInodeKey: childInodeKey,
	}
}

func NewCoordinatorRenameObjectOpFromMsg(msg RpcMsg) (CoordinatorOpBase, uint64, int32) {
	args := &common.RenameObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCoordinatorRenameObjectOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CoordinatorRenameObjectOp{
		id:           NewCoordinatorIdFromMsg(args.GetId()),
		srcParentKey: args.GetSrcParentKey(), srcParentInodeKey: InodeKeyType(args.GetSrcParentInodeKey()), srcName: args.GetSrcName(),
		dstParentKey: args.GetDstParentKey(), dstParentInodeKey: InodeKeyType(args.GetDstParentInodeKey()), dstName: args.GetDstName(),
		childInodeKey: InodeKeyType(args.GetChildInodeKey()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CoordinatorRenameObjectOp) name() string {
	return "CoordinatorRenameObject"
}

func (o CoordinatorRenameObjectOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorRenameObjectOp) CheckError(unlink UpdateParentRet, link UpdateParentRet) int32 {
	// TODO: rename ignores overwrite. is this okay?
	if (unlink.mayErrorReply == RaftReplyOk && link.mayErrorReply == RaftReplyOk) ||
		link.mayErrorReply == ErrnoToReply(unix.EEXIST) {
		return RaftReplyOk
	} else if unlink.mayErrorReply != RaftReplyOk {
		return unlink.mayErrorReply
	}
	return link.mayErrorReply
}

func (o CoordinatorRenameObjectOp) local(n *NodeServer, tm *TxIdMgr, _ *RaftNodeList) (interface{}, int32) {
	var parent *WorkingMeta
	removedKey := filepath.Join(o.srcParentKey, o.srcName)
	newKey := filepath.Join(o.dstParentKey, o.dstName)

	ccmd := NewRenameCoordinatorCommand(tm.GetCoordinatorTxId())

	txId := tm.GetNextId()
	op, r := n.rpcMgr.CallRpc(NewUpdateMetaKeyOp(txId, o.childInodeKey, removedKey, newKey, o.parent).GetCaller(n))
	if r.reply != RaftReplyOk {
		return nil, r.reply
	}
	metaTx := tm.AddTx(txId, op)
	ccmd.AddMeta(metaTx, removedKey, newKey, n.raftGroup.selfGroup)

	childMeta := op.ext.(*GetWorkingMetaRet).meta
	children := op.ext.(*GetWorkingMetaRet).children
	if childMeta.IsDir() {
		dirOldToNewPath := make(map[string]string)
		dirs := make(map[string]*WorkingMeta)
		dirChildren := make(map[string]map[string]InodeKeyType)
		nextDirOldToNewPath := make(map[string]string)
		nextDirs := make(map[string]*WorkingMeta)
		nextDirChildren := make(map[string]map[string]InodeKeyType)
		dirOldToNewPath[removedKey] = newKey
		dirs[removedKey] = childMeta
		dirChildren[removedKey] = children
		for {
			for dirRemovedKey, working := range dirs {
				dirNewKey := dirOldToNewPath[dirRemovedKey]
				for name, childInodeKey := range dirChildren[dirRemovedKey] {
					if name == "." || name == ".." {
						continue
					}
					childRemovedKey := filepath.Join(dirRemovedKey, name)
					childNewKey := filepath.Join(dirNewKey, name)
					txId2 := tm.GetNextId()
					op2, r2 := n.rpcMgr.CallRpc(NewUpdateMetaKeyOp(txId2, childInodeKey, childRemovedKey, childNewKey, working.inodeKey).GetCaller(n))
					if r2.reply != RaftReplyOk {
						return nil, r2.reply
					}
					metaTx2 := tm.AddTx(txId2, op2)
					ccmd.AddMeta(metaTx2, childRemovedKey, childNewKey, n.raftGroup.selfGroup)

					if newWorking := op2.ext.(*GetWorkingMetaRet).meta; newWorking.IsDir() {
						nextDirs[childRemovedKey] = newWorking
						nextDirChildren[childRemovedKey] = op2.ext.(*GetWorkingMetaRet).children
						nextDirOldToNewPath[childRemovedKey] = childNewKey
					}
				}
			}
			if len(nextDirs) == 0 {
				break
			}
			dirs = nextDirs
			dirChildren = nextDirChildren
			dirOldToNewPath = nextDirOldToNewPath
			nextDirOldToNewPath = make(map[string]string)
			nextDirs = make(map[string]*WorkingMeta)
		}
	}

	if o.dstParentInodeKey == o.srcParentInodeKey {
		// TODO: rename ignores overwrite. overwritten inodes become inaccessible but exists
		txId2 := tm.GetNextId()
		op2, r2 := n.rpcMgr.CallRpc(NewRenameMetaOp(txId2, o.srcParentInodeKey, o.srcParentKey, o.srcName, o.dstName, o.childInodeKey).GetCaller(n))
		if r2.reply != RaftReplyOk {
			return nil, r2.reply
		}
		oldParentTx := tm.AddTx(txId2, op2)
		ccmd.AddParent(oldParentTx, n.raftGroup.selfGroup)
	} else if o.srcParentInodeKey < o.dstParentInodeKey { // ensure lock ordering becomes globally consistent to avoid deadlocks
		txId2 := tm.GetNextId()
		op2, r2 := n.rpcMgr.CallRpc(NewUnlinkMetaOp(txId2, o.srcParentInodeKey, o.srcParentKey, o.srcName, childMeta.IsDir()).GetCaller(n))
		if r2.reply != RaftReplyOk {
			return nil, r2.reply
		}
		oldParentTx := tm.AddTx(txId2, op2)
		ccmd.AddParent(oldParentTx, n.raftGroup.selfGroup)

		txId3 := tm.GetNextId()
		op3, r3 := n.rpcMgr.CallRpc(NewCreateChildMetaOp(txId3, o.dstParentInodeKey, o.dstParentKey, o.dstName, o.childInodeKey, childMeta.IsDir()).GetCaller(n))
		if r3.reply != RaftReplyOk {
			return nil, r3.reply
		}
		newParentTx := tm.AddTx(txId3, op3)
		ccmd.AddParent(newParentTx, n.raftGroup.selfGroup)

		if reply4 := o.CheckError(op2.ext.(UpdateParentRet), op3.ext.(UpdateParentRet)); reply4 != RaftReplyOk {
			return nil, reply4
		}
	} else {
		txId2 := tm.GetNextId()
		op2, r2 := n.rpcMgr.CallRpc(NewCreateChildMetaOp(txId2, o.dstParentInodeKey, o.dstParentKey, o.dstName, o.childInodeKey, childMeta.IsDir()).GetCaller(n))
		if r2.reply != RaftReplyOk {
			return nil, r2.reply
		}
		newParentTx := tm.AddTx(txId2, op2)
		ccmd.AddParent(newParentTx, n.raftGroup.selfGroup)

		txId3 := tm.GetNextId()
		op3, r3 := n.rpcMgr.CallRpc(NewUnlinkMetaOp(txId3, o.srcParentInodeKey, o.srcParentKey, o.srcName, childMeta.IsDir()).GetCaller(n))
		if r3.reply != RaftReplyOk {
			return nil, r3.reply
		}
		oldParentTx := tm.AddTx(txId3, op3)
		ccmd.AddParent(oldParentTx, n.raftGroup.selfGroup)
		if reply4 := o.CheckError(op3.ext.(UpdateParentRet), op2.ext.(UpdateParentRet)); reply4 != RaftReplyOk {
			return nil, reply4
		}
	}

	if reply4 := n.rpcMgr.CommitCoordinator(ccmd, n.flags.CommitRpcTimeoutDuration); reply4 != RaftReplyOk {
		log.Errorf("Failed: %v, CommitRecord txId=%v, reply=%v", o.name(), ccmd.txId, reply4)
		return nil, reply4
	}
	return &CoordinatorRet{meta: childMeta, parent: parent}, RaftReplyOk
}

func (o CoordinatorRenameObjectOp) RetToMsg(ret interface{}, r RaftBasicReply) proto.Message {
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.(*CoordinatorRet).toMsg(r)
}

func (o CoordinatorRenameObjectOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, false)
}

func (o CoordinatorRenameObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply) {
	args := &common.RenameObjectArgs{
		Id:                o.id.toMsg(),
		NodeListVer:       nodeListVer,
		SrcParentKey:      o.srcParentKey,
		SrcParentInodeKey: uint64(o.srcParentInodeKey),
		SrcName:           o.srcName,
		DstParentKey:      o.dstParentKey,
		DstParentInodeKey: uint64(o.dstParentInodeKey),
		ChildInodeKey:     uint64(o.childInodeKey),
		DstName:           o.dstName,
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorRenameObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o CoordinatorRenameObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.dstParentInodeKey, o.srcParentInodeKey})
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorCreateObjectOp struct {
	id             CoordinatorId
	parentFullPath string
	parentInodeKey InodeKeyType
	childName      string
	childInodeKey  InodeKeyType
	chunkSize      int64
	mode           uint32
	expireMs       int32
}

func NewCoordinatorCreateObjectOp(id CoordinatorId, parentFullPath string, parentInodeKey InodeKeyType, childName string, childInodeKey InodeKeyType, mode uint32, chunkSize int64, expireMs int32) CoordinatorCreateObjectOp {
	return CoordinatorCreateObjectOp{
		id: id, parentFullPath: parentFullPath, parentInodeKey: parentInodeKey, mode: mode,
		childName: childName, childInodeKey: childInodeKey, chunkSize: chunkSize, expireMs: expireMs,
	}
}

func NewCoordinatorCreateObjectOpFromMsg(msg RpcMsg) (CoordinatorOpBase, uint64, int32) {
	args := &common.CreateObjectArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: CoordinatorCreateObject, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CoordinatorCreateObjectOp{
		id:             NewCoordinatorIdFromMsg(args.GetId()),
		parentFullPath: args.GetParentFullPath(), mode: args.GetMode(),
		parentInodeKey: InodeKeyType(args.GetParentInodeKey()), childName: args.GetChildName(),
		childInodeKey: InodeKeyType(args.GetChildInodeKey()), chunkSize: args.GetChunkSize(), expireMs: args.GetExpireMs(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CoordinatorCreateObjectOp) name() string {
	return "CoordinatorCreateObject"
}

func (o CoordinatorCreateObjectOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorCreateObjectOp) local(n *NodeServer, tm *TxIdMgr, _ *RaftNodeList) (interface{}, int32) {
	newKey := filepath.Join(o.parentFullPath, o.childName)
	txId := tm.GetNextId()
	op, r := n.rpcMgr.CallRpc(NewCreateMetaOp(txId, o.childInodeKey, o.parentInodeKey, newKey, o.mode, o.chunkSize, o.expireMs).GetCaller(n))
	if r.reply != RaftReplyOk {
		return nil, r.reply
	}
	metaTx := tm.AddTx(txId, op)
	childMeta := op.ext.(*WorkingMeta)

	txId2 := tm.GetNextId()
	op2, r2 := n.rpcMgr.CallRpc(NewCreateChildMetaOp(txId2, o.parentInodeKey, o.parentFullPath, o.childName, o.childInodeKey, childMeta.IsDir()).GetCaller(n))
	if r2.reply != RaftReplyOk {
		return nil, r2.reply
	}
	parentTx := tm.AddTx(txId2, op2)

	ccmd := NewCreateCoordinatorCommand(tm.GetCoordinatorTxId(), newKey, metaTx, parentTx, n.raftGroup, tm.GetNextId())
	if reply3 := n.rpcMgr.CommitCoordinator(ccmd, n.flags.CommitRpcTimeoutDuration); reply3 != RaftReplyOk {
		return nil, reply3
	}
	return &CoordinatorRet{meta: childMeta}, RaftReplyOk
}

func (o CoordinatorCreateObjectOp) RetToMsg(ret interface{}, r RaftBasicReply) proto.Message {
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.(*CoordinatorRet).toMsg(r)
}

func (o CoordinatorCreateObjectOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, false)
}

func (o CoordinatorCreateObjectOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply) {
	args := &common.CreateObjectArgs{
		Id:             o.id.toMsg(),
		NodeListVer:    nodeListVer,
		ParentFullPath: o.parentFullPath,
		ParentInodeKey: uint64(o.parentInodeKey),
		ChildName:      o.childName,
		ChildInodeKey:  uint64(o.childInodeKey),
		ChunkSize:      o.chunkSize,
		Mode:           o.mode,
		ExpireMs:       o.expireMs,
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorCreateObjectCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o CoordinatorCreateObjectOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.selectNodeWithRemoteMetaKey(l, []InodeKeyType{o.childInodeKey})
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorPersistOp struct {
	id       CoordinatorId
	inodeKey InodeKeyType
	priority int
}

func NewCoordinatorPersistOp(id CoordinatorId, inodeKey InodeKeyType, priority int) CoordinatorPersistOp {
	return CoordinatorPersistOp{id: id, inodeKey: inodeKey, priority: priority}
}

func NewCoordinatorPersistOpFromMsg(msg RpcMsg) (CoordinatorOpBase, uint64, int32) {
	args := &common.PersistArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCoordinatorPersistOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CoordinatorPersistOp{
		id: NewCoordinatorIdFromMsg(args.GetId()), inodeKey: InodeKeyType(args.GetInodeKey()), priority: int(args.GetPriority()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CoordinatorPersistOp) name() string {
	return "CoordinatorPersist"
}

func (o CoordinatorPersistOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorPersistOp) local(n *NodeServer, tm *TxIdMgr, _ *RaftNodeList) (interface{}, int32) {
	inode, metaKeys, working, reply := n.inodeMgr.PreparePersistMeta(o.inodeKey, n.dirtyMgr)
	if reply == ObjCacheIsNotDirty {
		return &CoordinatorRet{meta: working, extReply: reply}, RaftReplyOk
	} else if reply != RaftReplyOk {
		return &CoordinatorRet{meta: nil}, reply
	}
	defer n.inodeMgr.UnlockPersistInode(inode, o.inodeKey)
	var ts int64
	var chunks []*common.PersistedChunkInfoMsg = nil
	if working.IsDir() {
		ts, reply = n.inodeMgr.PutDirObject(inode, working, o.priority)
	} else if working.size == 0 {
		ts, reply = n.inodeMgr.PutEmptyObject(metaKeys, working, o.priority)
	} else if working.size < working.chunkSize {
		ts, chunks, reply = n.inodeMgr.PutObject(tm, metaKeys, working, o.priority, n.raftGroup.selfGroup, n.flags.DirtyFlusherIntervalDuration)
	} else {
		ts, chunks, reply = n.persistMultipleChunksMeta(tm, metaKeys, working, o.priority)
	}
	if reply != RaftReplyOk {
		return nil, reply
	}
	cmd := NewPersistCoordinatorCommand(tm.GetCoordinatorTxId(), o.inodeKey, working.version, ts, metaKeys, chunks, tm.GetNextId())
	if reply = n.rpcMgr.CommitCoordinator(cmd, n.flags.CoordinatorTimeoutDuration); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CommitCoordinator, reply=%v", o.name(), reply)
		return nil, reply
	}
	log.Debugf("Success: %v, keys=%v, reply=%v", o.name(), metaKeys, reply)
	return &CoordinatorRet{meta: working}, reply
}

func (o CoordinatorPersistOp) RetToMsg(ret interface{}, r RaftBasicReply) proto.Message {
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	return ret.(*CoordinatorRet).toMsg(r)
}

func (o CoordinatorPersistOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, false)
}

func (o CoordinatorPersistOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply) {
	args := &common.PersistArgs{
		InodeKey:    uint64(o.inodeKey),
		Priority:    int32(o.priority),
		Id:          o.id.toMsg(),
		NodeListVer: nodeListVer,
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorPersistCmdId, args, sa, time.Duration(0), n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o CoordinatorPersistOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

//////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorDeletePersistOp struct {
	id       CoordinatorId
	key      string
	inodeKey InodeKeyType // used only for selecting a coordinator
	priority int
}

func NewCoordinatorDeletePersistOp(id CoordinatorId, key string, inodeKey InodeKeyType, priority int) CoordinatorDeletePersistOp {
	return CoordinatorDeletePersistOp{id: id, key: key, inodeKey: inodeKey, priority: priority}
}

func NewCoordinatorDeletePersistOpFromMsg(msg RpcMsg) (CoordinatorOpBase, uint64, int32) {
	args := &common.DeletePersistArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCoordinatorDeletePersistOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CoordinatorDeletePersistOp{
		id: NewCoordinatorIdFromMsg(args.GetId()), priority: int(args.GetPriority()),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CoordinatorDeletePersistOp) name() string {
	return "CoordinatorDeletePersist"
}

func (o CoordinatorDeletePersistOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorDeletePersistOp) local(n *NodeServer, _ *TxIdMgr, _ *RaftNodeList) (interface{}, int32) {
	reply := n.inodeMgr.PreparePersistDeleteMeta(o.key, n.dirtyMgr)
	if reply == ObjCacheIsNotDirty {
		return &CoordinatorRet{extReply: reply}, RaftReplyOk
	}
	reply = n.inodeMgr.PersistDeleteObject(o.key, o.priority)
	if reply != RaftReplyOk {
		log.Debugf("Failed: %v, PersistDeleteMeta, reply=%v", o.name(), reply)
		return nil, reply
	}
	if reply = n.raft.AppendExtendedLogEntry(NewDeletePersistCoordinatorCommand(o.key)); reply != RaftReplyOk {
		log.Errorf("Failed: %v, AppendExtendedLogEntry, reply=%v", o.name(), reply)
		// TODO: must not be here
		return nil, reply
	}
	log.Debugf("Success: %v, key=%v, reply=%v", o.name(), o.key, reply)
	return &CoordinatorRet{}, reply
}

func (o CoordinatorDeletePersistOp) RetToMsg(ret interface{}, r RaftBasicReply) proto.Message {
	if r.reply != RaftReplyOk {
		return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
	}
	ret2 := ret.(*CoordinatorRet)
	return &common.CoordinatorRetMsg{Status: r.reply, Leader: r.GetLeaderNodeMsg(), ExtReply: ret2.extReply}
}

func (o CoordinatorDeletePersistOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, false)
}

func (o CoordinatorDeletePersistOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply) {
	args := &common.DeletePersistArgs{
		NodeListVer: nodeListVer,
		Id:          o.id.toMsg(),
		Key:         o.key,
		Priority:    int32(o.priority),
	}
	msg := &common.CoordinatorRetMsg{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorDeletePersistCmdId, args, sa, n.flags.PersistTimeoutDuration, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	r := NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
	if r.reply != RaftReplyOk {
		return nil, r
	}
	return NewCoordinatorRetFromMsg(msg), r
}

func (o CoordinatorDeletePersistOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.raftGroup.getKeyOwnerNodeLocalNew(l, o.inodeKey)
}

//////////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorUpdateNodeListOp struct {
	id          CoordinatorId
	trackerNode RaftNode
	isAdd       bool
	target      RaftNode
	nodeListVer uint64
	groupAddr   map[string]bool // for abort
}

func NewCoordinatorUpdateNodeListOp(id CoordinatorId, trackerNode RaftNode, isAdd bool, target RaftNode, groupAddr map[string]bool, nodeListVer uint64) CoordinatorUpdateNodeListOp {
	return CoordinatorUpdateNodeListOp{
		id: id, trackerNode: trackerNode, isAdd: isAdd, target: target, nodeListVer: nodeListVer, groupAddr: groupAddr,
	}
}

func NewCoordinatorUpdateNodeListOpFromMsg(msg RpcMsg) (CoordinatorOpBase, uint64, int32) {
	args := &common.CoordinatorUpdateNodeListArgs{}
	if reply := msg.ParseExecProtoBufMessage(args); reply != RaftReplyOk {
		log.Errorf("Failed: NewCoordinatorUpdateNodeListOpFromMsg, ParseExecProtoBufMessage, reply=%v", reply)
		return nil, uint64(math.MaxUint64), reply
	}
	fn := CoordinatorUpdateNodeListOp{
		id: NewCoordinatorIdFromMsg(args.GetId()), isAdd: args.GetIsAdd(),
		target: NewRaftNodeFromMsg(args.GetNode()), nodeListVer: args.GetNodeListVer(),
	}
	return fn, args.GetNodeListVer(), RaftReplyOk
}

func (o CoordinatorUpdateNodeListOp) name() string {
	return "CoordinatorUpdateNodeList"
}

func (o CoordinatorUpdateNodeListOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorUpdateNodeListOp) local(n *NodeServer, tm *TxIdMgr, l *RaftNodeList) (interface{}, int32) {
	if !n.raftGroup.beginUpdateGroup() {
		log.Infof("%v, another Node is joining/leaving... Retry", o.name())
		return nil, RaftReplyRetry
	}
	defer n.raftGroup.finishUpdateGroup()

	if _, ok := l.nodeAddr[o.target.nodeId]; (ok && o.isAdd) || (!ok && !o.isAdd) {
		return nil, RaftReplyOk
	}

	n.raftGroup.lock.RLock()
	gIds := make([]string, 0)
	for gId := range l.groupNode {
		gIds = append(gIds, gId)
	}
	sort.Strings(gIds)

	migrationId := tm.GetMigrationId()
	ccmd := NewUpdateNodeListCoordinatorCommand(tm.GetCoordinatorTxId(), migrationId, o.target, o.isAdd, n.selfNode)

	var reply int32
	if o.isAdd {
		if _, ok := l.groupNode[o.target.groupId]; ok {
			// migration is not required
			for _, gId := range gIds {
				txId := tm.GetNextId()
				ret, r := n.rpcMgr.CallRpc(NewUpdateNodeListOp(txId, o.target, gId, o.isAdd, nil, migrationId).GetCaller(n))
				if r.reply != RaftReplyOk {
					log.Errorf("Failed: %v, NewUpdateNodeListOp, r=%v", o.name(), r)
					reply = r.reply
					goto abortUnlock
				}
				nodeTx := tm.AddTx(txId, ret)
				ccmd.AddNode(nodeTx)
			}
		} else {
			// start migration
			for _, gId := range gIds {
				txId := tm.GetNextId()
				ret, r := n.rpcMgr.CallRpc(NewJoinMigrationOp(txId, o.target, gId, migrationId).GetCaller(n))
				if r.reply != RaftReplyOk {
					log.Errorf("Failed: %v, NewJoinMigrationOp, r=%v", o.name(), r)
					reply = r.reply
					goto abortUnlock
				}
				nodeTx := tm.AddTx(txId, ret)
				ccmd.AddNode(nodeTx)
			}
		}
		txId := tm.GetNextId()
		ret, r := n.rpcMgr.CallRpc(NewFillNodeListOp(txId, l, o.target, migrationId).GetCaller(n))
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: %v, NewFillNodeListOp, r=%v", o.name(), r)
			reply = r.reply
			goto abortUnlock
		}
		nodeTx := tm.AddTx(txId, ret)
		ccmd.AddNode(nodeTx)
	} else {
		nodes := l.groupNode[o.target.groupId]
		if len(nodes) == 1 {
			// need migration
			for _, gId := range gIds {
				txId := tm.GetNextId()
				ret, r := n.rpcMgr.CallRpc(NewLeaveMigrationOp(txId, o.target, gId, migrationId).GetCaller(n))
				if r.reply != RaftReplyOk {
					log.Errorf("Failed: %v, NewLeaveMigrationOp, r=%v", o.name(), r)
					reply = r.reply
					goto abortUnlock
				}
				nodeTx := tm.AddTx(txId, ret)
				ccmd.AddNode(nodeTx)
			}
		} else {
			for _, gId := range gIds {
				txId := tm.GetNextId()
				ret, r := n.rpcMgr.CallRpc(NewUpdateNodeListOp(txId, o.target, gId, false, nil, migrationId).GetCaller(n))
				if r.reply != RaftReplyOk {
					log.Errorf("Failed: %v, NewUpdateNodeListOp, r=%v", o.name(), r)
					reply = r.reply
					goto abortUnlock
				}
				nodeTx := tm.AddTx(txId, ret)
				ccmd.AddNode(nodeTx)
			}
		}
	}
	n.raftGroup.lock.RUnlock()
	if reply = n.rpcMgr.CommitCoordinator(ccmd, n.flags.CoordinatorTimeoutDuration); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CommitCoordinator, reply=%v", o.name(), reply)
		return nil, reply
	}
	log.Debugf("Success: %v, target=%v", o.name(), o.target)
	return &CoordinatorRet{}, RaftReplyOk
abortUnlock:
	n.raftGroup.lock.RUnlock()
	return nil, reply
}

func (o CoordinatorUpdateNodeListOp) RetToMsg(_ interface{}, r RaftBasicReply) proto.Message {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o CoordinatorUpdateNodeListOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, true)
}

func (o CoordinatorUpdateNodeListOp) remote(n *NodeServer, sa common.NodeAddrInet4, _ uint64) (interface{}, RaftBasicReply) {
	args := &common.CoordinatorUpdateNodeListArgs{
		Node:        o.target.toMsg(),
		Id:          o.id.toMsg(),
		IsAdd:       o.isAdd,
		NodeListVer: o.nodeListVer, // value from tracker Node
	}
	msg := &common.UpdateMembershipRet{}
	if reply := n.rpcClient.CallObjcacheRpc(RpcCoordinatorUpdateNodeListCmdId, args, sa, n.flags.CoordinatorTimeoutDuration, n.raft.extLogger, nil, nil, msg); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CallObjcacheRpc, sa=%v, reply=%v", o.name(), sa, reply)
		return nil, RaftBasicReply{reply: reply}
	}
	return &CoordinatorRet{}, NewRaftBasicReply(msg.GetStatus(), msg.GetLeader())
}

func (o CoordinatorUpdateNodeListOp) GetLeader(_ *NodeServer, _ *RaftNodeList) (RaftNode, bool) {
	return o.trackerNode, true
}

///////////////////////////////////////////////////////////////////////////////////////////////

type CoordinatorCompactLogOp struct {
	id CoordinatorId
}

func NewCoordinatorCompactLogOp(id CoordinatorId) CoordinatorCompactLogOp {
	return CoordinatorCompactLogOp{id: id}
}

func (o CoordinatorCompactLogOp) name() string {
	return "CoordinatorCompactLog"
}

func (o CoordinatorCompactLogOp) GetTxId() TxId {
	return o.id.toCoordinatorTxId()
}

func (o CoordinatorCompactLogOp) local(n *NodeServer, tm *TxIdMgr, nodeList *RaftNodeList) (interface{}, int32) {
	oldLogId := n.raft.GetExtLogId()
	logId := n.raft.GetExtLogIdForLogCompaction()
	n.raft.extLogger.Freeze(logId)

	dirtyMetas := n.dirtyMgr.GetAllDirtyMeta()
	dirtyChunks, err := n.inodeMgr.GetAllChunks(n.dirtyMgr.GetDirtyChunkAll())
	if err != nil {
		log.Errorf("Failed: %v, GetAllChunks, err=%v", o.name(), err)
		return nil, ErrnoToReply(err)
	}
	metas, files := n.inodeMgr.GetAllMeta()
	dirents := n.inodeMgr.GetAllDirInodes()
	nodes := nodeList.toMsg()

	s := NewSnapshot(int(n.flags.RpcChunkSizeBytes), tm.GetMigrationId(), logId.upper, metas, files, dirents, dirtyMetas, dirtyChunks, nodes)
	for switchFile := true; ; switchFile = false {
		next := s.GetNext()
		if next == nil {
			break
		}
		ccmd := NewCompactLogCoordinatorCommand(next)
		if reply := n.raft.__appendExtendedLogEntry(ccmd, logId, switchFile); reply != RaftReplyOk {
			log.Errorf("Failed: %v, AppendExtendedLogEntry, reply=%v", o.name(), reply)
			return nil, reply
		}
	}
	if reply := n.raft.log.CompactLog(); reply != RaftReplyOk {
		log.Errorf("Failed: %v, CompactLog, reply=%v", o.name(), reply)
		return nil, reply
	}
	if _, err = n.raft.extLogger.Remove(oldLogId); err != nil {
		log.Errorf("Failed (ignore): %v, Remove, oldLogId=%v, err=%v", o.name(), oldLogId, err)
	}
	log.Debugf("Success: %v", o.name())
	return &CoordinatorRet{}, RaftReplyOk
}

func (o CoordinatorCompactLogOp) RetToMsg(_ interface{}, r RaftBasicReply) proto.Message {
	return &common.Ack{Status: r.reply, Leader: r.GetLeaderNodeMsg()}
}

func (o CoordinatorCompactLogOp) GetCaller(n *NodeServer) RpcCaller {
	return NewCoordinatorRpcCaller(o, true)
}

func (o CoordinatorCompactLogOp) remote(n *NodeServer, sa common.NodeAddrInet4, nodeListVer uint64) (interface{}, RaftBasicReply) {
	return &CoordinatorRet{}, RaftBasicReply{reply: RaftReplyFail}
}

func (o CoordinatorCompactLogOp) GetLeader(n *NodeServer, l *RaftNodeList) (RaftNode, bool) {
	return n.selfNode, true
}

////////////////////////////////////////////////////////////

var CoordinatorOpMap = map[uint16]func(RpcMsg) (CoordinatorOpBase, uint64, int32){
	RpcCoordinatorUpdateNodeListCmdId: NewCoordinatorUpdateNodeListOpFromMsg,
	RpcCoordinatorFlushObjectCmdId:    NewCoordinatorFlushObjectOpFromMsg,
	RpcCoordinatorTruncateObjectCmdId: NewCoordinatorTruncateObjectOpFromMsg,
	RpcCoordinatorDeleteObjectCmdId:   NewCoordinatorDeleteObjectOpFromMsg,
	RpcCoordinatorHardLinkObjectCmdId: NewCoordinatorHardLinkOpFromMsg,
	RpcCoordinatorRenameObjectCmdId:   NewCoordinatorRenameObjectOpFromMsg,
	RpcCoordinatorCreateObjectCmdId:   NewCoordinatorCreateObjectOpFromMsg,
	RpcCoordinatorPersistCmdId:        NewCoordinatorPersistOpFromMsg,
	RpcCoordinatorDeletePersistCmdId:  NewCoordinatorDeletePersistOpFromMsg,
}
