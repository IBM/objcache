/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"math"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
)

type RpcMgr struct {
	lock  *sync.RWMutex
	calls map[TxId]RpcState
	n     *NodeServer
}

type RpcState struct {
	running int32
	ret     RpcRet
}

type RpcStatePointer struct {
	ts int64
}

type RpcRet struct {
	node   *common.NodeMsg
	unlock func(*NodeServer)
	ext    interface{}
}

func (o RpcRet) asWorkingMeta() *WorkingMeta {
	return o.ext.(*WorkingMeta)
}

func (o RpcStatePointer) Less(a btree.Item) bool {
	call := a.(RpcStatePointer)
	return o.ts < call.ts
}

func NewRpcManager(n *NodeServer) *RpcMgr {
	return &RpcMgr{lock: new(sync.RWMutex), calls: make(map[TxId]RpcState), n: n}
}

func (o *RpcMgr) CheckReset() (ok bool) {
	if ok = o.lock.TryLock(); !ok {
		log.Errorf("Failed: RpcMgr.CheckReset, o.lock is taken")
		return
	}
	if ok2 := len(o.calls) == 0; !ok2 {
		log.Errorf("Failed: RpcMgr.CheckReset, len(o.calls) != 0")
		ok = false
	}
	o.lock.Unlock()
	return
}

func (o *RpcMgr) Get(seq TxId) (call RpcState, ok bool) {
	o.lock.Lock()
	call, ok = o.calls[seq]
	o.lock.Unlock()
	return
}

func (o *RpcMgr) DeleteAndGet(seq TxId) (call RpcState, ok bool) {
	o.lock.Lock()
	call, ok = o.calls[seq]
	if ok {
		delete(o.calls, seq)
	}
	o.lock.Unlock()
	return
}

func (o *RpcMgr) DeleteAndUnlockLocalAll(seqs ...TxId) {
	unlocks := make([]func(*NodeServer), 0)
	o.lock.Lock()
	for _, seq := range seqs {
		call, ok := o.calls[seq]
		if ok {
			delete(o.calls, seq)
			if call.ret.unlock != nil {
				unlocks = append(unlocks, call.ret.unlock)
			}
		}
	}
	o.lock.Unlock()
	for i := len(unlocks) - 1; i >= 0; i-- {
		unlocks[i](o.n)
	}
}

func (o *RpcMgr) Enter(seq TxId) (call RpcState, duplicated bool, passed bool) {
	o.lock.Lock()
	call, duplicated = o.calls[seq]
	if !duplicated {
		call = RpcState{running: 1}
		o.calls[seq] = call
		passed = true
	} else {
		passed = call.running == 0
	}
	o.lock.Unlock()
	return
}

func (o *RpcMgr) Record(seq TxId, ret RpcRet) {
	o.lock.Lock()
	o.calls[seq] = RpcState{running: 0, ret: ret}
	o.lock.Unlock()
}

func (o *RpcMgr) __execSingle(fn ParticipantOp, seq TxId) (RpcRet, int32) {
	begin := time.Now()
	old, duplicated, passed := o.Enter(seq)
	if duplicated {
		if !passed {
			log.Warnf("__execSingle (%v), duplicated request. another process is executing, seq=%v", fn.name(), seq)
			return RpcRet{}, RaftReplyRetry
		}
		log.Warnf("__execSingle (%v), duplicated request. return old result, seq=%v", fn.name(), seq)
		return old.ret, RaftReplyOk
	}
	raftCheck := time.Now()
	var ret RpcRet
	var reply int32
	ret.ext, ret.unlock, reply = fn.local(o.n)
	if reply == RaftReplyOk {
		reply = fn.writeLog(o.n, ret.ext)
	}
	local := time.Now()
	o.Record(seq, ret) // Delete at CommitAbortRpc
	log.Debugf("__execSingle (%v), check=%v, %v.local=%v, %v.write=%v",
		fn.name(), raftCheck.Sub(begin), fn.name(), local.Sub(raftCheck), fn.name(), time.Since(local))
	return ret, reply
}

func (o *RpcMgr) __execParticipant(fn ParticipantOp, seq TxId, nodeListVer uint64, writeLog bool, commit bool) (RpcRet, RaftBasicReply) {
	begin := time.Now()
	var r RaftBasicReply
	if nodeListVer != uint64(math.MaxUint64) {
		r = o.n.raftGroup.BeginRaftRead(o.n.raft, nodeListVer)
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: __execParticipant (%v): BeginRaftRead, r=%v", fn.name(), r)
			return RpcRet{}, r
		}
	} else {
		r = o.n.raft.SyncBeforeClientQuery()
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: __execParticipant (%v): SyncBeforeClientQuery, r=%v", fn.name(), r)
			return RpcRet{}, r
		}
	}
	old, duplicated, passed := o.Enter(seq)
	if duplicated {
		if !passed {
			log.Warnf("__execParticipant (%v), duplicated request. another process is executing, seq=%v", fn.name(), seq)
			r.reply = RaftReplyRetry
			return RpcRet{}, r
		}
		log.Warnf("__execParticipant (%v), duplicated request. return old result, seq=%v", fn.name(), seq)
		return old.ret, r
	}
	raftCheck := time.Now()
	var ret RpcRet
	ret.ext, ret.unlock, r.reply = fn.local(o.n)
	if r.reply == RaftReplyOk && writeLog {
		r.reply = fn.writeLog(o.n, ret.ext)
	}
	local := time.Now()
	if !commit {
		o.Record(seq, ret) // Delete at CommitAbortRpc
	} else {
		o.DeleteAndGet(seq)
		if ret.unlock != nil {
			ret.unlock(o.n)
		}
	}
	log.Debugf("__execParticipant (%v), check=%v, %v.local=%v, %v.write=%v",
		fn.name(), raftCheck.Sub(begin), fn.name(), local.Sub(raftCheck), fn.name(), time.Since(local))
	return ret, r
}

func (o *RpcMgr) ExecPrepare(fn ParticipantOp, seq TxId, nodeListVer uint64) (RpcRet, RaftBasicReply) {
	return o.__execParticipant(fn, seq, nodeListVer, true, false)
}

func (o *RpcMgr) ExecCommitAbort(fn ParticipantOp, seq TxId, nodeListVer uint64) (RpcRet, RaftBasicReply) {
	return o.__execParticipant(fn, seq, nodeListVer, true, true)
}

//////////////////////////////////////////////////////////////////////////////////////////////////

func (o *RpcMgr) __callAny(fn ParticipantOp, txId TxId, remoteTimeout time.Duration, prepare bool) (ret RpcRet, r RaftBasicReply) {
	var nodeList *RaftNodeList = nil
	var leader RaftNode
	for i := 0; i < o.n.flags.MaxRetry; i++ {
		l := o.n.raftGroup.GetNodeListLocal()
		if nodeList == nil || nodeList.version != l.version {
			nodeList = l
			var ok bool
			leader, ok = fn.GetLeader(o.n, nodeList)
			if !ok {
				log.Errorf("Failed: __callAny(prepare=%v, %v), GetLeader, i=%v", prepare, fn.name(), i)
				return RpcRet{}, RaftBasicReply{reply: RaftReplyNoGroup}
			}
		}
		if leader.nodeId == o.n.raft.selfId {
			ret, r = o.__execParticipant(fn, txId, nodeList.version, !prepare, false)
		} else {
			ret.ext, r = fn.remote(o.n, leader.addr, nodeList.version, remoteTimeout)
		}
		ret.node = leader.toMsg()
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: __callAny (prepare=%v, %v), leader=%v, i=%v, l.version=%v, reply=%v", prepare, fn.name(), leader, i, nodeList.version, r.reply)
			if newLeader, found := fixupRpcErr(o.n, leader, r); found {
				leader = newLeader
			}
			if needRetry(r.reply) {
				continue
			}
		} else if i > 0 {
			o.n.raftGroup.UpdateLeader(leader)
		}
		return ret, r
	}
	if r.reply != RaftReplyOk {
		r.reply = ErrnoToReply(unix.ETIMEDOUT)
	}
	return ret, r
}

func (o *RpcMgr) CallAny(fn ParticipantOp, txId TxId, remoteTimeout time.Duration) (ret RpcRet, r RaftBasicReply) {
	return o.__callAny(fn, txId, remoteTimeout, false)
}

func (o *RpcMgr) CallPrepareAny(fn ParticipantOp, txId TxId, remoteTimeout time.Duration) (ret RpcRet, reply int32) {
	var r RaftBasicReply
	ret, r = o.__callAny(fn, txId, remoteTimeout, true)
	return ret, r.reply
}

func (o *RpcMgr) CallRpcAnyNoFail(fn ParticipantOp, txId TxId, remoteTimeout time.Duration) (ret RpcRet) {
	var nodeList *RaftNodeList = nil
	var leader RaftNode
	for i := 0; ; i++ {
		l := o.n.raftGroup.GetNodeListLocal()
		if nodeList == nil || nodeList.version != l.version {
			nodeList = l
			var ok bool
			leader, ok = fn.GetLeader(o.n, nodeList)
			if !ok {
				log.Errorf("BUG: CallRpcAnyNoFail (%v), GetLeader", fn.name())
				return
			}
		}
		var r RaftBasicReply
		if leader.nodeId == o.n.raft.selfId {
			ret, r = o.ExecCommitAbort(fn, txId, nodeList.version)
		} else {
			ret.ext, r = fn.remote(o.n, leader.addr, nodeList.version, remoteTimeout)
		}
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: CallRpcAnyNoFail (%v), local/remote, leader=%v, reply=%v", fn.name(), leader, r.reply)
			if newLeader, found := fixupRpcErr(o.n, leader, r); found {
				leader = newLeader
			}
			continue
		} else if i > 0 {
			o.n.raftGroup.UpdateLeader(leader)
		}
		return
	}
}

func (o *RpcMgr) AbortAll(nextTxId TxId, groups []string, txIds []TxId, retryInterval time.Duration) {
	for i := len(groups) - 1; i >= 0; i-- {
		txId := nextTxId.GetNext()
		nextTxId = txId
		o.CallRpcAnyNoFail(NewAbortParticipantOp(txId, []*common.TxIdMsg{txIds[i].toMsg()}, groups[i]), txId, retryInterval)
	}
}

func (o *RpcMgr) CallRemote(fn ParticipantOp, retryInterval time.Duration) bool {
	var nodeList *RaftNodeList = nil
	var leader RaftNode
	for i := 0; ; i++ {
		l := o.n.raftGroup.GetNodeListLocal()
		if nodeList == nil || nodeList.version != l.version {
			nodeList = l
			var ok bool
			leader, ok = fn.GetLeader(o.n, nodeList)
			if !ok {
				log.Errorf("BUG: CallRemote (%v), GetLeader", fn.name())
				return false
			}
		}
		if leader.nodeId != o.n.raft.selfId {
			_, r := fn.remote(o.n, leader.addr, nodeList.version, retryInterval)
			if r.reply != RaftReplyOk {
				log.Errorf("Failed: CallRemote (%v), leader=%v, retryInterval=%v reply=%v", fn.name(), leader, retryInterval, r.reply)
				if leader.groupId == "" { // for calling client nodes when a node joins/leaves
					return false
				}
				if newLeader, found := fixupRpcErr(o.n, leader, r); found {
					leader = newLeader
				}
				continue
			} else if i > 0 {
				o.n.raftGroup.UpdateLeader(leader)
			}
			return true
		}
		return false
	}
}

func (o *RpcMgr) CallCoordinatorMetaInRPC(fn CoordinatorOpBase, id CoordinatorId, nodeListVer uint64) (CoordinatorRet, RaftBasicReply) {
	begin := time.Now()
	r := o.n.raftGroup.BeginRaftRead(o.n.raft, nodeListVer)
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: CallCoordinatorMetaInRPC(%v): BeginRaftRead, reply=%v", fn.name(), r.reply)
		return CoordinatorRet{}, r
	}
	endRaftCheck := time.Now()
	ret, reply := fn.local(o.n, id, o.n.raftGroup.GetNodeListLocal())
	r.reply = reply
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: CallCoordinatorMetaInRPC(%v): local, reply=%v", fn.name(), reply)
		return CoordinatorRet{}, r
	}
	log.Debugf("CallCoordinatorMetaInRPC, raftCheck=%v, %v=%v",
		endRaftCheck.Sub(begin), fn.name(), time.Since(endRaftCheck))
	return ret, r
}

func (o *RpcMgr) CallCoordinatorUpdateNodeListInRPC(fn CoordinatorUpdateNodeListOp, id CoordinatorId, nodeListVer uint64) (*common.MembershipListMsg, RaftBasicReply) {
	begin := time.Now()
	r := o.n.raftGroup.BeginRaftRead(o.n.raft, nodeListVer)
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: CallCoordinatorUpdateNodeListInRPC: BeginRaftRead, reply=%v", r.reply)
		return nil, r
	}
	endRaftCheck := time.Now()
	ret, reply := fn.local(o.n, id, o.n.raftGroup.GetNodeListLocal())
	r.reply = reply
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: CallCoordinatorUpdateNodeListInRPC: local, reply=%v", reply)
		return nil, r
	}
	log.Debugf("CallCoordinatorUpdateNodeListInRPC, raftCheck=%v, %v=%v",
		endRaftCheck.Sub(begin), fn.name(), time.Since(endRaftCheck))
	return ret, r
}
