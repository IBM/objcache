/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"fmt"
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
	node   RaftNode
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
		//log.Debugf("RpcMgr.DeleteAndGet, seq=%v, ret=%v", seq, call)
		delete(o.calls, seq)
	}
	o.lock.Unlock()
	return
}

func (o *RpcMgr) DeleteAndUnlockLocalAll(tm *TxIdMgr) {
	unlocks := make([]func(*NodeServer), 0)
	o.lock.Lock()
	for _, seq := range tm.GetLocalTxIds(o.n.raftGroup.selfGroup) {
		call, ok := o.calls[seq]
		if ok {
			delete(o.calls, seq)
			if call.ret.unlock != nil {
				unlocks = append(unlocks, call.ret.unlock)
			}
			//log.Debugf("RpcMgr.DeleteAndUnlockLocalAll, seq=%v, ret=%v", seq, call)
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
	//log.Debugf("RpcMgr.Record, seq=%v, ret=%v", seq, ret)
	o.lock.Unlock()
}

//////////////////////////////////////////////////////////////////////////////////////////////////

func (o *RpcMgr) AbortAll(tm *TxIdMgr, retryInterval time.Duration, nodeLock bool) {
	for txId, groupId := range tm.GetTxGroupIds() {
		o.CallRpc(NewAbortParticipantOp(tm.GetNextId(), []*common.TxIdMsg{txId.toMsg()}, groupId, nodeLock).GetCaller(o.n))
	}
}

func (o *RpcMgr) CommitCoordinator(extLog CoordinatorCommand, retryInterval time.Duration) (reply int32) {
	if reply = o.n.raft.AppendExtendedLogEntry(extLog); reply != RaftReplyOk {
		log.Errorf("Failed: RpcMgr.CommitCoordinator, AppendExtendedLogEntry, reply=%v", reply)
		return reply
	}
	o.Commit(extLog, retryInterval)
	return
}

func (o *RpcMgr) Commit(extLog CoordinatorCommand, retryInterval time.Duration) (reply int32) {
	if extLog.NeedTwoPhaseCommit(o.n.raftGroup) {
		for _, fn := range extLog.RemoteCommit(o.n.raftGroup) {
			o.CallRpc(fn.GetCaller(o.n))
		}
		if _, _, reply = o.n.raft.AppendEntriesLocal(NewCommitCommand(extLog.GetTxId()), nil); reply != RaftReplyOk {
			log.Errorf("Failed: RpcMgr.Commit, AppendEntriesLocal, reply=%v", reply)
			// TODO: must not be here
		}
	}
	return
}

type TxIdMgr struct {
	txIdGroupIds map[TxId]string
	nextTxId     TxId
}

func NewTxIdMgr(txId TxId) *TxIdMgr {
	return &TxIdMgr{txIdGroupIds: make(map[TxId]string), nextTxId: txId}
}

func (t *TxIdMgr) GetCoordinatorTxId() TxId {
	return t.nextTxId.GetVariant(0)
}

func (t *TxIdMgr) GetNextId() TxId {
	t.nextTxId = t.nextTxId.GetNext()
	return t.nextTxId
}

func (t *TxIdMgr) GetMigrationId() MigrationId {
	return MigrationId{ClientId: t.nextTxId.ClientId, SeqNum: t.nextTxId.SeqNum}
}

func (t *TxIdMgr) AddTx(txId TxId, ret RpcRet) TxRet {
	t.txIdGroupIds[txId] = ret.node.groupId
	return TxRet{txId: txId, ret: ret}
}

func (t *TxIdMgr) GetTxGroupIds() map[TxId]string {
	return t.txIdGroupIds
}

func (t *TxIdMgr) GetLocalTxIds(selfGroup string) []TxId {
	ret := make([]TxId, 0)
	for txId, groupId := range t.txIdGroupIds {
		if groupId == selfGroup {
			ret = append(ret, txId)
		}
	}
	return ret
}

////////////////////////////////////////////////////////////////////////

func (o *RpcMgr) CallRpcOneShot(c RpcCaller, nodeList *RaftNodeList) (ret RpcRet, r RaftBasicReply) {
	leader, ok := c.GetLeader(o.n, nodeList)
	if !ok {
		log.Errorf("Failed: CallRpcOneShot (%v), GetLeader", c.name())
		return RpcRet{}, RaftBasicReply{reply: RaftReplyNoGroup}
	}
	if leader.nodeId == o.n.raft.selfId {
		ret, r = c.ExecLocal(o.n, nodeList.version)
	} else {
		ret, r = c.ExecRemote(o.n, leader.addr, nodeList.version)
	}
	if r.reply == RaftReplyOk {
		ret.node = leader
	}
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: CallRpcOneShot (%v), leader=%v, l.version=%v, reply=%v", c.name(), leader, nodeList.version, r.reply)
		if newLeader, found := fixupRpcErr(o.n, leader, r); found {
			leader = newLeader
		}
	}
	return
}
func (o *RpcMgr) CallRpc(c RpcCaller) (ret RpcRet, r RaftBasicReply) {
	var nodeList *RaftNodeList = nil
	var leader RaftNode
	for i := 0; c.TryNext(o.n, i); i++ {
		l := o.n.raftGroup.GetNodeListLocal()
		if nodeList == nil || nodeList.version != l.version {
			nodeList = l
			var ok bool
			leader, ok = c.GetLeader(o.n, nodeList)
			if !ok {
				log.Errorf("Failed: CallRpc (%v), GetLeader, i=%v", c.name(), i)
				return RpcRet{}, RaftBasicReply{reply: RaftReplyNoGroup}
			}
		}
		if leader.nodeId == o.n.raft.selfId {
			ret, r = c.ExecLocal(o.n, nodeList.version)
		} else {
			ret, r = c.ExecRemote(o.n, leader.addr, nodeList.version)
		}
		if r.reply == RaftReplyOk {
			ret.node = leader
		}
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: CallRpc (%v), leader=%v, i=%v, l.version=%v, reply=%v", c.name(), leader, i, nodeList.version, r.reply)
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

type RpcCaller interface {
	name() string
	GetLeader(n *NodeServer, nodeList *RaftNodeList) (RaftNode, bool)
	ExecLocal(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply)
	ExecLocalInRpc(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply)
	ExecRemote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64) (ret RpcRet, r RaftBasicReply)
	TryNext(n *NodeServer, i int) bool
}

type SingleShotRpcCaller struct {
	fn            SingleShotOp
	remoteTimeout time.Duration
	noTryBeginOp  bool
}

func NewSingleShotRpcCaller(fn SingleShotOp, remoteTimeout time.Duration, noTryBeginOp bool) SingleShotRpcCaller {
	return SingleShotRpcCaller{fn, remoteTimeout, noTryBeginOp}
}

func (c SingleShotRpcCaller) name() string {
	return fmt.Sprintf("SingleShotRpcCaller(%s)", c.fn.name())
}

func (c SingleShotRpcCaller) GetLeader(n *NodeServer, nodeList *RaftNodeList) (RaftNode, bool) {
	return c.fn.GetLeader(n, nodeList)
}

func (c SingleShotRpcCaller) ExecLocal(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	if !c.noTryBeginOp {
		if !n.TryBeginOp() {
			log.Errorf("SingleShotRpcCaller.ExecLocal (%v): node is locked", c.fn.name())
			r.reply = ObjCacheReplySuspending
			return RpcRet{}, r
		}
		defer n.EndOp()
	}

	if nodeListVer != uint64(math.MaxUint64) {
		r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer)
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: SingleShotRpcCaller.ExecLocalInRpc (%v): BeginRaftRead, r=%v", c.fn.name(), r)
			return RpcRet{}, r
		}
	} else {
		r = n.raft.SyncBeforeClientQuery()
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: SingleShotRpcCaller.ExecLocalInRpc (%v): SyncBeforeClientQuery, r=%v", c.fn.name(), r)
			return RpcRet{}, r
		}
	}
	var unlock func(*NodeServer)
	ret.ext, unlock, r.reply = c.fn.local(n)
	ret.node = n.selfNode
	if unlock != nil {
		unlock(n)
	}
	return
}

func (c SingleShotRpcCaller) ExecLocalInRpc(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	// do not check duplicated requests
	return c.ExecLocal(n, nodeListVer)
}

func (c SingleShotRpcCaller) ExecRemote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	ret.ext, r = c.fn.remote(n, addr, nodeListVer, c.remoteTimeout)
	return
}

func (c SingleShotRpcCaller) TryNext(n *NodeServer, i int) bool {
	return i < n.flags.MaxRetry
}

type CoordinatorRpcCaller struct {
	fn       CoordinatorOpBase
	nodeLock bool
}

func NewCoordinatorRpcCaller(fn CoordinatorOpBase, nodeLock bool) CoordinatorRpcCaller {
	return CoordinatorRpcCaller{fn, nodeLock}
}

func (c CoordinatorRpcCaller) name() string {
	return fmt.Sprintf("CoordinatorRpcCaller(%s)", c.fn.name())
}

func (c CoordinatorRpcCaller) GetLeader(n *NodeServer, nodeList *RaftNodeList) (RaftNode, bool) {
	return c.fn.GetLeader(n, nodeList)
}

func (c CoordinatorRpcCaller) ExecLocal(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	if c.nodeLock {
		if !n.TryLockNode() {
			log.Errorf("CoordinatorRpcCaller.ExecLocal(%v): some operations are on-going. retry", c.fn.name())
			r.reply = ObjCacheReplySuspending
			return RpcRet{}, r
		}
		defer n.UnlockNode()

		r = n.raft.SyncBeforeClientQuery()
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: CoordinatorRpcCaller.ExecLocalInRpc (%v): SyncBeforeClientQuery, r=%v", c.fn.name(), r)
			return RpcRet{}, r
		}
	} else {
		if !n.TryBeginOp() {
			log.Errorf("CoordinatorRpcCaller.ExecLocal(%v): node is locked", c.fn.name())
			r.reply = ObjCacheReplySuspending
			return RpcRet{}, r
		}
		defer n.EndOp()

		r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer)
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: CoordinatorRpcCaller.ExecLocal(%v): BeginRaftRead, reply=%v", c.fn.name(), r.reply)
			return RpcRet{}, r
		}
	}
	tm := NewTxIdMgr(c.fn.GetTxId())
	ret.ext, r.reply = c.fn.local(n, tm, n.raftGroup.GetNodeListLocal())
	if r.reply != RaftReplyOk {
		n.rpcMgr.AbortAll(tm, n.flags.CoordinatorTimeoutDuration, c.nodeLock)
		log.Errorf("Failed: CoordinatorRpcCaller.ExecLocal(%v): local, reply=%v", c.fn.name(), r.reply)
		return RpcRet{}, r
	}
	n.rpcMgr.DeleteAndUnlockLocalAll(tm)
	return
}

func (c CoordinatorRpcCaller) ExecLocalInRpc(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	seq := c.fn.GetTxId()
	old, duplicated, passed := n.rpcMgr.Enter(seq)
	if duplicated {
		if !passed {
			log.Warnf("CoordinatorRpcCaller.ExecLocalInRpc(%v), duplicated request. another process is executing, seq=%v", c.fn.name(), seq)
			r.reply = RaftReplyRetry
			return RpcRet{}, r
		}
		// should not be here
		log.Warnf("CoordinatorRpcCaller.ExecLocalInRpc(%v), duplicated request. return old result, seq=%v", c.fn.name(), seq)
		return old.ret, r
	}
	ret, r = c.ExecLocal(n, nodeListVer)
	n.rpcMgr.DeleteAndGet(seq)
	return
}

func (c CoordinatorRpcCaller) ExecRemote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	ret.ext, r = c.fn.remote(n, addr, nodeListVer)
	return
}

func (c CoordinatorRpcCaller) TryNext(n *NodeServer, i int) bool {
	return i < n.flags.MaxRetry
}

type UpadteChunkRpcCaller struct {
	fn            ParticipantOp
	remoteTimeout time.Duration
}

func NewUpadteChunkRpcCaller(fn ParticipantOp, remoteTimeout time.Duration) UpadteChunkRpcCaller {
	return UpadteChunkRpcCaller{fn, remoteTimeout}
}

func (c UpadteChunkRpcCaller) name() string {
	return fmt.Sprintf("UpadteChunkRpcCaller(%s)", c.fn.name())
}

func (c UpadteChunkRpcCaller) GetLeader(n *NodeServer, nodeList *RaftNodeList) (RaftNode, bool) {
	return c.fn.GetLeader(n, nodeList)
}

func (c UpadteChunkRpcCaller) ExecLocal(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	//TOFIX: UpdateChunk reuses the logic for request duplication detection for special commit protocol.
	// This is too complex.
	if !n.TryBeginOp() {
		log.Errorf("UpadteChunkRpcCaller.ExecLocal(%v): node is locked", c.fn.name())
		r.reply = ObjCacheReplySuspending
		return RpcRet{}, r
	}
	defer n.EndOp()
	r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer)
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: UpadteChunkRpcCaller.ExecLocal (%v): BeginRaftRead, r=%v", c.fn.name(), r)
		return RpcRet{}, r
	}

	ret.ext, ret.unlock, r.reply = c.fn.local(n)
	ret.node = n.selfNode
	if r.reply == RaftReplyOk {
		n.rpcMgr.Record(c.fn.GetTxId(), ret) // Delete at CommitUpdateChunk
	}
	return
}

func (c UpadteChunkRpcCaller) ExecLocalInRpc(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	seq := c.fn.GetTxId()
	old, duplicated, passed := n.rpcMgr.Enter(seq)
	if duplicated {
		if !passed {
			log.Warnf("UpadteChunkRpcCaller.ExecLocalInRpc (%v), duplicated request. another process is executing, seq=%v", c.fn.name(), seq)
			r.reply = RaftReplyRetry
			return RpcRet{}, r
		}
		log.Warnf("UpadteChunkRpcCaller.ExecLocalInRpc (%v), duplicated request. return old result, seq=%v", c.fn.name(), seq)
		return old.ret, r
	}
	ret, r = c.ExecLocal(n, nodeListVer)
	if r.reply != RaftReplyOk {
		n.rpcMgr.DeleteAndGet(seq)
	}
	return
}

func (c UpadteChunkRpcCaller) ExecRemote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	ret.ext, r = c.fn.remote(n, addr, nodeListVer, c.remoteTimeout)
	return
}

func (c UpadteChunkRpcCaller) TryNext(n *NodeServer, i int) bool {
	return i < n.flags.MaxRetry
}

type PrepareRpcCaller struct {
	fn            ParticipantOp
	remoteTimeout time.Duration
	nodeLock      bool
}

func NewPrepareRpcCaller(fn ParticipantOp, remoteTimeout time.Duration, nodeLock bool) PrepareRpcCaller {
	return PrepareRpcCaller{fn, remoteTimeout, nodeLock}
}

func (c PrepareRpcCaller) name() string {
	return fmt.Sprintf("PrepareRpcCaller(%s)", c.fn.name())
}

func (c PrepareRpcCaller) GetLeader(n *NodeServer, nodeList *RaftNodeList) (RaftNode, bool) {
	return c.fn.GetLeader(n, nodeList)
}

func (c PrepareRpcCaller) ExecLocal(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	// Coordinator or ExecLocalInRpc already synced the Raft log and took the node lock. we just call local() here.
	ret.ext, ret.unlock, r.reply = c.fn.local(n)
	ret.node = n.selfNode
	// skip writeLog() since Coordinator will aggregate it.
	if r.reply == RaftReplyOk {
		n.rpcMgr.Record(c.fn.GetTxId(), ret) // Delete at CommitAbortRpc
	}
	return
}

func (c PrepareRpcCaller) ExecLocalInRpc(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	seq := c.fn.GetTxId()
	old, duplicated, passed := n.rpcMgr.Enter(seq)
	if duplicated {
		if !passed {
			log.Warnf("PrepareRpcCaller.ExecLocalInRpc (%v), duplicated request. another process is executing, seq=%v", c.fn.name(), seq)
			r.reply = RaftReplyRetry
			return RpcRet{}, r
		}
		log.Warnf("PrepareRpcCaller.ExecLocalInRpc (%v), duplicated request. return old result, seq=%v", c.fn.name(), seq)
		return old.ret, r
	}
	defer func() {
		if r.reply == RaftReplyOk {
			n.rpcMgr.Record(seq, ret) // Delete at CommitAbortRpc
		} else {
			n.rpcMgr.DeleteAndGet(seq)
		}
	}()

	if c.nodeLock {
		n.LockNode()
		//n.UnlockNode() will be called by CommitAbortRpc. this blocks node list updates during a transaction
		r = n.raft.SyncBeforeClientQuery()
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: PrepareRpcCaller.ExecLocalInRpc (%v): SyncBeforeClientQuery, r=%v", c.fn.name(), r)
			return RpcRet{}, r
		}
	} else {
		if !n.TryBeginOp() {
			log.Errorf("PrepareRpcCaller.ExecLocalInRpc(%v): node is locked", c.fn.name())
			r.reply = ObjCacheReplySuspending
			return RpcRet{}, r
		}
		//n.EndOp() will be called by CommitAbortRpc. this blocks node list updates during a transaction
		r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer)
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: PrepareRpcCaller.ExecLocalInRpc (%v): BeginRaftRead, r=%v", c.fn.name(), r)
			return RpcRet{}, r
		}
	}
	ret.ext, ret.unlock, r.reply = c.fn.local(n)
	ret.node = n.selfNode
	if r.reply == RaftReplyOk {
		r.reply = c.fn.writeLog(n, ret.ext)
	}
	return
}

func (c PrepareRpcCaller) ExecRemote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	ret.ext, r = c.fn.remote(n, addr, nodeListVer, c.remoteTimeout)
	return
}

func (c PrepareRpcCaller) TryNext(n *NodeServer, i int) bool {
	return i < n.flags.MaxRetry
}

type CommitAbortRpcCaller struct {
	fn            ParticipantOp
	remoteTimeout time.Duration
	nodeLock      bool
}

func NewCommitAbortRpcCaller(fn ParticipantOp, remoteTimeout time.Duration, nodeLock bool) CommitAbortRpcCaller {
	return CommitAbortRpcCaller{fn, remoteTimeout, nodeLock}
}

func (c CommitAbortRpcCaller) name() string {
	return fmt.Sprintf("CommitAbortRpcCaller(%s)", c.fn.name())
}

func (c CommitAbortRpcCaller) GetLeader(n *NodeServer, nodeList *RaftNodeList) (RaftNode, bool) {
	return c.fn.GetLeader(n, nodeList)
}

func (c CommitAbortRpcCaller) ExecLocal(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	// we always need to call writeLog()
	if c.nodeLock {
		r = n.raft.SyncBeforeClientQuery()
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: CommitAbortRpcCaller.ExecLocal (%v): SyncBeforeClientQuery, r=%v", c.fn.name(), r)
			return RpcRet{}, r
		}
	} else {
		r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer)
		if r.reply != RaftReplyOk {
			log.Errorf("Failed: CommitAbortRpcCaller.ExecLocal (%v): BeginRaftRead, r=%v", c.fn.name(), r)
			return RpcRet{}, r
		}
	}
	ret.ext, ret.unlock, r.reply = c.fn.local(n)
	ret.node = n.selfNode
	if r.reply == RaftReplyOk {
		r.reply = c.fn.writeLog(n, ret.ext)
	}
	if ret.unlock != nil {
		ret.unlock(n)
	}
	// Release the node lock at prepare
	if r.reply == RaftReplyOk {
		if c.nodeLock {
			n.UnlockNode()
		} else {
			n.EndOp()
		}
	}
	return
}

func (c CommitAbortRpcCaller) ExecLocalInRpc(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	seq := c.fn.GetTxId()
	old, duplicated, passed := n.rpcMgr.Enter(seq)
	if duplicated {
		if !passed {
			log.Warnf("CommitAbortRpcCaller.ExecLocalInRpc (%v), duplicated request. another process is executing, seq=%v", c.fn.name(), seq)
			r.reply = RaftReplyRetry
			return RpcRet{}, r
		}
		log.Warnf("CommitAbortRpcCaller.ExecLocalInRpc (%v), duplicated request. return old result, seq=%v", c.fn.name(), seq)
		return old.ret, r
	}
	ret, r = c.ExecLocal(n, nodeListVer)
	n.rpcMgr.DeleteAndGet(seq)
	return
}

func (c CommitAbortRpcCaller) ExecRemote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	ret.ext, r = c.fn.remote(n, addr, nodeListVer, c.remoteTimeout)
	return
}

func (c CommitAbortRpcCaller) TryNext(n *NodeServer, i int) bool {
	return true
}

type CommitChunkRpcCaller struct {
	fn            ParticipantOp
	remoteTimeout time.Duration
}

func NewCommitChunkRpcCaller(fn ParticipantOp, remoteTimeout time.Duration) CommitChunkRpcCaller {
	return CommitChunkRpcCaller{fn, remoteTimeout}
}

func (c CommitChunkRpcCaller) name() string {
	return fmt.Sprintf("CommitChunkRpcCaller(%s)", c.fn.name())
}

func (c CommitChunkRpcCaller) GetLeader(n *NodeServer, nodeList *RaftNodeList) (RaftNode, bool) {
	return c.fn.GetLeader(n, nodeList)
}

func (c CommitChunkRpcCaller) ExecLocal(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	// UpdateChunkRpcCaller releases the node lock unlike PrepareRpcCaller.
	// CommitChunkRpcCaller separately acquires/releases the node lock
	if !n.TryBeginOp() {
		log.Errorf("CommitChunkRpcCaller.ExecLocal(%v): node is locked", c.fn.name())
		r.reply = ObjCacheReplySuspending
		return RpcRet{}, r
	}
	defer n.EndOp()
	r = n.raftGroup.BeginRaftRead(n.raft, nodeListVer)
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: CommitChunkRpcCaller.ExecLocal (%v): BeginRaftRead, r=%v", c.fn.name(), r)
		return RpcRet{}, r
	}
	ret.ext, ret.unlock, r.reply = c.fn.local(n)
	ret.node = n.selfNode
	if r.reply == RaftReplyOk {
		r.reply = c.fn.writeLog(n, ret.ext)
	}
	if ret.unlock != nil {
		ret.unlock(n)
	}
	return
}

func (c CommitChunkRpcCaller) ExecLocalInRpc(n *NodeServer, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	seq := c.fn.GetTxId()
	old, duplicated, passed := n.rpcMgr.Enter(seq)
	if duplicated {
		if !passed {
			log.Warnf("CommitChunkRpcCaller.ExecLocalInRpc (%v), duplicated request. another process is executing, seq=%v", c.fn.name(), seq)
			r.reply = RaftReplyRetry
			return RpcRet{}, r
		}
		log.Warnf("CommitChunkRpcCaller.ExecLocalInRpc (%v), duplicated request. return old result, seq=%v", c.fn.name(), seq)
		return old.ret, r
	}
	ret, r = c.ExecLocal(n, nodeListVer)
	n.rpcMgr.DeleteAndGet(seq)
	return
}

func (c CommitChunkRpcCaller) ExecRemote(n *NodeServer, addr common.NodeAddrInet4, nodeListVer uint64) (ret RpcRet, r RaftBasicReply) {
	ret.ext, r = c.fn.remote(n, addr, nodeListVer, c.remoteTimeout)
	return
}

func (c CommitChunkRpcCaller) TryNext(n *NodeServer, i int) bool {
	return true
}
