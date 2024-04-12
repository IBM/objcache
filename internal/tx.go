/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/takeshi-yoshimura/fuse"
	"google.golang.org/protobuf/proto"

	"github.com/IBM/objcache/common"
)

type TxId struct {
	ClientId uint32
	SeqNum   uint32
	TxSeqNum uint64
}

func NewTxIdFromMsg(msg *common.TxIdMsg) TxId {
	return TxId{ClientId: msg.GetClientId(), SeqNum: msg.GetSeqNum(), TxSeqNum: msg.GetTxSeqNum()}
}

func (t *TxId) toMsg() *common.TxIdMsg {
	return &common.TxIdMsg{ClientId: t.ClientId, SeqNum: t.SeqNum, TxSeqNum: t.TxSeqNum}
}

func (t *TxId) GetNext() TxId {
	return TxId{ClientId: t.ClientId, SeqNum: t.SeqNum, TxSeqNum: t.TxSeqNum + 1}
}

func (t *TxId) GetVariant(TxSeqNum uint64) TxId {
	return TxId{ClientId: t.ClientId, SeqNum: t.SeqNum, TxSeqNum: TxSeqNum}
}

type Tx struct {
	txTime  time.Time
	txType  int
	txId    TxId
	newData interface{}
}

func (p Tx) Less(b btree.Item) bool {
	return p.txTime.Before(b.(Tx).txTime)
}

const (
	TxUpdateMeta       = 0
	TxUpdateChunk      = 1
	TxDeleteChunk      = 2
	TxAddNodes         = 3
	TxRemoveNodes      = 4
	TxUpdateMetaKey    = 5
	TxDeleteMeta       = 6
	TxCreateMeta       = 7
	TxUpdateParentMeta = 8

	TxUpdateCoordinator         = 30
	TxDeleteCoordinator         = 31
	TxBeginPersistCoordinator   = 32
	TxPersistCoordinator        = 33
	TxUpdateNodeListCoordinator = 34
	TxCreateCoordinator         = 35
	TxRenameCoordinator         = 36
	TxTruncateCoordinator       = 37
)

/////////////////////////////////////////////////////////////////////////////////

type TxMgr struct {
	activeTx map[TxId]Tx
	lock     *sync.RWMutex
}

func NewTxMgr() *TxMgr {
	ret := &TxMgr{
		activeTx: make(map[TxId]Tx),
		lock:     new(sync.RWMutex),
	}
	return ret
}

func (m *TxMgr) getTx(txId TxId) (Tx, error) {
	m.lock.RLock()
	tx, ok := m.activeTx[txId]
	m.lock.RUnlock()
	if !ok {
		return Tx{}, fuse.ENOENT
	}
	return tx, nil
}

func (m *TxMgr) registerTx(newData interface{}, txType int, txId TxId) {
	var newTx = Tx{newData: newData, txId: txId, txType: txType}
	m.lock.Lock()
	m.activeTx[txId] = newTx
	m.lock.Unlock()
	log.Debugf("Success: activateTx, txId=%v", txId)
}

func (m *TxMgr) deactivateTx(txId TxId) bool {
	m.lock.Lock()
	_, ok := m.activeTx[txId]
	if !ok {
		m.lock.Unlock()
		log.Warnf("Failed: deactivateTx, no txId found, txId=%v", txId)
		return false
	}
	delete(m.activeTx, txId)
	m.lock.Unlock()
	log.Debugf("Success: deactivateTx, txId=%v", txId)
	return true
}

func (m *TxMgr) ApplyAsUpdateNodeList(extBuf []byte) int32 {
	record := &common.UpdateNodeListMsg{}
	if err := proto.Unmarshal(extBuf, record); err != nil {
		log.Errorf("Failed: ApplyAsUpdateNodeList, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txType := TxAddNodes
	if !record.IsAdd {
		txType = TxRemoveNodes
	}
	txId := NewTxIdFromMsg(record.GetTxId())
	m.registerTx(record, txType, txId)
	log.Debugf("Success: ApplyAsUpdateNodeList, txId=%v", txId)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsUpdateNodeListCoordinator(extBuf []byte) int32 {
	record := &common.TwoPCNodeListCommitRecordMsg{}
	if err := proto.Unmarshal(extBuf, record); err != nil {
		log.Errorf("Failed: ApplyAsUpdateNodeListCoordinator, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	m.registerTx(record, TxUpdateNodeListCoordinator, NewTxIdFromMsg(record.TxId))
	log.Debugf("Success: ApplyAsUpdateNodeListCoordinator, txId=%v", record.TxId)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsUpdateMeta(extBuf []byte) (reply int32) {
	msg := &common.UpdateMetaMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsUpdateMeta, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	m.registerTx(msg, TxUpdateMeta, txId)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsUpdateParentMeta(extBuf []byte) (reply int32) {
	msg := &common.UpdateParentMetaMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsUpdateParentMeta, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	m.registerTx(msg, TxUpdateParentMeta, txId)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsCreateMeta(extBuf []byte) (reply int32) {
	msg := &common.CreateMetaMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsCreateMeta, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	m.registerTx(msg, TxCreateMeta, txId)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsDeleteMeta(extBuf []byte) (reply int32) {
	msg := &common.DeleteMetaMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsDeleteMeta, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	m.registerTx(msg, TxDeleteMeta, txId)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsUpdateMetaKey(extBuf []byte) (reply int32) {
	msg := &common.UpdateMetaKeyMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsUpdateMetaKey, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	m.registerTx(msg, TxUpdateMetaKey, txId)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsBeginPersist(extBuf []byte) int32 {
	l := &common.MpuBeginMsg{}
	if err := proto.Unmarshal(extBuf, l); err != nil {
		log.Errorf("Failed: ApplyAsBeginPersist, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	m.registerTx(l, TxBeginPersistCoordinator, NewTxIdFromMsg(l.GetTxId()))
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsUpdateMetaCoordinator(extBuf []byte, inodeMgr *InodeMgr, dirtyMgr *DirtyMgr, raftGroup *RaftGroupMgr) (reply int32) {
	begin := time.Now()
	msg := &common.TwoPCCommitRecordMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsUpdateMetaCoordinator, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	l := NewTwoPCCommitRecordFromMsg(msg)
	unmarshalTime := time.Now()
	if _, err := m.getTx(l.txId); err == nil {
		log.Infof("Duplicated: ApplyAsUpdateMetaCoordinator, txId=%v, meta=%v", l.txId, *l.primaryMeta)
		return RaftReplyOk // return early without commits to stop duplicated commits. readers cannot observe this writes until the ongoing commit completes
	}
	m.registerTx(l, l.txType, l.txId)
	var quickCommit = l.CountRemote(raftGroup) == 0
	if quickCommit {
		m.ApplyAsCommitTx(inodeMgr, dirtyMgr, raftGroup, l.txId.ClientId, l.txId.SeqNum, l.txId.TxSeqNum)
	}
	log.Debugf("Success: ApplyAsUpdateMetaCoordinator, txId=%v, unmarshalTime=%v, commitTime=%v (quickCommit=%v)", l.txId, unmarshalTime.Sub(begin), time.Since(unmarshalTime), quickCommit)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsCreateMetaCoordinator(extBuf []byte, inodeMgr *InodeMgr, dirtyMgr *DirtyMgr, raftGroup *RaftGroupMgr) (reply int32) {
	begin := time.Now()
	msg := &common.TwoPCCreateMetaCommitRecordMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsCreateMetaCoordinator, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	unmarshalTime := time.Now()
	if _, err := m.getTx(txId); err == nil {
		log.Infof("Duplicated: ApplyAsCreateMetaCoordinator, txId=%v", txId)
		return RaftReplyOk // return early without commits to stop duplicated commits. readers cannot observe this writes until the ongoing commit completes
	}
	m.registerTx(msg, TxCreateCoordinator, txId)
	var quickCommit = msg.GetRemoteParent() == nil && msg.GetRemoteMeta() == nil
	if quickCommit {
		m.ApplyAsCommitTx(inodeMgr, dirtyMgr, raftGroup, txId.ClientId, txId.SeqNum, txId.TxSeqNum)
	}
	log.Debugf("Success: ApplyAsCreateMetaCoordinator, txId=%v, unmarshalTime=%v, commitTime=%v (quickCommit=%v)", txId, unmarshalTime.Sub(begin), time.Since(unmarshalTime), quickCommit)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsDeleteMetaCoordinator(extBuf []byte, inodeMgr *InodeMgr, dirtyMgr *DirtyMgr, raftGroup *RaftGroupMgr) (reply int32) {
	begin := time.Now()
	msg := &common.TwoPCDeleteMetaCommitRecordMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsDeleteMetaCoordinator, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	unmarshalTime := time.Now()
	if _, err := m.getTx(txId); err == nil {
		log.Infof("Duplicated: ApplyAsDeleteMetaCoordinator, txId=%v", txId)
		return RaftReplyOk // return early without commits to stop duplicated commits. readers cannot observe this writes until the ongoing commit completes
	}
	m.registerTx(msg, TxDeleteCoordinator, txId)
	var quickCommit = msg.GetRemoteParent() == nil && msg.GetRemoteMeta() == nil
	if quickCommit {
		m.ApplyAsCommitTx(inodeMgr, dirtyMgr, raftGroup, txId.ClientId, txId.SeqNum, txId.TxSeqNum)
	}
	log.Debugf("Success: ApplyAsDeleteMetaCoordinator, txId=%v, unmarshalTime=%v, commitTime=%v (quickCommit=%v)", txId, unmarshalTime.Sub(begin), time.Since(unmarshalTime), quickCommit)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsRenameCoordinator(extBuf []byte, inodeMgr *InodeMgr, dirtyMgr *DirtyMgr, raftGroup *RaftGroupMgr) (reply int32) {
	begin := time.Now()
	msg := &common.TwoPCRenameCommitRecordMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: ApplyAsRenameCoordinator, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	unmarshalTime := time.Now()
	if _, err := m.getTx(txId); err == nil {
		log.Infof("Duplicated: ApplyAsRenameCoordinator, txId=%v", txId)
		return RaftReplyOk // return early without commits to stop duplicated commits. readers cannot observe this writes until the ongoing commit completes
	}
	m.registerTx(msg, TxRenameCoordinator, txId)
	quickCommit := len(msg.GetRemoteOps()) == 0
	if quickCommit {
		m.ApplyAsCommitTx(inodeMgr, dirtyMgr, raftGroup, txId.ClientId, txId.SeqNum, txId.TxSeqNum)
	}
	log.Debugf("Success: ApplyAsRenameCoordinator, txId=%v, unmarshalTime=%v, commitTime=%v (quickCommit=%v)", txId, unmarshalTime.Sub(begin), time.Since(unmarshalTime), quickCommit)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsCommitTx(inodeMgr *InodeMgr, dirtyMgr *DirtyMgr, raftGroup *RaftGroupMgr, clientId uint32, seqNum uint32, txSeqNum uint64) int32 {
	txId := TxId{ClientId: clientId, SeqNum: seqNum, TxSeqNum: txSeqNum}
	tx, err := m.getTx(txId)
	if err != nil {
		log.Errorf("Failed: RaftCommitTxExtCommand.Apply, txId=%v does not exist. aborted?", txId)
		return RaftReplyOk
	}
	m.deactivateTx(txId)
	switch tx.txType {
	case TxUpdateMeta:
		ops := tx.newData.(*common.UpdateMetaMsg)
		inodeMgr.CommitUpdateMeta(NewWorkingMetaFromMsg(ops.GetMeta()), dirtyMgr)
	case TxUpdateParentMeta:
		ops := tx.newData.(*common.UpdateParentMetaMsg)
		inodeMgr.CommitUpdateParentMeta(NewWorkingMetaFromMsg(ops.GetMeta()), ops.GetNewKey(), dirtyMgr)
	case TxUpdateMetaKey:
		ops := tx.newData.(*common.UpdateMetaKeyMsg)
		inodeMgr.CommitUpdateMetaKey(NewWorkingMetaFromMsg(ops.GetMeta()), ops.GetOldKey(), ops.GetNewKey(), dirtyMgr)
	case TxCreateMeta:
		ops := tx.newData.(*common.CreateMetaMsg)
		inodeMgr.CommitCreateMeta(NewWorkingMetaFromMsg(ops.GetMeta()), ops.GetNewKey(), dirtyMgr)
	case TxDeleteMeta:
		ops := tx.newData.(*common.DeleteMetaMsg)
		inodeMgr.CommitDeleteMeta(NewWorkingMetaFromMsg(ops.GetMeta()), ops.GetKey(), dirtyMgr)
	case TxUpdateCoordinator:
		ops := tx.newData.(*TwoPCCommitRecord)
		inodeMgr.QuickCommitUpdateChunk(ops.primaryMeta, raftGroup.selfGroup, ops.chunks, dirtyMgr)
		for _, lMeta := range ops.localMetas {
			inodeMgr.CommitUpdateMeta(NewWorkingMetaFromMsg(lMeta), dirtyMgr)
		}
		if ops.primaryLocal {
			inodeMgr.CommitUpdateMeta(ops.primaryMeta, dirtyMgr)
		}
	case TxTruncateCoordinator:
		ops := tx.newData.(*TwoPCCommitRecord)
		ring := raftGroup.GetNodeListLocal().ring
		inodeMgr.QuickCommitDeleteChunk(ring, raftGroup.selfGroup, ops.primaryMeta, dirtyMgr) //shrink
		inodeMgr.QuickCommitExpandChunk(ring, raftGroup.selfGroup, ops.primaryMeta, dirtyMgr) //expand
		for _, lMeta := range ops.localMetas {
			inodeMgr.CommitUpdateMeta(NewWorkingMetaFromMsg(lMeta), dirtyMgr)
		}
		if ops.primaryLocal {
			inodeMgr.CommitUpdateMeta(ops.primaryMeta, dirtyMgr)
		}
	case TxDeleteCoordinator:
		ops := tx.newData.(*common.TwoPCDeleteMetaCommitRecordMsg)
		meta := NewWorkingMetaFromMsg(ops.GetMeta())
		meta.prevVer = NewWorkingMetaFromMsg(ops.GetPrev())
		inodeMgr.QuickCommitDeleteChunk(raftGroup.GetNodeListLocal().ring, raftGroup.selfGroup, meta, dirtyMgr)
		if ops.GetRemoteParent() == nil {
			inodeMgr.CommitUpdateParentMeta(NewWorkingMetaFromMsg(ops.GetParent()), filepath.Dir(ops.GetKey()), dirtyMgr)
		}
		if ops.GetRemoteMeta() == nil {
			inodeMgr.CommitDeleteMeta(NewWorkingMetaFromMsg(ops.GetMeta()), ops.GetKey(), dirtyMgr)
		}
	case TxBeginPersistCoordinator:
		log.Errorf("BUG: RaftCommitTxExtCommand.Apply. ignore txType TxBeginPersistCoordinator, txType=%v, txId=%v", tx.txType, tx.txId)
	case TxPersistCoordinator:
		ops := tx.newData.(*common.TwoPCPersistRecordMsg)
		metaKey := ops.GetMetaKeys()[0]
		for _, chunk := range ops.GetPrimaryMeta().GetChunks() {
			if chunk.GetGroupId() == raftGroup.selfGroup {
				inodeMgr.CommitPersistChunk(InodeKeyType(ops.GetPrimaryMeta().GetInodeKey()), metaKey, chunk.GetOffsets(), chunk.GetCVers(), dirtyMgr)
			}
		}
		inodeMgr.CommitPersistMeta(InodeKeyType(ops.GetPrimaryMeta().GetInodeKey()), ops.GetPrimaryMeta().GetVersion(), ops.GetPrimaryMeta().GetTs(), dirtyMgr)
	case TxUpdateNodeListCoordinator:
		ops := tx.newData.(*common.TwoPCNodeListCommitRecordMsg)
		dirtyMgr.commitMigratedDataLocal(inodeMgr, NewMigrationIdFromMsg(ops.GetMigrationId()))
		if ops.IsAdd {
			raftGroup.Add(ops.GetTarget())
		} else {
			raftGroup.Remove(ops.GetTarget().GetNodeId(), ops.GetTarget().GetGroupId())
		}
	case TxCreateCoordinator:
		ops := tx.newData.(*common.TwoPCCreateMetaCommitRecordMsg)
		if ops.GetRemoteParent() == nil {
			inodeMgr.CommitUpdateParentMeta(NewWorkingMetaFromMsg(ops.GetParent()), filepath.Dir(ops.GetNewKey()), dirtyMgr)
		}
		if ops.GetRemoteMeta() == nil {
			inodeMgr.CommitCreateMeta(NewWorkingMetaFromMsg(ops.GetMeta()), ops.GetNewKey(), dirtyMgr)
		}
	case TxRenameCoordinator:
		ops := tx.newData.(*common.TwoPCRenameCommitRecordMsg)
		for _, commitMsg := range ops.GetLocalUpdateMetaKeys() {
			inodeMgr.CommitUpdateMetaKey(NewWorkingMetaFromMsg(commitMsg.GetMeta()), commitMsg.GetRemovedKey(), commitMsg.GetNewKey(), dirtyMgr)
		}
		for _, parentMsg := range ops.GetLocalUpdateParents() {
			inodeMgr.CommitUpdateParentMeta(NewWorkingMetaFromMsg(parentMsg.GetMeta()), parentMsg.GetKey(), dirtyMgr)
		}
	default:
		log.Errorf("Failed: RaftCommitTxExtCommand.Apply. ignore txType, txType=%v, txId=%v", tx.txType, tx.txId)
	}
	log.Debugf("Success: RaftCommitTxExtCommand.Apply, txId=%v, txType=%v", tx.txId, tx.txType)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsAbortTx(extBuf []byte) (reply int32) {
	args := &common.AbortCommitArgs{}
	if err := proto.Unmarshal(extBuf, args); err != nil {
		log.Errorf("Failed: ApplyAsAbortTx, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	for _, msg := range args.GetAbortTxIds() {
		txId := NewTxIdFromMsg(msg)
		tx, err := m.getTx(txId)
		if err != nil {
			continue
		}
		switch tx.txType {
		case TxUpdateChunk:
		case TxUpdateMeta:
		case TxUpdateMetaKey:
		case TxCreateMeta:
		case TxDeleteMeta:
		case TxUpdateCoordinator:
		case TxTruncateCoordinator:
		case TxCreateCoordinator:
		case TxDeleteCoordinator:
		case TxAddNodes:
		case TxRemoveNodes:
		case TxUpdateNodeListCoordinator:
		case TxBeginPersistCoordinator:
		case TxPersistCoordinator:
		}
		m.deactivateTx(txId)
	}
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsPersist(extBuf []byte, inodeMgr *InodeMgr, dirtyMgr *DirtyMgr, raftGroup *RaftGroupMgr) int32 {
	ops := &common.TwoPCPersistRecordMsg{}
	if err := proto.Unmarshal(extBuf, ops); err != nil {
		log.Errorf("Failed: ApplyAsPersist, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(ops.TxId)
	tx, err := m.getTx(txId)
	if err == nil && tx.txType != TxBeginPersistCoordinator {
		log.Infof("Duplicated: ApplyAsPersist, txId=%v", txId)
		return RaftReplyOk
	}
	m.registerTx(ops, TxPersistCoordinator, txId)
	var remote = false
	for _, chunk := range ops.GetPrimaryMeta().GetChunks() {
		if chunk.GetGroupId() != raftGroup.selfGroup {
			remote = true
			break
		}
	}
	if remote {
		log.Debugf("Success: ApplyAsPersist, txId=%v", txId)
		return RaftReplyOk
	}
	fetchKey := ops.GetMetaKeys()[0]
	for _, chunk := range ops.GetPrimaryMeta().GetChunks() {
		inodeMgr.CommitPersistChunk(InodeKeyType(ops.GetPrimaryMeta().GetInodeKey()), fetchKey, chunk.GetOffsets(), chunk.GetCVers(), dirtyMgr)
	}
	inodeMgr.CommitPersistMeta(InodeKeyType(ops.GetPrimaryMeta().GetInodeKey()), ops.GetPrimaryMeta().GetVersion(), ops.GetPrimaryMeta().GetTs(), dirtyMgr)
	m.deactivateTx(txId)
	log.Debugf("Success: ApplyAsPersist, txId=%v", txId)
	return RaftReplyOk
}

func (m *TxMgr) ApplyAsCommitMigration(extBuf []byte, inodeMgr *InodeMgr, dirtyMgr *DirtyMgr, groupMgr *RaftGroupMgr) int32 {
	l := &common.MigrationMsg{}
	if err := proto.Unmarshal(extBuf, l); err != nil {
		log.Errorf("Failed: ApplyAsCommitMigration, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	txId := NewTxIdFromMsg(l.GetTxId())
	tx, err := m.getTx(txId)
	if err != nil {
		log.Errorf("Failed: ApplyAsCommitMigration, txId=%v does not exist. aborted?", txId)
		return RaftReplyOk
	}
	m.deactivateTx(txId)
	ops := tx.newData.(*common.UpdateNodeListMsg)
	dirtyMgr.commitMigratedDataLocal(inodeMgr, NewMigrationIdFromMsg(ops.GetMigrationId()))
	if ops.GetIsAdd() {
		for _, node := range ops.GetNodes().GetServers() {
			groupMgr.Add(node)
			log.Debugf("Success: ApplyAsCommitMigration (Add Server), Node=%v", node)
		}
	} else {
		for _, node := range ops.GetNodes().GetServers() {
			groupMgr.Remove(node.GetNodeId(), node.GetGroupId())
			log.Debugf("Success: ApplyAsCommitMigration (Remove Server), Node=%v", node)
		}
	}
	ver := ops.GetNodeListVer()
	groupMgr.lock.Lock()
	if ver > 0 && groupMgr.nodeList != nil && groupMgr.nodeList.version != ver {
		groupMgr.nodeList.version = ver
		log.Debugf("Success: ApplyAsCommitMigration, overwrite nodeListVer=%v", ver)
	}
	groupMgr.lock.Unlock()
	return RaftReplyOk
}

func (n *NodeServer) Apply(l *AppendEntryCommand) (reply int32) {
	begin := time.Now()
	var readTime time.Duration
	extCmdId := l.GetExtCmdId()
	var extBuf []byte
	if extCmdId&DataCmdIdBit != 0 {
		fileId, fileLength, fileOffset := l.GetAsAppendEntryFile()
		//atomic.AddInt64(&n.raft.files.totalDiskUsage, int64(fileLength))
		if extCmdId != AppendEntryFillChunkCmdId && extCmdId != AppendEntryUpdateChunkCmdId {
			extBuf, reply = n.raft.files.OpenAndReadCache(fileId, fileOffset, fileLength)
			if reply != RaftReplyOk {
				log.Errorf("Failed: AppendEntryCommand.Apply, OpenAndReadCache, fileId=%v, fileOffset=%v, fileLength=%v", fileId, fileOffset, fileLength)
				return
			}
		}
		readTime = time.Since(begin)
	}
	applyBegin := time.Now()
	switch extCmdId {
	case AppendEntryNoOpCmdId:
		reply = RaftReplyOk
	case AppendEntryCreateChunkCmdId:
		reply = n.inodeMgr.ApplyAsCreateChunk(extBuf)
	case AppendEntryAddServerCmdId:
		serverId, ip, port := l.GetAsAddServer()
		n.raft.lock.Lock()
		if _, ok := n.raft.serverIds[serverId]; !ok {
			sa := common.NodeAddrInet4{Addr: ip, Port: int(port)}
			n.raft.serverIds[serverId] = sa
			n.raft.sas[sa] = serverId
			log.Debugf("Success: AppendEntryCommand.Apply (AddServer), serverId=%v, as=%v", serverId, sa)
		}
		n.raft.lock.Unlock()
		reply = RaftReplyOk
	case AppendEntryRemoveServerCmdId:
		serverId := l.GetAsRemoveServer()
		n.raft.lock.Lock()
		if sa, ok := n.raft.serverIds[serverId]; ok {
			delete(n.raft.serverIds, serverId)
			delete(n.raft.sas, sa)
			log.Debugf("Success: AppendEntryCommand.Apply (RemoveServer), serverId=%v", serverId)
		}
		n.raft.lock.Unlock()
		reply = RaftReplyOk
	case AppendEntryResetExtLogCmdId:
		fileId, nextSeqNum := l.GetAsResetExtLog()
		n.raft.SetExt(fileId, nextSeqNum)
		reply = RaftReplyOk
	case AppendEntryCommitTxCmdId:
		clientId, seqNum, txSeqNum := l.GetAsCommitTx()
		reply = n.txMgr.ApplyAsCommitTx(n.inodeMgr, n.dirtyMgr, n.raftGroup, clientId, seqNum, txSeqNum)
	case AppendEntryFillChunkCmdId:
		reply = RaftReplyOk
	case AppendEntryUpdateChunkCmdId:
		reply = RaftReplyOk
	case AppendEntryUpdateMetaCmdId:
		reply = n.txMgr.ApplyAsUpdateMeta(extBuf)
	case AppendEntryUpdateMetaCoordinatorCmdId:
		reply = n.txMgr.ApplyAsUpdateMetaCoordinator(extBuf, n.inodeMgr, n.dirtyMgr, n.raftGroup)
	case AppendEntryCommitChunkCmdId:
		reply = n.inodeMgr.ApplyAsCommitUpdateChunk(extBuf, n.dirtyMgr)
	case AppendEntryAbortTxCmdId:
		reply = n.txMgr.ApplyAsAbortTx(extBuf)
	case AppendEntryPersistChunkCmdId:
		reply = n.inodeMgr.ApplyAsPersistChunk(extBuf, n.dirtyMgr)
	case AppendEntryBeginPersistCmdId:
		reply = n.txMgr.ApplyAsBeginPersist(extBuf)
	case AppendEntryPersistCmdId:
		reply = n.txMgr.ApplyAsPersist(extBuf, n.inodeMgr, n.dirtyMgr, n.raftGroup)
	case AppendEntryUpdateNodeListCmdId:
		reply = n.txMgr.ApplyAsUpdateNodeList(extBuf)
	case AppendEntryUpdateNodeListCoordinatorCmdId:
		reply = n.txMgr.ApplyAsUpdateNodeListCoordinator(extBuf)
	case AppendEntryUpdateNodeListLocalCmdId:
		reply = n.raftGroup.ApplyAsUpdateNodeListLocal(extBuf)
	case AppendEntryCommitMigrationCmdId:
		reply = n.txMgr.ApplyAsCommitMigration(extBuf, n.inodeMgr, n.dirtyMgr, n.raftGroup)
	case AppendEntryCreateMetaCoordinatorCmdId:
		reply = n.txMgr.ApplyAsCreateMetaCoordinator(extBuf, n.inodeMgr, n.dirtyMgr, n.raftGroup)
	case AppendEntryUpdateMetaKeyCmdId:
		reply = n.txMgr.ApplyAsUpdateMetaKey(extBuf)
	case AppendEntryRenameCoordinatorCmdId:
		reply = n.txMgr.ApplyAsRenameCoordinator(extBuf, n.inodeMgr, n.dirtyMgr, n.raftGroup)
	case AppendEntryCreateMetaCmdId:
		reply = n.txMgr.ApplyAsCreateMeta(extBuf)
	case AppendEntryDeleteMetaCmdId:
		reply = n.txMgr.ApplyAsDeleteMeta(extBuf)
	case AppendEntryDeleteMetaCoordinatorCmdId:
		reply = n.txMgr.ApplyAsDeleteMetaCoordinator(extBuf, n.inodeMgr, n.dirtyMgr, n.raftGroup)
	case AppendEntryDeletePersistCmdId:
		reply = n.inodeMgr.ApplyAsDeletePersist(extBuf, n.dirtyMgr)
	case AppendEntryUpdateParentMetaCmdId:
		reply = n.txMgr.ApplyAsUpdateParentMeta(extBuf)
	case AppendEntryAddInodeFileMapCmdId:
		reply = n.inodeMgr.ApplyAsAddInodeFileMap(extBuf)
	case AppendEntryDropLRUChunksCmdId:
		reply = n.inodeMgr.ApplyAsDropLRUChunks(extBuf)
	case AppendEntryUpdateMetaAttrCmdId:
		reply = n.inodeMgr.ApplyAsUpdateMetaAttr(extBuf)
	case AppendEntryDeleteInodeFileMapCmdId:
		reply = n.inodeMgr.ApplyAsDeleteInodeMap(extBuf)
	case AppendEntryRemoveNonDirtyChunksCmdId:
		reply = n.dirtyMgr.ApplyAsRemoveNonDirtyChunks(extBuf)
	case AppendEntryForgetAllDirtyLogCmdId:
		reply = n.dirtyMgr.ApplyAsForgetAllDirty(extBuf)
	default:
		log.Errorf("Failed: AppendEntryCommand.Apply, unknown command, extCmdId=%v", extCmdId)
		reply = RaftReplyFail
	}
	if reply == RaftReplyOk {
		elapsed := time.Since(applyBegin)
		log.Debugf("Success: AppendEntryCommand.Apply, diskUsage=%v, extCmdId=%v, readTime=%v, applyTime=%v",
			n.raft.files.GetDiskUsage(), extCmdId, readTime, elapsed)
	}
	return reply
}

func (n *NodeServer) ResumeCoordinatorCommit() {
	var i = 0
	for txId, tx := range n.txMgr.activeTx {
		if tx.txType == TxBeginPersistCoordinator {
			c := tx.newData.(*common.MpuBeginMsg)
			for i := 0; i < len(c.Keys); i++ {
				if reply := n.inodeMgr.MpuAbort(c.Keys[i], c.UploadIds[i]); reply != RaftReplyOk {
					log.Errorf("Failed: resumeCoordinatorCommit (TxBeginPersistCoordinator), mpuAbort, txId=%v, reply=%v", txId, reply)
				}
			}
			if reply := n.raft.AppendExtendedLogEntry(AppendEntryAbortTxCmdId, &common.AbortCommitArgs{AbortTxIds: []*common.TxIdMsg{txId.toMsg()}}); reply != RaftReplyOk {
				log.Errorf("Failed: resumeCoordinatorCommit (TxBeginPersistCoordinator), AppendExtendedLogEntry, txId=%v, reply=%v", txId, reply)
			}
			continue
		} else if tx.txType == TxPersistCoordinator {
			c := tx.newData.(*common.TwoPCPersistRecordMsg)
			newFetchKey := c.GetMetaKeys()[0]
			for _, chunk := range c.GetPrimaryMeta().GetChunks() {
				seqNum := txId.GetVariant(c.NextTxSeqNum)
				c.NextTxSeqNum += 1
				commitTx := txId.GetVariant(chunk.GetTxSeqNum())
				n.rpcMgr.CallRemote(NewCommitPersistChunkOp(seqNum, commitTx, chunk.GetGroupId(), chunk.GetOffsets(), chunk.GetCVers(), InodeKeyType(chunk.GetInodeKey()), newFetchKey), n.flags.CommitRpcTimeoutDuration)
			}
		} else if tx.txType == TxUpdateCoordinator || tx.txType == TxTruncateCoordinator {
			c := tx.newData.(*TwoPCCommitRecord)
			c.CommitRemote(n, n.flags.CommitRpcTimeoutDuration)
		} else if tx.txType == TxUpdateNodeListCoordinator {
			c := tx.newData.(*common.TwoPCNodeListCommitRecordMsg)
			for _, node := range c.Remotes {
				txId2 := txId.GetVariant(c.NextTxSeqNum)
				c.NextTxSeqNum += 1
				n.rpcMgr.CallRemote(NewCommitParticipantOp(txId2, txId.GetVariant(node.TxSeqNum), node.Node.GroupId), n.flags.CommitRpcTimeoutDuration)
			}
		} else if tx.txType == TxCreateCoordinator {
			c := tx.newData.(*common.TwoPCCreateMetaCommitRecordMsg)
			if c.RemoteParent != nil {
				txId2 := txId.GetVariant(c.NextTxSeqNum)
				c.NextTxSeqNum += 1
				n.rpcMgr.CallRemote(NewCommitParticipantOp(txId2, txId.GetVariant(c.GetRemoteParent().GetTxSeqNum()), c.GetRemoteParent().GetGroupId()), n.flags.CommitRpcTimeoutDuration)
			}
			if c.RemoteMeta != nil {
				txId2 := txId.GetVariant(c.NextTxSeqNum)
				c.NextTxSeqNum += 1
				n.rpcMgr.CallRemote(NewCommitParticipantOp(txId2, txId.GetVariant(c.GetRemoteMeta().GetTxSeqNum()), c.GetRemoteMeta().GetGroupId()), n.flags.CommitRpcTimeoutDuration)
			}
		} else if tx.txType == TxRenameCoordinator {
			c := tx.newData.(*common.TwoPCRenameCommitRecordMsg)
			for _, remoteMsg := range c.GetRemoteOps() {
				txId2 := txId.GetVariant(c.NextTxSeqNum)
				c.NextTxSeqNum += 1
				n.rpcMgr.CallRemote(NewCommitParticipantOp(txId2, txId.GetVariant(remoteMsg.GetTxSeqNum()), remoteMsg.GetGroupId()), n.flags.CommitRpcTimeoutDuration)
			}
		} else {
			log.Infof("Ignore non-coordinator or unkown txType: txId=%v, txType=%v", txId, tx.txType)
			continue
		}

		if _, _, reply := n.raft.AppendEntriesLocal(NewAppendEntryCommitTxCommand(n.raft.currentTerm.Get(), txId.toMsg())); reply != RaftReplyOk {
			log.Errorf("Failed: resumeCoordinatorCommit, AppendEntriesLocal, txId=%v, reply=%v", txId, reply)
			// TODO: must not be here
			continue
		}
		n.txMgr.deactivateTx(txId)
		log.Debugf("Success: resumeCoordinatorCommit, txId=%v", txId)
		i += 1
	}
	if i == 0 {
		log.Debugf("Success: resumeCoordinatorCommit, no suspended Tx")
	}
}
