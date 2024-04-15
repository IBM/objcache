/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/takeshi-yoshimura/fuse"

	"github.com/IBM/objcache/common"
)

type TxRet struct {
	txId TxId
	ret  RpcRet
}

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
	txTime    time.Time
	txId      TxId
	extLogCmd ExtLogCommandImpl
}

func (p Tx) Less(b btree.Item) bool {
	return p.txTime.Before(b.(Tx).txTime)
}

const (
	TxUpdateMeta       = 0
	TxUpdateChunk      = 1
	TxDeleteChunk      = 2
	TxUpdateNodeList   = 3
	TxUpdateMetaKey    = 4
	TxDeleteMeta       = 5
	TxCreateMeta       = 6
	TxUpdateParentMeta = 7

	TxUpdateCoordinator         = 30
	TxDeleteCoordinator         = 31
	TxBeginPersistCoordinator   = 32
	TxPersistCoordinator        = 33
	TxDeletePersistCoordinator  = 34
	TxUpdateNodeListCoordinator = 35
	TxCreateCoordinator         = 36
	TxRenameCoordinator         = 37
	TxTruncateCoordinator       = 38
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

func (m *TxMgr) registerTx(extLog ExtLogCommandImpl, txId TxId) {
	var newTx = Tx{txId: txId, extLogCmd: extLog}
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

func (m *TxMgr) ResumeTx(n *NodeServer) int32 {
	var i = 0
	for txId, tx := range m.activeTx {
		if coordinator, ok := tx.extLogCmd.(CoordinatorCommand); ok {
			n.rpcMgr.Commit(coordinator, n.flags.CoordinatorTimeoutDuration)
			n.txMgr.deactivateTx(txId)
			i += 1
		}
	}
	if i == 0 {
		log.Debugf("Success: ResumeTx, resumed %d transactions", i)
	}
	return RaftReplyOk
}
