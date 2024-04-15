/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"encoding/binary"
	"hash/crc32"
	"path/filepath"

	"github.com/IBM/objcache/common"
	"google.golang.org/protobuf/proto"
)

const (
	AppendEntryCommandLogBaseSize = int32(crc32.Size + 7)
	AppendEntryCommandLogSize     = AppendEntryCommandLogBaseSize + int32(AppendEntryExtLogCmdLength)
	AppendEntryExtLogCmdLength    = uint8(28)

	DataCmdIdBit = uint16(1 << 15)

	AppendEntryNoOpCmdId                      = uint16(0)
	AppendEntryAddServerCmdId                 = uint16(1)
	AppendEntryRemoveServerCmdId              = uint16(2)
	AppendEntryCommitTxCmdId                  = uint16(3)
	AppendEntryResetExtLogCmdId               = uint16(4)
	AppendEntryFillChunkCmdId                 = DataCmdIdBit | uint16(1)
	AppendEntryUpdateChunkCmdId               = DataCmdIdBit | uint16(2)
	AppendEntryUpdateMetaCmdId                = DataCmdIdBit | uint16(3)
	AppendEntryCommitChunkCmdId               = DataCmdIdBit | uint16(4)
	AppendEntryAbortTxCmdId                   = DataCmdIdBit | uint16(5)
	AppendEntryPersistChunkCmdId              = DataCmdIdBit | uint16(6)
	AppendEntryBeginPersistCmdId              = DataCmdIdBit | uint16(7)
	AppendEntryInitNodeListCmdId              = DataCmdIdBit | uint16(8)
	AppendEntryUpdateNodeListCmdId            = DataCmdIdBit | uint16(9)
	AppendEntryUpdateMetaKeyCmdId             = DataCmdIdBit | uint16(10)
	AppendEntryCreateMetaCmdId                = DataCmdIdBit | uint16(11)
	AppendEntryDeleteMetaCmdId                = DataCmdIdBit | uint16(12)
	AppendEntryUpdateParentMetaCmdId          = DataCmdIdBit | uint16(13)
	AppendEntryAddInodeFileMapCmdId           = DataCmdIdBit | uint16(14)
	AppendEntryDropLRUChunksCmdId             = DataCmdIdBit | uint16(15)
	AppendEntryCreateChunkCmdId               = DataCmdIdBit | uint16(16)
	AppendEntryUpdateMetaAttrCmdId            = DataCmdIdBit | uint16(17)
	AppendEntryDeleteInodeFileMapCmdId        = DataCmdIdBit | uint16(18)
	AppendEntryRemoveNonDirtyChunksCmdId      = DataCmdIdBit | uint16(19)
	AppendEntryForgetAllDirtyLogCmdId         = DataCmdIdBit | uint16(20)
	AppendEntryRecordMigratedAddMetaCmdId     = DataCmdIdBit | uint16(21)
	AppendEntryRecordMigratedRemoveMetaCmdId  = DataCmdIdBit | uint16(22)
	AppendEntryRecordMigratedAddChunkCmdId    = DataCmdIdBit | uint16(23)
	AppendEntryRecordMigratedRemoveChunkCmdId = DataCmdIdBit | uint16(24)
	AppendEntryTruncateCoordinatorCmdId       = DataCmdIdBit | uint16(25)
	AppendEntryFlushCoordinatorCmdId          = DataCmdIdBit | uint16(26)
	AppendEntryHardLinkCoordinatorCmdId       = DataCmdIdBit | uint16(27)
	AppendEntryPersistCoordinatorCmdId        = DataCmdIdBit | uint16(28)
	AppendEntryDeletePersistCoordinatorCmdId  = DataCmdIdBit | uint16(29)
	AppendEntryUpdateNodeListCoordinatorCmdId = DataCmdIdBit | uint16(30)
	AppendEntryCreateCoordinatorCmdId         = DataCmdIdBit | uint16(31)
	AppendEntryRenameCoordinatorCmdId         = DataCmdIdBit | uint16(32)
	AppendEntryDeleteCoordinatorCmdId         = DataCmdIdBit | uint16(33)
	AppendEntryCompactLogCoordinatorCmdId     = DataCmdIdBit | uint16(34)
)

type AppendEntryCommand struct {
	buf [AppendEntryCommandLogSize]byte
	/**
	 AppendEntryCommand format:
	-----------------------------------------------------------------------
	| Bits | 0 - 3 | 4 - 4       | 5 - 8   | 9 - 10    | 10 - entryLength |
	| Name | CRC32 | entryLength | term    | extCmdId  | extEntryPayload  |
	-----------------------------------------------------------------------
	*/
}

func (l *AppendEntryCommand) setChecksum() {
	copy(l.buf[:crc32.Size], GetBufCheckSum(l.buf[crc32.Size:l.GetEntryLength()]))
}
func (l *AppendEntryCommand) setControlBits(extPayloadLength uint8, term uint32, extCmdId uint16) {
	l.buf[crc32.Size] = uint8(AppendEntryCommandLogBaseSize) + extPayloadLength
	binary.LittleEndian.PutUint32(l.buf[crc32.Size+1:crc32.Size+5], term)
	binary.LittleEndian.PutUint16(l.buf[crc32.Size+5:AppendEntryCommandLogBaseSize], extCmdId)
}
func (l *AppendEntryCommand) GetExtPayload() []byte {
	return l.buf[AppendEntryCommandLogBaseSize:]
}
func (l *AppendEntryCommand) GetChecksum() []byte {
	return l.buf[0:crc32.Size]
}
func (l *AppendEntryCommand) GetEntryLength() uint8 {
	return l.buf[crc32.Size]
}
func (l *AppendEntryCommand) GetTerm() uint32 {
	return binary.LittleEndian.Uint32(l.buf[crc32.Size+1 : crc32.Size+5])
}
func (l *AppendEntryCommand) GetExtCmdId() uint16 {
	return binary.LittleEndian.Uint16(l.buf[crc32.Size+5 : crc32.Size+7])
}

func (l *AppendEntryCommand) AppendToRpcMsg(d *RpcMsg) (newOptHeaderLength uint16, newTotalExtLogSize uint32) {
	var logLength = uint32(0)
	var entryLength = l.GetEntryLength()
	if l.GetExtCmdId()&DataCmdIdBit != 0 {
		logLength = NewExtLogCommandFromBytes(l).(*ExtLogCommand).dataLen
	}
	off := d.GetOptHeaderLength()
	if off == 0 {
		off = uint16(RpcOptControlHeaderLength)
	}
	d.SetOptHeaderLength(off + uint16(entryLength))
	curExtLogSize, curNrEntries := d.GetOptControlHeader()
	if logLength > 0 {
		d.SetTotalExtLogLength(curExtLogSize + logLength)
	}
	d.SetNrEntries(curNrEntries + 1)
	copy(d.optBuf[off:off+uint16(entryLength)], l.buf[:entryLength])
	log.Debugf("AppendToRpcMsg: MyCopy(d.optBuf[%v+crc32.Size:%v] from buf=%v", off, off+uint16(entryLength), l.buf[:entryLength])
	return off + uint16(entryLength), curExtLogSize + uint32(logLength)
}

func LoadExtBuf(extLogger *OnDiskLogger, l *AppendEntryCommand) (extCmd ExtLogCommandImpl, err error) {
	if l.GetExtCmdId()&DataCmdIdBit == 0 {
		return nil, nil
	}
	cmd := NewExtLogCommandFromBytes(l).(*ExtLogCommand)
	logId := cmd.logId
	logLength := cmd.dataLen
	logOffset := cmd.logOffset
	extBuf, err := extLogger.Read(logId, logOffset, int64(logLength))
	if err != nil {
		log.Errorf("Failed: LoadExtBuf, Read, logId=%v, offset=%v, length=%v, errno=%v", logId, logOffset, logLength, err)
		return nil, err
	}
	newfn, ok := extCommandNewFns[cmd.extCmdId]
	if !ok {
		log.Errorf("Failed: LoadExtBuf, no extCmdId exists, logId=%v, offset=%v, length=%v, extCmdId=%v", logId, logOffset, logLength, cmd.extCmdId)
		return nil, err
	}
	return newfn(extBuf)
}

func GetAppendEntryCommand(term uint32, rc RaftCommand) (cmd AppendEntryCommand) {
	cmd.setControlBits(rc.GetExtPayloadSize(), term, rc.GetCmdId())
	extPayload := cmd.GetExtPayload()
	rc.SetExtPayload(extPayload)
	cmd.setChecksum()
	return
}

type RaftCommand interface {
	GetCmdId() uint16
	GetExtPayloadSize() uint8
	SetExtPayload([]byte)
	Apply(*NodeServer, ExtLogCommandImpl)
}

type NoOpCommand struct {
}

func NewNoOpCommand() NoOpCommand {
	return NoOpCommand{}
}

func NewNoOpCommandFromBytes(_ *AppendEntryCommand) RaftCommand {
	return NewNoOpCommand()
}

func (c NoOpCommand) GetCmdId() uint16 {
	return AppendEntryNoOpCmdId
}

func (c NoOpCommand) GetExtPayloadSize() uint8 {
	return 0
}

func (c NoOpCommand) SetExtPayload([]byte) {
}

func (c NoOpCommand) Apply(*NodeServer, ExtLogCommandImpl) {
}

type ResetExtCommand struct {
	logId      uint32
	nextSeqNum uint32
}

func NewResetExtCommand(logId uint32, nextSeqNum uint32) ResetExtCommand {
	return ResetExtCommand{logId: logId, nextSeqNum: nextSeqNum}
}

func NewResetExtCommandFromBytes(l *AppendEntryCommand) RaftCommand {
	extEntryPayload := l.GetExtPayload()
	logId := binary.LittleEndian.Uint32(extEntryPayload[0:4])
	nextSeqNum := binary.LittleEndian.Uint32(extEntryPayload[4:8])
	return NewResetExtCommand(logId, nextSeqNum)
}

func (c ResetExtCommand) GetCmdId() uint16 {
	return AppendEntryResetExtLogCmdId
}

func (c ResetExtCommand) GetExtPayloadSize() uint8 {
	return 8
}

func (c ResetExtCommand) SetExtPayload(extPayload []byte) {
	binary.LittleEndian.PutUint32(extPayload[0:4], c.logId)
	binary.LittleEndian.PutUint32(extPayload[4:8], c.nextSeqNum)
}

func (c ResetExtCommand) Apply(n *NodeServer, _ ExtLogCommandImpl) {
	n.raft.SetExt(c.logId, c.nextSeqNum)
}

type AddServerCommand struct {
	serverId uint32
	ip       [4]byte
	port     uint16
}

func NewAddServerCommand(serverId uint32, ip [4]byte, port uint16) AddServerCommand {
	return AddServerCommand{serverId: serverId, ip: ip, port: port}
}

func NewAddServerCommandFromBytes(l *AppendEntryCommand) RaftCommand {
	extEntryPayload := l.GetExtPayload()
	serverId := binary.LittleEndian.Uint32(extEntryPayload[0:4])
	var ip [4]byte
	for i := int32(0); i < int32(4); i++ {
		ip[i] = extEntryPayload[4+i]
	}
	port := binary.LittleEndian.Uint16(extEntryPayload[8:10])
	return NewAddServerCommand(serverId, ip, port)
}

func (c AddServerCommand) GetCmdId() uint16 {
	return AppendEntryAddServerCmdId
}

func (c AddServerCommand) GetExtPayloadSize() uint8 {
	return 10
}

func (c AddServerCommand) SetExtPayload(extPayload []byte) {
	binary.LittleEndian.PutUint32(extPayload[0:4], c.serverId)
	for i := int32(0); i < int32(4); i++ {
		extPayload[4+i] = c.ip[i]
	}
	binary.LittleEndian.PutUint16(extPayload[8:10], c.port)
}

func (c AddServerCommand) Apply(n *NodeServer, _ ExtLogCommandImpl) {
	n.raft.lock.Lock()
	if _, ok := n.raft.serverIds[c.serverId]; !ok {
		sa := common.NodeAddrInet4{Addr: c.ip, Port: int(c.port)}
		n.raft.serverIds[c.serverId] = sa
		n.raft.sas[sa] = c.serverId
		log.Debugf("Success: AddServerCommand, serverId=%v, as=%v", c.serverId, sa)
	}
	n.raft.lock.Unlock()
}

type RemoveServerCommand struct {
	serverId uint32
}

func NewRemoveServerCommand(serverId uint32) RemoveServerCommand {
	return RemoveServerCommand{serverId: serverId}
}

func NewRemoveServerCommandFromBytes(l *AppendEntryCommand) RaftCommand {
	extEntryPayload := l.GetExtPayload()
	serverId := binary.LittleEndian.Uint32(extEntryPayload[0:4])
	return NewRemoveServerCommand(serverId)
}

func (c RemoveServerCommand) GetCmdId() uint16 {
	return AppendEntryRemoveServerCmdId
}

func (c RemoveServerCommand) GetExtPayloadSize() uint8 {
	return 4
}

func (c RemoveServerCommand) SetExtPayload(extPayload []byte) {
	binary.LittleEndian.PutUint32(extPayload[0:4], c.serverId)
}

func (c RemoveServerCommand) Apply(n *NodeServer, _ ExtLogCommandImpl) {
	n.raft.lock.Lock()
	if sa, ok := n.raft.serverIds[c.serverId]; ok {
		delete(n.raft.serverIds, c.serverId)
		delete(n.raft.sas, sa)
		log.Debugf("Success: RemoveServerCommand, serverId=%v", c.serverId)
	}
	n.raft.lock.Unlock()
}

type CommitCommand struct {
	txId TxId
}

func NewCommitCommand(txId TxId) CommitCommand {
	return CommitCommand{txId: txId}
}

func NewCommitCommandFromBytes(l *AppendEntryCommand) RaftCommand {
	extEntryPayload := l.GetExtPayload()
	clientId := binary.LittleEndian.Uint32(extEntryPayload[0:4])
	seqNum := binary.LittleEndian.Uint32(extEntryPayload[4:8])
	txSeqNum := binary.LittleEndian.Uint64(extEntryPayload[8:16])
	return NewCommitCommand(TxId{ClientId: clientId, SeqNum: seqNum, TxSeqNum: txSeqNum})
}

func (c CommitCommand) GetCmdId() uint16 {
	return AppendEntryCommitTxCmdId
}

func (c CommitCommand) GetExtPayloadSize() uint8 {
	return 16
}

func (c CommitCommand) SetExtPayload(extPayload []byte) {
	binary.LittleEndian.PutUint32(extPayload[0:4], c.txId.ClientId)
	binary.LittleEndian.PutUint32(extPayload[4:8], c.txId.SeqNum)
	binary.LittleEndian.PutUint64(extPayload[8:16], c.txId.TxSeqNum)
}

func (c CommitCommand) Apply(n *NodeServer, _ ExtLogCommandImpl) {
	tx, err := n.txMgr.getTx(c.txId)
	if err != nil {
		log.Errorf("Failed (ignore): CommitCommand.Apply, txId=%v does not exist. aborted?", c.txId)
		return
	}
	tx.extLogCmd.Commit(n)
	n.txMgr.deactivateTx(c.txId)
}

type ExtLogCommand struct {
	extCmdId     uint16
	logId        LogIdType
	logOffset    int64
	dataLen      uint32
	extBufChksum [4]byte
}

func NewExtLogCommand(extCmdId uint16, logId LogIdType, logOffset int64, dataLen uint32, extBufChksum [4]byte) ExtLogCommand {
	return ExtLogCommand{extCmdId: extCmdId, logId: logId, logOffset: logOffset, dataLen: dataLen, extBufChksum: extBufChksum}
}

func NewExtLogCommandFromExtBuf(extCmdId uint16, logId LogIdType, logOffset int64, extBuf []byte) ExtLogCommand {
	var extBufChksum [4]byte
	copy(extBufChksum[:], GetBufCheckSum(extBuf))
	return ExtLogCommand{extCmdId: extCmdId | DataCmdIdBit, logId: logId, logOffset: logOffset, dataLen: uint32(len(extBuf)), extBufChksum: extBufChksum}
}

func NewExtLogCommandFromBytes(l *AppendEntryCommand) RaftCommand {
	extCmdId := l.GetExtCmdId()
	extEntryPayload := l.GetExtPayload()
	logId := NewLogIdTypeFromBuf(extEntryPayload[0:12])
	dataLen := binary.LittleEndian.Uint32(extEntryPayload[12:16])
	logOffset := int64(binary.LittleEndian.Uint64(extEntryPayload[16 : AppendEntryExtLogCmdLength-4]))
	var extBufChksum [4]byte
	copy(extBufChksum[:], extEntryPayload[AppendEntryExtLogCmdLength-4:])
	return NewExtLogCommand(extCmdId, logId, logOffset, dataLen, extBufChksum)
}

func (c ExtLogCommand) GetCmdId() uint16 {
	return c.extCmdId
}

func (c ExtLogCommand) GetExtPayloadSize() uint8 {
	return AppendEntryExtLogCmdLength
}

func (c ExtLogCommand) SetExtPayload(extPayload []byte) {
	c.logId.Put(extPayload[0:12])
	binary.LittleEndian.PutUint32(extPayload[12:16], uint32(c.dataLen))
	binary.LittleEndian.PutUint64(extPayload[16:AppendEntryExtLogCmdLength-4], uint64(c.logOffset))
	copy(extPayload[AppendEntryExtLogCmdLength-4:], c.extBufChksum[:])
}

func (c ExtLogCommand) Apply(n *NodeServer, extLog ExtLogCommandImpl) {
	if extLog.IsSingleShot() {
		extLog.Commit(n)
		return
	}
	txId := extLog.GetTxId()
	tx, err := n.txMgr.getTx(txId)
	if err == nil && tx.extLogCmd.GetExtCmdId() != AppendEntryBeginPersistCmdId {
		log.Infof("Duplicated: ExtLogCommand.Apply, txId=%v", txId)
		return // return early without commits to stop duplicated commits. readers cannot observe this writes until the ongoing commit completes
	}
	if extLog.NeedTwoPhaseCommit(n.raftGroup) {
		n.txMgr.registerTx(extLog, txId) // CommitCommand will call extLog.Commit() later
	} else {
		extLog.Commit(n)
		if err == nil {
			n.txMgr.deactivateTx(txId)
		}
	}
}

var commandMap = map[uint16]func(*AppendEntryCommand) RaftCommand{
	AppendEntryNoOpCmdId:         NewNoOpCommandFromBytes,
	AppendEntryAddServerCmdId:    NewAddServerCommandFromBytes,
	AppendEntryRemoveServerCmdId: NewRemoveServerCommandFromBytes,
	AppendEntryCommitTxCmdId:     NewCommitCommandFromBytes,
	AppendEntryResetExtLogCmdId:  NewResetExtCommandFromBytes,
	DataCmdIdBit:                 NewExtLogCommandFromBytes,
}

func (l *AppendEntryCommand) AsRaftCommand() (RaftCommand, bool) {
	cmdId := l.GetExtCmdId()
	if cmdId&DataCmdIdBit != 0 {
		cmdId = DataCmdIdBit
	}
	fn, ok := commandMap[cmdId]
	if !ok {
		return nil, ok
	}
	return fn(l), ok
}

type ExtLogCommandImpl interface {
	toMsg() proto.Message
	GetExtCmdId() uint16
	GetTxId() TxId
	IsSingleShot() bool
	NeedTwoPhaseCommit(*RaftGroupMgr) bool
	Commit(*NodeServer)
}

type UpdateMetaCommand struct {
	txId    TxId
	working *WorkingMeta
}

func NewUpdateMetaCommand(txId TxId, working *WorkingMeta) UpdateMetaCommand {
	return UpdateMetaCommand{txId: txId, working: working}
}

func NewUpdateMetaCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.UpdateMetaMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewUpdateMetaCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	working := NewWorkingMetaFromMsg(msg.GetMeta())
	return NewUpdateMetaCommand(txId, working), nil
}

func (c UpdateMetaCommand) GetExtCmdId() uint16 {
	return AppendEntryUpdateMetaCmdId
}

func (c UpdateMetaCommand) toMsg() proto.Message {
	return &common.UpdateMetaMsg{TxId: c.txId.toMsg(), Meta: c.working.toMsg()}
}

func (c UpdateMetaCommand) GetTxId() TxId {
	return c.txId
}

func (c UpdateMetaCommand) IsSingleShot() bool {
	return false
}

func (c UpdateMetaCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return true
}

func (c UpdateMetaCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitUpdateMeta(c.working, n.dirtyMgr)
}

type UpdateParentMetaCommand struct {
	txId TxId
	info *UpdateParentInfo
}

func NewUpdateParentMetaCommand(txId TxId, info *UpdateParentInfo) UpdateParentMetaCommand {
	return UpdateParentMetaCommand{txId: txId, info: info}
}

func NewUpdateParentMetaCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.UpdateParentMetaMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewUpdateParentMetaCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	info := NewUpdateParentInfoFromMsg(msg.GetInfo())
	return NewUpdateParentMetaCommand(txId, info), nil
}

func (c UpdateParentMetaCommand) GetExtCmdId() uint16 {
	return AppendEntryUpdateParentMetaCmdId
}

func (c UpdateParentMetaCommand) toMsg() proto.Message {
	return &common.UpdateParentMetaMsg{TxId: c.txId.toMsg(), Info: c.info.toMsg()}
}

func (c UpdateParentMetaCommand) GetTxId() TxId {
	return c.txId
}

func (c UpdateParentMetaCommand) IsSingleShot() bool {
	return false
}

func (c UpdateParentMetaCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return true
}

func (c UpdateParentMetaCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitUpdateParentMeta(c.info, n.dirtyMgr)
}

type UpdateMetaKeyCommand struct {
	txId     TxId
	working  *WorkingMeta
	children map[string]InodeKeyType
	oldKey   string
	newKey   string
}

func NewUpdateMetaKeyCommand(txId TxId, parentWorking *WorkingMeta, children map[string]InodeKeyType, oldKey string, newKey string) UpdateMetaKeyCommand {
	return UpdateMetaKeyCommand{txId: txId, working: parentWorking, children: children, oldKey: oldKey, newKey: newKey}
}

func NewUpdateMetaKeyCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.UpdateMetaKeyMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewUpdateMetaKeyCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	working := NewWorkingMetaFromMsg(msg.GetMeta())
	children := make(map[string]InodeKeyType)
	for _, child := range msg.GetChildren() {
		children[child.GetName()] = InodeKeyType(child.GetInodeKey())
	}
	return NewUpdateMetaKeyCommand(txId, working, children, msg.GetOldKey(), msg.GetNewKey()), nil
}

func (c UpdateMetaKeyCommand) GetExtCmdId() uint16 {
	return AppendEntryUpdateMetaKeyCmdId
}

func (c UpdateMetaKeyCommand) toMsg() proto.Message {
	children := make([]*common.CopiedMetaChildMsg, 0)
	for name, inodeKey := range c.children {
		children = append(children, &common.CopiedMetaChildMsg{Name: name, InodeKey: uint64(inodeKey)})
	}
	return &common.UpdateMetaKeyMsg{
		TxId: c.txId.toMsg(), Meta: c.working.toMsg(), Children: children, OldKey: c.oldKey, NewKey: c.newKey,
	}
}

func (c UpdateMetaKeyCommand) GetTxId() TxId {
	return c.txId
}

func (c UpdateMetaKeyCommand) IsSingleShot() bool {
	return false
}

func (c UpdateMetaKeyCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return true
}

func (c UpdateMetaKeyCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitUpdateInodeToFile(c.working, c.children, c.oldKey, c.newKey, n.dirtyMgr)
}

type CreateMetaCommand struct {
	txId    TxId
	working *WorkingMeta
	newKey  string
	parent  InodeKeyType
}

func NewCreateMetaCommand(txId TxId, meta *WorkingMeta, newKey string, parent InodeKeyType) CreateMetaCommand {
	return CreateMetaCommand{txId: txId, working: meta, newKey: newKey, parent: parent}
}

func NewCreateMetaCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.CreateMetaMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewCreateMetaCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	working := NewWorkingMetaFromMsg(msg.GetMeta())
	return NewCreateMetaCommand(txId, working, msg.GetNewKey(), InodeKeyType(msg.GetParentInodeKey())), nil
}

func (c CreateMetaCommand) GetExtCmdId() uint16 {
	return AppendEntryCreateMetaCmdId
}

func (c CreateMetaCommand) toMsg() proto.Message {
	return &common.CreateMetaMsg{TxId: c.txId.toMsg(), Meta: c.working.toMsg(), ParentInodeKey: uint64(c.parent), NewKey: c.newKey}
}

func (c CreateMetaCommand) GetTxId() TxId {
	return c.txId
}

func (c CreateMetaCommand) IsSingleShot() bool {
	return false
}

func (c CreateMetaCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return true
}

func (c CreateMetaCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitCreateMeta(c.working, c.parent, c.newKey, n.dirtyMgr)
}

type DeleteMetaCommand struct {
	txId    TxId
	working *WorkingMeta
	key     string
}

func NewDeleteMetaCommand(txId TxId, parentWorking *WorkingMeta, key string) DeleteMetaCommand {
	return DeleteMetaCommand{txId: txId, working: parentWorking, key: key}
}

func NewDeleteMetaCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.DeleteMetaMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewDeleteMetaCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	working := NewWorkingMetaFromMsg(msg.GetMeta())
	return NewDeleteMetaCommand(txId, working, msg.GetKey()), nil
}

func (c DeleteMetaCommand) GetExtCmdId() uint16 {
	return AppendEntryDeleteMetaCmdId
}

func (c DeleteMetaCommand) toMsg() proto.Message {
	return &common.DeleteMetaMsg{TxId: c.txId.toMsg(), Meta: c.working.toMsg(), Key: c.key}
}

func (c DeleteMetaCommand) GetTxId() TxId {
	return c.txId
}

func (c DeleteMetaCommand) IsSingleShot() bool {
	return false
}

func (c DeleteMetaCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return true
}

func (c DeleteMetaCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitUpdateInodeToFile(c.working, nil, c.key, "", n.dirtyMgr)
}

type InitNodeListCommand struct {
	node RaftNode
}

func NewInitNodeListCommand(node RaftNode) InitNodeListCommand {
	return InitNodeListCommand{node: node}
}

func NewInitNodeListCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.InitNodeListMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewInitNodeListCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewInitNodeListCommand(NewRaftNodeFromMsg(msg.GetNode())), nil
}

func (c InitNodeListCommand) toMsg() proto.Message {
	return &common.InitNodeListMsg{Node: c.node.toMsg()}
}

func (c InitNodeListCommand) GetExtCmdId() uint16 {
	return AppendEntryInitNodeListCmdId
}

func (c InitNodeListCommand) GetTxId() TxId {
	return TxId{}
}

func (c InitNodeListCommand) IsSingleShot() bool {
	return true
}

func (c InitNodeListCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c InitNodeListCommand) Commit(n *NodeServer) {
	n.raftGroup.CommitUpdate([]RaftNode{c.node}, true, 0)
}

type UpdateNodeListCommand struct {
	txId        TxId
	migrationId MigrationId
	isAdd       bool
	needRestore bool
	nodes       []RaftNode
	nodeListVer uint64
}

func NewUpdateNodeListCommand(txId TxId, migrationId MigrationId, isAdd bool, needRestore bool, nodes []RaftNode, nodeListVer uint64) UpdateNodeListCommand {
	return UpdateNodeListCommand{txId: txId, migrationId: migrationId, isAdd: isAdd, needRestore: needRestore, nodes: nodes, nodeListVer: nodeListVer}
}

func NewUpdateNodeListCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.UpdateNodeListMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewUpdateNodeListCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	migrationId := NewMigrationIdFromMsg(msg.GetMigrationId())
	nodeList := make([]RaftNode, 0)
	for _, nodeMsg := range msg.GetNodes() {
		nodeList = append(nodeList, NewRaftNodeFromMsg(nodeMsg))
	}
	return NewUpdateNodeListCommand(txId, migrationId, msg.GetIsAdd(), msg.GetNeedRestore(), nodeList, msg.GetNodeListVer()), nil
}

func (c UpdateNodeListCommand) toMsg() proto.Message {
	nodeList := make([]*common.NodeMsg, 0)
	for _, node := range c.nodes {
		nodeList = append(nodeList, node.toMsg())
	}
	return &common.UpdateNodeListMsg{
		TxId: c.txId.toMsg(), MigrationId: c.migrationId.toMsg(), IsAdd: c.isAdd, NeedRestore: c.needRestore,
		Nodes: nodeList, NodeListVer: c.nodeListVer,
	}
}

func (c UpdateNodeListCommand) GetExtCmdId() uint16 {
	return AppendEntryUpdateNodeListCmdId
}

func (c UpdateNodeListCommand) GetTxId() TxId {
	return c.txId
}

func (c UpdateNodeListCommand) IsSingleShot() bool {
	return false
}

func (c UpdateNodeListCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return true
}

func (c UpdateNodeListCommand) Commit(n *NodeServer) {
	n.dirtyMgr.CommitMigratedDataLocal(n.inodeMgr, c.migrationId)
	n.raftGroup.CommitUpdate(c.nodes, c.isAdd, c.nodeListVer)
}

type CreateChunkCommand struct {
	inodeKey  InodeKeyType
	offset    int64
	version   uint32
	logOffset int64
	length    int64
	key       string
	chunkIdx  uint32
}

func NewCreateChunkCommand(inodeKey InodeKeyType, offset int64, version uint32, logOffset int64, length int64, key string, chunkIdx uint32) CreateChunkCommand {
	return CreateChunkCommand{inodeKey: inodeKey, offset: offset, version: version, logOffset: logOffset, length: length, key: key, chunkIdx: chunkIdx}
}

func NewCreateChunkCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.CreateChunkArgs{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewCreateChunkCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	inodeKey := InodeKeyType(msg.GetInodeKey())
	offset := msg.GetOffset()
	version := msg.GetVersion()
	logOffset := msg.GetLogOffset()
	length := msg.GetLength()
	key := msg.GetKey()
	chunkIdx := msg.GetLogIdx()
	return NewCreateChunkCommand(inodeKey, offset, version, logOffset, length, key, chunkIdx), nil
}

func (c CreateChunkCommand) GetExtCmdId() uint16 {
	return AppendEntryCreateChunkCmdId
}

func (c CreateChunkCommand) toMsg() proto.Message {
	return &common.CreateChunkArgs{
		InodeKey: uint64(c.inodeKey), Offset: c.offset, Version: c.version, LogOffset: c.logOffset, Length: c.length, Key: c.key, LogIdx: c.chunkIdx,
	}
}

func (c CreateChunkCommand) GetTxId() TxId {
	return TxId{}
}

func (c CreateChunkCommand) IsSingleShot() bool {
	return true
}

func (c CreateChunkCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c CreateChunkCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitCreateChunk(c.inodeKey, c.offset, c.version, c.logOffset, c.length, c.key, c.chunkIdx)
}

type UpdateChunkCommand struct {
}

func NewUpdateChunkCommand() UpdateChunkCommand {
	return UpdateChunkCommand{}
}

func NewUpdateChunkCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	return NewUpdateChunkCommand(), nil
}

func (c UpdateChunkCommand) GetExtCmdId() uint16 {
	return AppendEntryUpdateChunkCmdId
}

func (c UpdateChunkCommand) toMsg() proto.Message {
	return &common.Ack{}
}

func (c UpdateChunkCommand) GetTxId() TxId {
	return TxId{}
}

func (c UpdateChunkCommand) IsSingleShot() bool {
	return true
}

func (c UpdateChunkCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c UpdateChunkCommand) Commit(n *NodeServer) {}

type CommitChunkCommand struct {
	inodeKey   InodeKeyType
	version    uint32
	chunkSize  int64
	objectSize int64
	isDelete   bool
	chunks     map[int64][]*common.StagingChunkAddMsg
}

func NewCommitChunkCommand(meta *WorkingMeta, chunks map[int64]*WorkingChunk, isDelete bool) CommitChunkCommand {
	ret := CommitChunkCommand{
		inodeKey: meta.inodeKey, version: meta.version, chunkSize: meta.chunkSize,
		objectSize: meta.size, isDelete: isDelete, chunks: make(map[int64][]*common.StagingChunkAddMsg),
	}
	for offset, chunk := range chunks {
		if _, ok := chunks[offset]; !ok {
			ret.chunks[offset] = make([]*common.StagingChunkAddMsg, 0)
		}
		ret.chunks[offset] = append(ret.chunks[offset], chunk.toStagingChunkAddMsg()...)
	}
	return ret
}

func NewCommitChunkCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.AppendCommitUpdateChunksMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewCommitChunkCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	inodeKey := InodeKeyType(msg.GetInodeKey())
	chunks := make(map[int64][]*common.StagingChunkAddMsg)
	for _, chunk := range msg.GetChunks() {
		if _, ok := chunks[chunk.GetOffset()]; !ok {
			chunks[chunk.GetOffset()] = make([]*common.StagingChunkAddMsg, 0)
		}
		chunks[chunk.GetOffset()] = append(chunks[chunk.GetOffset()], chunk.GetStagings()...)
	}
	return CommitChunkCommand{
		inodeKey: inodeKey, version: msg.GetVersion(), chunkSize: msg.GetChunkSize(),
		objectSize: msg.GetObjectSize(), isDelete: msg.GetIsDelete(), chunks: chunks,
	}, nil
}

func (c CommitChunkCommand) GetExtCmdId() uint16 {
	return AppendEntryCommitChunkCmdId
}

func (c CommitChunkCommand) toMsg() proto.Message {
	msg := &common.AppendCommitUpdateChunksMsg{
		InodeKey: uint64(c.inodeKey), Version: c.version, ChunkSize: c.chunkSize, ObjectSize: c.objectSize, IsDelete: c.isDelete,
		Chunks: make([]*common.WorkingChunkAddMsg, 0),
	}
	for offset, ss := range c.chunks {
		msg.Chunks = append(msg.Chunks, &common.WorkingChunkAddMsg{Offset: offset, Stagings: ss})
	}
	return msg
}

func (c CommitChunkCommand) GetTxId() TxId {
	return TxId{}
}

func (c CommitChunkCommand) IsSingleShot() bool {
	return true
}

func (c CommitChunkCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c CommitChunkCommand) Commit(n *NodeServer) {
	if c.isDelete {
		for offset, stags := range c.chunks {
			n.inodeMgr.CommitDeleteChunk(c.inodeKey, offset, c.chunkSize, c.version, stags, n.dirtyMgr)
		}
	} else {
		for offset, stags := range c.chunks {
			n.inodeMgr.CommitUpdateChunk(c.inodeKey, offset, c.chunkSize, c.version, stags, c.objectSize, n.dirtyMgr)
		}
	}
}

type AbortTxCommand struct {
	txIds []TxId
}

func NewAbortTxCommand(txIds []TxId) AbortTxCommand {
	return AbortTxCommand{txIds: txIds}
}

func NewAbortTxCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.AbortCommitArgs{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewCommitChunkCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txIds := make([]TxId, 0)
	for _, txIdMsg := range msg.GetAbortTxIds() {
		txIds = append(txIds, NewTxIdFromMsg(txIdMsg))
	}
	return NewAbortTxCommand(txIds), nil
}

func (c AbortTxCommand) GetExtCmdId() uint16 {
	return AppendEntryAbortTxCmdId
}

func (c AbortTxCommand) toMsg() proto.Message {
	ret := &common.AbortCommitArgs{AbortTxIds: make([]*common.TxIdMsg, 0)}
	for _, txId := range c.txIds {
		ret.AbortTxIds = append(ret.AbortTxIds, txId.toMsg())
	}
	return ret
}

func (c AbortTxCommand) GetTxId() TxId {
	return TxId{}
}

func (c AbortTxCommand) IsSingleShot() bool {
	return true
}

func (c AbortTxCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c AbortTxCommand) Commit(n *NodeServer) {
	for _, txId := range c.txIds {
		if _, err := n.txMgr.getTx(txId); err == nil {
			n.txMgr.deactivateTx(txId)
		}
	}
}

type PersistChunkCommand struct {
	inodeKey  InodeKeyType
	offsets   []int64
	chunkVers []uint32
}

func NewPersistChunkCommand(inodeKey InodeKeyType, offsets []int64, chunkVers []uint32) PersistChunkCommand {
	return PersistChunkCommand{inodeKey: inodeKey, offsets: offsets, chunkVers: chunkVers}
}

func NewPersistChunkCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.PersistedChunkInfoMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewPersistChunkCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewPersistChunkCommand(InodeKeyType(msg.GetInodeKey()), msg.GetOffsets(), msg.GetCVers()), nil
}

func (c PersistChunkCommand) GetExtCmdId() uint16 {
	return AppendEntryPersistChunkCmdId
}

func (c PersistChunkCommand) toMsg() proto.Message {
	return &common.PersistedChunkInfoMsg{InodeKey: uint64(c.inodeKey), Offsets: c.offsets, CVers: c.chunkVers}
}

func (c PersistChunkCommand) GetTxId() TxId {
	return TxId{}
}

func (c PersistChunkCommand) IsSingleShot() bool {
	return true
}

func (c PersistChunkCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c PersistChunkCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitPersistChunk(c.inodeKey, c.offsets, c.chunkVers, n.dirtyMgr)
}

type AddInodeFileMapCommand struct {
	working  *WorkingMeta
	children map[string]InodeKeyType
	key      string
}

func NewAddInodeFileMapCommand(working *WorkingMeta, children map[string]InodeKeyType, key string) AddInodeFileMapCommand {
	return AddInodeFileMapCommand{working: working, children: children, key: key}
}

func NewAddInodeFileMapCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.AddInodeFileMapMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewAddInodeFileMapCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	children := make(map[string]InodeKeyType)
	for _, child := range msg.GetChildren() {
		children[child.GetName()] = InodeKeyType(child.GetInodeKey())
	}
	return NewAddInodeFileMapCommand(NewWorkingMetaFromMsg(msg.GetMeta()), children, msg.GetKey()), nil
}

func (c AddInodeFileMapCommand) GetExtCmdId() uint16 {
	return AppendEntryAddInodeFileMapCmdId
}

func (c AddInodeFileMapCommand) toMsg() proto.Message {
	children := make([]*common.CopiedMetaChildMsg, 0)
	for name, inodeKey := range c.children {
		children = append(children, &common.CopiedMetaChildMsg{Name: name, InodeKey: uint64(inodeKey)})
	}
	return &common.AddInodeFileMapMsg{
		Meta: c.working.toMsg(), Children: children, Key: c.key,
	}
}

func (c AddInodeFileMapCommand) GetTxId() TxId {
	return TxId{}
}

func (c AddInodeFileMapCommand) IsSingleShot() bool {
	return true
}

func (c AddInodeFileMapCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c AddInodeFileMapCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitSetMetaAndInodeFile(c.working, c.children, c.key)
}

type DropLRUChunksCommand struct {
	inodeKeys []uint64
	offsets   []int64
}

func NewDropLRUChunksCommand(inodeKeys []uint64, offsets []int64) DropLRUChunksCommand {
	return DropLRUChunksCommand{inodeKeys: inodeKeys, offsets: offsets}
}

func NewDropLRUChunksCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.DropLRUChunksArgs{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewAddInodeFileMapCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewDropLRUChunksCommand(msg.GetInodeKeys(), msg.GetOffsets()), nil
}

func (c DropLRUChunksCommand) GetExtCmdId() uint16 {
	return AppendEntryDropLRUChunksCmdId
}

func (c DropLRUChunksCommand) toMsg() proto.Message {
	return &common.DropLRUChunksArgs{InodeKeys: c.inodeKeys, Offsets: c.offsets}
}

func (c DropLRUChunksCommand) GetTxId() TxId {
	return TxId{}
}

func (c DropLRUChunksCommand) IsSingleShot() bool {
	return true
}

func (c DropLRUChunksCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c DropLRUChunksCommand) Commit(n *NodeServer) {
	n.inodeMgr.DropLRUChunk(c.inodeKeys, c.offsets)
}

type UpdateMetaAttrCommand struct {
	inodeKey InodeKeyType
	mode     uint32
	ts       int64
}

func NewUpdateMetaAttrCommand(inodeKey InodeKeyType, mode uint32, ts int64) UpdateMetaAttrCommand {
	return UpdateMetaAttrCommand{inodeKey: inodeKey, mode: mode, ts: ts}
}

func NewUpdateMetaAttrCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.UpdateMetaAttrMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewUpdateMetaAttrCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewUpdateMetaAttrCommand(InodeKeyType(msg.GetInodeKey()), msg.GetMode(), msg.GetTs()), nil
}

func (c UpdateMetaAttrCommand) GetExtCmdId() uint16 {
	return AppendEntryUpdateMetaAttrCmdId
}

func (c UpdateMetaAttrCommand) toMsg() proto.Message {
	return &common.UpdateMetaAttrMsg{InodeKey: uint64(c.inodeKey), Mode: c.mode, Ts: c.ts}
}

func (c UpdateMetaAttrCommand) GetTxId() TxId {
	return TxId{}
}

func (c UpdateMetaAttrCommand) IsSingleShot() bool {
	return true
}

func (c UpdateMetaAttrCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c UpdateMetaAttrCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitUpdateMetaAttr(c.inodeKey, c.mode, c.ts)
}

type DeleteInodeFileMapCommand struct {
	inodeKeys []uint64
	keys      []string
}

func NewDeleteInodeFileMapCommand(inodeKeys []uint64, keys []string) DeleteInodeFileMapCommand {
	return DeleteInodeFileMapCommand{inodeKeys: inodeKeys, keys: keys}
}

func NewDeleteInodeFileMapCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.DeleteInodeMapArgs{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewDeleteInodeFileMapCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewDeleteInodeFileMapCommand(msg.GetInodeKey(), msg.GetKey()), nil
}

func (c DeleteInodeFileMapCommand) GetExtCmdId() uint16 {
	return AppendEntryDeleteInodeFileMapCmdId
}

func (c DeleteInodeFileMapCommand) toMsg() proto.Message {
	return &common.DeleteInodeMapArgs{InodeKey: c.inodeKeys, Key: c.keys}
}

func (c DeleteInodeFileMapCommand) GetTxId() TxId {
	return TxId{}
}

func (c DeleteInodeFileMapCommand) IsSingleShot() bool {
	return true
}

func (c DeleteInodeFileMapCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c DeleteInodeFileMapCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitDeleteInodeMap(c.inodeKeys, c.keys)
}

type RemoveNonDirtyChunksCommand struct {
	inodeKeys []uint64
}

func NewRemoveNonDirtyChunksCommand(inodeKeys []uint64) RemoveNonDirtyChunksCommand {
	return RemoveNonDirtyChunksCommand{inodeKeys: inodeKeys}
}

func NewRemoveNonDirtyChunksCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.RemoveNonDirtyChunksMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewRemoveNonDirtyChunksCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewRemoveNonDirtyChunksCommand(msg.GetInodeKeys()), nil
}

func (c RemoveNonDirtyChunksCommand) GetExtCmdId() uint16 {
	return AppendEntryRemoveNonDirtyChunksCmdId
}

func (c RemoveNonDirtyChunksCommand) toMsg() proto.Message {
	return &common.RemoveNonDirtyChunksMsg{InodeKeys: c.inodeKeys}
}

func (c RemoveNonDirtyChunksCommand) GetTxId() TxId {
	return TxId{}
}

func (c RemoveNonDirtyChunksCommand) IsSingleShot() bool {
	return true
}

func (c RemoveNonDirtyChunksCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c RemoveNonDirtyChunksCommand) Commit(n *NodeServer) {
	n.dirtyMgr.RemoveNonDirtyChunks(c.inodeKeys)
}

type ForgetAllDirtyLogCommand struct {
}

func NewForgetAllDirtyLogCommand() ForgetAllDirtyLogCommand {
	return ForgetAllDirtyLogCommand{}
}

func NewForgetAllDirtyLogCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	return NewForgetAllDirtyLogCommand(), nil
}

func (c ForgetAllDirtyLogCommand) GetExtCmdId() uint16 {
	return AppendEntryForgetAllDirtyLogCmdId
}

func (c ForgetAllDirtyLogCommand) toMsg() proto.Message {
	return &common.Ack{}
}

func (c ForgetAllDirtyLogCommand) GetTxId() TxId {
	return TxId{}
}

func (c ForgetAllDirtyLogCommand) IsSingleShot() bool {
	return true
}

func (c ForgetAllDirtyLogCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c ForgetAllDirtyLogCommand) Commit(n *NodeServer) {}

type RecordMigratedAddMetaCommand struct {
	s *Snapshot
}

func NewRecordMigratedAddMetaCommand(s *Snapshot) RecordMigratedAddMetaCommand {
	return RecordMigratedAddMetaCommand{s: s}
}

func NewRecordMigratedAddMetaCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.SnapshotMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewRecordMigratedAddMetaCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewRecordMigratedAddMetaCommand(NewSnapshotFromMsg(msg)), nil
}

func (c RecordMigratedAddMetaCommand) GetExtCmdId() uint16 {
	return AppendEntryRecordMigratedAddMetaCmdId
}

func (c RecordMigratedAddMetaCommand) toMsg() proto.Message {
	return c.s.toMsg()
}

func (c RecordMigratedAddMetaCommand) GetTxId() TxId {
	return TxId{}
}

func (c RecordMigratedAddMetaCommand) IsSingleShot() bool {
	return true // Note: do not use registerTx() although we run 2PC on migrationId...
}

func (c RecordMigratedAddMetaCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c RecordMigratedAddMetaCommand) Commit(n *NodeServer) {
	n.dirtyMgr.AddMigratedAddMetas(c.s)
}

type RecordMigratedRemoveMetaCommand struct {
	migrationId MigrationId
	inodeKeys   []uint64
	dirKeys     []uint64
}

func NewRecordMigratedRemoveMetaCommand(migrationId MigrationId, inodeKeys map[InodeKeyType]bool, dirInodes []*common.InodeTreeMsg) RecordMigratedRemoveMetaCommand {
	dirKeys := make([]uint64, 0)
	for _, inode := range dirInodes {
		dirKeys = append(dirKeys, inode.GetInodeKey())
	}
	keys := make([]uint64, 0)
	for inode := range inodeKeys {
		keys = append(keys, uint64(inode))
	}
	return RecordMigratedRemoveMetaCommand{migrationId: migrationId, inodeKeys: keys, dirKeys: dirKeys}
}

func NewRecordMigratedRemoveMetaCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.PrepareRemoveDirtyMetas{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewRecordMigratedRemoveMetaCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return RecordMigratedRemoveMetaCommand{migrationId: NewMigrationIdFromMsg(msg.GetMigrationId()), inodeKeys: msg.GetInodeKeys(), dirKeys: msg.GetDirInodeKeys()}, nil
}

func (c RecordMigratedRemoveMetaCommand) GetExtCmdId() uint16 {
	return AppendEntryRecordMigratedRemoveMetaCmdId
}

func (c RecordMigratedRemoveMetaCommand) toMsg() proto.Message {
	return &common.PrepareRemoveDirtyMetas{MigrationId: c.migrationId.toMsg(), InodeKeys: c.inodeKeys, DirInodeKeys: c.dirKeys}
}

func (c RecordMigratedRemoveMetaCommand) GetTxId() TxId {
	return TxId{}
}

func (c RecordMigratedRemoveMetaCommand) IsSingleShot() bool {
	return true // Note: do not use registerTx() although we run 2PC on migrationId...
}

func (c RecordMigratedRemoveMetaCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c RecordMigratedRemoveMetaCommand) Commit(n *NodeServer) {
	n.dirtyMgr.AddMigratedRemoveMetas(c.migrationId, c.inodeKeys, c.dirKeys)
}

type RecordMigratedAddChunkCommand struct {
	migrationId MigrationId
	chunk       *common.AppendCommitUpdateChunksMsg
}

func NewRecordMigratedAddChunkCommand(migrationId MigrationId, chunk *common.AppendCommitUpdateChunksMsg) RecordMigratedAddChunkCommand {
	return RecordMigratedAddChunkCommand{migrationId: migrationId, chunk: chunk}
}

func NewRecordMigratedAddChunkCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.RestoreDirtyAddChunkMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewRecordMigratedAddChunkCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewRecordMigratedAddChunkCommand(NewMigrationIdFromMsg(msg.GetMigrationId()), msg.GetChunk()), nil
}

func (c RecordMigratedAddChunkCommand) GetExtCmdId() uint16 {
	return AppendEntryRecordMigratedAddChunkCmdId
}

func (c RecordMigratedAddChunkCommand) toMsg() proto.Message {
	return &common.RestoreDirtyAddChunkMsg{MigrationId: c.migrationId.toMsg(), Chunk: c.chunk}
}

func (c RecordMigratedAddChunkCommand) GetTxId() TxId {
	return TxId{}
}

func (c RecordMigratedAddChunkCommand) IsSingleShot() bool {
	return true // Note: do not use registerTx() although we run 2PC on migrationId...
}

func (c RecordMigratedAddChunkCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c RecordMigratedAddChunkCommand) Commit(n *NodeServer) {
	n.dirtyMgr.AddMigratedAddChunk(c.migrationId, c.chunk)
}

type RecordMigratedRemoveChunkCommand struct {
	migrationId MigrationId
	chunks      []*common.ChunkRemoveDirtyMsg
}

func NewRecordMigratedRemoveChunkCommand(migrationId MigrationId, chunks []*common.ChunkRemoveDirtyMsg) RecordMigratedRemoveChunkCommand {
	return RecordMigratedRemoveChunkCommand{migrationId: migrationId, chunks: chunks}
}

func NewRecordMigratedRemoveChunkCommandFromExtbuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.RestoreDirtyRemoveChunkMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewRecordMigratedRemoveChunkCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewRecordMigratedRemoveChunkCommand(NewMigrationIdFromMsg(msg.GetMigrationId()), msg.GetRemoveDirtyChunks()), nil
}

func (c RecordMigratedRemoveChunkCommand) GetExtCmdId() uint16 {
	return AppendEntryRecordMigratedRemoveChunkCmdId
}

func (c RecordMigratedRemoveChunkCommand) toMsg() proto.Message {
	return &common.RestoreDirtyRemoveChunkMsg{MigrationId: c.migrationId.toMsg(), RemoveDirtyChunks: c.chunks}
}

func (c RecordMigratedRemoveChunkCommand) GetTxId() TxId {
	return TxId{}
}

func (c RecordMigratedRemoveChunkCommand) IsSingleShot() bool {
	return true // Note: do not use registerTx() although we run 2PC on migrationId...
}

func (c RecordMigratedRemoveChunkCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c RecordMigratedRemoveChunkCommand) Commit(n *NodeServer) {
	n.dirtyMgr.AddMigratedRemoveChunk(c.migrationId, c.chunks)
}

type CoordinatorCommand interface {
	RemoteCommit(raftGroup *RaftGroupMgr) []ParticipantOp
	ExtLogCommandImpl
}

type BeginPersistCommand struct {
	txId      TxId
	keys      []string
	uploadIds []string
}

func NewBeginPersistCommand(txId TxId, keys []string, uploadIds []string) BeginPersistCommand {
	return BeginPersistCommand{txId: txId, keys: keys, uploadIds: uploadIds}
}

func NewBeginPersistCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.MpuBeginMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewBeginPersistCommandFromExtbuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	return NewBeginPersistCommand(txId, msg.GetKeys(), msg.GetUploadIds()), nil
}

func (c BeginPersistCommand) GetExtCmdId() uint16 {
	return AppendEntryBeginPersistCmdId
}

func (c BeginPersistCommand) toMsg() proto.Message {
	return &common.MpuBeginMsg{TxId: c.txId.toMsg(), Keys: c.keys, UploadIds: c.uploadIds}
}

func (c BeginPersistCommand) GetTxId() TxId {
	return c.txId
}

func (c BeginPersistCommand) IsSingleShot() bool {
	return false
}

func (c BeginPersistCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return true
}

func (c BeginPersistCommand) Commit(n *NodeServer) {}

func (c BeginPersistCommand) RemoteCommit(*RaftGroupMgr) []ParticipantOp {
	// Note: called only when resme after a crash
	return []ParticipantOp{NewMpuAbortOp(c.txId, c.keys, c.uploadIds)}
}

type PersistCoordinatorCommand struct {
	txId       TxId
	inodeKey   InodeKeyType
	metaVer    uint32
	ts         int64
	keys       []string
	chunks     []*common.PersistedChunkInfoMsg
	commitTxId TxId
}

func NewPersistCoordinatorCommand(txId TxId, inodeKey InodeKeyType, metaVer uint32, ts int64, keys []string, chunks []*common.PersistedChunkInfoMsg, commitTxId TxId) PersistCoordinatorCommand {
	return PersistCoordinatorCommand{txId: txId, inodeKey: inodeKey, metaVer: metaVer, ts: ts, keys: keys, chunks: chunks, commitTxId: commitTxId}
}

func NewPersistCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.PersistCoordinatorMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewPersistCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return &PersistCoordinatorCommand{
		txId: NewTxIdFromMsg(msg.GetTxId()), inodeKey: InodeKeyType(msg.GetInodeKey()), metaVer: msg.GetMetaVer(), ts: msg.GetTs(),
		keys: msg.GetMetaKeys(), chunks: msg.GetChunks(), commitTxId: NewTxIdFromMsg(msg.GetCommitTxId()),
	}, nil
}

func (c PersistCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryPersistCoordinatorCmdId
}

func (c PersistCoordinatorCommand) toMsg() proto.Message {
	return &common.PersistCoordinatorMsg{
		TxId: c.txId.toMsg(), InodeKey: uint64(c.inodeKey), MetaVer: c.metaVer, Ts: c.ts,
		MetaKeys: c.keys, Chunks: c.chunks, CommitTxId: c.commitTxId.toMsg(),
	}
}

func (c PersistCoordinatorCommand) GetTxId() TxId {
	return c.txId
}

func (c PersistCoordinatorCommand) IsSingleShot() bool {
	return false
}

func (c PersistCoordinatorCommand) NeedTwoPhaseCommit(raftGroup *RaftGroupMgr) bool {
	for _, chunk := range c.chunks {
		if chunk.GetGroupId() != raftGroup.selfGroup {
			return true
		}
	}
	return false
}

func (c PersistCoordinatorCommand) Commit(n *NodeServer) {
	for _, chunk := range c.chunks {
		if chunk.GetGroupId() == n.raftGroup.selfGroup {
			n.inodeMgr.CommitPersistChunk(c.inodeKey, chunk.GetOffsets(), chunk.GetCVers(), n.dirtyMgr)
		}
	}
	n.inodeMgr.CommitPersistMeta(c.inodeKey, c.keys[0], c.metaVer, c.ts, n.dirtyMgr)
}

func (c PersistCoordinatorCommand) RemoteCommit(raftGroup *RaftGroupMgr) []ParticipantOp {
	fns := make([]ParticipantOp, 0)
	nextTxId := c.commitTxId
	for _, chunk := range c.chunks {
		if chunk.GetGroupId() == raftGroup.selfGroup {
			continue
		}
		partTxId := NewTxIdFromMsg(chunk.GetTxId())
		fns = append(fns, NewCommitPersistChunkOp(nextTxId, partTxId, chunk.GetGroupId(), chunk.GetOffsets(), chunk.GetCVers(), c.inodeKey))
		nextTxId = nextTxId.GetNext()
	}
	return fns
}

type DeletePersistCoordinatorCommand struct {
	key string
}

func NewDeletePersistCoordinatorCommand(key string) DeletePersistCoordinatorCommand {
	return DeletePersistCoordinatorCommand{key: key}
}

func NewDeletePersistCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.DeletePersistCoordinatorMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewDeletePersistCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return NewDeletePersistCoordinatorCommand(msg.GetMetaKeys()[0]), nil
}

func (c DeletePersistCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryDeletePersistCoordinatorCmdId
}

func (c DeletePersistCoordinatorCommand) toMsg() proto.Message {
	return &common.DeletePersistCoordinatorMsg{MetaKeys: []string{c.key}}
}

func (c DeletePersistCoordinatorCommand) GetTxId() TxId {
	return TxId{}
}

func (c DeletePersistCoordinatorCommand) IsSingleShot() bool {
	return true // no 2PC since no remote participants exist
}

func (c DeletePersistCoordinatorCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return false
}

func (c DeletePersistCoordinatorCommand) Commit(n *NodeServer) {
	n.inodeMgr.CommitDeletePersistMeta(c.key, n.dirtyMgr)
}

func (c DeletePersistCoordinatorCommand) RemoteCommit(*RaftGroupMgr) []ParticipantOp {
	// nothing to do since no remote participants exist
	return nil
}

type UpdateNodeListCoordinatorCommand struct {
	txId        TxId
	migrationId MigrationId
	target      RaftNode
	isAdd       bool
	remotes     map[TxId]RaftNode
	commitTxId  TxId
	selfNode    RaftNode
}

func NewUpdateNodeListCoordinatorCommand(txId TxId, migrationId MigrationId, target RaftNode, isAdd bool, selfNode RaftNode) UpdateNodeListCoordinatorCommand {
	return UpdateNodeListCoordinatorCommand{
		txId: txId, migrationId: migrationId, target: target, isAdd: isAdd, remotes: make(map[TxId]RaftNode),
		selfNode: selfNode,
	}
}

func (c *UpdateNodeListCoordinatorCommand) AddNode(txRet TxRet) {
	if txRet.ret.node != c.selfNode {
		c.remotes[txRet.txId] = txRet.ret.node
	}
	c.commitTxId = txRet.txId.GetNext()
}

func NewUpdateNodeListCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.UpdateNodeListCoordinatorMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewUpdateNodeListCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	commitTxId := NewTxIdFromMsg(msg.GetCommitTxId())
	migrationId := NewMigrationIdFromMsg(msg.GetMigrationId())
	target := NewRaftNodeFromMsg(msg.GetTarget())
	r := make(map[TxId]RaftNode)
	for _, remote := range msg.GetRemotes() {
		r[NewTxIdFromMsg(remote.GetTxId())] = NewRaftNodeFromMsg(remote.GetNode())
	}
	return UpdateNodeListCoordinatorCommand{
		txId: txId, migrationId: migrationId, target: target, isAdd: msg.GetIsAdd(), remotes: r, commitTxId: commitTxId,
	}, nil
}

func (c UpdateNodeListCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryUpdateNodeListCoordinatorCmdId
}

func (c UpdateNodeListCoordinatorCommand) toMsg() proto.Message {
	remotes := make([]*common.UpdateNodeInfoMsg, 0)
	for txId, node := range c.remotes {
		remotes = append(remotes, &common.UpdateNodeInfoMsg{TxId: txId.toMsg(), Node: node.toMsg()})
	}
	return &common.UpdateNodeListCoordinatorMsg{
		TxId: c.txId.toMsg(), MigrationId: c.migrationId.toMsg(), Target: c.target.toMsg(),
		IsAdd: c.isAdd, Remotes: remotes, CommitTxId: c.commitTxId.toMsg(),
	}
}

func (c UpdateNodeListCoordinatorCommand) GetTxId() TxId {
	return c.txId
}

func (c UpdateNodeListCoordinatorCommand) IsSingleShot() bool {
	return false
}

func (c UpdateNodeListCoordinatorCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return true // no quick commits since remote participants always exist
}

func (c UpdateNodeListCoordinatorCommand) Commit(n *NodeServer) {
	n.dirtyMgr.CommitMigratedDataLocal(n.inodeMgr, c.migrationId)
	n.raftGroup.CommitUpdate([]RaftNode{c.target}, c.isAdd, 0)
}

func (c UpdateNodeListCoordinatorCommand) RemoteCommit(_ *RaftGroupMgr) []ParticipantOp {
	fns := make([]ParticipantOp, 0)
	nextTxId := c.commitTxId
	for txId, node := range c.remotes {
		fns = append(fns, NewCommitParticipantOpForUpdateNode(nextTxId, txId, node))
		nextTxId = nextTxId.GetNext()
	}
	return fns
}

type RenameMetaInfo struct {
	meta   *WorkingMeta
	oldKey string
	newKey string
}

func NewRenameMetaInfo(meta *WorkingMeta, oldKey string, newKey string) RenameMetaInfo {
	return RenameMetaInfo{meta: meta, oldKey: oldKey, newKey: newKey}
}

func NewRenameMetaInfoFromMsg(msg *common.RenameMetaInfoMsg) RenameMetaInfo {
	return RenameMetaInfo{meta: NewWorkingMetaFromMsg(msg.GetMeta()), oldKey: msg.GetOldKey(), newKey: msg.GetNewKey()}
}

func (r *RenameMetaInfo) toMsg() *common.RenameMetaInfoMsg {
	return &common.RenameMetaInfoMsg{Meta: r.meta.toMsg(), OldKey: r.oldKey, NewKey: r.newKey}
}

type UpdateParentInfo struct {
	inodeKey    InodeKeyType
	key         string
	addInodeKey InodeKeyType
	addChild    string
	removeChild string
	childIsDir  bool
}

func NewUpdateParentInfo(inodeKey InodeKeyType, key string, addInodeKey InodeKeyType, addChild string, removedChild string, childIsDir bool) *UpdateParentInfo {
	return &UpdateParentInfo{inodeKey: inodeKey, key: key, addInodeKey: addInodeKey, addChild: addChild, removeChild: removedChild, childIsDir: childIsDir}
}

func NewUpdateParentInfoFromMsg(msg *common.UpdateParentMetaInfoMsg) *UpdateParentInfo {
	return &UpdateParentInfo{
		inodeKey: InodeKeyType(msg.GetInodeKey()), key: msg.GetKey(), addInodeKey: InodeKeyType(msg.GetAddInodeKey()),
		addChild: msg.GetAddChild(), removeChild: msg.GetRemoveChild(), childIsDir: msg.GetChildIsDir(),
	}
}

func (r *UpdateParentInfo) toMsg() *common.UpdateParentMetaInfoMsg {
	return &common.UpdateParentMetaInfoMsg{
		InodeKey: uint64(r.inodeKey), Key: r.key, AddInodeKey: uint64(r.addInodeKey),
		AddChild: r.addChild, RemoveChild: r.removeChild, ChildIsDir: r.childIsDir,
	}
}

type RenameCoordinatorCommand struct {
	txId       TxId
	metas      []RenameMetaInfo
	parents    []*UpdateParentInfo
	remotes    []ParticipantTx
	commitTxId TxId
}

func NewRenameCoordinatorCommand(txId TxId) RenameCoordinatorCommand {
	return RenameCoordinatorCommand{txId: txId}
}

func (r *RenameCoordinatorCommand) AddMeta(txRet TxRet, oldKey string, newKey string, selfGroup string) {
	if txRet.ret.node.groupId == selfGroup {
		r.metas = append(r.metas, NewRenameMetaInfo(txRet.ret.ext.(*GetWorkingMetaRet).meta, oldKey, newKey))
	} else {
		r.remotes = append(r.remotes, NewParticipantTx(txRet))
	}
	r.commitTxId = txRet.txId.GetNext()
}

func (r *RenameCoordinatorCommand) AddParent(txRet TxRet, selfGroup string) {
	if txRet.ret.node.groupId == selfGroup {
		r.parents = append(r.parents, txRet.ret.ext.(UpdateParentRet).info)
	} else {
		r.remotes = append(r.remotes, NewParticipantTx(txRet))
	}
	r.commitTxId = txRet.txId.GetNext()
}

func NewRenameCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.RenameCoordinatorMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewRenameCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	ret := RenameCoordinatorCommand{txId: NewTxIdFromMsg(msg.GetTxId()), commitTxId: NewTxIdFromMsg(msg.GetCommitTxId())}
	for _, metaMsg := range msg.GetMetas() {
		ret.metas = append(ret.metas, NewRenameMetaInfoFromMsg(metaMsg))
	}
	for _, parentMsg := range msg.GetParents() {
		ret.parents = append(ret.parents, NewUpdateParentInfoFromMsg(parentMsg))
	}
	for _, remoteMsg := range msg.GetRemotes() {
		ret.remotes = append(ret.remotes, NewParticipantTxFromMsg(remoteMsg))
	}
	return ret, nil
}

func (c RenameCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryRenameCoordinatorCmdId
}

func (c RenameCoordinatorCommand) toMsg() proto.Message {
	ret := &common.RenameCoordinatorMsg{TxId: c.txId.toMsg(), CommitTxId: c.commitTxId.toMsg()}
	for _, meta := range c.metas {
		ret.Metas = append(ret.Metas, meta.toMsg())
	}
	for _, parent := range c.parents {
		ret.Parents = append(ret.Parents, parent.toMsg())
	}
	for _, remote := range c.remotes {
		ret.Remotes = append(ret.Remotes, remote.toMsg())
	}
	return ret
}

func (c RenameCoordinatorCommand) GetTxId() TxId {
	return c.txId
}

func (c RenameCoordinatorCommand) IsSingleShot() bool {
	return false
}

func (c RenameCoordinatorCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return len(c.remotes) > 0
}

func (c RenameCoordinatorCommand) Commit(n *NodeServer) {
	for _, parent := range c.parents {
		n.inodeMgr.CommitUpdateParentMeta(parent, n.dirtyMgr)
	}
	for _, meta := range c.metas {
		n.inodeMgr.CommitUpdateInodeToFile(meta.meta, nil, meta.oldKey, meta.newKey, n.dirtyMgr)
	}
}

func (c RenameCoordinatorCommand) RemoteCommit(_ *RaftGroupMgr) []ParticipantOp {
	ret := make([]ParticipantOp, 0)
	nextTxId := c.commitTxId
	for _, remote := range c.remotes {
		ret = append(ret, NewCommitParticipantOp(nextTxId, remote.txId, remote.groupId))
		nextTxId = nextTxId.GetNext()
	}
	return ret
}

type CreateCoordinatorCommand struct {
	txId       TxId
	newKey     string
	meta       *WorkingMeta
	parent     InodeKeyType
	parentTx   ParticipantTx
	commitTxId TxId
}

func NewCreateCoordinatorCommand(txId TxId, newKey string, metaTx TxRet, parentTx TxRet, raftGroup *RaftGroupMgr, commitTxId TxId) CreateCoordinatorCommand {
	ret := CreateCoordinatorCommand{txId: txId, newKey: newKey, meta: metaTx.ret.asWorkingMeta(), commitTxId: commitTxId}
	if parentTx.ret.node.groupId == raftGroup.selfGroup {
		ret.parent = parentTx.ret.ext.(UpdateParentRet).info.inodeKey
	} else {
		ret.parentTx = NewParticipantTx(parentTx)
	}
	return ret
}

func NewCreateCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.CreateCoordinatorMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewCreateCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	ret := CreateCoordinatorCommand{
		txId: NewTxIdFromMsg(msg.GetTxId()), newKey: msg.GetNewKey(), meta: NewWorkingMetaFromMsg(msg.GetMeta()),
		commitTxId: NewTxIdFromMsg(msg.GetCommitTxId()),
	}
	if len(msg.GetRemoteParent()) == 0 {
		ret.parent = InodeKeyType(msg.GetParentInodeKey())
	} else {
		ret.parentTx = NewParticipantTxFromMsg(msg.GetRemoteParent()[0])
	}
	return ret, nil
}

func (c CreateCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryCreateCoordinatorCmdId
}

func (c CreateCoordinatorCommand) toMsg() proto.Message {
	ret := &common.CreateCoordinatorMsg{
		TxId: c.txId.toMsg(), NewKey: c.newKey, Meta: c.meta.toMsg(), CommitTxId: c.commitTxId.toMsg(),
	}
	if c.parent != 0 {
		ret.ParentInodeKey = uint64(c.parent)
	} else {
		ret.RemoteParent = []*common.ParticipantTxMsg{c.parentTx.toMsg()}
	}
	return ret
}

func (c CreateCoordinatorCommand) GetTxId() TxId {
	return c.txId
}

func (c CreateCoordinatorCommand) IsSingleShot() bool {
	return false
}

func (c CreateCoordinatorCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return c.parent == 0
}

func (c CreateCoordinatorCommand) Commit(n *NodeServer) {
	if c.parent != 0 {
		n.inodeMgr.CommitUpdateParentMeta(NewUpdateParentInfo(c.parent, filepath.Dir(c.newKey), c.meta.inodeKey, filepath.Base(c.newKey), "", c.meta.IsDir()), n.dirtyMgr)
	}
	n.inodeMgr.CommitCreateMeta(c.meta, c.parent, c.newKey, n.dirtyMgr)
}

func (c CreateCoordinatorCommand) RemoteCommit(_ *RaftGroupMgr) []ParticipantOp {
	ret := make([]ParticipantOp, 0)
	if c.parent == 0 {
		ret = append(ret, NewCommitParticipantOp(c.commitTxId, c.parentTx.txId, c.parentTx.groupId))
	}
	return ret
}

type DeleteCoordinatorCommand struct {
	txId          TxId
	key           string
	meta          *WorkingMeta
	parent        InodeKeyType
	parentTx      ParticipantTx
	localOffsets  map[int64]int64
	remoteOffsets map[int64]int64
	commitTxId    TxId
}

func NewDeleteCoordinatorCommand(txId TxId, key string, prevSize int64, metaTx TxRet, parentTx TxRet, raftGroup *RaftGroupMgr, commitTxId TxId) DeleteCoordinatorCommand {
	meta := metaTx.ret.asWorkingMeta()
	l, r := calcRemoteOffsets(meta, prevSize, raftGroup)
	ret := DeleteCoordinatorCommand{txId: txId, key: key, meta: meta, localOffsets: l, remoteOffsets: r, commitTxId: commitTxId}
	if parentTx.ret.node.groupId == raftGroup.selfGroup {
		ret.parent = parentTx.ret.ext.(UpdateParentRet).info.inodeKey
	} else {
		ret.parentTx = NewParticipantTx(parentTx)
	}
	return ret
}

func NewDeleteCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.DeleteCoordinatorMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewDeleteCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	txId := NewTxIdFromMsg(msg.GetTxId())
	commitTxId := NewTxIdFromMsg(msg.GetCommitTxId())
	meta := NewWorkingMetaFromMsg(msg.GetMeta())
	l := make(map[int64]int64)
	for _, chunk := range msg.GetLocalChunks() {
		l[chunk.GetOffset()] = chunk.GetLength()
	}
	r := make(map[int64]int64)
	for _, chunk := range msg.GetRemoteChunks() {
		r[chunk.GetOffset()] = chunk.GetLength()
	}
	ret := DeleteCoordinatorCommand{txId: txId, key: msg.GetKey(), meta: meta, localOffsets: l, remoteOffsets: r, commitTxId: commitTxId}
	if len(msg.GetRemoteParent()) == 0 {
		ret.parent = InodeKeyType(msg.GetParent())
	} else {
		ret.parentTx = NewParticipantTxFromMsg(msg.GetRemoteParent()[0])
	}
	return ret, nil
}

func (c DeleteCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryDeleteCoordinatorCmdId
}

func (c DeleteCoordinatorCommand) toMsg() proto.Message {
	msg := &common.DeleteCoordinatorMsg{
		TxId: c.txId.toMsg(), Key: c.key, Meta: c.meta.toMsg(),
		LocalChunks: make([]*common.TruncateChunkInfoMsg, 0), RemoteChunks: make([]*common.TruncateChunkInfoMsg, 0),
		CommitTxId: c.commitTxId.toMsg(),
	}
	for off, len := range c.localOffsets {
		msg.LocalChunks = append(msg.LocalChunks, &common.TruncateChunkInfoMsg{Offset: off, Length: len})
	}
	for off, len := range c.remoteOffsets {
		msg.RemoteChunks = append(msg.RemoteChunks, &common.TruncateChunkInfoMsg{Offset: off, Length: len})
	}
	if c.parent != 0 {
		msg.Parent = uint64(c.parent)
	} else {
		msg.RemoteParent = []*common.ParticipantTxMsg{c.parentTx.toMsg()}
	}
	return msg
}

func (c DeleteCoordinatorCommand) GetTxId() TxId {
	return c.txId
}

func (c DeleteCoordinatorCommand) IsSingleShot() bool {
	return false
}

func (c DeleteCoordinatorCommand) NeedTwoPhaseCommit(*RaftGroupMgr) bool {
	return c.parent == 0 || len(c.remoteOffsets) > 0
}

func (c DeleteCoordinatorCommand) Commit(n *NodeServer) {
	n.inodeMgr.QuickCommitDeleteChunk(c.localOffsets, c.meta, n.dirtyMgr)
	if c.parent != 0 {
		n.inodeMgr.CommitUpdateParentMeta(NewUpdateParentInfo(c.parent, filepath.Dir(c.key), 0, "", filepath.Base(c.key), c.meta.IsDir()), n.dirtyMgr)
	}
	n.inodeMgr.CommitUpdateInodeToFile(c.meta, nil, c.key, "", n.dirtyMgr)
}

func (c DeleteCoordinatorCommand) RemoteCommit(_ *RaftGroupMgr) []ParticipantOp {
	fns := make([]ParticipantOp, 0)
	nextTxId := c.commitTxId
	for offset, length := range c.remoteOffsets {
		fns = append(fns, NewCommitDeleteChunkOp(nextTxId, c.meta, offset, length))
		nextTxId = nextTxId.GetNext()
	}
	if c.parent == 0 {
		fns = append(fns, NewCommitParticipantOp(nextTxId, c.parentTx.txId, c.parentTx.groupId))
		nextTxId = nextTxId.GetNext()
	}
	return fns
}

type TruncateCoordinatorCommand struct {
	txId          TxId
	meta          *WorkingMeta
	prevSize      int64
	localOffsets  map[int64]int64
	remoteOffsets map[int64]int64
	commitTxId    TxId
}

func calcRemoteOffsets(meta *WorkingMeta, prevSize int64, raftGroup *RaftGroupMgr) (map[int64]int64, map[int64]int64) {
	localOffsets := make(map[int64]int64)
	remoteOffsets := make(map[int64]int64)
	// shrink
	for offset := meta.size; offset < prevSize; {
		var length = meta.chunkSize
		slop := offset % meta.chunkSize
		if slop > 0 {
			length -= slop
		}
		if offset+length > meta.prevVer.size {
			length = meta.prevVer.size - offset
		}
		groupId, ok := raftGroup.GetChunkOwnerGroupId(meta.inodeKey, offset, meta.chunkSize)
		if ok && groupId == raftGroup.selfGroup {
			localOffsets[offset] = length
		} else {
			remoteOffsets[offset] = length
		}
		offset += length
	}
	// expand
	for offset := prevSize; offset < meta.size; {
		var length = meta.chunkSize
		slop := offset % meta.chunkSize
		if slop > 0 {
			length -= slop
		}
		if offset+length > meta.size {
			length = meta.size - offset
		}
		groupId, ok := raftGroup.GetChunkOwnerGroupId(meta.inodeKey, offset, meta.chunkSize)
		if ok && groupId == raftGroup.selfGroup {
			localOffsets[offset] = length
		} else {
			remoteOffsets[offset] = length
		}
		offset += length
	}
	return localOffsets, remoteOffsets
}

func NewTruncateCoordinatorCommand(txId TxId, metaTx TxRet, raftGroup *RaftGroupMgr, commitTxId TxId) TruncateCoordinatorCommand {
	meta := metaTx.ret.asWorkingMeta()
	l, r := calcRemoteOffsets(meta, meta.prevVer.size, raftGroup)
	return TruncateCoordinatorCommand{
		txId: txId, meta: meta, prevSize: meta.prevVer.size, localOffsets: l, remoteOffsets: r, commitTxId: commitTxId,
	}
}

func NewTruncateCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.TruncateCoordinatorMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewTruncateCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	meta := NewWorkingMetaFromMsg(msg.GetMeta())
	l := make(map[int64]int64)
	for _, chunk := range msg.GetLocalChunks() {
		l[chunk.GetOffset()] = chunk.GetLength()
	}
	r := make(map[int64]int64)
	for _, chunk := range msg.GetRemoteChunks() {
		r[chunk.GetOffset()] = chunk.GetLength()
	}
	return TruncateCoordinatorCommand{
		txId: NewTxIdFromMsg(msg.GetTxId()), meta: meta, prevSize: msg.GetPrevSize(),
		localOffsets: l, remoteOffsets: r, commitTxId: NewTxIdFromMsg(msg.GetCommitTxId()),
	}, nil
}

func (c TruncateCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryTruncateCoordinatorCmdId
}

func (c TruncateCoordinatorCommand) toMsg() proto.Message {
	ls := make([]*common.TruncateChunkInfoMsg, 0)
	for offset, length := range c.localOffsets {
		ls = append(ls, &common.TruncateChunkInfoMsg{Offset: offset, Length: length})
	}
	rs := make([]*common.TruncateChunkInfoMsg, 0)
	for offset, length := range c.remoteOffsets {
		rs = append(rs, &common.TruncateChunkInfoMsg{Offset: offset, Length: length})
	}
	return &common.TruncateCoordinatorMsg{
		TxId: c.txId.toMsg(), Meta: c.meta.toMsg(), PrevSize: c.prevSize,
		LocalChunks: ls, RemoteChunks: rs, CommitTxId: c.commitTxId.toMsg(),
	}
}

func (c TruncateCoordinatorCommand) GetTxId() TxId {
	return c.txId
}

func (c TruncateCoordinatorCommand) IsSingleShot() bool {
	return false
}

func (c TruncateCoordinatorCommand) NeedTwoPhaseCommit(raftGroup *RaftGroupMgr) bool {
	return len(c.remoteOffsets) > 0
}

func (c TruncateCoordinatorCommand) Commit(n *NodeServer) {
	if c.meta.size < c.prevSize {
		n.inodeMgr.QuickCommitDeleteChunk(c.localOffsets, c.meta, n.dirtyMgr) //shrink
	} else {
		n.inodeMgr.QuickCommitExpandChunk(c.localOffsets, c.meta, n.dirtyMgr) //expand
	}
	n.inodeMgr.CommitUpdateMeta(c.meta, n.dirtyMgr)
}

func (c TruncateCoordinatorCommand) RemoteCommit(raftGroup *RaftGroupMgr) []ParticipantOp {
	fns := make([]ParticipantOp, 0)
	nextTxId := c.commitTxId
	if c.meta.size < c.prevSize {
		for offset, length := range c.remoteOffsets {
			fns = append(fns, NewCommitDeleteChunkOp(nextTxId, c.meta, offset, length))
			nextTxId = nextTxId.GetNext()
		}
	} else {
		for offset, length := range c.remoteOffsets {
			fns = append(fns, NewCommitExpandChunkOp(nextTxId, c.meta, offset, length))
			nextTxId = nextTxId.GetNext()
		}
	}
	return fns
}

type FlushCoordinatorCommand struct {
	txId       TxId
	meta       *WorkingMeta
	chunks     []*common.UpdateChunkRecordMsg
	commitTxId TxId
}

func NewFlushCoordinatorCommand(txId TxId, chunks []*common.UpdateChunkRecordMsg, metaTx TxRet, commitTxId TxId) FlushCoordinatorCommand {
	return FlushCoordinatorCommand{
		txId: txId, meta: metaTx.ret.asWorkingMeta(), chunks: chunks, commitTxId: commitTxId,
	}
}

func NewFlushCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.FlushCoordinatorMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewFlushCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return FlushCoordinatorCommand{
		txId: NewTxIdFromMsg(msg.GetTxId()), meta: NewWorkingMetaFromMsg(msg.GetMeta()), chunks: msg.GetChunks(),
		commitTxId: NewTxIdFromMsg(msg.GetCommitTxId()),
	}, nil
}

func (c FlushCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryFlushCoordinatorCmdId
}

func (c FlushCoordinatorCommand) toMsg() proto.Message {
	return &common.FlushCoordinatorMsg{
		TxId: c.txId.toMsg(), Meta: c.meta.toMsg(), Chunks: c.chunks, CommitTxId: c.commitTxId.toMsg(),
	}
}

func (c FlushCoordinatorCommand) GetTxId() TxId {
	return c.txId
}

func (c FlushCoordinatorCommand) IsSingleShot() bool {
	return false
}

func (c FlushCoordinatorCommand) NeedTwoPhaseCommit(raftGroup *RaftGroupMgr) bool {
	for _, c := range c.chunks {
		if c.GroupId != raftGroup.selfGroup {
			return true
		}
	}
	return false
}

func (c FlushCoordinatorCommand) Commit(n *NodeServer) {
	n.inodeMgr.QuickCommitUpdateChunk(c.meta, n.raftGroup.selfGroup, c.chunks, n.dirtyMgr)
	n.inodeMgr.CommitUpdateMeta(c.meta, n.dirtyMgr)
}

func (c FlushCoordinatorCommand) RemoteCommit(raftGroup *RaftGroupMgr) []ParticipantOp {
	fns := make([]ParticipantOp, 0)
	nextTxId := c.commitTxId
	for _, chunk := range c.chunks {
		if chunk.GroupId != raftGroup.selfGroup {
			fns = append(fns, NewCommitUpdateChunkOp(nextTxId, chunk, c.meta))
			nextTxId = nextTxId.GetNext()
		}
	}
	return fns
}

type ParticipantTx struct {
	txId    TxId
	groupId string
}

func NewParticipantTx(txRet TxRet) ParticipantTx {
	return ParticipantTx{txId: txRet.txId, groupId: txRet.ret.node.groupId}
}

func NewParticipantTxFromMsg(msg *common.ParticipantTxMsg) ParticipantTx {
	return ParticipantTx{txId: NewTxIdFromMsg(msg.GetTxId()), groupId: msg.GetGroupId()}
}

func (p *ParticipantTx) toMsg() *common.ParticipantTxMsg {
	return &common.ParticipantTxMsg{TxId: p.txId.toMsg(), GroupId: p.groupId}
}

type HardLinkCoordinatorCommand struct {
	txId       TxId
	newKey     string
	meta       *WorkingMeta
	parent     InodeKeyType
	parentTx   ParticipantTx
	commitTxId TxId
}

func NewHardLinkCoordinatorCommand(txId TxId, groupId string, newKey string, metaTx TxRet, parentTx TxRet, commitTxId TxId) HardLinkCoordinatorCommand {
	ret := HardLinkCoordinatorCommand{
		txId: txId, newKey: newKey, meta: metaTx.ret.asWorkingMeta(), commitTxId: commitTxId,
	}
	if groupId == parentTx.ret.node.groupId {
		ret.parent = parentTx.ret.ext.(UpdateParentRet).info.inodeKey
	} else {
		ret.parentTx = NewParticipantTx(parentTx)
	}
	return ret
}

func NewHardLinkCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.HardLinkCoordinatorMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewHardLinkCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	cmd := HardLinkCoordinatorCommand{
		txId: NewTxIdFromMsg(msg.GetTxId()), newKey: msg.GetNewKey(), meta: NewWorkingMetaFromMsg(msg.GetMeta()),
		commitTxId: NewTxIdFromMsg(msg.GetCommitTxId()),
	}
	if msg.GetRemoteParent() == nil {
		cmd.parent = InodeKeyType(msg.GetParent())
	} else {
		cmd.parentTx = NewParticipantTxFromMsg(msg.GetRemoteParent()[0])
	}
	return cmd, nil
}

func (c HardLinkCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryHardLinkCoordinatorCmdId
}

func (c HardLinkCoordinatorCommand) toMsg() proto.Message {
	ret := &common.HardLinkCoordinatorMsg{
		TxId: c.txId.toMsg(), NewKey: c.newKey, Meta: c.meta.toMsg(), CommitTxId: c.commitTxId.toMsg(),
	}
	if c.parent != 0 {
		ret.Parent = uint64(c.parent)
	} else {
		ret.RemoteParent = []*common.ParticipantTxMsg{c.parentTx.toMsg()}
	}
	return ret
}

func (c HardLinkCoordinatorCommand) GetTxId() TxId {
	return c.txId
}

func (c HardLinkCoordinatorCommand) IsSingleShot() bool {
	return false
}

func (c HardLinkCoordinatorCommand) NeedTwoPhaseCommit(raftGroup *RaftGroupMgr) bool {
	return c.parent == 0
}

func (c HardLinkCoordinatorCommand) Commit(n *NodeServer) {
	if c.parent != 0 {
		n.inodeMgr.CommitUpdateParentMeta(NewUpdateParentInfo(c.parent, filepath.Dir(c.newKey), c.meta.inodeKey, filepath.Base(c.newKey), "", c.meta.IsDir()), n.dirtyMgr)
	}
	n.inodeMgr.CommitUpdateMeta(c.meta, n.dirtyMgr)
}

func (c HardLinkCoordinatorCommand) RemoteCommit(_ *RaftGroupMgr) []ParticipantOp {
	fns := make([]ParticipantOp, 0)
	if c.parent == 0 {
		fns = append(fns, NewCommitParticipantOp(c.commitTxId, c.parentTx.txId, c.parentTx.groupId))
	}
	return fns
}

type CompactLogCoordinatorCommand struct {
	s *Snapshot
}

func NewCompactLogCoordinatorCommand(s *Snapshot) CompactLogCoordinatorCommand {
	ret := CompactLogCoordinatorCommand{s: s}
	return ret
}

func NewCompactLogCoordinatorCommandFromExtBuf(extBuf []byte) (ExtLogCommandImpl, error) {
	msg := &common.SnapshotMsg{}
	if err := proto.Unmarshal(extBuf, msg); err != nil {
		log.Errorf("Failed: NewCompactLogCoordinatorCommandFromExtBuf, Unmarshal, err=%v", err)
		return nil, err
	}
	return CompactLogCoordinatorCommand{s: NewSnapshotFromMsg(msg)}, nil
}

func (c CompactLogCoordinatorCommand) GetExtCmdId() uint16 {
	return AppendEntryCompactLogCoordinatorCmdId
}

func (c CompactLogCoordinatorCommand) toMsg() proto.Message {
	return c.s.toMsg()
}

func (c CompactLogCoordinatorCommand) GetTxId() TxId {
	return TxId{ClientId: c.s.migrationId.ClientId, SeqNum: c.s.migrationId.SeqNum}
}

func (c CompactLogCoordinatorCommand) IsSingleShot() bool {
	return false
}

func (c CompactLogCoordinatorCommand) NeedTwoPhaseCommit(raftGroup *RaftGroupMgr) bool {
	return false
}

func (c CompactLogCoordinatorCommand) Commit(n *NodeServer) {
	if !n.raft.replaying {
		n.raft.SwitchExtLog(c.s.newExtLogId)
		return
	}

	n.inodeMgr.RestoreMetas(c.s.metas, c.s.files)
	n.inodeMgr.RestoreInodeTree(c.s.dirents)
	for _, entry := range c.s.dirtyChunks {
		for _, chunk := range entry.GetChunks() {
			c := n.inodeMgr.GetChunk(InodeKeyType(entry.GetInodeKey()), chunk.GetOffset(), entry.GetChunkSize())
			working := c.NewWorkingChunk(entry.GetVersion())
			working.AddStagingChunkFromAddMsg(chunk)
			c.lock.Lock()
			c.AddWorkingChunk(n.inodeMgr, working, working.prevVer)
			c.lock.Unlock()
		}
	}

	n.dirtyMgr.lock.Lock()
	for _, metaMsg := range c.s.dirtyMetas {
		inodeKey := InodeKeyType(metaMsg.GetInodeKey())
		n.dirtyMgr.mTable[inodeKey] = DirtyMetaInfo{version: metaMsg.GetVersion(), timestamp: metaMsg.GetTimestamp()}
		if metaMsg.GetExpireMs() > 0 {
			n.dirtyMgr.expireTable.ReplaceOrInsert(ExpireInfo{inodeKey: inodeKey, timestamp: metaMsg.GetTimestamp(), expireMs: metaMsg.GetExpireMs()})
		}
	}
	for _, entry := range c.s.dirtyChunks {
		for _, chunk := range entry.GetChunks() {
			n.dirtyMgr.AddChunkNoLock(InodeKeyType(entry.GetInodeKey()), entry.GetChunkSize(), entry.GetVersion(), chunk.GetOffset(), entry.GetObjectSize())
		}
	}
	n.dirtyMgr.lock.Unlock()
	if c.s.nodeList != nil {
		n.raftGroup.ResetWithRaftNodeListMsg(c.s.nodeList)
	}
}

func (c CompactLogCoordinatorCommand) RemoteCommit(_ *RaftGroupMgr) []ParticipantOp {
	return nil
}

var extCommandNewFns = map[uint16]func([]byte) (ExtLogCommandImpl, error){
	AppendEntryUpdateMetaCmdId:                NewUpdateMetaCommandFromExtbuf,
	AppendEntryUpdateParentMetaCmdId:          NewUpdateParentMetaCommandFromExtbuf,
	AppendEntryCreateMetaCmdId:                NewCreateMetaCommandFromExtbuf,
	AppendEntryDeleteMetaCmdId:                NewDeleteMetaCommandFromExtbuf,
	AppendEntryUpdateMetaKeyCmdId:             NewUpdateMetaKeyCommandFromExtbuf,
	AppendEntryUpdateNodeListCmdId:            NewUpdateNodeListCommandFromExtbuf,
	AppendEntryCreateChunkCmdId:               NewCreateChunkCommandFromExtbuf,
	AppendEntryUpdateChunkCmdId:               NewUpdateChunkCommandFromExtbuf,
	AppendEntryCommitChunkCmdId:               NewCommitChunkCommandFromExtbuf,
	AppendEntryAbortTxCmdId:                   NewAbortTxCommandFromExtbuf,
	AppendEntryPersistChunkCmdId:              NewPersistChunkCommandFromExtbuf,
	AppendEntryAddInodeFileMapCmdId:           NewAddInodeFileMapCommandFromExtbuf,
	AppendEntryDropLRUChunksCmdId:             NewDropLRUChunksCommandFromExtbuf,
	AppendEntryUpdateMetaAttrCmdId:            NewUpdateMetaAttrCommandFromExtbuf,
	AppendEntryDeleteInodeFileMapCmdId:        NewDeleteInodeFileMapCommandFromExtbuf,
	AppendEntryRemoveNonDirtyChunksCmdId:      NewRemoveNonDirtyChunksCommandFromExtbuf,
	AppendEntryForgetAllDirtyLogCmdId:         NewForgetAllDirtyLogCommandFromExtbuf,
	AppendEntryRecordMigratedAddMetaCmdId:     NewRecordMigratedAddMetaCommandFromExtbuf,
	AppendEntryRecordMigratedRemoveMetaCmdId:  NewRecordMigratedRemoveMetaCommandFromExtbuf,
	AppendEntryRecordMigratedAddChunkCmdId:    NewRecordMigratedAddChunkCommandFromExtbuf,
	AppendEntryRecordMigratedRemoveChunkCmdId: NewRecordMigratedRemoveChunkCommandFromExtbuf,

	AppendEntryFlushCoordinatorCmdId:          NewFlushCoordinatorCommandFromExtBuf,
	AppendEntryTruncateCoordinatorCmdId:       NewTruncateCoordinatorCommandFromExtBuf,
	AppendEntryHardLinkCoordinatorCmdId:       NewHardLinkCoordinatorCommandFromExtBuf,
	AppendEntryDeleteCoordinatorCmdId:         NewDeleteCoordinatorCommandFromExtBuf,
	AppendEntryBeginPersistCmdId:              NewBeginPersistCommandFromExtBuf,
	AppendEntryPersistCoordinatorCmdId:        NewPersistCoordinatorCommandFromExtBuf,
	AppendEntryDeletePersistCoordinatorCmdId:  NewDeletePersistCoordinatorCommandFromExtBuf,
	AppendEntryUpdateNodeListCoordinatorCmdId: NewUpdateNodeListCoordinatorCommandFromExtBuf,
	AppendEntryCreateCoordinatorCmdId:         NewCreateCoordinatorCommandFromExtBuf,
	AppendEntryRenameCoordinatorCmdId:         NewRenameCoordinatorCommandFromExtBuf,
	AppendEntryCompactLogCoordinatorCmdId:     NewCompactLogCoordinatorCommandFromExtBuf,
}
