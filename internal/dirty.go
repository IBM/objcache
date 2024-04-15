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
	"github.com/serialx/hashring"
	"google.golang.org/protobuf/proto"

	"github.com/IBM/objcache/common"
)

type DirtyChunkInfo struct {
	OffsetVersions map[int64]uint32
	chunkSize      int64
	objectSize     int64
}

func NewDirtyChunkInfoFromMsg(msg *common.DirtyChunkInfoMsg) DirtyChunkInfo {
	ret := DirtyChunkInfo{OffsetVersions: make(map[int64]uint32), chunkSize: msg.GetChunkSize(), objectSize: msg.GetObjectSize()}
	for i := 0; i < len(msg.GetOffsets()); i++ {
		ret.OffsetVersions[msg.GetOffsets()[i]] = msg.GetVersions()[i]
	}
	return ret
}

type ExpireInfo struct {
	inodeKey  InodeKeyType
	timestamp int64
	expireMs  int32
}

func NewExpireInfoFromMsg(msg *common.DirtyMetaInfoMsg) ExpireInfo {
	return ExpireInfo{inodeKey: InodeKeyType(msg.GetInodeKey()), timestamp: msg.GetTimestamp(), expireMs: msg.GetExpireMs()}
}
func NewExpireInfoFromMeta(meta *WorkingMeta, timestamp int64) ExpireInfo {
	return ExpireInfo{inodeKey: meta.inodeKey, timestamp: timestamp, expireMs: meta.expireMs}
}

func (i ExpireInfo) Less(l btree.Item) bool {
	r := l.(ExpireInfo)
	left := i.timestamp + int64(i.expireMs)*1e6
	right := r.timestamp + int64(r.expireMs)*1e6
	if left != right {
		return left < right
	}
	return i.inodeKey < r.inodeKey
}

type DirtyMetaInfo struct {
	version   uint32
	timestamp int64
	expireMs  int32
}

func NewDirtyMetaInfoFromMsg(msg *common.DirtyMetaInfoMsg) DirtyMetaInfo {
	return DirtyMetaInfo{version: msg.GetVersion(), timestamp: msg.GetTimestamp(), expireMs: msg.GetExpireMs()}
}

func NewDirtyMetaInfoFromMeta(meta *WorkingMeta) DirtyMetaInfo {
	return DirtyMetaInfo{version: meta.version, timestamp: time.Now().UnixNano(), expireMs: meta.expireMs}
}

type DeletedFileInfo struct {
	inodeKey  InodeKeyType
	timestamp int64
	expireMs  int32
}

func NewDeleteFileInfoFromMsg(msg *common.DeletedFileInfoMsg) DeletedFileInfo {
	return DeletedFileInfo{inodeKey: InodeKeyType(msg.GetInodeKey()), timestamp: msg.GetTimestamp(), expireMs: msg.GetExpireMs()}
}

func NewDeleteFileInfoFromMeta(meta *WorkingMeta) DeletedFileInfo {
	return DeletedFileInfo{inodeKey: meta.inodeKey, timestamp: time.Now().UnixNano(), expireMs: meta.expireMs}
}

type ExpireDeleteInfo struct {
	key       string
	timestamp int64
	expireMs  int32
}

func NewExpireDeleteInfoFromMsg(msg *common.DeletedFileInfoMsg) ExpireDeleteInfo {
	return ExpireDeleteInfo{key: msg.GetKey(), timestamp: msg.GetTimestamp(), expireMs: msg.GetExpireMs()}
}
func NewExpireDeleteInfoFromMeta(key string, meta *WorkingMeta, timestamp int64) ExpireDeleteInfo {
	return ExpireDeleteInfo{key: key, timestamp: timestamp, expireMs: meta.expireMs}
}

func (i ExpireDeleteInfo) Less(l btree.Item) bool {
	r := l.(ExpireDeleteInfo)
	left := i.timestamp + int64(i.expireMs)*1e6
	right := r.timestamp + int64(r.expireMs)*1e6
	if left != right {
		return left < right
	}
	return i.key < r.key
}

//////////////////////////////////////////////////////////////////////////

type MigrationId struct {
	ClientId uint32
	SeqNum   uint32
}

func NewMigrationIdFromMsg(migrationId *common.MigrationIdMsg) MigrationId {
	return MigrationId{ClientId: migrationId.GetClientId(), SeqNum: migrationId.GetSeqNum()}
}

func (m *MigrationId) toMsg() *common.MigrationIdMsg {
	return &common.MigrationIdMsg{ClientId: m.ClientId, SeqNum: m.SeqNum}
}

type DirtyMgr struct {
	cTable           map[InodeKeyType]DirtyChunkInfo // key: inodeKey
	mTable           map[InodeKeyType]DirtyMetaInfo  // DirtyMetaInfo
	addChildTable    map[InodeKeyType]map[string]bool
	removeChildTable map[InodeKeyType]map[string]bool
	expireTable      *btree.BTree
	dTable           map[string]DeletedFileInfo // deleted file paths
	expireDTable     *btree.BTree
	migrating        map[MigrationId]*common.MigrationMsg

	lock          *sync.RWMutex
	migratingLock *sync.RWMutex
}

func NewDirtyMgr() *DirtyMgr {
	ret := &DirtyMgr{
		cTable:           make(map[InodeKeyType]DirtyChunkInfo),
		mTable:           make(map[InodeKeyType]DirtyMetaInfo),
		addChildTable:    make(map[InodeKeyType]map[string]bool),
		removeChildTable: make(map[InodeKeyType]map[string]bool),
		expireTable:      btree.New(3),
		dTable:           make(map[string]DeletedFileInfo),
		expireDTable:     btree.New(3),
		lock:             new(sync.RWMutex),
		migrating:        make(map[MigrationId]*common.MigrationMsg),
		migratingLock:    new(sync.RWMutex),
	}
	return ret
}

func NewDirtyMgrFromMsg(msg *common.DirtyMgrSnapshotMsg) *DirtyMgr {
	ret := NewDirtyMgr()
	for _, chunkInfo := range msg.GetCTable() {
		ret.cTable[InodeKeyType(chunkInfo.GetInodeKey())] = NewDirtyChunkInfoFromMsg(chunkInfo)
	}
	for _, metaInfo := range msg.GetMTable() {
		ret.mTable[InodeKeyType(metaInfo.GetInodeKey())] = NewDirtyMetaInfoFromMsg(metaInfo)
		if metaInfo.GetExpireMs() > 0 {
			ret.expireTable.ReplaceOrInsert(NewExpireInfoFromMsg(metaInfo))
		}
	}
	for _, fileInfo := range msg.GetDTable() {
		ret.dTable[fileInfo.GetKey()] = NewDeleteFileInfoFromMsg(fileInfo)
		if fileInfo.GetExpireMs() > 0 {
			ret.expireDTable.ReplaceOrInsert(NewExpireDeleteInfoFromMsg(fileInfo))
		}
	}
	for _, migrationInfo := range msg.GetMigrating() {
		ret.migrating[NewMigrationIdFromMsg(migrationInfo.GetMigrationId())] = migrationInfo
	}
	return ret
}

func (d *DirtyMgr) CheckReset() (ok bool) {
	ok = true
	if ok2 := d.lock.TryLock(); !ok2 {
		log.Errorf("Failed: DirtyMgr.CheckReset, d.lock is taken")
		ok = false
	} else {
		if ok2 := len(d.cTable) == 0; !ok2 {
			log.Errorf("Failed: DirtyMgr.CheckReset, len(d.cTable) != 0")
			ok = false
		}
		if ok2 := len(d.dTable) == 0; !ok2 {
			log.Errorf("Failed: DirtyMgr.CheckReset, len(d.dTable) != 0")
			ok = false
		}
		if ok2 := len(d.mTable) == 0; !ok2 {
			log.Errorf("Failed: DirtyMgr.CheckReset, len(d.mTable) != 0")
			ok = false
		}
		if ok2 := d.expireTable.Len() == 0; !ok2 {
			log.Errorf("Failed: DirtyMgr.CheckReset, d.expireTable.Len() != 0")
			ok = false
		}
		if ok2 := d.expireDTable.Len() == 0; !ok2 {
			log.Errorf("Failed: DirtyMgr.CheckReset, d.expireDTable.Len() != 0")
			ok = false
		}
		d.lock.Unlock()
	}
	if ok2 := d.migratingLock.TryLock(); !ok2 {
		log.Errorf("Failed: DirtyMgr.CheckReset, d.migratingLock is taken")
		ok = false
	} else {
		if ok2 := len(d.migrating) == 0; !ok2 {
			log.Errorf("Failed: DirtyMgr.CheckReset, len(d.migrating) != 0")
			ok = false
		}
		d.migratingLock.Unlock()
	}
	return
}

func (d *DirtyMgr) AddChunkNoLock(inodeKey InodeKeyType, chunkSize int64, chunkVer uint32, offset int64, objectSize int64) {
	aligned := offset - offset%chunkSize
	c, ok := d.cTable[inodeKey]
	if !ok {
		d.cTable[inodeKey] = DirtyChunkInfo{OffsetVersions: map[int64]uint32{aligned: chunkVer}, chunkSize: chunkSize, objectSize: objectSize}
	} else {
		c.OffsetVersions[aligned] = chunkVer
		c.objectSize = objectSize
		d.cTable[inodeKey] = c
	}
	//log.Debugf("Success: DirtyMgr.AddChunkNoLock, metaKey=%v, working=%v", metaKey, *chunk)
}

func (d *DirtyMgr) RemoveChunkNoLock(inodeKey InodeKeyType, offset int64, chunkVer uint32) {
	if c, ok := d.cTable[inodeKey]; ok {
		if version, ok2 := c.OffsetVersions[offset]; ok2 {
			if version != chunkVer {
				log.Errorf("DirtyMgr.RemoveChunkNoLock, ignore stale remove request, inodeKey=%v, offset=%v, chunkVer=%v", inodeKey, offset, chunkVer)
				return
			}
			delete(c.OffsetVersions, offset)
		}
	}
	if len(d.cTable[inodeKey].OffsetVersions) == 0 {
		delete(d.cTable, inodeKey)
	}
	//log.Debugf("Success: DirtyMgr.RemoveChunkNoLock, metaKey=%v, offset=%v, chunkVer=%v", metaKey, offset, chunkVer)
}

func (d *DirtyMgr) IsDirtyChunk(chunk *Chunk) bool {
	d.lock.RLock()
	_, ok := d.cTable[chunk.inodeKey]
	d.lock.RUnlock()
	return ok
}

func (d *DirtyMgr) GetLikelyDirtyChunkInodeIds() []InodeKeyType {
	ret := make([]InodeKeyType, 0)
	d.lock.RLock()
	for inodeId := range d.cTable {
		ret = append(ret, inodeId)
	}
	d.lock.RUnlock()
	return ret
}

func (d *DirtyMgr) RemoveNonDirtyChunks(fps []uint64) {
	d.lock.Lock()
	for _, inodeKey := range fps {
		delete(d.cTable, InodeKeyType(inodeKey))
	}
	d.lock.Unlock()
}

func (d *DirtyMgr) AddChildMetaNoLock(meta *WorkingMeta, name string) {
	dirtyMap, ok := d.removeChildTable[meta.inodeKey]
	if ok {
		delete(dirtyMap, name)
		if len(dirtyMap) == 0 {
			delete(d.removeChildTable, meta.inodeKey)
		}
		return
	}
	dirtyMap, ok = d.addChildTable[meta.inodeKey]
	if !ok {
		dirtyMap = make(map[string]bool)
		d.addChildTable[meta.inodeKey] = dirtyMap
	}
	dirtyMap[name] = true
}

func (d *DirtyMgr) RemoveChildMetaNoLock(meta *WorkingMeta, name string) {
	dirtyMap, ok := d.addChildTable[meta.inodeKey]
	if ok {
		delete(dirtyMap, name)
		if len(dirtyMap) == 0 {
			delete(d.addChildTable, meta.inodeKey)
		}
		return
	}
	dirtyMap, ok = d.removeChildTable[meta.inodeKey]
	if !ok {
		dirtyMap = make(map[string]bool)
		d.removeChildTable[meta.inodeKey] = dirtyMap
	}
	dirtyMap[name] = true
}

func (d *DirtyMgr) AddMetaNoLock(meta *WorkingMeta) {
	old, ok := d.mTable[meta.inodeKey]
	info := NewDirtyMetaInfoFromMeta(meta)
	d.mTable[meta.inodeKey] = info
	if meta.expireMs > 0 {
		if ok {
			d.expireTable.Delete(ExpireInfo{expireMs: old.expireMs, timestamp: old.timestamp, inodeKey: meta.inodeKey})
		}
		d.expireTable.ReplaceOrInsert(NewExpireInfoFromMeta(meta, info.timestamp))
	}
	log.Debugf("Success: DirtyMgr.AddMetaNoLock, inodeKey=%v", meta.inodeKey)
}

func (d *DirtyMgr) RemoveMetaNoLock(inodeId InodeKeyType) {
	metaInfo, ok := d.mTable[inodeId]
	if ok {
		delete(d.mTable, inodeId)
		if metaInfo.expireMs > 0 {
			d.expireTable.Delete(ExpireInfo{expireMs: metaInfo.expireMs, timestamp: metaInfo.timestamp, inodeKey: inodeId})
		}
		log.Debugf("Success: DirtyMgr.RemoveMetaNoLock, metaInfo=%v", metaInfo)
	}
}

func (d *DirtyMgr) RemoveChunkNoLockAllOffsets(inodeId InodeKeyType) {
	chunkInfo, ok := d.cTable[inodeId]
	if ok {
		delete(d.cTable, inodeId)
	}
	log.Debugf("Success: DirtyMgr.RemoveChunkNoLockAllOffsets, chunkInfo=%v", chunkInfo)
}

func (d *DirtyMgr) RemoveMetaNoLockIfLatest(inodeId InodeKeyType, version uint32) bool {
	metaInfo := d.mTable[inodeId]
	if version == metaInfo.version {
		delete(d.mTable, inodeId)
		if metaInfo.expireMs > 0 {
			d.expireTable.Delete(ExpireInfo{expireMs: metaInfo.expireMs, timestamp: metaInfo.timestamp, inodeKey: inodeId})
		}
		log.Debugf("Success: DirtyMgr.RemoveMetaNoLockIfLatest, inodeKey=%v, version=%v", inodeId, version)
		return true
	}
	log.Infof("DirtyMgr.RemoveMetaNoLockIfLatest, potential racy write. do not remove dirty, inodeKey=%v, version=%v, latest=%v", inodeId, version, metaInfo.version)
	return false
}

func (d *DirtyMgr) AddDeleteKeyNoLock(key string, meta *WorkingMeta) {
	old, ok := d.dTable[key]
	info := NewDeleteFileInfoFromMeta(meta)
	d.dTable[key] = info
	if meta.expireMs > 0 {
		if ok {
			d.expireDTable.Delete(ExpireDeleteInfo{expireMs: old.expireMs, timestamp: old.timestamp, key: key})
		}
		d.expireDTable.ReplaceOrInsert(NewExpireDeleteInfoFromMeta(key, meta, info.timestamp))
	}
	log.Debugf("Success: DirtyMgr.AddDeleteKeyNoLock, key=%v, inodeKey=%v", key, meta.inodeKey)
}

func (d *DirtyMgr) RemoveDeleteKeyNoLock(key string) (InodeKeyType, bool) {
	deleted, ok := d.dTable[key]
	if ok {
		delete(d.dTable, key)
		if deleted.expireMs > 0 {
			d.expireDTable.Delete(ExpireDeleteInfo{expireMs: deleted.expireMs, timestamp: deleted.timestamp, key: key})
		}
		log.Debugf("Success: DirtyMgr.RemoveDeleteKeyNoLock, key=%v", key)
	}
	return deleted.inodeKey, ok
}

func (d *DirtyMgr) GetDeleteKey(key string) (inodeKey InodeKeyType, ok bool) {
	d.lock.RLock()
	deleted, ok2 := d.dTable[key]
	d.lock.RUnlock()
	return deleted.inodeKey, ok2
}

func (d *DirtyMgr) IsDirtyMeta(inodeKey InodeKeyType) (ok bool) {
	d.lock.RLock()
	_, ok = d.mTable[inodeKey]
	d.lock.RUnlock()
	return
}

func (d *DirtyMgr) CopyAllPrimaryDirtyMeta() []InodeKeyType {
	ret := make([]InodeKeyType, len(d.mTable))
	d.lock.RLock()
	var i = 0
	for inodeKey := range d.mTable {
		ret[i] = inodeKey
		i += 1
	}
	d.lock.RUnlock()
	return ret
}

func (d *DirtyMgr) CopyAllPrimaryDeletedKeys() map[string]InodeKeyType {
	ret := map[string]InodeKeyType{}
	d.lock.RLock()
	for key, deleted := range d.dTable {
		ret[key] = deleted.inodeKey
	}
	d.lock.RUnlock()
	return ret
}

func (d *DirtyMgr) GetAllDirtyMeta() []*common.DirtyMetaInfoMsg {
	ret := make([]*common.DirtyMetaInfoMsg, 0)
	for inodeKey, info := range d.mTable {
		ret = append(ret, &common.DirtyMetaInfoMsg{InodeKey: uint64(inodeKey), Version: info.version, Timestamp: info.timestamp, ExpireMs: info.expireMs})
	}
	return ret
}

func (d *DirtyMgr) CopyAllExpiredPrimaryDirtyMeta() []InodeKeyType {
	timestamp := time.Now().UnixNano()
	ret := make([]InodeKeyType, 0)
	d.lock.RLock()
	d.expireTable.AscendLessThan(ExpireInfo{expireMs: 0, timestamp: timestamp}, func(i btree.Item) bool {
		expireInfo := i.(ExpireInfo)
		ret = append(ret, expireInfo.inodeKey)
		return true
	})
	d.lock.RUnlock()
	return ret
}

func (d *DirtyMgr) CopyAllExpiredPrimaryDeletedDirtyMeta() map[string]InodeKeyType {
	timestamp := time.Now().UnixNano()
	ret := make(map[string]InodeKeyType)
	d.lock.RLock()
	d.expireDTable.AscendLessThan(ExpireDeleteInfo{expireMs: 0, timestamp: timestamp}, func(i btree.Item) bool {
		expireDeleteInfo := i.(ExpireDeleteInfo)
		expireInfo, ok := d.dTable[expireDeleteInfo.key]
		if !ok {
			log.Warnf("Failed (ignore), CopyAllExpiredPrimaryDeletedDirtyMeta, cannot find key=%v in dTable", expireDeleteInfo.key)
			return true
		}
		ret[expireDeleteInfo.key] = expireInfo.inodeKey
		return true
	})
	d.lock.RUnlock()
	return ret
}

// GetDirtyMetaForNodeLeave returns a blank string if the number of participant Node is < nrReplicas
func (d *DirtyMgr) GetDirtyMetaForNodeLeave(nodeList *RaftNodeList) (map[InodeKeyType]bool, map[string]map[InodeKeyType]bool) {
	ret2 := make(map[InodeKeyType]bool)
	ret := map[string]map[InodeKeyType]bool{}
	d.lock.RLock()
	for inodeKey := range d.mTable {
		g, ok := GetGroupForMeta(nodeList.ring, inodeKey)
		if !ok {
			continue
		}
		if _, ok2 := ret[g]; !ok2 {
			ret[g] = make(map[InodeKeyType]bool)
		}
		ret[g][inodeKey] = true
		ret2[inodeKey] = true
	}
	d.lock.RUnlock()
	return ret2, ret
}

func (d *DirtyMgr) GetDirMetaForNodeLeave(keys []*common.InodeTreeMsg, nodeList *RaftNodeList) map[string][]*common.InodeTreeMsg {
	ret := map[string][]*common.InodeTreeMsg{}
	d.lock.RLock()
	for _, inodeKey := range keys {
		g, ok := GetGroupForMeta(nodeList.ring, InodeKeyType(inodeKey.GetInodeKey()))
		if !ok {
			continue
		}
		if _, ok2 := ret[g]; !ok2 {
			ret[g] = make([]*common.InodeTreeMsg, 0)
		}
		ret[g] = append(ret[g], inodeKey)
	}
	d.lock.RUnlock()
	return ret
}

func (d *DirtyMgr) GetDirtyChunkAll() map[InodeKeyType]DirtyChunkInfo {
	ret := make(map[InodeKeyType]DirtyChunkInfo)
	d.lock.RLock()
	for inodeKey, chunkInfo := range d.cTable {
		copied := chunkInfo
		offsetVersions := make(map[int64]uint32)
		for offset, version := range copied.OffsetVersions {
			offsetVersions[offset] = version
		}
		copied.OffsetVersions = offsetVersions
		ret[inodeKey] = copied
	}
	d.lock.RUnlock()
	return ret
}

func (d *DirtyMgr) GetDirtyMetasForNodeJoin(migrationId MigrationId, nodeList *RaftNodeList, newRing *hashring.HashRing, selfGroup string, joinGroup string) map[InodeKeyType]bool {
	keys := make(map[InodeKeyType]bool)
	d.lock.RLock()
	d.migratingLock.RLock()
	for inodeKey := range d.mTable {
		oldOwner, ok := GetGroupForMeta(nodeList.ring, inodeKey)
		if !ok || oldOwner != selfGroup {
			continue
		}
		newOwner, ok := GetGroupForMeta(newRing, inodeKey)
		if ok && newOwner == joinGroup {
			keys[inodeKey] = true
		}
	}
	d.migratingLock.RUnlock()
	d.lock.RUnlock()
	return keys
}

func (d *DirtyMgr) GetDirInodesForNodeJoin(dirInodes []*common.InodeTreeMsg, migrationId MigrationId, nodeList *RaftNodeList, newRing *hashring.HashRing, selfGroup string, joinGroup string) []*common.InodeTreeMsg {
	keys := make([]*common.InodeTreeMsg, 0)
	d.lock.RLock()
	d.migratingLock.RLock()
	for _, inodeMsg := range dirInodes {
		oldOwner, ok := GetGroupForMeta(nodeList.ring, InodeKeyType(inodeMsg.InodeKey))
		if !ok || oldOwner != selfGroup {
			continue
		}
		newOwner, ok := GetGroupForMeta(newRing, InodeKeyType(inodeMsg.InodeKey))
		if ok && newOwner == joinGroup {
			keys = append(keys, inodeMsg)
		}
	}
	d.migratingLock.RUnlock()
	d.lock.RUnlock()
	return keys
}

func (d *DirtyMgr) GetDirtyChunkForNodeJoin(migrationId MigrationId, nodeList *RaftNodeList, newRing *hashring.HashRing, selfGroupId string, joinGroupId string) map[InodeKeyType]DirtyChunkInfo {
	ret := make(map[InodeKeyType]DirtyChunkInfo)
	d.lock.RLock()
	d.migratingLock.RLock()
	for inodeKey, chunkInfo := range d.cTable {
		for offset, version := range chunkInfo.OffsetVersions {
			oldOwner, ok := GetGroupForChunk(nodeList.ring, inodeKey, offset, chunkInfo.chunkSize)
			if !ok || oldOwner != selfGroupId {
				continue
			}
			newOwner, ok := GetGroupForChunk(newRing, inodeKey, offset, chunkInfo.chunkSize)
			if ok && newOwner == joinGroupId {
				if _, ok2 := ret[inodeKey]; !ok2 {
					copied := chunkInfo
					copied.OffsetVersions = make(map[int64]uint32)
					ret[inodeKey] = copied
				}
				ret[inodeKey].OffsetVersions[offset] = version
			}
		}
	}
	d.migratingLock.RUnlock()
	d.lock.RUnlock()
	return ret
}

func (d *DirtyMgr) CommitMigratedDataLocal(inodeMgr *InodeMgr, migrationId MigrationId) {
	d.migratingLock.Lock()
	m, ok := d.migrating[migrationId]
	delete(d.migrating, migrationId)
	d.migratingLock.Unlock()
	if !ok {
		return
	}
	inodeMgr.RestoreMetas(m.GetAddMetas(), m.GetAddFiles())
	inodeMgr.RestoreInodeTree(m.GetDirInodes())
	inodeMgr.DeleteInode(m.GetRemoveDirtyInodeIds())
	inodeMgr.DeleteInode(m.GetRemoveDirInodeIds())
	for _, entry := range m.GetAddChunks() {
		for _, chunk := range entry.GetChunks() {
			c := inodeMgr.GetChunk(InodeKeyType(entry.GetInodeKey()), chunk.GetOffset(), entry.GetChunkSize())
			working := c.NewWorkingChunk(entry.GetVersion())
			working.AddStagingChunkFromAddMsg(chunk)
			c.lock.Lock()
			c.AddWorkingChunk(inodeMgr, working, working.prevVer)
			c.lock.Unlock()
		}
	}
	d.lock.Lock()
	for _, inodeKey := range m.GetRemoveDirtyInodeIds() {
		d.RemoveMetaNoLock(InodeKeyType(inodeKey))
	}
	for _, chunk := range m.GetRemoveDirtyChunks() {
		d.RemoveChunkNoLock(InodeKeyType(chunk.GetInodeKey()), chunk.GetOffset(), chunk.GetVersion())
	}
	for _, metaMsg := range m.GetAddMetas() {
		inodeKey := InodeKeyType(metaMsg.GetInodeKey())
		d.mTable[inodeKey] = DirtyMetaInfo{version: metaMsg.GetVersion(), timestamp: metaMsg.GetLastModified()}
		if metaMsg.GetExpireMs() > 0 {
			d.expireTable.ReplaceOrInsert(ExpireInfo{inodeKey: inodeKey, timestamp: metaMsg.GetLastModified(), expireMs: metaMsg.GetExpireMs()})
		}
	}
	for _, entry := range m.GetAddChunks() {
		for _, chunk := range entry.GetChunks() {
			d.AddChunkNoLock(InodeKeyType(entry.GetInodeKey()), entry.GetChunkSize(), entry.GetVersion(), chunk.GetOffset(), entry.GetObjectSize())
		}
	}
	d.lock.Unlock()
}

func (n *InodeMgr) MpuAbort(key string, uploadId string) (reply int32) {
	_, awsErr := n.back.multipartBlobAbort(&MultipartBlobCommitInput{Key: &key, UploadId: &uploadId})
	if reply = AwsErrToReply(awsErr); reply != RaftReplyOk {
		log.Errorf("Failed: mpuAbort, multipartAbort: name=%v, uploadId=%v, awsErr=%v, reply=%v", key, uploadId, awsErr, reply)
	} else {
		log.Infof("Success: mpuAbort, MpuAbort: key=%v, uploadId=%v", key, uploadId)
	}
	return
}

func (d *DirtyMgr) DropMigratingData(migrationId MigrationId) {
	d.migratingLock.Lock()
	delete(d.migrating, migrationId)
	d.migratingLock.Unlock()
}

func (d *DirtyMgr) AppendRemoveNonDirtyChunksLog(raft *RaftInstance, fps []uint64) int32 {
	if len(fps) == 0 {
		return RaftReplyOk
	}
	reply := raft.AppendExtendedLogEntry(NewRemoveNonDirtyChunksCommand(fps))
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendRemoveNonDirtyChunksLog, AppendExtendedLogEntry, reply=%v", reply)
	}
	return reply
}

func (d *DirtyMgr) ApplyAsRemoveNonDirtyChunks(pm proto.Message) int32 {
	l := pm.(*common.RemoveNonDirtyChunksMsg)
	d.RemoveNonDirtyChunks(l.InodeKeys)
	return RaftReplyOk
}

func (d *DirtyMgr) ForgetAllDirty() {
	d.lock.Lock()
	d.cTable = make(map[InodeKeyType]DirtyChunkInfo)
	d.mTable = make(map[InodeKeyType]DirtyMetaInfo)
	d.expireTable = btree.New(3)
	d.dTable = make(map[string]DeletedFileInfo)
	d.expireDTable = btree.New(3)
	d.lock.Unlock()
	d.migratingLock.Lock()
	d.migrating = make(map[MigrationId]*common.MigrationMsg)
	d.migratingLock.Unlock()
}

func (d *DirtyMgr) AppendForgetAllDirtyLog(raft *RaftInstance) int32 {
	reply := raft.AppendExtendedLogEntry(NewForgetAllDirtyLogCommand())
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendForgetAllDirtyLog, AppendExtendedLogEntry, reply=%v", reply)
	}
	return reply
}

func (d *DirtyMgr) AddMigratedAddMetas(s *Snapshot) {
	d.migratingLock.Lock()
	mig, ok := d.migrating[s.migrationId]
	if !ok {
		mig = &common.MigrationMsg{}
		d.migrating[s.migrationId] = mig
	}
	mig.AddMetas = append(mig.AddMetas, s.metas...)
	mig.AddFiles = append(mig.AddFiles, s.files...)
	mig.DirInodes = append(mig.DirInodes, s.dirents...)
	d.migratingLock.Unlock()
}

func (d *DirtyMgr) AddMigratedRemoveMetas(migrationId MigrationId, inodeKeys []uint64, dirKeys []uint64) {
	d.migratingLock.Lock()
	mig, ok := d.migrating[migrationId]
	if !ok {
		mig = &common.MigrationMsg{}
		d.migrating[migrationId] = mig
	}
	for _, inodeKey := range inodeKeys {
		mig.RemoveDirtyInodeIds = append(mig.RemoveDirtyInodeIds, uint64(inodeKey))
	}
	for _, inodeKey := range dirKeys {
		mig.RemoveDirInodeIds = append(mig.RemoveDirInodeIds, uint64(inodeKey))
	}
	d.migratingLock.Unlock()
}

func (d *DirtyMgr) AddMigratedAddChunk(migrationId MigrationId, chunk *common.AppendCommitUpdateChunksMsg) {
	d.migratingLock.Lock()
	mig, ok := d.migrating[migrationId]
	if !ok {
		mig = &common.MigrationMsg{}
		d.migrating[migrationId] = mig
	}
	mig.AddChunks = append(mig.AddChunks, chunk)
	d.migratingLock.Unlock()
}

func (d *DirtyMgr) AddMigratedRemoveChunk(migrationId MigrationId, chunks []*common.ChunkRemoveDirtyMsg) {
	d.migratingLock.Lock()
	mig, ok := d.migrating[migrationId]
	if !ok {
		mig = &common.MigrationMsg{}
		d.migrating[migrationId] = mig
	}
	mig.RemoveDirtyChunks = append(mig.RemoveDirtyChunks, chunks...)
	d.migratingLock.Unlock()
}

type Snapshot struct {
	metas       []*common.CopiedMetaMsg
	mIdx        int
	files       []*common.InodeToFileMsg
	fIdx        int
	dirents     []*common.InodeTreeMsg
	dIdx        int
	dirtyMetas  []*common.DirtyMetaInfoMsg
	dmIdx       int
	dirtyChunks []*common.AppendCommitUpdateChunksMsg
	dcIdx       int
	nodeList    *common.RaftNodeListMsg
	nIdx        int

	migrationId MigrationId
	newExtLogId uint32
	maxBytes    int
}

func NewSnapshot(maxBytes int, migrationId MigrationId, newExtLogId uint32,
	metas []*common.CopiedMetaMsg,
	files []*common.InodeToFileMsg,
	dirents []*common.InodeTreeMsg,
	dirtyMetas []*common.DirtyMetaInfoMsg,
	dirtyChunks []*common.AppendCommitUpdateChunksMsg,
	nodeList *common.RaftNodeListMsg) *Snapshot {

	// NOTE: we need to record all metadata including non-dirty ones because we may encouter a race condition of PrepareUpdateMeta vs. Snapshot.
	// In that case, PrepareUpdateMeta() falsely returns ENOENT at the error path for inode.meta == nil.
	// PrepareUpdateMeta() may update non-dirty metadata but it cannot fetch from S3 due to the lack of information of accessed inodes (fetch keys, etc.)
	// also we do not want to send such redundant information just for a very tiny race condition at every update during write().
	ret := &Snapshot{
		migrationId: migrationId, newExtLogId: newExtLogId, mIdx: 0, fIdx: 0, dIdx: 0, dmIdx: 0, nIdx: 0, maxBytes: maxBytes, nodeList: nodeList,
		metas: metas, files: files, dirents: dirents, dirtyMetas: dirtyMetas, dirtyChunks: dirtyChunks,
	}
	if nodeList == nil {
		ret.nIdx = 1
	}
	return ret
}

func NewSnapshotFromMsg(msg *common.SnapshotMsg) *Snapshot {
	var nodeList *common.RaftNodeListMsg
	if nodeListMsg := msg.GetNodeList(); len(nodeListMsg) > 0 {
		nodeList = nodeListMsg[0]
	}
	migrationId := NewMigrationIdFromMsg(msg.GetMigrationId())
	return NewSnapshot(math.MaxInt, migrationId, msg.GetNewExtLogId(), msg.GetMetas(), msg.GetFiles(), msg.GetDirents(), msg.GetDirtyMetas(), msg.GetDirtyChunks(), nodeList)
}

func (s *Snapshot) GetNext() *Snapshot {
	if s.mIdx >= len(s.metas) && s.fIdx >= len(s.files) && s.dIdx >= len(s.dirents) && s.dmIdx >= len(s.dirtyMetas) && s.dcIdx >= len(s.dirtyChunks) && s.nIdx >= 1 {
		return nil
	}
	ret := &common.SnapshotMsg{MigrationId: s.migrationId.toMsg(), NewExtLogId: s.newExtLogId}
	lastmIdx := s.mIdx
	lastfIdx := s.fIdx
	lastdIdx := s.dIdx
	lastdmIdx := s.dmIdx
	lastdcIdx := s.dcIdx
	lastnIdx := s.nIdx
	for attempt := 1; attempt == 1 || proto.Size(ret) > s.maxBytes; attempt += 1 {
		if lastmIdx = len(s.metas); s.mIdx < lastmIdx {
			lastmIdx = s.mIdx + (lastmIdx-s.mIdx+attempt-1)/attempt
			ret.Metas = s.metas[s.mIdx:lastmIdx]
		}
		if lastfIdx = len(s.files); s.fIdx < lastfIdx {
			lastfIdx = s.fIdx + (lastfIdx-s.fIdx+attempt-1)/attempt
			ret.Files = s.files[s.fIdx:lastfIdx]
		}
		if lastdIdx = len(s.dirents); s.dIdx < lastdIdx {
			lastdIdx = s.dIdx + (lastdIdx-s.dIdx+attempt-1)/attempt
			ret.Dirents = s.dirents[s.dIdx:lastdIdx]
		}
		if lastdmIdx = len(s.dirtyMetas); s.dmIdx < lastdmIdx {
			lastdmIdx = s.dmIdx + (lastdmIdx-s.dmIdx+attempt-1)/attempt
			ret.DirtyMetas = s.dirtyMetas[s.dmIdx:lastdmIdx]
		}
		if lastdcIdx = len(s.dirtyChunks); s.dcIdx < lastdcIdx {
			lastdcIdx = s.dcIdx + (lastdcIdx-s.dcIdx+attempt-1)/attempt
			ret.DirtyChunks = s.dirtyChunks[s.dcIdx:lastdcIdx]
		}
		if lastnIdx = 1; s.nIdx < lastnIdx {
			ret.NodeList = []*common.RaftNodeListMsg{s.nodeList}
		}
		if attempt > 1 && lastmIdx <= s.mIdx+1 && lastfIdx <= s.fIdx+1 && lastdIdx <= s.dIdx+1 && lastdmIdx <= s.mIdx+1 && lastdcIdx <= s.dcIdx+1 {
			break
		}
	}
	s.mIdx = lastmIdx
	s.fIdx = lastfIdx
	s.dIdx = lastdIdx
	s.dmIdx = lastdmIdx
	s.dcIdx = lastdcIdx
	s.nIdx = lastnIdx
	return NewSnapshotFromMsg(ret)
}

func (s *Snapshot) toMsg() *common.SnapshotMsg {
	ret := &common.SnapshotMsg{
		MigrationId: s.migrationId.toMsg(), NewExtLogId: s.newExtLogId, Metas: s.metas, Files: s.files, Dirents: s.dirents, DirtyMetas: s.dirtyMetas, DirtyChunks: s.dirtyChunks,
	}
	if s.nodeList != nil {
		ret.NodeList = []*common.RaftNodeListMsg{s.nodeList}
	}
	return ret
}
