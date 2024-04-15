/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"hash/crc32"
	"hash/crc64"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/serialx/hashring"
	"github.com/spaolacci/murmur3"
	"github.com/IBM/objcache/api"
	"github.com/IBM/objcache/common"
)

type RaftNode struct {
	addr    common.NodeAddrInet4
	nodeId  uint32
	groupId string
}

func (n *RaftNode) toMsg() *common.NodeMsg {
	ret := &common.NodeMsg{NodeId: n.nodeId, GroupId: n.groupId, Addr: make([]byte, 4), Port: int32(n.addr.Port)}
	copy(ret.Addr, n.addr.Addr[:])
	return ret
}

func (n *RaftNode) toApiMsg() *api.ApiNodeMsg {
	ret := &api.ApiNodeMsg{NodeId: n.nodeId, GroupId: n.groupId, Addr: make([]byte, 4), Port: int32(n.addr.Port)}
	copy(ret.Addr, n.addr.Addr[:])
	return ret
}

func NewRaftNodeFromMsg(msg *common.NodeMsg) RaftNode {
	return RaftNode{addr: NewSaFromNodeMsg(msg), nodeId: msg.GetNodeId(), groupId: msg.GetGroupId()}
}

func NewRaftNodeFromApiMsg(msg *api.ApiNodeMsg) RaftNode {
	return RaftNode{addr: NewSaFromApiNodeMsg(msg), nodeId: msg.GetNodeId(), groupId: msg.GetGroupId()}
}

func NewSaFromNodeMsg(msg *common.NodeMsg) (ret common.NodeAddrInet4) {
	ret.Port = int(msg.GetPort())
	copy(ret.Addr[:], msg.GetAddr())
	return ret
}

func NewSaFromApiNodeMsg(msg *api.ApiNodeMsg) (ret common.NodeAddrInet4) {
	ret.Port = int(msg.GetPort())
	copy(ret.Addr[:], msg.GetAddr())
	return ret
}

func NewNodeMsgFromAddr(addr common.NodeAddrInet4) *common.NodeMsg {
	msg := &common.NodeMsg{Addr: make([]byte, 4), Port: int32(addr.Port)}
	copy(msg.Addr, addr.Addr[:])
	return msg
}

type RaftNodeList struct {
	nodeAddr  map[uint32]common.NodeAddrInet4
	groupNode map[string]map[uint32]RaftNode
	ring      *hashring.HashRing
	version   uint64
}

func NewRaftNodeList(ring *hashring.HashRing, version uint64) *RaftNodeList {
	return &RaftNodeList{
		nodeAddr: make(map[uint32]common.NodeAddrInet4), groupNode: make(map[string]map[uint32]RaftNode),
		ring: ring, version: version,
	}
}

func (l *RaftNodeList) toMsg() *common.RaftNodeListMsg {
	ret := &common.RaftNodeListMsg{Nodes: make([]*common.NodeMsg, 0), Version: l.version}
	for _, nodes := range l.groupNode {
		for _, node := range nodes {
			ret.Nodes = append(ret.Nodes, node.toMsg())
		}
	}
	return ret
}

func (r *RaftNodeList) CheckReset() (ok bool) {
	ok = true
	if ok2 := len(r.nodeAddr) == 0; !ok2 {
		log.Errorf("Failed: RaftNodeList.CheckReset, len(r.nodeAddr) != 0")
		ok = false
	}
	if ok2 := len(r.groupNode) == 0; !ok2 {
		log.Errorf("Failed: RaftNodeList.CheckReset, len(r.groupNode) != 0")
		ok = false
	}
	if ok2 := r.ring.Size() == 0; !ok2 {
		log.Errorf("Failed: RaftNodeList.CheckReset, r.ring.Size() != 0")
		ok = false
	}
	return
}

type MyHashKey struct {
	key uint32
}

func (m MyHashKey) Less(l hashring.HashKey) bool {
	return m.key < l.(MyHashKey).key
}

func MyHashFunc(in []byte) hashring.HashKey {
	return MyHashKey{key: crc32.ChecksumIEEE(in)}
}

func MyHashFunc64(in []byte) hashring.HashKey {
	t := crc64.MakeTable(crc64.ISO)
	return MyHashKey{key: uint32(crc64.Checksum(in, t))}
}

func MyHashFuncV2(in []byte) hashring.HashKey {
	return MyHashKey{key: murmur3.Sum32(in)}
}

func MyHashFunc64V2(in []byte) hashring.HashKey {
	return MyHashKey{key: uint32(murmur3.Sum64(in))}
}

func NewRaftNodeListFromMsg(nrVirt int, msg *common.RaftNodeListMsg) *RaftNodeList {
	ret := NewRaftNodeList(hashring.NewWithHash([]string{}, MyHashFunc), msg.GetVersion())
	for _, nodeMsg := range msg.GetNodes() {
		node := NewRaftNodeFromMsg(nodeMsg)
		ret.nodeAddr[node.nodeId] = node.addr
		if _, ok := ret.groupNode[node.groupId]; !ok {
			ret.groupNode[node.groupId] = make(map[uint32]RaftNode)
		}
		ret.groupNode[node.groupId][node.nodeId] = node
		ret.ring.AddWeightedNode(node.groupId, nrVirt)
	}
	return ret
}

func GetGroupForMeta(ring *hashring.HashRing, inodeKey InodeKeyType) (groupId string, ok bool) {
	return ring.GetNode(strconv.FormatUint(uint64(inodeKey), 36))
}

func GetGroupForChunk(ring *hashring.HashRing, inodeKey InodeKeyType, offset int64, chunkSize int64) (groupId string, ok bool) {
	aligned := offset - offset%chunkSize
	if aligned == 0 {
		return ring.GetNode(strconv.FormatUint(uint64(inodeKey), 36))
	}
	return ring.GetNode(strconv.FormatUint(uint64(inodeKey), 36) + "/" + strconv.FormatInt(aligned, 36))
}

type RaftGroupMgr struct {
	lock        *sync.RWMutex
	nrVirt      int
	selfGroup   string
	nodeList    *RaftNodeList
	leaderCache map[string]RaftNode // logIndex, RaftNode (logIndex == version at updater Node, but others can be different)
	leaderLock  *sync.RWMutex
	updateCount int32
}

func NewRaftGroupMgr(groupId string, nrVirt int) *RaftGroupMgr {
	ret := &RaftGroupMgr{
		lock:        new(sync.RWMutex),
		nrVirt:      nrVirt,
		selfGroup:   groupId,
		nodeList:    NewRaftNodeList(hashring.NewWithHash([]string{}, MyHashFunc), 1),
		leaderCache: make(map[string]RaftNode),
		leaderLock:  new(sync.RWMutex),
		updateCount: 0,
	}
	return ret
}

func (m *RaftGroupMgr) ResetWithRaftNodeListMsg(msg *common.RaftNodeListMsg) {
	m.lock.Lock()
	m.nodeList = NewRaftNodeListFromMsg(m.nrVirt, msg)
	m.lock.Unlock()
	m.leaderLock.Lock()
	m.leaderCache = make(map[string]RaftNode)
	for _, nodes := range m.nodeList.groupNode {
		for _, node := range nodes {
			m.leaderCache[node.groupId] = node
			break
		}
	}
	m.leaderLock.Unlock()
}

func (m *RaftGroupMgr) Clean() {
	m.lock.Lock()
	m.nodeList = NewRaftNodeList(hashring.NewWithHash([]string{}, MyHashFunc), 1)
	m.lock.Unlock()
	m.leaderLock.Lock()
	m.leaderCache = make(map[string]RaftNode)
	m.leaderLock.Unlock()
}

func (m *RaftGroupMgr) CheckReset() (ok bool) {
	if ok = m.lock.TryLock(); !ok {
		log.Errorf("Failed: RaftGroupMgr.CheckReset, m.lock is taken")
	} else {
		if ok2 := m.nodeList.CheckReset(); !ok2 {
			log.Errorf("Failed: RaftGroupMgr.CheckReset, nodeList")
			ok = false
		}
		m.lock.Unlock()
	}
	if ok2 := m.leaderLock.TryLock(); !ok2 {
		log.Errorf("Failed: RaftGroupMgr.CheckReset, m.leaderLock is taken")
		ok = false
	} else {
		if ok2 := len(m.leaderCache) == 0; !ok2 {
			log.Errorf("Failed: RaftGroupMgr.CheckReset, len(m.leaderCache) != 0")
			ok = false
		}
		m.leaderLock.Unlock()
	}
	return
}

func (m *RaftGroupMgr) GetNumberOfGroups() int {
	m.lock.RLock()
	r := len(m.nodeList.groupNode)
	m.lock.RUnlock()
	return r
}

func (m *RaftGroupMgr) GetGroupLeaderNoLock(groupId string, l *RaftNodeList) (RaftNode, bool) {
	node, ok := m.leaderCache[groupId]
	if ok {
		return node, true
	}
	for _, n := range l.groupNode[groupId] {
		m.leaderCache[groupId] = n
		return n, true
	}
	return RaftNode{}, false
}

func (m *RaftGroupMgr) GetGroupLeader(groupId string, l *RaftNodeList) (RaftNode, bool) {
	m.lock.RLock()
	node, ok := m.leaderCache[groupId]
	m.lock.RUnlock()
	if ok {
		return node, true
	}
	m.lock.Lock()
	node, ok = m.leaderCache[groupId]
	if ok {
		m.lock.Unlock()
		return node, true
	}
	for _, n := range l.groupNode[groupId] {
		m.leaderCache[groupId] = n
		m.lock.Unlock()
		return n, true
	}
	m.lock.Unlock()
	return RaftNode{}, false
}

func (m *RaftGroupMgr) UpdateLeader(newLeader RaftNode) bool {
	m.leaderLock.Lock()
	old, ok := m.leaderCache[newLeader.groupId]
	if ok && old.nodeId == newLeader.nodeId {
		m.leaderLock.Unlock()
		return false
	}
	m.leaderCache[newLeader.groupId] = newLeader
	m.leaderLock.Unlock()
	log.Debugf("Success: UpdateLeader, groupId=%v, leader:%v->%v", newLeader.groupId, old, newLeader)
	return true
}

func (m *RaftGroupMgr) GetReplica(leader RaftNode) (replica RaftNode, found bool) {
	m.leaderLock.Lock()
	groupLen := len(m.nodeList.groupNode[leader.groupId])
	if groupLen > 1 {
		replicas := make([]RaftNode, 0)
		for nodeId, node := range m.nodeList.groupNode[leader.groupId] {
			if nodeId != leader.nodeId {
				replicas = append(replicas, node)
			}
		}
		rand.Seed(time.Now().UnixNano())
		replica = replicas[rand.Int()%len(replicas)]
		m.leaderCache[leader.groupId] = replica
		found = true
		log.Infof("Success: GetReplica, groupId=%v, replica=%v->%v", leader.groupId, leader.nodeId, replica.nodeId)
	}
	m.leaderLock.Unlock()
	return
}

// GetNodeListLocal must be called after raft.SyncBeforeClientQuery()
func (m *RaftGroupMgr) GetNodeListLocal() *RaftNodeList {
	m.lock.RLock()
	r := m.nodeList
	m.lock.RUnlock()
	return r // NOTE: r is valid even after the critical section. m.nodeList is immutable and always overwritten by new RaftNodeList.
}

func (m *RaftGroupMgr) GetRemovedNodeListLocal(removed RaftNode) *RaftNodeList {
	m.lock.RLock()
	if m.nodeList == nil {
		m.lock.RUnlock()
		return NewRaftNodeList(hashring.New([]string{}), 1)
	}
	var newNodeList *RaftNodeList
	if nodes, ok := m.nodeList.groupNode[removed.groupId]; ok {
		if _, ok = nodes[removed.nodeId]; ok && len(nodes) == 1 {
			newNodeList = m.copyNodeListNoLock(m.nodeList.ring.RemoveNode(removed.groupId))
		} else {
			newNodeList = m.copyNodeListNoLock(m.nodeList.ring)
		}
	} else {
		newNodeList = m.copyNodeListNoLock(m.nodeList.ring)
	}
	delete(newNodeList.nodeAddr, removed.nodeId)
	delete(newNodeList.groupNode[removed.groupId], removed.nodeId)
	if len(newNodeList.groupNode[removed.groupId]) == 0 {
		delete(newNodeList.groupNode, removed.groupId)
	}
	m.lock.RUnlock()
	return newNodeList
}

func (m *RaftGroupMgr) BeginRaftRead(raft *RaftInstance, nodeListVer uint64) (r RaftBasicReply) {
	r = raft.SyncBeforeClientQuery()
	if r.reply != RaftReplyOk {
		log.Errorf("Failed: BeginRaftRead, SyncBeforeClientQuery, r=%v", r)
		return
	}
	nodeList := m.GetNodeListLocal()
	if nodeList.version != nodeListVer {
		log.Errorf("Failed: BeginRaftRead, a given version of nodeList doesn't match current one, ver=%v, current=%v", nodeListVer, nodeList.version)
		r.reply = RaftReplyMismatchVer
	}
	return
}

func (m *RaftGroupMgr) copyNodeListNoLock(ring *hashring.HashRing) *RaftNodeList {
	newNodeList := NewRaftNodeList(ring, m.nodeList.version+1)
	for nId, gId := range m.nodeList.nodeAddr {
		newNodeList.nodeAddr[nId] = gId
	}
	for gId, nodes := range m.nodeList.groupNode {
		if _, ok := newNodeList.groupNode[gId]; !ok {
			newNodeList.groupNode[gId] = map[uint32]RaftNode{}
		}
		for nId, n := range nodes {
			newNodeList.groupNode[gId][nId] = n
		}
	}
	return newNodeList
}

func (m *RaftGroupMgr) Add(node RaftNode) {
	m.lock.Lock()
	var newNodeList *RaftNodeList
	if _, ok := m.nodeList.groupNode[node.groupId]; ok {
		newNodeList = m.copyNodeListNoLock(m.nodeList.ring)
	} else {
		newNodeList = m.copyNodeListNoLock(m.nodeList.ring.AddWeightedNode(node.groupId, m.nrVirt))
	}
	newNodeList.nodeAddr[node.nodeId] = node.addr
	if _, ok := newNodeList.groupNode[node.groupId]; !ok {
		newNodeList.groupNode[node.groupId] = make(map[uint32]RaftNode)
	}
	newNodeList.groupNode[node.groupId][node.nodeId] = node
	m.nodeList = newNodeList
	if _, ok := m.leaderCache[node.groupId]; !ok {
		m.leaderCache[node.groupId] = node
	}
	m.lock.Unlock()
	log.Infof("Success: RaftGroupMgr.Add, node=%v, newVer=%v", node, newNodeList.version)
}

func (m *RaftGroupMgr) Remove(nodeId uint32, groupId string) {
	m.lock.Lock()
	var newNodeList *RaftNodeList
	if nodes, ok := m.nodeList.groupNode[groupId]; ok {
		if _, ok = nodes[nodeId]; ok && len(nodes) == 1 {
			newNodeList = m.copyNodeListNoLock(m.nodeList.ring.RemoveNode(groupId))
		} else {
			newNodeList = m.copyNodeListNoLock(m.nodeList.ring)
		}
	} else {
		newNodeList = m.copyNodeListNoLock(m.nodeList.ring)
	}
	delete(newNodeList.nodeAddr, nodeId)
	delete(newNodeList.groupNode[groupId], nodeId)
	if len(newNodeList.groupNode[groupId]) == 0 {
		delete(newNodeList.groupNode, groupId)
	}
	m.nodeList = newNodeList

	if leaderNode, ok := m.leaderCache[groupId]; ok && leaderNode.nodeId == nodeId {
		delete(m.leaderCache, groupId)
		for _, newLeaderNode := range m.nodeList.groupNode[groupId] {
			m.leaderCache[groupId] = newLeaderNode
		}
	}
	m.lock.Unlock()
	log.Infof("Success: RaftGroupMgr.Remove, nodeId=%v, groupId=%v, newVer=%v", nodeId, groupId, newNodeList.version)
}

func (m *RaftGroupMgr) CommitUpdate(nodes []RaftNode, isAdd bool, nodeListVer uint64) {
	if isAdd {
		for _, node := range nodes {
			m.Add(node)
		}
	} else {
		for _, node := range nodes {
			m.Remove(node.nodeId, node.groupId)
		}
	}
	m.lock.Lock()
	if nodeListVer > 0 && m.nodeList != nil && m.nodeList.version != nodeListVer {
		m.nodeList.version = nodeListVer
		log.Debugf("Success: RaftGroupMgr.CommitUpdate, overwrite nodeListVer=%v", nodeListVer)
	}
	m.lock.Unlock()
}

func (m *RaftGroupMgr) SetNodeListDirect(nodes []*api.ApiNodeMsg, nodeListVer uint64) {
	// only clients can call this method. servers must use raft.AppendExtendedLogEntry(AppendEntryUpdateNodeListLocalCmdId, ...)
	newNodeList := NewRaftNodeList(hashring.NewWithHash([]string{}, MyHashFunc), nodeListVer)
	leaders := make(map[string]RaftNode)
	for _, nodeMsg := range nodes {
		node := NewRaftNodeFromApiMsg(nodeMsg)
		newNodeList.nodeAddr[node.nodeId] = node.addr
		if _, ok := newNodeList.groupNode[node.groupId]; !ok {
			newNodeList.groupNode[node.groupId] = make(map[uint32]RaftNode)
			newNodeList.ring = newNodeList.ring.AddWeightedNode(node.groupId, m.nrVirt)
			leaders[node.groupId] = node
		}
		newNodeList.groupNode[node.groupId][node.nodeId] = node
	}
	m.lock.Lock()
	m.nodeList = newNodeList
	m.lock.Unlock()
	m.leaderLock.Lock()
	m.leaderCache = leaders
	m.leaderLock.Unlock()
}

// UpdateNodeListLocal appends a new server with a group (the group can be duplicated in the existing entry)
func (m *RaftGroupMgr) UpdateNodeListLocal(isAdd bool, nodes []RaftNode, nodeListVer uint64) {
	if isAdd {
		for _, node := range nodes {
			m.Add(node)
			log.Debugf("Success: UpdateNodeListLocal (Add Server), Node=%v", node)
		}
	} else {
		for _, node := range nodes {
			m.Remove(node.nodeId, node.groupId)
			log.Debugf("Success: UpdateNodeListLocal (Remove Server), Node=%v", node)
		}
	}
	m.lock.Lock()
	if nodeListVer > 0 && m.nodeList != nil && m.nodeList.version != nodeListVer {
		m.nodeList.version = nodeListVer // only called by RequestJoinLocal after Rejuvenation
		log.Debugf("Success: UpdateNodeListLocal, overwrite nodeListVer=%v", nodeListVer)
	}
	m.lock.Unlock()
}

func (m *RaftGroupMgr) GetChunkOwnerGroupId(inodeKey InodeKeyType, offset int64, chunkSize int64) (string, bool) {
	m.lock.RLock()
	groupId, ok := GetGroupForChunk(m.nodeList.ring, inodeKey, offset, chunkSize)
	m.lock.RUnlock()
	return groupId, ok
}

// Last two arguments are reserved for clientId and seqNum
func (m *RaftGroupMgr) getChunkOwnerNodeLocal(inodeKey InodeKeyType, offset int64, chunkSize int64) (leader RaftNode, nodeList *RaftNodeList, reply int32) {
	m.lock.RLock()
	nodeList = m.nodeList
	groupId, ok := GetGroupForChunk(nodeList.ring, inodeKey, offset, chunkSize)
	if !ok {
		m.lock.RUnlock()
		log.Errorf("Failed: RaftGroupMgr.getChunkOwnerNodeLocal, GetGroupForChunk, not found, inodeKey=%v", inodeKey)
		reply = RaftReplyNoGroup
		return
	}
	ret, ok := m.GetGroupLeaderNoLock(groupId, nodeList)
	m.lock.RUnlock()
	if !ok {
		log.Errorf("Failed: RaftGroupMgr.getChunkOwnerNodeLocal, GetGroupLeader, not found, inodeKey=%v, groupId=%v", inodeKey, groupId)
		return RaftNode{}, nodeList, RaftReplyNoGroup
	}
	return ret, nodeList, RaftReplyOk
}

func (m *RaftGroupMgr) getMetaOwnerNodeLocal(inodeKey InodeKeyType) (leader RaftNode, nodeList *RaftNodeList, reply int32) {
	m.lock.RLock()
	nodeList = m.nodeList
	groupId, ok := GetGroupForMeta(nodeList.ring, inodeKey)
	if !ok {
		m.lock.RUnlock()
		log.Errorf("Failed: RaftGroupMgr.getMetaOwnerNodeLocal, GetNode, not found, inodeKey=%v", inodeKey)
		reply = RaftReplyNoGroup
		return
	}
	ret, ok := m.GetGroupLeaderNoLock(groupId, nodeList)
	m.lock.RUnlock()
	if !ok {
		log.Errorf("Failed: RaftGroupMgr.getMetaOwnerNodeLocal, GetGroupLeader, not found, inodeKey=%v, groupId=%v", inodeKey, groupId)
		return RaftNode{}, nodeList, RaftReplyNoGroup
	}
	return ret, nodeList, RaftReplyOk
}

// NOTE: this assumes that nodeList contains at least one Node
func (m *RaftGroupMgr) getKeyOwnerNodeLocalNew(nodeList *RaftNodeList, inodeKey InodeKeyType) (RaftNode, bool) {
	groupId, ok := GetGroupForMeta(nodeList.ring, inodeKey)
	if !ok {
		log.Errorf("Failed: RaftGroupMgr.getKeyOwnerNodeLocalNew, GetNode, not found, inodeKey=%v", inodeKey)
		return RaftNode{}, false
	}
	return m.GetGroupLeader(groupId, nodeList)
}

func (m *RaftGroupMgr) getChunkOwnerNodeLocalNew(nodeList *RaftNodeList, inodeKey InodeKeyType, offset int64, chunkSize int64) (RaftNode, bool) {
	groupId, ok := GetGroupForChunk(nodeList.ring, inodeKey, offset, chunkSize)
	if !ok {
		log.Errorf("Failed: RaftGroupMgr.getChunkOwnerNodeLocalNew, GetNode, not found, inodeKey=%v, offset=%v, chunkSize=%v", inodeKey, offset, chunkSize)
		return RaftNode{}, false
	}
	return m.GetGroupLeader(groupId, nodeList)
}

// useful to select a Node for coordinator because it should be a remote Node from client.
// if no remote nodes exist, this returns the last name's owner
func (m *RaftGroupMgr) selectNodeWithRemoteMetaKey(nodeList *RaftNodeList, inodeKeys []InodeKeyType) (leader RaftNode, ok bool) {
	ok = false
	for _, inodeKey := range inodeKeys {
		var ok2 bool
		leader, ok2 = m.getKeyOwnerNodeLocalNew(nodeList, inodeKey)
		if !ok2 {
			continue
		}
		ok = true
		if leader.groupId != m.selfGroup {
			return leader, true
		}
	}
	return
}

func (m *RaftGroupMgr) beginUpdateGroup() bool {
	return atomic.CompareAndSwapInt32(&m.updateCount, 0, 1)
}

func (m *RaftGroupMgr) finishUpdateGroup() {
	atomic.StoreInt32(&m.updateCount, 0)
}
