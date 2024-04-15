/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/takeshi-yoshimura/fuse"

	"github.com/IBM/objcache/api"
	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

type RaftLogger struct {
	filePrefix string
	disk       *OnDiskLog
	lock       *sync.RWMutex
	headIndex  uint64
	maxNrCache int
}

func NewRaftLogger(rootDir string, filePrefix string, maxNrCache int) (ret *RaftLogger, reply int32) {
	stat := unix.Stat_t{}
	errno := unix.Stat(rootDir, &stat)
	if errno == unix.ENOENT {
		if errno = os.MkdirAll(rootDir, 0755); errno != nil {
			log.Errorf("Failed: NewRaftLogger, MkdirAll, dir=%v, errno=%v", rootDir, errno)
			return nil, ErrnoToReply(errno)
		}
	} else if errno != nil {
		log.Errorf("Failed: NewRaftLogger, Stat, dir=%v, err=%v", rootDir, errno)
		return nil, ErrnoToReply(errno)
	}

	ret = &RaftLogger{
		filePrefix: filepath.Join(rootDir, filePrefix), headIndex: 0, lock: new(sync.RWMutex), maxNrCache: maxNrCache,
	}
	err := filepath.WalkDir(rootDir, func(filePath string, info fs.DirEntry, err error) error {
		if err != nil || info.IsDir() || !strings.HasPrefix(info.Name(), filePrefix) {
			return nil
		}
		buf := [8]byte{}
		_, errno := NewOnDiskLog(filePath, 0, 1).ReadNoCache(buf[:], 0, 8, false)
		if errno != nil {
			log.Errorf("Failed (ignore): NewRaftLogger, ReadNoCache, filePath=%v, errno=%v", filePath, errno)
			return errno
		}
		headIndex := binary.LittleEndian.Uint64(buf[:])
		filePath2 := ret.__getFilePath(headIndex)
		if filePath2 != filePath {
			log.Errorf("Failed: NewRaftLogger, invalid file path: filePath=%v (correct: %v), headIndex=%v", filePath, filePath2, headIndex)
			return unix.EINVAL
		}
		if ret.headIndex < headIndex {
			ret.headIndex = headIndex
		}
		return nil
	})
	if err != nil {
		return nil, ErrnoToReply(err)
	}
	if ret.headIndex == 0 {
		reply = ret.__createNewFile(1)
	} else {
		filePath := ret.__getFilePath(ret.headIndex)
		ret.disk = NewOnDiskLog(filePath, maxNrCache, 1)
		reply = RaftReplyOk
	}
	return
}

func (f *RaftLogger) __createNewFile(newHeadIndex uint64) (reply int32) {
	f.headIndex = newHeadIndex
	filePath := f.__getFilePath(newHeadIndex)
	f.disk = NewOnDiskLog(filePath, f.maxNrCache, 1)
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], newHeadIndex)
	_, _, errno := f.disk.AppendSingleBuffer(buf[:])
	if errno != nil {
		log.Errorf("Failed: __createNewFile, AppendSingleBuffer, errno=%v", errno)
		return ErrnoToReply(errno)
	}
	return RaftReplyOk
}

func (f *RaftLogger) __getFilePath(headIndex uint64) string {
	return fmt.Sprintf("%s%d.log", f.filePrefix, headIndex)
}

func (f *RaftLogger) __logOffsetToLogIndex(logOffset int64) (logIndex uint64) {
	if logOffset < 8 {
		return 0
	}
	return f.headIndex + uint64((logOffset-8)/int64(AppendEntryCommandLogSize))
}

func (f *RaftLogger) __logIndexToLogOffset(logIndex uint64) (logOffset int64) {
	if logIndex == 0 || logIndex < f.headIndex {
		return 0
	}
	return 8 + int64(logIndex-f.headIndex)*int64(AppendEntryCommandLogSize)
}

func (f *RaftLogger) LoadCommandAt(logIndex uint64) (cmd AppendEntryCommand, reply int32) {
	if logIndex == 0 {
		return GetAppendEntryCommand(math.MaxUint32, NewNoOpCommand()), RaftReplyOk
	}
	f.lock.RLock()
	logOffset := f.__logIndexToLogOffset(logIndex)
	if logOffset == 0 {
		f.lock.RUnlock()
		log.Errorf("Failed: LoadCommandAt, no log exists, filePath=%v, logIndex=%v, headIndex=%v", f.disk.filePath, logIndex, f.headIndex)
		return cmd, ErrnoToReply(unix.EOVERFLOW)
	}
	bufs, _, errno := f.disk.Read(logOffset, int64(len(cmd.buf)))
	if errno != nil {
		log.Errorf("Failed: LoadCommandAt, Read, filePath=%v, logIndex=%v, headIndex=%v, errno=%v", f.disk.filePath, logIndex, f.headIndex, errno)
	} else {
		var c = 0
		for _, buf := range bufs {
			c += copy(cmd.buf[c:], buf)
		}
	}
	f.lock.RUnlock()
	return cmd, ErrnoToReply(errno)
}

func (f *RaftLogger) AppendCommand(cmd AppendEntryCommand) (logIndex uint64, reply int32) {
	f.lock.RLock()
	vec, _, errno := f.disk.AppendSingleBuffer(cmd.buf[:])
	logIndex = f.__logOffsetToLogIndex(vec.logOffset)
	if errno != nil {
		log.Errorf("Failed: AppendCommand, AppendSingleBuffer, logOffset=%v, errno=%v", vec.logOffset, errno)
	}
	//log.Debugf("Success: AppendCommand, Term=%v, Index=%v", cmd.GetTerm(), logIndex)
	f.lock.RUnlock()
	return logIndex, ErrnoToReply(errno)
}

func (f *RaftLogger) Shrink(logIndex uint64) (reply int32) {
	f.lock.RLock()
	newSize := f.__logIndexToLogOffset(logIndex)
	if newSize == 0 {
		log.Errorf("Failed: Shrink, logIndex < headIndex, filePath=%v, logIndex=%v, headIndex=%v", f.disk.filePath, logIndex, f.headIndex)
		f.lock.RUnlock()
		return ErrnoToReply(unix.EOVERFLOW)
	}
	oldSize, errno := f.disk.Shrink(newSize)
	if errno != nil {
		log.Errorf("Failed: Shrink, Shrink, filePath=%v, logIndex=%v, headIndex=%v, errno=%v", f.disk.filePath, logIndex, f.headIndex, errno)
		f.lock.RUnlock()
		return ErrnoToReply(errno)
	}
	oldLogLength := f.__logOffsetToLogIndex(oldSize - int64(AppendEntryCommandLogSize))
	log.Debugf("Success: Shrink, logLength=%v->%v, size=%v->%v", oldLogLength, logIndex, oldSize, newSize)
	f.lock.RUnlock()
	return RaftReplyOk
}

func (f *RaftLogger) SwitchFileAndAppendCommand(cmd AppendEntryCommand) (logIndex uint64, reply int32) {
	f.lock.Lock()
	size := f.disk.WaitWrites()
	newHeadIndex := f.__logOffsetToLogIndex(size)
	reply = f.__createNewFile(newHeadIndex)
	if reply != RaftReplyOk {
		log.Errorf("Failed: SwitchFileAndAppendCommand, __createNewFile, filePath=%v, newHeadIndex=%v, reply=%v", f.disk.filePath, newHeadIndex, reply)
		f.lock.Unlock()
		return 0, reply
	}
	vec, _, errno := f.disk.AppendSingleBuffer(cmd.buf[:])
	if errno != nil {
		log.Errorf("Failed: SwitchFileAndAppendCommand, AppendSingleBuffer, filePath=%v, newHeadIndex=%v, errno=%v", f.disk.filePath, newHeadIndex, errno)
		f.lock.Unlock()
		return 0, ErrnoToReply(errno)
	}
	logIndex = f.__logOffsetToLogIndex(vec.logOffset)
	size = f.disk.WaitWrites()
	f.lock.Unlock()
	log.Infof("Success: SwitchFileAndAppendCommand, filePath=%v, newHeadIndex=%v, newSize=%v, logIndex=%v, errno=%v", f.disk.filePath, newHeadIndex, size, logIndex, errno)
	return logIndex, RaftReplyOk
}

func (f *RaftLogger) CompactLog() (reply int32) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.headIndex == 1 {
		log.Warnf("BUG: CompactLog, need SwitchFileAndAppendCommand before compaction")
		return RaftReplyOk
	}
	f.disk.WaitWrites()
	newPath := f.__getFilePath(1)
	err := unix.Unlink(newPath)
	if err != nil {
		log.Errorf("Failed: CompactLog, Unlink, newPath=%v, err=%v", newPath, err)
		return ErrnoToReply(err)
	}
	newDisk := NewOnDiskLog(newPath, 0, 1)
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], 1)
	_, _, errno := newDisk.AppendSingleBuffer(buf[:])
	if errno != nil {
		newDisk.Clear()
		log.Errorf("Failed: CompactLog, AppendSingleBuffer, errno=%v", errno)
		return ErrnoToReply(errno)
	}
	bufs, _, err := f.disk.Read(8, f.disk.size)
	if err != nil {
		newDisk.Clear()
		log.Errorf("Failed: CompactLog, Read, err=%v", err)
		return ErrnoToReply(err)
	}
	for _, buf := range bufs {
		_, _, err := newDisk.AppendSingleBuffer(buf)
		if err != nil {
			newDisk.Clear()
			log.Errorf("Failed: CompactLog, AppendSingleBuffer, errno=%v", errno)
			return ErrnoToReply(errno)
		}
	}
	err = unix.Unlink(f.__getFilePath(f.headIndex))
	if err != nil && err != unix.ENOENT {
		newDisk.Clear()
		log.Errorf("Failed: CompactLog, Unlink, err=%v", err)
		return ErrnoToReply(err)
	}
	log.Infof("Success: CompactLog, headIndex=%v->1", f.headIndex)
	f.headIndex = 1
	f.disk = newDisk
	return RaftReplyOk
}

func (f *RaftLogger) GetCurrentLogLength() uint64 {
	// NOTE: wait for all outstanding writes.
	f.lock.RLock()
	size := f.disk.WaitWrites()
	ret := f.__logOffsetToLogIndex(size - int64(AppendEntryCommandLogSize))
	f.lock.RUnlock()
	return ret
}

func (f *RaftLogger) Clear() {
	f.lock.RLock()
	f.disk.Clear()
	f.lock.RUnlock()
}

func (f *RaftLogger) CheckReset() (ok bool) {
	if ok = f.lock.TryLock(); !ok {
		log.Errorf("Failed: RaftLogger.CheckReset, f.lock is taken")
		return
	}
	if ok2 := f.disk.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftLogger.CheckReset, disk")
		ok = false
	}
	f.lock.Unlock()
	return
}

/**
 * RaftInstance
 */

const (
	RaftInit      = 0
	RaftFollower  = 1
	RaftCandidate = 2
	RaftLeader    = 3

	RaftReplyOk          = int32(api.Reply_Ok)
	RaftReplyFail        = int32(api.Reply_Fail)
	RaftReplyNotLeader   = int32(api.Reply_NotLeader)
	RaftReplyTimeout     = int32(api.Reply_Timeout)
	RaftReplyContinue    = int32(api.Reply_Continue)
	RaftReplyMismatchVer = int32(api.Reply_MismatchVer)
	RaftReplyRetry       = int32(api.Reply_Retry)
	RaftReplyVoting      = int32(api.Reply_Voting)
	RaftReplyNoGroup     = int32(api.Reply_NoGroup)
	RaftReplyExt         = int32(api.Reply_Ext)
)

type RaftInstance struct {
	lock           *sync.RWMutex
	applyCond      *sync.Cond  // to ensure ordering of Apply since apply may be very slow (e.g., huge truncate)
	hbLock         *sync.Mutex // to ensure ordering of heart beats.
	selfId         uint32
	selfAddr       common.NodeAddrInet4
	leaderId       uint32
	state          int32 // RaftInit, RaftFollower, RaftCandidate, RaftLeader, or RaftDead
	recvLastHB     int64
	lastMajorityHB int64
	hbInterval     time.Duration
	electTimeout   int64
	stopped        int32
	nrThreads      int32
	maxHBBytes     int
	resumeCallback func(*NodeServer) int32
	nodeServer     *NodeServer
	rpcClient      *RpcClient
	replyClient    *RpcReplyClient
	rpcServer      *RpcThreads
	serverIds      map[uint32]common.NodeAddrInet4
	sas            map[common.NodeAddrInet4]uint32

	// Extended log information
	extLogger       *OnDiskLogger
	extNextSeqNum   uint32
	extRaftLoggerId LogIdType
	extLock         *sync.RWMutex

	// Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	log         *RaftLogger  // log entries; each entry contains command for state machine, and Term when entry was received by leader (first reqId is 1)
	currentTerm *OnDiskState // latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    *OnDiskState // candidateId that received vote in current Term (or null if none) (NOTE: null -> uint32 MAX)

	// Volatile state on all servers:
	commitIndex uint64 // reqId of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied uint64 // reqId of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  map[uint32]uint64 // for each server, reqId of the next log entry to send to that server (initialized to leader last log reqId + 1)
	matchIndex map[uint32]uint64 // for each server, reqId of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	replaying bool
}

func (n *RaftInstance) IsLeader() (r RaftBasicReply) {
	n.lock.RLock()
	if n.leaderId == OnDiskStateReset {
		r = RaftBasicReply{reply: RaftReplyVoting}
	} else if n.selfId != n.leaderId || n.state != RaftLeader {
		r = RaftBasicReply{reply: RaftReplyNotLeader, leaderId: n.leaderId, leaderAddr: n.serverIds[n.leaderId]}
	} else {
		r = RaftBasicReply{reply: RaftReplyOk, leaderId: n.selfId, leaderAddr: n.selfAddr}
	}
	n.lock.RUnlock()
	return r
}

func (n *RaftInstance) GetExtLogId() LogIdType {
	n.extLock.RLock()
	ret := n.extRaftLoggerId
	n.extLock.RUnlock()
	return ret
}

func (n *RaftInstance) GetExtLogIdForLogCompaction() LogIdType {
	n.extLock.RLock()
	ret := n.extRaftLoggerId
	ret.upper += 1
	n.extLock.RUnlock()
	return ret
}

func (n *RaftInstance) GenerateCoordinatorId() CoordinatorId {
	n.extLock.Lock()
	seqNum := n.extNextSeqNum
	n.extNextSeqNum = seqNum + 1
	n.extLock.Unlock()
	return CoordinatorId{clientId: n.selfId, seqNum: seqNum}
}

func (n *RaftInstance) SetExt(logId uint32, seqNum uint32) {
	n.extLock.Lock()
	n.extRaftLoggerId = LogIdType{lower: 0, upper: logId}
	n.extNextSeqNum = seqNum
	n.extLock.Unlock()
}

func (n *RaftInstance) SwitchExtLog(logId uint32) {
	n.extLock.Lock()
	n.extRaftLoggerId = LogIdType{lower: 0, upper: logId}
	n.extLock.Unlock()
}

func (n *RaftInstance) CleanExtLogger() {
	n.extLock.Lock()
	if _, err := n.extLogger.Remove(n.extRaftLoggerId); err != nil {
		log.Errorf("Failed: CleanExtLogger, Remove, extRaftLoggerId=%v, err=%v", n.extRaftLoggerId, err)
	} else {
		n.extRaftLoggerId = LogIdType{lower: 0, upper: 0}
		n.extNextSeqNum = 0
	}
	n.extLock.Unlock()
}

func (n *RaftInstance) updateLeader(leaderId uint32) error {
	n.lock.Lock()
	if n.state != RaftFollower {
		log.Infof("updateLeader, convert to follower from %v, leaderId=%v, selfId=%v", n.state, leaderId, n.selfId)
		n.state = RaftFollower
	}
	if n.leaderId != leaderId {
		log.Infof("updateLeader, changed leader, leaderId=%v->%v, selfId=%v", n.leaderId, leaderId, n.selfId)
		n.leaderId = leaderId
	}
	n.lock.Unlock()
	return nil
}

func (n *RaftInstance) AppendEntriesRpcTopHalf(msg RpcMsg, sa common.NodeAddrInet4, fd int) (success bool, abort bool) {
	term, prevLogTerm, prevLogIndex, _, leaderId := msg.GetAppendEntryArgs()
	atomic.StoreInt64(&n.recvLastHB, time.Now().UnixNano())
	currentTerm := n.currentTerm.Get()
	n.lock.RLock()
	currentLeader := n.leaderId
	currentState := n.state
	n.lock.RUnlock()
	currentLogLength := n.log.GetCurrentLogLength()
	priorCmd, priorCmdFound := n.log.LoadCommandAt(prevLogIndex)

	var reply = RaftReplyOk
	abort = true // cancel the connection
	success = false
	// Rule for Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (3.3)
	if term > currentTerm {
		if err := n.updateLeader(leaderId); err != nil {
			log.Errorf("Failed: AppendEntriesRpcTopHalf, RemoveAllAndAddSaFd, leaderId=%v, err=%v", leaderId, err)
			goto fail
		}
		if reply = n.currentTerm.Set(term); reply != RaftReplyOk {
			log.Errorf("Failed: AppendEntriesRpcTopHalf, currentTerm.Set, newTerm=%v, reply=%v", term, reply)
			goto fail
		}
		log.Infof("AppendEntriesRpcTopHalf, set currentTerm = %v -> %v", term, currentTerm)
		currentLeader = leaderId
		currentTerm = term
	}

	// Rules for Candidates: If AppendEntries RPC received from new leader: convert to follower
	if currentState == RaftCandidate || currentState == RaftInit {
		if err := n.updateLeader(leaderId); err != nil {
			log.Errorf("Failed: CheckAndEntriesRpc, RemoveAllAndAddSaFd, leaderId=%v, err=%v", leaderId, err)
			goto fail
		}
		if reply = n.votedFor.Reset(); reply != RaftReplyOk {
			log.Errorf("Failed: AppendEntriesRpcTopHalf, votedFor.Reset, reply=%v", reply)
			goto fail
		}
		currentLeader = leaderId
	}
	if currentState == RaftFollower && currentLeader != leaderId {
		if err := n.updateLeader(leaderId); err != nil {
			log.Errorf("Failed: CheckAndEntriesRpc, RemoveAllAndAddSaFd, leaderId=%v, err=%v", leaderId, err)
			goto fail
		}
	}

	abort = false

	// 1. Reply false if Term < currentTerm (§5.1)
	if term < currentTerm {
		log.Errorf("Reject: AppendEntriesRpcTopHalf, term (%v) < currentTerm (%v)", term, currentTerm)
		goto fail
	}

	if prevLogIndex != 0 && prevLogTerm != 0 {
		// 2. Reply false if log doesn't contain an entry at prevLogIndex whose Term matches prevLogTerm (§5.3)
		if priorCmdFound != RaftReplyOk {
			// NOTE: need to wait for preceding log entries from the leader and ensure their ordering (Log Matching Property)
			log.Errorf("Reject: AppendEntriesRpcTopHalf, log doesn't contain an entry at prevLogIndex (%v)", prevLogIndex)
			goto fail
		} else if priorCmd.GetTerm() != prevLogTerm {
			log.Errorf("Reject: AppendEntriesRpcTopHalf, log doesn't contain an entry at prevLogIndex (%v) whose Term (%v) matches prevLogTerm (%v)",
				prevLogIndex, priorCmd.GetTerm(), prevLogTerm)
			goto fail
		}
	}
	success = true
	return
fail:
	msg.FillAppendEntriesResponseArgs(currentTerm, false, currentLogLength, reply)
	if reply = n.replyClient.ReplyRpcMsg(msg, fd, sa, n.extLogger, n.hbInterval, nil); reply != RaftReplyOk {
		log.Errorf("Failed: AppendEntriesRpcTopHalf, ReplyRpcMsg, reply=%v", reply)
	}
	return
}

func (n *RaftInstance) AppendEntriesRpcBottomHalf(msg RpcMsg, sa common.NodeAddrInet4, fd int) (hadEntry bool) {
	begin := time.Now()
	term, _, prevLogIndex, leaderCommit, _ := msg.GetAppendEntryArgs()
	nrEntries := msg.GetAppendEntryNrEntries()
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	currentLogLength := n.log.GetCurrentLogLength()
	for newIndex := prevLogIndex + 1; newIndex < currentLogLength && newIndex < prevLogIndex+1+uint64(nrEntries); newIndex++ {
		currentCmd, currentCmdFound := n.log.LoadCommandAt(newIndex)
		if currentCmdFound == RaftReplyOk && currentCmd.GetTerm() != term {
			if reply := n.log.Shrink(newIndex); reply != RaftReplyOk {
				msg = RpcMsg{}
				currentTerm := n.currentTerm.Get()
				msg.FillAppendEntriesResponseArgs(currentTerm, true, currentLogLength, reply)
				log.Errorf("Failed: AppendEntriesRpcBottomHalf, Shrink, newIndex=%v, reply=%v", newIndex, reply)
				if reply := n.replyClient.ReplyRpcMsg(msg, fd, sa, n.extLogger, n.hbInterval, nil); reply != RaftReplyOk {
					log.Errorf("Failed: AppendEntriesRpcBottomHalf, ReplyRpcMsg, term=%v, logLength=%v, leaderCommit=%v, reply=%v", currentTerm, currentLogLength, leaderCommit, reply)
				}
				return
			}
			currentLogLength = n.log.GetCurrentLogLength()
			break
		}
	}
	seek := time.Now()

	// 4. Append any new entries not already in the log
	optHeaderLength := msg.GetOptHeaderLength()
	hadEntry = false
	var i = uint64(0)
	cmds := make([]*AppendEntryCommand, 0)
	extLogCmds := make([]ExtLogCommandImpl, 0)
	if 0 < optHeaderLength {
		for off := uint16(RpcOptControlHeaderLength); off < optHeaderLength; {
			cmd, nextOff := msg.GetAppendEntryCommandDiskFormat(off)
			var reply int32
			if currentLogLength == prevLogIndex+i {
				currentLogLength, reply = n.log.AppendCommand(cmd)
				if reply == RaftReplyOk {
					log.Debugf("AppendEntriesRpcBottomHalf, AppendCommandAt, Index=%v, extCmdId=%v, off=%v, nextOff=%v", prevLogIndex+1+i, cmd.GetExtCmdId(), off, nextOff)
				} else {
					log.Errorf("Failed: AppendEntriesRpcBottomHalf, AppendCommandAt, Index=%v, extCmdId=%v, reply=%v", prevLogIndex+1+i, cmd.GetExtCmdId(), reply)
				}
			} else {
				cmd, reply = n.log.LoadCommandAt(prevLogIndex + i + 1)
				if reply == RaftReplyOk {
					log.Warnf("AppendEntriesRpcBottomHalf, skip duplicated AppendCommand, Index=%v, extCmdId=%v, currentLogIndex=%v",
						prevLogIndex+1+i, cmd.GetExtCmdId(), currentLogLength)
				} else {
					log.Errorf("Failed: AppendEntriesRpcBottomHalf, LoadCommandAt, Index=%v, extCmdId=%v, reply=%v", prevLogIndex+1+i, cmd.GetExtCmdId(), reply)
				}
				currentLogLength = prevLogIndex + i + 1
			}
			if reply == RaftReplyOk {
				extLogCmd, err := LoadExtBuf(n.extLogger, &cmd)
				if err == nil {
					extLogCmds = append(extLogCmds, extLogCmd)
				} else {
					log.Errorf("Failed: AppendEntriesRpcBottomHalf, LoadExtBuf, Index=%v, extCmdId=%v, err=%v", prevLogIndex+1+i, cmd.GetExtCmdId(), err)
					reply = ErrnoToReply(err)
				}
			}
			if reply != RaftReplyOk {
				msg = RpcMsg{}
				currentTerm := n.currentTerm.Get()
				msg.FillAppendEntriesResponseArgs(currentTerm, true, currentLogLength, reply)
				if reply := n.replyClient.ReplyRpcMsg(msg, fd, sa, n.extLogger, n.hbInterval, nil); reply != RaftReplyOk {
					log.Errorf("Failed: AppendEntriesRpcBottomHalf, ReplyRpcMsg, term=%v, logLength=%v, leaderCommit=%v, reply=%v", currentTerm, currentLogLength, leaderCommit, reply)
				}
				return
			}
			cmds = append(cmds, &cmd)
			hadEntry = true
			off = nextOff
			i += 1
		}
	}
	appendTime := time.Now()

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	n.lock.Lock()
	if hadEntry && leaderCommit > n.commitIndex {
		lastNewEntryIndex := prevLogIndex + i
		old := n.commitIndex
		if lastNewEntryIndex > leaderCommit {
			n.commitIndex = leaderCommit
		} else {
			n.commitIndex = lastNewEntryIndex
		}
		log.Debugf("AppendEntriesRpcBottomHalf, commitIndex: %v -> %v, lastNewEntryIndex=%v, leaderCommit=%v", old, n.commitIndex, lastNewEntryIndex, leaderCommit)
	}
	n.lock.Unlock()
	updateCommitIndexTime := time.Now()

	for j := uint64(0); j < i-1; j += 1 {
		n.ApplyAll(cmds[j], prevLogIndex+1+j, extLogCmds[j])
	}
	apply := time.Now()

	msg = RpcMsg{}
	currentTerm := n.currentTerm.Get()
	msg.FillAppendEntriesResponseArgs(currentTerm, true, currentLogLength, RaftReplyOk)
	if reply := n.replyClient.ReplyRpcMsg(msg, fd, sa, n.extLogger, n.hbInterval, nil); reply != RaftReplyOk {
		log.Errorf("Failed: AppendEntriesRpcBottomHalf, ReplyRpcMsg, term=%v, logLength=%v, leaderCommit=%v, reply=%v", currentTerm, currentLogLength, leaderCommit, reply)
		return
	}
	reply := time.Now()
	if hadEntry {
		log.Debugf("Success: AppendEntriesRpcBottomHalf, term=%v, logLength=%v, leaderCommit=%v, seekTime=%v, updateCommitIndexTime=%v, appendTime=%v, applyTime=%v, replyTime=%v",
			currentTerm, currentLogLength, leaderCommit, seek.Sub(begin), appendTime.Sub(seek), updateCommitIndexTime.Sub(appendTime), apply.Sub(updateCommitIndexTime), reply.Sub(apply))
	}
	return
}

func (n *RaftInstance) HandleAppendEntriesResponse(msg RpcMsg, sa common.NodeAddrInet4) int32 {
	nodeId, ok := n.sas[sa]
	if !ok {
		log.Errorf("Failed: HandleAppendEntriesResponse, unknown sa, sa=%v", sa)
		return RaftReplyFail
	}
	term, success, logLength, reply := msg.GetAppendEntriesResponseArgs()
	if reply != RaftReplyOk {
		log.Errorf("Failed: HandleAppendEntriesResponse, remote AppendEntry failed, sa=%v, reply=%v", sa, reply)
		return reply
	}
	if !success {
		T := n.currentTerm.Get()
		log.Errorf("Rejected: HandleAppendEntriesResponse, AppendEntries, Term=%v (peer: %v)", T, term)
		if T < term {
			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (3.3)
			if reply = n.votedFor.Reset(); reply != RaftReplyOk {
				log.Errorf("Failed: HandleAppendEntriesResponse, votedFor.Reset, reply=%v", reply)
				return reply
			}
			if reply = n.currentTerm.Set(term); reply != RaftReplyOk {
				log.Errorf("Failed: HandleAppendEntriesResponse, currentTerm.Set, term=%v, reply=%v", term, reply)
				return reply
			}
			n.lock.Lock()
			n.state = RaftFollower
			n.leaderId = OnDiskStateReset
			n.lock.Unlock()
			log.Infof("HandleAppendEntriesResponse, converted to follower state from leader")
			return RaftReplyNotLeader
		}
		n.lock.Lock()
		nextIndex, ok := n.nextIndex[nodeId]
		if !ok {
			nextIndex = 0
		}
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (3.5)
		log.Debugf("HandleAppendEntriesResponse, decrement nextIndex and retry, nextIndex[%v]: %v -> %v", nodeId, nextIndex, logLength)
		n.nextIndex[nodeId] = logLength
		n.lock.Unlock()
		return RaftReplyFail
	}
	// If successful, update nextIndex and matchIndex for followers (3.5)
	lastReplicatedIndex := logLength - 1
	n.lock.Lock()
	old := n.nextIndex[nodeId]
	if old != lastReplicatedIndex+1 {
		log.Debugf("HandleAppendEntriesResponse, update nextIndex for followers, nextIndex[%v]: %v -> %v", nodeId, old, lastReplicatedIndex+1)
		n.nextIndex[nodeId] = lastReplicatedIndex + 1
	}
	n.matchIndex[nodeId] = lastReplicatedIndex
	n.lock.Unlock()
	return RaftReplyOk
}

func (n *RaftInstance) ReplicateLog(lastLogIndex uint64, added *uint32, addedSa *common.NodeAddrInet4, removedNodeId *uint32, cmd *AppendEntryCommand, extLogCmd ExtLogCommandImpl) (reply int32) {
	begin := time.Now()
	if reply = n.heartBeatOneRound(&lastLogIndex, added, addedSa, removedNodeId); reply != RaftReplyOk {
		elapsed := time.Since(begin)
		log.Errorf("Failed: ReplicateLog, heartBeatOneRound, reply=%v, elapsed=%v", reply, elapsed)
		return
	}
	endHb := time.Now()
	n.lock.Lock()
	if n.commitIndex < lastLogIndex {
		n.commitIndex = lastLogIndex
	}
	n.lock.Unlock()
	n.ApplyAll(cmd, lastLogIndex, extLogCmd)

	log.Debugf("Success: ReplicateLog, lastLogIndex=%v, hb=%v, apply=%v", lastLogIndex, endHb.Sub(begin), time.Since(endHb))
	return RaftReplyOk
}

func (n *RaftInstance) SwitchFileAndAppendEntriesLocal(rc RaftCommand, extLogCmd ExtLogCommandImpl) (ret interface{}, lastLogIndex uint64, reply int32) {
	if r := n.IsLeader(); r.reply != RaftReplyOk {
		return nil, 0, r.reply
	}
	if len(n.serverIds) > 1 {
		log.Errorf("Not implemented: SwitchFileAndAppendEntriesLocal, need to implement new RPC for switching logs at followers")
		return nil, 0, ErrnoToReply(unix.ENOSPC)
	}
	//begin := time.Now()
	cmd := GetAppendEntryCommand(n.currentTerm.Get(), rc)
	lastLogIndex, reply = n.log.SwitchFileAndAppendCommand(cmd)
	if reply != RaftReplyOk {
		log.Errorf("Failed: SwitchFileAndAppendEntriesLocal, SwitchFileAndAppendCommand, reply=%v", reply)
		return nil, 0, reply
	}
	/*endLocal := time.Now()
	reply = n.ReplicateLog(lastLogIndex, nil, nil, nil, &cmd, extLogCmd)
	endAll := time.Now()
	if reply != RaftReplyOk {
		log.Errorf("Failed: SwitchFileAndAppendEntriesLocal, ReplicateLog, lastLogIndex=%v, replicate=%v, reply=%v", lastLogIndex, endAll.Sub(endLocal), reply)
		return nil, 0, reply
	}*/

	//log.Debugf("Success: SwitchFileAndAppendEntriesLocal, lastLogIndex=%v, writeLocal=%v, replicate=%v",
	//	lastLogIndex, endLocal.Sub(begin), endAll.Sub(endLocal))
	return ret, lastLogIndex, RaftReplyOk
}

func (n *RaftInstance) AppendEntriesLocal(rc RaftCommand, extLogCmd ExtLogCommandImpl) (ret interface{}, lastLogIndex uint64, reply int32) {
	if r := n.IsLeader(); r.reply != RaftReplyOk {
		return nil, 0, r.reply
	}
	//begin := time.Now()
	cmd := GetAppendEntryCommand(n.currentTerm.Get(), rc)
	lastLogIndex, reply = n.log.AppendCommand(cmd)
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendEntriesLocal, AppendCommand, reply=%v", reply)
		return nil, 0, reply
	}
	endLocal := time.Now()
	reply = n.ReplicateLog(lastLogIndex, nil, nil, nil, &cmd, extLogCmd)
	endAll := time.Now()
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendEntriesLocal, ReplicateLog, lastLogIndex=%v, replicate=%v, reply=%v", lastLogIndex, endAll.Sub(endLocal), reply)
		return nil, 0, reply
	}

	//log.Debugf("Success: AppendEntriesLocal, lastLogIndex=%v, writeLocal=%v, replicate=%v",
	//	lastLogIndex, endLocal.Sub(begin), endAll.Sub(endLocal))
	return ret, lastLogIndex, RaftReplyOk
}

func (n *RaftInstance) __appendExtendedLogEntry(extLogCmd ExtLogCommandImpl, logId LogIdType, switchLogFile bool) int32 {
	if r := n.IsLeader(); r.reply != RaftReplyOk {
		return r.reply
	}
	begin := time.Now()
	buf, err := proto.Marshal(extLogCmd.toMsg())
	if err != nil {
		log.Errorf("Failed: AppendExtendedLogEntry, Marshal, err=%v", err)
		return RaftReplyFail
	}
	marshalTime := time.Now()
	logOffset, err := n.extLogger.AppendSingleBuffer(logId, buf)
	if err != nil {
		log.Errorf("Failed: AppendExtendedLogEntry, AppendSingleBuffer, logId=%v, len(buf)=%v, err=%v", logId, len(buf), err)
		return ErrnoToReply(err)
	}
	writeTime := time.Now()
	if switchLogFile {
		_, _, reply := n.SwitchFileAndAppendEntriesLocal(NewExtLogCommandFromExtBuf(extLogCmd.GetExtCmdId(), logId, logOffset, buf), extLogCmd)
		if reply != RaftReplyOk {
			log.Errorf("Failed: AppendExtendedLogEntry, SwitchFileAndAppendEntriesLocal, reply=%v", reply)
			return reply
		}
	} else {
		_, _, reply := n.AppendEntriesLocal(NewExtLogCommandFromExtBuf(extLogCmd.GetExtCmdId(), logId, logOffset, buf), extLogCmd)
		if reply != RaftReplyOk {
			log.Errorf("Failed: AppendExtendedLogEntry, AppendEntriesLocal, reply=%v", reply)
			return reply
		}
	}
	appendEntryTime := time.Now()
	log.Debugf("Success: AppendExtendedLogEntry, logId=%v, extCmdId=%v, marshalTime=%v, writeTime=%v, appendEntryTime=%v",
		logId, extLogCmd.GetExtCmdId(), marshalTime.Sub(begin), writeTime.Sub(marshalTime), appendEntryTime.Sub(writeTime))
	return RaftReplyOk
}

func (n *RaftInstance) AppendExtendedLogEntry(extLogCmd ExtLogCommandImpl) int32 {
	return n.__appendExtendedLogEntry(extLogCmd, n.GetExtLogId(), false)
}

// heartBeatOneRound is thread-safe with n.hbLock
func (n *RaftInstance) heartBeatOneRound(maxLogIndexPointer *uint64, added *uint32, addedSa *common.NodeAddrInet4, removedNodeId *uint32) int32 {
	begin := time.Now()
	n.hbLock.Lock()
	n.lock.RLock()
	currentTerm := n.currentTerm.Get()
	if n.state != RaftLeader {
		n.lock.RUnlock()
		n.hbLock.Unlock()
		return RaftReplyNotLeader
	}
	logLength := n.log.GetCurrentLogLength()
	if logLength == 0 {
		n.lock.RUnlock()
		n.hbLock.Unlock()
		return RaftReplyOk
	}
	if maxLogIndexPointer != nil {
		logLength = *maxLogIndexPointer + 1
	} else if time.Now().UnixNano()-atomic.LoadInt64(&n.lastMajorityHB) < n.hbInterval.Nanoseconds() {
		n.lock.RUnlock()
		n.hbLock.Unlock()
		return RaftReplyOk
	}
	selfId := n.selfId
	commitIndex := n.commitIndex
	serverNextIndex := map[common.NodeAddrInet4]uint64{}
	for nodeId, sa := range n.serverIds {
		if nodeId == n.selfId || (removedNodeId != nil && *removedNodeId == nodeId) {
			continue
		}
		nextIndex, ok := n.nextIndex[nodeId]
		if !ok {
			nextIndex = logLength + 1 // initialized to leader last log index + 1
		}
		serverNextIndex[sa] = nextIndex
	}
	if added != nil && addedSa != nil {
		nextIndex, ok := n.nextIndex[*added]
		if !ok {
			nextIndex = logLength + 1 // initialized to leader last log index + 1
		}
		serverNextIndex[*addedSa] = nextIndex
	}
	n.lock.RUnlock()
	section1 := time.Now()

	var updateIndex = false
	var numServers = 1
	var allBytes = int32(0)
	messages := map[int]RpcMsg{}
	var quorum = 1
	for sa, nextIndex := range serverNextIndex {
		numServers += 1
		if maxLogIndexPointer != nil && logLength <= nextIndex {
			quorum += 1
		}
		fd, err := n.rpcClient.Connect(sa)
		if err != nil {
			continue
		}
		msg := RpcMsg{}
		if prevCmd, reply := n.log.LoadCommandAt(nextIndex - 1); reply == RaftReplyOk {
			msg.FillAppendEntryArgs(currentTerm, prevCmd.GetTerm(), nextIndex-1, commitIndex, selfId)
		} else {
			log.Errorf("Failed: heartBeatOneRound, LoadCommandAt, nextIndex=%v, reply=%v", nextIndex, reply)
		}
		// If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		updateIndex = nextIndex < logLength
		if updateIndex {
			preallocateBytes := RpcOptControlHeaderLength + AppendEntryCommandLogSize*int32(logLength-nextIndex)
			if preallocateBytes > int32(n.maxHBBytes) {
				preallocateBytes = int32(n.maxHBBytes)
			}
			msg.optBuf = make([]byte, preallocateBytes)
		}
		var appendedBytes = int(RpcMsgMaxLength)
		for i := uint64(0); nextIndex+i < logLength && i < uint64(math.MaxUint16) && appendedBytes < n.maxHBBytes; i++ {
			cmd, reply := n.log.LoadCommandAt(nextIndex + i)
			if reply != RaftReplyOk {
				log.Errorf("Failed: heartBeatOneRound, LoadCommandAt, logIndex=%v, reply=%v", nextIndex+i, reply)
				break
			}
			optHeaderLength, totalExtLogLength := cmd.AppendToRpcMsg(&msg)
			appendedBytes = int(RpcMsgMaxLength) + int(optHeaderLength) + int(totalExtLogLength)
			log.Debugf("heartBeatOneRound, prepare: sa=%v, Term=%v, Index=%v, extCmdId=%v, optHeaderLength=%v, totalExtLogLength=%v",
				sa, cmd.GetTerm(), nextIndex+i, cmd.GetExtCmdId(), optHeaderLength, totalExtLogLength)
		}
		allBytes += int32(appendedBytes)
		messages[fd] = msg
	}
	section2 := time.Now()
	if maxLogIndexPointer != nil && quorum >= numServers {
		// another caller has been completed replication
	} else {
		quorum = n.rpcClient.BroadcastAndWaitRpcMsg(messages, n, n.hbInterval, updateIndex) + 1
	}
	section3 := time.Now()
	// Note: we always need to check responses if any failures happen (failed writes delete fds, and so, we unlikely wait all)
	if quorum < 1+numServers/2 {
		n.hbLock.Unlock()
		log.Errorf("Failed: heartBeatOneRound, BroadcastAndWaitRpcMsg, logLength=%v, quorum=%v, numServers=%v, allBytes=%v, raft Lock=%v, AppendToRpcMsg=%v, BroadcastAndWaitRpcMsg=%v",
			logLength, quorum, numServers, allBytes, section1.Sub(begin), section2.Sub(section1), section3.Sub(section2))
		return RaftReplyRetry
	}
	atomic.StoreInt64(&n.lastMajorityHB, time.Now().UnixNano())
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm;
	// set commitIndex = N (3.5, 3.6)
	if updateIndex {
		n.lock.RLock()
		indices := make([]uint64, 0)
		for _, matchIndex := range n.matchIndex {
			indices = append(indices, matchIndex)
		}
		n.lock.RUnlock()

		indices = append(indices, logLength-1)
		sort.Slice(indices, func(i int, j int) bool { return indices[i] < indices[j] })
		N := indices[len(indices)/2]
		prevLog, reply := n.log.LoadCommandAt(N)

		n.lock.Lock()
		if reply == RaftReplyOk && N > n.commitIndex {
			currentTerm = n.currentTerm.Get()
			if prevLog.GetTerm() == currentTerm {
				log.Debugf("heartBeatOneRound, update commitIndex, commitIndex: %v -> %v, Term=%v", n.commitIndex, N, currentTerm)
				n.commitIndex = N
			}
		}
		n.lock.Unlock()
	}
	//endAll := time.Now()
	//if maxLogIndexPointer != nil {
	//log.Debugf("heartBeatOneRound, logIndex=%v, bytes=%v, raft Lock=%v, AppendToRpcMsg=%v, BroadcastAndWaitRpcMsg=%v, UpdateIndex=%v",
	//	*maxLogIndexPointer, allBytes, section1.Sub(begin), section2.Sub(section1), section3.Sub(section2), endAll.Sub(section3))
	//}
	n.hbLock.Unlock()
	return RaftReplyOk
}

func (n *RaftInstance) HeartBeaterThread() {
	atomic.AddInt32(&n.nrThreads, 1)
	for atomic.LoadInt32(&n.stopped) == 0 {
		reply := n.heartBeatOneRound(nil, nil, nil, nil)
		if reply == RaftReplyNotLeader {
			log.Infof("HeartBeaterThread, this node became non-leader. exit")
			break
		}
		// if election timeout elapses without successful round of heartbeats to majority of servers, convert to follower (6.2)
		elapsed := time.Now().UnixNano() - atomic.LoadInt64(&n.lastMajorityHB)
		if elapsed >= n.electTimeout {
			log.Errorf("Failed, HeartBeaterThread, timeout and step down to a follower, elapsed=%v, electTimeout=%v", elapsed, time.Duration(n.electTimeout))
			n.lock.Lock()
			n.StepDown(n.commitIndex)
			n.lock.Unlock()
			break
		}
		time.Sleep(n.hbInterval)
	}
	atomic.AddInt32(&n.nrThreads, -1)
}

func (n *RaftInstance) RequestVoteRpc(msg RpcMsg, sa common.NodeAddrInet4, fd int) bool {
	T, candidateId, candidateLogTerm, candidateLogIndex := msg.GetRequestVoteArgs()
	atomic.StoreInt64(&n.recvLastHB, time.Now().UnixNano())
	n.lock.RLock()
	currentTerm := n.currentTerm.Get()
	votedFor := n.votedFor.Get()
	currentState := n.state
	n.lock.RUnlock()

	var reply = RaftReplyOk
	voteGranted := false

	// RaftInit is cleared during Node join/leave not to disrupt voting
	if currentState == RaftInit {
		goto respond
	}
	// Rule for Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (3.3)
	if T > currentTerm {
		if reply = n.votedFor.Reset(); reply != RaftReplyOk {
			log.Errorf("Failed: HandleAppendEntriesResponse, votedFor.Reset, reply=%v", reply)
			goto respond
		}
		if reply = n.currentTerm.Set(T); reply != RaftReplyOk {
			log.Errorf("Failed: RequestVoteRpc, currentTerm.Set, T=%v, reply=%v", T, reply)
			goto respond
		}
		if err := n.updateLeader(candidateId); err != nil {
			log.Errorf("Failed: RequestVoteRpc, updateLeader, candidateId=%v, err=%v", candidateId, err)
			goto respond
		}
		currentTerm = T
		votedFor = OnDiskStateReset
	}

	// 1. Reply false if T < currentTerm (3.3)
	if T < currentTerm {
		log.Errorf("Failed: RequestVoteRpc, T (%v) < currentTerm (%v), candidateId=%v", T, currentTerm, candidateId)
		goto respond
	}
	// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (3.4, 3.6)
	if ok := votedFor == OnDiskStateReset || votedFor == candidateId; ok {
		logLength := n.log.GetCurrentLogLength()
		lastLog, reply := n.log.LoadCommandAt(logLength - 1)
		if reply != RaftReplyOk {
			log.Errorf("Failed: RequestVoteRpc, LoadCommandAt, logLength=%v, reply=%v", logLength, reply)
		} else {
			// precise definition of "up-to-date":
			// If the logs have last entries with different terms, then the log with the later T is more up-to-date.
			// If the log end with the T, then whichever log is longer is more up-to-date.
			ok = lastLog.GetTerm() < candidateLogTerm || (lastLog.GetTerm() == candidateLogTerm && logLength-1 <= candidateLogIndex)
			if !ok {
				log.Errorf("Failed: RequestVoteRpc, do not grant vote (reason: candidate's log (Id=%v, Term=%v, Index=%v) "+
					"is not at least as up-to-date as receiver's log (Term=%v, Index=%v)), T=%v",
					candidateId, candidateLogTerm, candidateLogIndex, lastLog.GetTerm(), logLength-1, currentTerm)
			}
		}
		if ok {
			if votedFor != candidateId {
				if reply = n.votedFor.Set(candidateId); reply != RaftReplyOk {
					log.Errorf("Failed: RequestVoteRpc, votedFor.Set, candidateId=%v, reply=%v", candidateId, reply)
					goto respond
				}
			}
			log.Infof("Success: RequestVoteRpc, votedFor=%v, T=%v", candidateId, currentTerm)
			voteGranted = true
		}
	} else {
		log.Errorf("Failed: RequestVoteRpc, do not grant vote (reason: votedFor (%v) was not null or candidateId (%v)), T=%v",
			votedFor, candidateId, currentTerm)
	}
respond:
	msg.FillRequestVoteResponseArgs(T, voteGranted, reply)
	if reply = n.replyClient.ReplyRpcMsg(msg, fd, sa, n.extLogger, n.hbInterval, nil); reply != RaftReplyOk {
		log.Errorf("Failed: RequestVoteRpc, ReplyRpcMsg, sa=%v, reply=%v", sa, reply)
	} else {
		log.Errorf("Success: RequestVoteRpc, T=%v, votedFor=%v, candidateId=%v, voteGranted=%v", T, votedFor, candidateId, voteGranted)
	}
	return true
}

func (n *RaftInstance) HandleRequestVoteResponse(msg RpcMsg, sa common.NodeAddrInet4) int32 {
	term, voteGranted, reply := msg.GetRequestVoteResponseArgs()
	if reply != RaftReplyOk {
		log.Errorf("Failed: HandleRequestVoteResponse, GetRequestVoteResponseArgs, reply=%v", reply)
		return reply
	}
	if !voteGranted {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (3.3)
		currentTerm := n.currentTerm.Get()
		if currentTerm < term {
			if reply = n.votedFor.Reset(); reply != RaftReplyOk {
				log.Errorf("Failed: HandleRequestVoteResponse, votedFor.Reset, reply=%v", reply)
				return reply
			}
			if reply = n.currentTerm.Set(term); reply != RaftReplyOk {
				log.Errorf("Failed: HandleRequestVoteResponse, currentTerm.Set, Term=%v, reply=%v", term, reply)
				return reply
			}
			n.lock.Lock()
			n.state = RaftFollower
			n.lock.Unlock()
			log.Infof("Raft: HandleRequestVoteResponse, Convert to follower")
			return RaftReplyNotLeader
		}
		log.Debugf("Rejected: HandleRequestVoteResponse, RequestVote, sa=%v, Term=%v, currentTerm=%v", sa, term, currentTerm)
		return RaftReplyFail
	}
	log.Debugf("Success: HandleRequestVoteResponse, RequestVote, sa=%v", sa)
	return RaftReplyOk
}

func (n *RaftInstance) requestVotesAny() bool {
	logLength := n.log.GetCurrentLogLength()
	var lastLogTerm = uint32(math.MaxUint32)
	if logLength > 0 {
		lastLog, reply := n.log.LoadCommandAt(logLength - 1)
		if reply != RaftReplyOk {
			log.Errorf("Failed: requestVotesAny, LoadCommandAt, logLength=%v, reply=%v", logLength, reply)
			return false
		}
		lastLogTerm = lastLog.GetTerm()
	} else {
		logLength = 1
	}

	n.lock.RLock()
	currentTerm := n.currentTerm.Get()
	var numServers = 0
	messages := map[int]RpcMsg{}
	sas := map[common.NodeAddrInet4]uint32{}
	for nodeId, sa := range n.serverIds {
		numServers += 1
		if nodeId == n.selfId {
			continue
		}
		fd, err := n.rpcClient.Connect(sa)
		if err != nil {
			log.Errorf("Failed: requestVotesAny, Connect, sa=%v, err=%v", sa, err)
			continue
		}
		msg := RpcMsg{}
		msg.FillRequestVoteArgs(currentTerm, n.selfId, lastLogTerm, logLength)
		messages[fd] = msg
		sas[sa] = nodeId
	}
	n.lock.RUnlock()
	n.hbLock.Lock()
	quorum := n.rpcClient.BroadcastAndWaitRpcMsg(messages, n, time.Duration(n.electTimeout), true) + 1
	n.hbLock.Unlock()
	if reply2 := n.votedFor.Reset(); reply2 != RaftReplyOk {
		log.Errorf("Failed: requestVotesAny, votedFor.Reset, reply=%v", reply2)
		return false
	}
	if quorum < 1+numServers/2 {
		log.Errorf("Failed: requestVotesAny, failed to become a leader, quroum=%v, numServers=%v", quorum, numServers)
		return false
	}
	atomic.StoreInt64(&n.lastMajorityHB, time.Now().UnixNano())
	n.lock.Lock()
	n.state = RaftLeader
	n.leaderId = n.selfId
	n.lock.Unlock()

	go n.HeartBeaterThread()
	// Upon becoming leader, append no-op entry to log (6.2)

	if _, _, reply2 := n.AppendEntriesLocal(NewNoOpCommand(), nil); reply2 != RaftReplyOk {
		log.Warnf("Failed: requestVotesAny, AppendEntriesLocal (NoOp), reply=%v", reply2)
		return false
	}
	n.extLogger.Reset()
	n.resumeCallback(n.nodeServer)
	log.Debugf("Success: requestVotesAny, became a leader, quorum=%v, numServers=%v", quorum, numServers)
	return true
}

func (n *RaftInstance) StartVoting() {
	if reply := n.currentTerm.Increment(); reply != RaftReplyOk {
		log.Errorf("Failed: __requestVote, currentTerm.Increment, reply=%v", reply)
		return
	}
	if reply := n.votedFor.Set(n.selfId); reply != RaftReplyOk {
		log.Errorf("Failed: __requestVote, votedFor.Set, selfId=%v, reply=%v", n.selfId, reply)
		return
	}
	n.lock.Lock()
	n.state = RaftCandidate
	n.leaderId = OnDiskStateReset
	atomic.StoreInt64(&n.recvLastHB, time.Now().UnixNano())
	n.lock.Unlock()
	n.requestVotesAny()
}

func (n *RaftInstance) Shutdown() {
	atomic.StoreInt32(&n.stopped, 1)
	var i = 0
	for atomic.LoadInt32(&n.nrThreads) > 0 {
		time.Sleep(time.Millisecond)
		i += 1
		if i > 10000 {
			log.Errorf("Timeout: RaftInstance.Shutdown, heart beat threads do not stop > 10 sec")
			break
		}
	}
	if len(n.serverIds) > 1 {
		log.Errorf("Failed: Shutdown, followers are still active, len(n.serverIds)=%v", len(n.serverIds))
		return
	}
	n.CleanExtLogger()
	_ = n.rpcClient.Close()
	n.replyClient.Close()
	n.rpcServer.Close()
	n.extLogger.Clear()
	n.log.Clear()
	n.votedFor.Clean()
	n.currentTerm.Clean()
}

func (n *RaftInstance) CheckReset() (ok bool) {
	ok = true
	if ok2 := atomic.LoadInt32(&n.nrThreads) == 0; !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, n.nrThreads != 0")
		ok = false
	}
	if ok2 := n.rpcClient.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, rpcClient")
		ok = false
	}
	if ok2 := n.replyClient.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, replyClient")
		ok = false
	}
	if ok2 := n.rpcServer.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, rpcServer")
		ok = false
	}
	if ok2 := n.extLogger.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, extLogger")
		ok = false
	}
	if ok2 := n.log.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, log")
		ok = false
	}
	if ok2 := n.votedFor.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, votedFor")
		ok = false
	}
	if ok2 := n.currentTerm.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, currentTerm")
		ok = false
	}
	if ok2 := n.lock.TryLock(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, n.lock is taken")
		ok = false
	} else {
		if ok2 := len(n.serverIds) == 0; !ok2 {
			log.Errorf("Failed: RaftInstance.CheckReset, len(n.serverIds) != 0")
			ok = false
		}
		if ok2 := len(n.sas) == 0; !ok2 {
			log.Errorf("Failed: RaftInstance.CheckReset, len(n.sas) != 0")
			ok = false
		}
		n.lock.Unlock()
	}
	if ok2 := n.applyCond.L.(*sync.Mutex).TryLock(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, n.aplyLock is taken")
		ok = false
	} else {
		n.applyCond.L.Unlock()
	}
	if ok2 := n.hbLock.TryLock(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, n.hbLock is taken")
		ok = false
	} else {
		n.hbLock.Unlock()
	}
	if ok2 := n.extLock.TryLock(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, n.extLock is taken")
		ok = false
	} else {
		if n.extRaftLoggerId.lower != 0 || n.extRaftLoggerId.upper != 0 {
			log.Errorf("Failed: RaftInstance.CheckReset, n.extRaftLoggerId != 0")
			ok = false
		}
		n.extLock.Unlock()
	}
	return
}

func GetRandF(r rand.Source) float64 {
	var f = float64(1)
	for { // borrowed from rand.Float64
		f = float64(r.Int63()) / (1 << 63)
		if f != 1 {
			break
		}
	}
	return f
}

func (n *RaftInstance) HeartBeatRecvThread(interval time.Duration) {
	atomic.AddInt32(&n.nrThreads, 1)
	r := rand.NewSource(time.Now().UnixNano())
	var weighedElectTimeout = time.Duration(float64(n.electTimeout) * (GetRandF(r) + 1))
	log.Debugf("Elect timeout: %v", weighedElectTimeout)
	for atomic.LoadInt32(&n.stopped) == 0 {
		n.lock.RLock()
		raftState := n.state
		n.lock.RUnlock()
		if raftState != RaftInit && raftState != RaftLeader {
			elapsed := time.Now().UnixNano() - atomic.LoadInt64(&n.recvLastHB)
			if weighedElectTimeout.Nanoseconds() <= elapsed {
				log.Infof("Timeout: This Node becomes a candidate and starts voting, elapsed time after last heartbeat=%v, timeout=%v", time.Duration(elapsed), weighedElectTimeout)
				n.StartVoting()
				weighedElectTimeout = time.Duration(float64(n.electTimeout) * (GetRandF(r) + 1))
				log.Debugf("Elect timeout: %v", weighedElectTimeout)
				atomic.StoreInt64(&n.recvLastHB, time.Now().UnixNano())
			}
		} else {
			atomic.StoreInt64(&n.recvLastHB, time.Now().UnixNano())
		}
		time.Sleep(interval)
	}
	atomic.AddInt32(&n.nrThreads, -1)
	log.Infof("RaftInstance.HeartBeatRecvThread: stopped")
}

func (n *RaftInstance) CatchUpLog(sa common.NodeAddrInet4, serverId uint32, timeout time.Duration) int32 {
	logLength := n.log.GetCurrentLogLength()
	if logLength == 0 {
		return RaftReplyOk
	}
	n.lock.RLock()
	currentTerm := n.currentTerm.Get()
	leaderCommit := n.commitIndex
	nextIndex, ok := n.nextIndex[serverId]
	if !ok {
		nextIndex = 1
	}
	n.lock.RUnlock()

	msg := RpcMsg{}
	if prevCmd, reply := n.log.LoadCommandAt(nextIndex - 1); reply == RaftReplyOk {
		msg.FillAppendEntryArgs(currentTerm, prevCmd.GetTerm(), nextIndex-1, leaderCommit, n.selfId)
	} else {
		log.Errorf("Failed: CatchUpLog, LoadCommandAt, nextIndex=%v, reply=%v", nextIndex, reply)
	}
	updatedNextIndex := logLength

	// collect logs in [nextIndex, updatedNextIndex]. no locks are held.
	var lastErrReply = RaftReplyOk
	var appendedBytes = int(RpcMsgMaxLength)
	if nextIndex < updatedNextIndex {
		preallocateBytes := RpcOptControlHeaderLength + AppendEntryCommandLogSize*int32(logLength-nextIndex)
		if preallocateBytes > int32(n.maxHBBytes) {
			preallocateBytes = int32(n.maxHBBytes)
		}
		msg.optBuf = make([]byte, preallocateBytes)
	}
	for logIndex := nextIndex; logIndex < updatedNextIndex && appendedBytes < n.maxHBBytes; logIndex++ {
		cmd, reply := n.log.LoadCommandAt(logIndex)
		if reply != RaftReplyOk {
			lastErrReply = reply
			log.Errorf("Failed: CatchUpLog, LoadCommandAt, logIndex=%v, reply=%v", logIndex, reply)
			break
		}
		extHeaderLength, totalSize := cmd.AppendToRpcMsg(&msg)
		appendedBytes = int(RpcMsgMaxLength) + int(extHeaderLength) + int(totalSize)
		log.Debugf("CatchUpLog, AppendToRpcMsg, sa=%v, Term=%v, Index=%v", sa, cmd.GetTerm(), logIndex)
	}

	if appendedBytes == int(RpcMsgMaxLength) {
		return lastErrReply
	}
	n.hbLock.Lock()
	_, err := n.rpcClient.SendAndWait(msg, sa, n.extLogger, timeout)
	n.hbLock.Unlock()
	if err != nil {
		log.Warnf("Failed: CatchUpLog, SendAndWait, Term=%v, appendedBytes=%v, err=%v", currentTerm, appendedBytes, err)
		return ErrnoToReply(err)
	}
	curLogIndex := n.log.GetCurrentLogLength()
	if updatedNextIndex < curLogIndex {
		return RaftReplyContinue
	}
	return RaftReplyOk
}

func (n *RaftInstance) AddServerLocal(sa common.NodeAddrInet4, serverId uint32) int32 {
	// (SKIPPED) 1. Reply NOT_LEADER if not leader
	// 2. Catch up new server for fixed number of rounds. Reply TIMEOUT if new server does not make progress for an election timeout or
	// if the last round takes longer than the election timeout.
	begin := time.Now().UnixNano()
	var lastRound int64
	for round := 0; ; round++ {
		lastRound = time.Now().UnixNano()
		if r := n.CatchUpLog(sa, serverId, time.Duration(n.electTimeout-(lastRound-begin))); r != RaftReplyOk {
			if r != RaftReplyContinue {
				log.Errorf("Failed: AddServerLocal, CatchUpLog, sa=%v, r=%v", sa, r)
			}
			if time.Now().UnixNano()-begin > n.electTimeout {
				log.Errorf("Timeout, AddServerLocal, make no progresses for an election timeout, sa=%v, round=%v, electTimeout=%v",
					sa, round, n.electTimeout)
				return RaftReplyTimeout
			}
			continue
		}
		break
	}
	if time.Now().UnixNano()-lastRound > n.electTimeout {
		log.Errorf("Timeout, AddServerLocal, the last round takes longer than the election timeout, sa=%v, electTImeout=%v", sa, n.electTimeout)
		return RaftReplyTimeout
	}

	// 3. Wait until previous configuration in log is committed
	n.lock.RLock()
	n.WaitPreviousCommits()
	n.lock.RUnlock()

	// 4. Append new configuration entry to log (old configuration plus new Server), commit it using majority of new configuration
	rc := NewAddServerCommand(serverId, sa.Addr, uint16(sa.Port))
	cmd := GetAppendEntryCommand(n.currentTerm.Get(), rc)
	lastLogIndex, reply := n.log.AppendCommand(cmd)
	if reply != RaftReplyOk {
		log.Errorf("Failed: AddServerLocal, AppendLocalLogFromExt, serverId=%v, reply=%v", serverId, reply)
		return reply
	}
	reply = n.ReplicateLog(lastLogIndex, &serverId, &sa, nil, &cmd, nil)
	if reply != RaftReplyOk {
		log.Errorf("Failed: AddServerLocal, AppendReplicatedLog, serverId=%v, reply=%v", serverId, reply)
		return reply
	}

	// 5. Reply OK
	log.Debugf("Success: AddServerLocal, sa=%v", sa)
	return RaftReplyOk
}

func (n *RaftInstance) WaitPreviousCommits() {
	for {
		logLength := n.log.GetCurrentLogLength()
		if logLength == 0 || logLength <= n.commitIndex+1 {
			break
		}
		n.lock.RUnlock()
		time.Sleep(n.hbInterval)
		n.lock.RLock()
	}
}

func (n *RaftInstance) AppendInitEntry(rc RaftCommand) int32 {
	cmd := GetAppendEntryCommand(n.currentTerm.Get(), rc)
	lastLogIndex, reply := n.log.AppendCommand(cmd)
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendInitEntry, AppendCommand, reply=%v", reply)
		return reply
	}
	// skip replication because this is the only single Node in this cluster
	n.lock.Lock()
	if n.commitIndex < lastLogIndex {
		n.commitIndex = lastLogIndex
	}
	n.lock.Unlock()
	n.ApplyAll(&cmd, lastLogIndex, nil)
	return RaftReplyOk
}

func (n *RaftInstance) AppendBootstrapLogs(groupId string) int32 {
	// initialize serverIds with selfId and groupId and initialize ext logs
	// 3. Wait until previous configuration in log is committed
	n.lock.RLock()
	n.WaitPreviousCommits()
	if _, ok := n.serverIds[n.selfId]; ok {
		n.lock.RUnlock()
		log.Debugf("Success: AppendBootstrapLogs, selfId (%v) is already added", n.selfId)
		return RaftReplyOk
	}
	n.lock.RUnlock()
	if reply := n.AppendInitEntry(NewAddServerCommand(n.selfId, n.selfAddr.Addr, uint16(n.selfAddr.Port))); reply != RaftReplyOk {
		log.Errorf("Failed: AppendBootstrapLogs (AddServer), AppendInitEntry, reply=%v", reply)
		return reply
	}
	if reply := n.AppendInitEntry(NewResetExtCommand(100, 1)); reply != RaftReplyOk {
		log.Errorf("Failed: AppendBootstrapLogs (ResetExtLog), AppendInitEntry, reply=%v", reply)
		return reply
	}
	node := RaftNode{nodeId: n.selfId, addr: n.selfAddr, groupId: groupId}
	if reply := n.AppendExtendedLogEntry(NewInitNodeListCommand(node)); reply != RaftReplyOk {
		log.Errorf("Failed: AppendBootstrapLogs, AppendExtendedLogEntry, reply=%v", reply)
		return reply
	}
	log.Debugf("Success: AppendBootstrapLogs, selfId=%v, logId=%v, nextSeqNum=%v", n.selfId, 100, 1)
	return RaftReplyOk
}

func (n *RaftInstance) ApplyAll(cmd *AppendEntryCommand, logIndex uint64, extLogCmd ExtLogCommandImpl) {
	n.applyCond.L.Lock()
	for n.lastApplied+1 < logIndex {
		n.applyCond.Wait()
	}
	bug := n.lastApplied+1 > logIndex
	n.applyCond.L.Unlock()
	if bug {
		log.Warnf("BUG: RaftInstance.ApplyAll, some threads stealed processing commitIndex (%v)", logIndex)
		return
	}
	// n.lastApplied+1 == logIndex

	rc, ok := cmd.AsRaftCommand()
	if !ok {
		log.Errorf("BUG: command is not implemented, logIndex=%v, cmdId=%v", logIndex, cmd.GetExtCmdId())
		return
	}
	rc.Apply(n.nodeServer, extLogCmd)
	log.Debugf("Success: RaftInstance.ApplyAll, Apply, Index=%v, extCmdId=%v", logIndex, cmd.GetExtCmdId())
	n.applyCond.L.Lock()
	if n.lastApplied < logIndex {
		n.lastApplied = logIndex
	}
	n.applyCond.Broadcast()
	n.applyCond.L.Unlock()
}

func (n *RaftInstance) ReplayAll() int32 {
	n.replaying = true
	var index = uint64(0)
	logLength := n.log.GetCurrentLogLength()
	for ; index < logLength; index++ {
		cmd, reply := n.log.LoadCommandAt(index)
		if reply != RaftReplyOk {
			log.Errorf("Failed: ReplayAll, LoadCommandAt, index=%v, reply=%v", index, reply)
			return reply
		}
		extLogCmd, err := LoadExtBuf(n.extLogger, &cmd)
		if err != nil {
			log.Errorf("Failed: ReplayAll, LoadExtBuf, index=%v, err=%v", index, err)
			return ErrnoToReply(err)
		}
		rc, ok := cmd.AsRaftCommand()
		if !ok {
			log.Errorf("BUG: command is not implemented, index=%v, cmdId=%v", index, cmd.GetExtCmdId())
			return RaftReplyFail
		}
		rc.Apply(n.nodeServer, extLogCmd)
		log.Debugf("Success: ReplayAll, Apply, index=%v, extCmdId=%v", index, cmd.GetExtCmdId())
	}
	n.applyCond.L.Lock()
	if n.lastApplied < index {
		n.lastApplied = index
	}
	n.applyCond.Broadcast()
	n.applyCond.L.Unlock()
	n.replaying = false
	return RaftReplyOk
}

func (n *RaftInstance) StepDown(lastLogIndex uint64) {
	for {
		var found = false
		var exist = false
		for serverId := range n.serverIds {
			if serverId != n.selfId {
				exist = true
				if n.matchIndex[serverId] == n.commitIndex {
					found = true
					break
				}
			}
		}
		if !exist {
			break
		}
		if found && n.commitIndex >= lastLogIndex {
			break
		}
		n.lock.Unlock()
		time.Sleep(n.hbInterval)
		n.lock.Lock()
	}
}

func (n *RaftInstance) RemoveServerLocal(serverId uint32) int32 {
	// (SKIPPED) 1. Reply NOT_LEADER if not leader
	// 2. Wait until previous configuration in log is committed
	n.lock.RLock()
	n.WaitPreviousCommits()
	n.lock.RUnlock()

	// 3. Append new configuration entry to log (old configuration without old Server), commit it using majority of new configuration
	rc := NewRemoveServerCommand(serverId)
	cmd := GetAppendEntryCommand(n.currentTerm.Get(), rc)
	lastLogIndex, reply := n.log.AppendCommand(cmd)
	if reply != RaftReplyOk {
		log.Errorf("Failed: RemoveServerLocal, AppendCommand, serverId=%v, reply=%v", serverId, reply)
		return reply
	}
	reply = n.ReplicateLog(lastLogIndex, nil, nil, &n.selfId, &cmd, nil)
	if reply != RaftReplyOk {
		log.Errorf("Failed: RemoveServerLocal, ReplicateLog, serverId=%v, reply=%v", serverId, reply)
		return reply
	}

	// 5. Reply OK and, if this server was removed, step down
	if serverId == n.selfId {
		// make sure followers apply the remove server log for this node. the previous replicateLog notifies leader commit == lastLogIndex - 1.
		// this means followers apply lastLogIndex - 1, but we want to make sure lastLogIndex is also applied
		if reply = n.heartBeatOneRound(nil, nil, nil, nil); reply != RaftReplyOk {
			log.Errorf("Failed: RemoveServerLocal, heartBeatOneRound, reply=%v", reply)
			return reply
		}
		n.lock.Lock()
		n.StepDown(lastLogIndex)
		n.lock.Unlock()
	}

	log.Errorf("Success: RemoveServerLocal, ReplicateLog, serverId=%v", serverId)
	return RaftReplyOk
}

func (n *RaftInstance) RemoveAllServerIds() int32 {
	var lastLogIndex uint64
	serverIds := make([]uint32, 0)
	n.lock.RLock()
	n.WaitPreviousCommits()
	for serverId := range n.serverIds {
		serverIds = append(serverIds, serverId)
	}
	n.lock.RUnlock()
	cmds := make([]*AppendEntryCommand, 0)
	nrRemoved := uint64(len(serverIds))
	for _, serverId := range serverIds {
		rc := NewRemoveServerCommand(serverId)
		cmd := GetAppendEntryCommand(n.currentTerm.Get(), rc)
		last, reply := n.log.AppendCommand(cmd)
		if reply != RaftReplyOk {
			log.Errorf("Failed: RemoveAllServerIds, AppendCommand, serverId=%v, reply=%v", serverId, reply)
			return reply
		}
		cmds = append(cmds, &cmd)
		lastLogIndex = last
	}

	// skip replication because this is the only single Node in this cluster
	n.lock.Lock()
	if n.commitIndex < lastLogIndex {
		n.commitIndex = lastLogIndex
	}
	n.lock.Unlock()
	for i := uint64(0); i < nrRemoved; i++ {
		n.ApplyAll(cmds[i], lastLogIndex-nrRemoved+1+i, nil)
	}
	return RaftReplyOk
}

// SyncBeforeClientQuery Original method is ClientQuery, which is invoked by clients to query the replicated state (read-only commands). 6.4
// Note: no GRPC is provided and only sync code for linearizability is implemented. must be accessed by leader's context.
func (n *RaftInstance) SyncBeforeClientQuery() (r RaftBasicReply) {
	begin := time.Now()
	// 1. Reply NOT LEADER if not leader, providing hint when available (6.2)
	r = n.IsLeader()
	if r.reply != RaftReplyOk {
		return r
	}

	// 2. Wait until last committed entry is from this leader's term
	var readIndex uint64
	for {
		currentTerm := n.currentTerm.Get()
		n.lock.RLock()
		// 3. Save commitIndex as local variable readIndex (used below)
		readIndex = n.commitIndex
		n.lock.RUnlock()
		cmd, reply := n.log.LoadCommandAt(readIndex)
		if reply == RaftReplyOk && cmd.GetTerm() == currentTerm {
			break
		}
		time.Sleep(n.hbInterval)
	}
	waitTerm := time.Now()

	var wait = false
	if time.Now().UnixNano()-atomic.LoadInt64(&n.lastMajorityHB) > n.hbInterval.Nanoseconds() {
		// 4. Send new round of heartbeats, and wait for reply from majority of servers
		// TODO: this assumes electTimeout >>> hbInterval. is it okay to ignore clock drift?
		if reply := n.heartBeatOneRound(nil, nil, nil, nil); reply != RaftReplyOk {
			log.Errorf("Failed: SyncBeforeClientQuery, heartBeatOneRound, reply=%v", reply)
			return r
		}
		wait = true
	}
	waitHb := time.Now()

	// 5. Wait for state machine to advance at least to the readIndex log entry
	n.applyCond.L.(*sync.RWMutex).RLock()
	var condWait = n.lastApplied < readIndex
	n.applyCond.L.(*sync.RWMutex).RUnlock()
	if condWait {
		n.applyCond.L.Lock()
		for n.lastApplied < readIndex {
			n.applyCond.Wait()
		}
		n.applyCond.L.Unlock()
	}

	// 6. Process query
	// 7. Reply OK with state machine output
	if wait || condWait {
		log.Debugf("Success: SyncBeforeClientQuery, readIndex=%v, waitTerm=%v, waitHb=%v (waited: %v, condWait=%v), waitApply=%v",
			readIndex, waitTerm.Sub(begin), waitHb.Sub(waitTerm), wait, condWait, time.Since(waitHb))
	}
	return r
}

func checkState(stateFile string, selfIdString string, mountPoint string) error {
	if _, err := os.Stat(stateFile); err == nil {
		stateArray, err := common.ReadState(stateFile)
		if err != nil || len(stateArray) < 2 {
			log.Errorf("Failed: checkState, ReadState, file=%v, len(stateArray)=%v, err=%v", stateFile, len(stateArray), err)
			return err
		}
		name := stateArray[0]
		path := stateArray[1]
		if name != selfIdString {
			log.Errorf("Failed: checkState, another objcache instance %v is already running, this=%v", name, selfIdString)
			return err
		}
		if path == mountPoint {
			var stat unix.Stat_t
			if err := unix.Stat(path+"/.", &stat); err != nil {
				if !os.IsNotExist(err) {
					log.Warnf("Failed: checkState, Stat (0), raftF=%v, err=%v", path+"/.", err)
					if err == unix.ENOTCONN { // the prev instance crashed without umount
						// mounted
						if err3 := fuse.Unmount(path); err3 == nil {
							log.Infof("Success: checkState, unmount %v", path)
						} else {
							log.Errorf("Failed: checkState, unmount %v, err=%v", path, err3)
							return err
						}
					}
				}
			}
		}
		log.Infof("Success: checkState, restart an objcache instance, name=%v", selfIdString)
		_ = os.Remove(stateFile)
	}
	if err := common.MarkRunning(stateFile, selfIdString, mountPoint); err != nil {
		log.Errorf("Failed: checkState, MarkRunning, file=%v, selfAddr=%v, mountPoint=%v, err=%v", stateFile, selfIdString, mountPoint, err)
		return err
	}
	log.Infof("Success: checkState, start an objcache instance, this=%v", selfIdString)
	return nil
}

// NewRaftInstance Raft Command Format: | command Id (byte) | Term (uint64) | log Index (uint64) | cmd (size returned by GetSize())
func NewRaftInstance(server *NodeServer) (*RaftInstance, uint64) {
	dataDir := server.getDataDir()
	stat, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dataDir, 0700); err != nil {
			log.Errorf("Failed: NewRaftInstance, cannot create directory for cache root: %v", err)
			return nil, 0
		}
	} else if !stat.IsDir() {
		log.Errorf("Failed: NewRaftInstance, cannot create directory for cache root: path %v already exist", dataDir)
		return nil, 0
	}
	if err := checkState(server.getStateFile(), fmt.Sprintf("%d", server.args.ServerId), server.args.MountPoint); err != nil {
		return nil, 0
	}
	RaftLogger, reply := NewRaftLogger(dataDir, fmt.Sprintf("%d-raft-", server.args.ServerId), 32)
	if reply != RaftReplyOk {
		log.Errorf("Failed: NewRaftInstance, NewRaftLogger, rootDir=%v, reply=%v", dataDir, reply)
		return nil, 0
	}
	logLength := RaftLogger.GetCurrentLogLength() // get logLength here since we want to get this before recording no op at leader election
	var externalIp net.IP
	externalIpStr := server.args.ExternalIp
	iFName := server.flags.IfName
	if iFName != "" {
		iFace, err2 := net.InterfaceByName(iFName)
		if err2 != nil {
			log.Errorf("Failed: NewRafntInstace, InterfaceAddrs, err=%v", err2)
			return nil, 0
		}
		addrs, err3 := iFace.Addrs()
		if err3 != nil || len(addrs) == 0 {
			log.Errorf("Failed: NewRaftInstance, Addrs, err or no IP is assigned, err=%v, len(addrs) == %v, iFace=%v", err3, len(addrs), iFace.Name)
			return nil, 0
		}
		var found = false
		for _, addr := range addrs {
			addrSplit := strings.Split(addr.String(), "/")
			addrIp := net.ParseIP(addrSplit[0])
			if addrIp == nil {
				log.Errorf("Failed: NewRaftInstance, ParseIP, addr=%v", addr)
				return nil, 0
			}
			if addrIp.To4() != nil {
				externalIp = addrIp
				found = true
				break
			}
		}
		if !found {
			log.Errorf("Failed: NewRaftInstance, IP v4 address is not found, addrs=%v, iFace=%v", addrs, iFace.Name)
			return nil, 0
		}
	} else if externalIpStr != "" {
		externalIp = net.ParseIP(externalIpStr)
		if externalIp == nil {
			log.Errorf("Failed: NewRaftInstance, ParseIP, ExternalIp=%v", externalIpStr)
			return nil, 0
		}
	} else {
		log.Errorf("Failed: NewRaftInstance, ExternalIp or InterfaceName must be provided")
		return nil, 0
	}
	externalIpV4 := externalIp.To4()
	selfSa := common.NodeAddrInet4{Addr: [4]byte{externalIpV4[0], externalIpV4[1], externalIpV4[2], externalIpV4[3]}, Port: server.args.RaftPort}
	listenIp := net.ParseIP(server.args.ListenIp)
	if listenIp == nil {
		log.Errorf("Failed: NewRaftInstance, ParseIpString, ListenIp=%v", server.args.ListenIp)
		return nil, 0
	}
	listenIpV4 := listenIp.To4()
	listenSa := common.NodeAddrInet4{Addr: [4]byte{listenIpV4[0], listenIpV4[1], listenIpV4[2], listenIpV4[3]}, Port: server.args.RaftPort}

	selfId := uint32(server.args.ServerId)
	currentTerm, reply := NewOnDiskState(dataDir, selfId, "currentTerm")
	if reply != RaftReplyOk {
		log.Errorf("Failed: NewRaftInstance, NewOnDiskState, dataDir=%v, selfId=%v, h=currentTerm, reply=%v", dataDir, selfId, reply)
		return nil, 0
	}
	if currentTerm.Get() == OnDiskStateReset {
		if reply = currentTerm.Set(0); reply != RaftReplyOk {
			log.Errorf("Failed: NewRaftInstance, NewOnDiskState, currentTerm.Get, reply=%v", reply)
		}
	}
	votedFor, reply := NewOnDiskState(dataDir, selfId, "votedFor")
	if reply != RaftReplyOk {
		log.Errorf("Failed: NewRaftInstance, NewOnDiskState, dataDir=%v, selfId=%v, h=votedFor, reply=%v", dataDir, selfId, reply)
		return nil, 0
	}
	rpcServer, err := NewRpcThreads(listenSa, iFName)
	if err != nil {
		log.Errorf("Failed: NewRaftInstance, NewRpcThreads, listenSa=%v, err=%v", listenSa, err)
		return nil, 0
	}
	rpcClient, err := NewRpcClient(iFName)
	if err != nil {
		rpcServer.Close()
		log.Errorf("Failed: NewRaftInstance, NewRpcClient, err=%v", err)
		return nil, 0
	}
	replyClient, err := NewRpcReplyClient()
	if err != nil {
		_ = rpcClient.Close()
		rpcServer.Close()
		log.Errorf("Failed: NewRaftInstance, NewRpcReplyClient, err=%v", err)
		return nil, 0
	}
	ret := &RaftInstance{
		lock:           new(sync.RWMutex),
		applyCond:      sync.NewCond(new(sync.RWMutex)),
		hbLock:         new(sync.Mutex),
		selfId:         selfId,
		selfAddr:       selfSa,
		leaderId:       OnDiskStateReset,
		state:          RaftInit,
		recvLastHB:     time.Now().UnixNano(),
		hbInterval:     server.flags.HeartBeatIntervalDuration,
		electTimeout:   server.flags.ElectTimeoutDuration.Nanoseconds(),
		stopped:        0,
		extLock:        new(sync.RWMutex),
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make(map[uint32]uint64),
		matchIndex:     make(map[uint32]uint64),
		log:            RaftLogger,
		extLogger:      NewOnDiskLogger(filepath.Join(dataDir, fmt.Sprintf("%d-ext", selfId))),
		currentTerm:    currentTerm,
		votedFor:       votedFor,
		maxHBBytes:     int(server.flags.RpcChunkSizeBytes),
		resumeCallback: server.txMgr.ResumeTx,
		nodeServer:     server,
		rpcClient:      rpcClient,
		replyClient:    replyClient,
		rpcServer:      &rpcServer,
		serverIds:      make(map[uint32]common.NodeAddrInet4),
		sas:            make(map[common.NodeAddrInet4]uint32),
		replaying:      false,
	}
	if err = rpcServer.Start(2, server, ret); err != nil {
		replyClient.Close()
		_ = rpcClient.Close()
		rpcServer.Close()
		log.Errorf("Failed: NewRaftInstance, rpcServer.Start, err=%v", err)
		return nil, 0
	}
	log.Infof("Success: NewRaftInstance, rpcServer.Start, selfSa=%v", selfSa)
	return ret, logLength
}

func (n *RaftInstance) Init(passive bool) {
	if !passive {
		n.state = RaftFollower
	}
	go n.HeartBeatRecvThread(n.hbInterval)
	if !passive {
		for {
			r := n.IsLeader()
			if r.reply != RaftReplyVoting {
				break
			}
			time.Sleep(time.Duration(n.electTimeout))
		}
	}
}
