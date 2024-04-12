/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/sirupsen/logrus"
	"github.com/IBM/objcache/api"
	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
)

const (
	ObjCacheReplyErrBase    = RaftReplyExt + 0
	ObjCacheReplySuspending = ObjCacheReplyErrBase + 1
	ObjCacheIsNotDirty      = ObjCacheReplyErrBase + 2
	FuseReplyErrBase        = RaftReplyExt + 10
)

var s3Log = common.GetLogger("s3")
var log *common.LogHandle = nil

func InitLog(args *common.ObjcacheCmdlineArgs) *common.LogHandle {
	conf := args.GetObjcacheConfig()
	log = common.GetLoggerFile("objcache", args.LogFile)
	if conf.DebugFuse {
		log.Level = logrus.DebugLevel
	}
	if conf.DebugS3 {
		s3Log.Level = logrus.DebugLevel
	}
	return log
}

var MaxTime = time.Unix(1<<63-62135596801, 999999999)

func needRetry(reply int32) bool {
	return reply == RaftReplyNotLeader ||
		reply == RaftReplyMismatchVer ||
		reply == RaftReplyVoting ||
		reply == RaftReplyRetry ||
		isReplyNetworkError(reply) ||
		(ObjCacheReplyErrBase <= reply && reply < FuseReplyErrBase)
}

func isReplyNetworkError(reply int32) bool {
	return reply == ErrnoToReply(unix.ECONNREFUSED) || reply == ErrnoToReply(unix.ETIMEDOUT) || reply == ErrnoToReply(unix.EPIPE)
}

func fixupRpcErr(n *NodeServer, node RaftNode, r RaftBasicReply) (newLeader RaftNode, found bool) {
	if isReplyNetworkError(r.reply) {
		newLeader, found = n.raftGroup.GetReplica(node)
		if !found {
			if n.args.ClientMode {
				n.UpdateNodeListAsClient()
			} else {
				time.Sleep(time.Duration(n.raft.electTimeout))
			}
		}
	} else if r.reply == RaftReplyNotLeader {
		newLeader = RaftNode{groupId: node.groupId, nodeId: r.leaderId, addr: r.leaderAddr}
		found = node.nodeId != r.leaderId
		if !found {
			time.Sleep(time.Duration(n.raft.electTimeout))
		}
	} else if r.reply == RaftReplyVoting {
		time.Sleep(time.Duration(n.raft.electTimeout))
	} else if r.reply == RaftReplyMismatchVer {
		if n.args.ClientMode {
			n.UpdateNodeListAsClient()
		} else {
			time.Sleep(n.raft.hbInterval)
		}
	} else if r.reply == ObjCacheReplySuspending {
		time.Sleep(time.Millisecond * 10)
	} else if r.reply == RaftReplyRetry {
		time.Sleep(time.Millisecond * 10)
	}
	return
}

func ErrnoToReply(err error) int32 {
	if err == nil {
		return RaftReplyOk
	}
	if errno, ok := err.(unix.Errno); ok {
		return int32(errno) + FuseReplyErrBase
	}
	return RaftReplyFail
}

func ReplyToFuseErr(reply int32) error {
	if reply >= FuseReplyErrBase {
		return unix.Errno(reply - FuseReplyErrBase)
	} else if reply == RaftReplyRetry {
		return unix.ETIMEDOUT
	} else if reply != RaftReplyOk {
		return unix.EIO
	}
	return nil
}

func HttpErrToReply(status int) int32 {
	switch status {
	case 400:
		return ErrnoToReply(unix.EINVAL)
	case 401:
		return ErrnoToReply(unix.EACCES)
	case 403:
		return ErrnoToReply(unix.EACCES)
	case 404:
		return ErrnoToReply(unix.ENOENT)
	case 405:
		return ErrnoToReply(unix.ENOTSUP)
	case http.StatusConflict:
		return ErrnoToReply(unix.EINTR)
	case 429:
		return ErrnoToReply(unix.EAGAIN)
	case 500:
		return ErrnoToReply(unix.EAGAIN)
	default:
		return RaftReplyOk
	}
}

func AwsErrToReply(err error) int32 {
	if err == nil {
		return RaftReplyOk
	}

	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case "BucketRegionError":
			// don't need to log anything, we should detect region after
			return ErrnoToReply(unix.EIO)
		case "NoSuchBucket":
			return ErrnoToReply(unix.ENXIO)
		case "BucketAlreadyOwnedByYou":
			return ErrnoToReply(unix.EEXIST)
		case "InvalidRange":
			return ErrnoToReply(unix.EFAULT)
		}

		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// A service error occurred
			r := HttpErrToReply(reqErr.StatusCode())
			if r != RaftReplyOk {
				return r
			} else {
				s3Log.Errorf("http=%v %v s3=%v request=%v\n",
					reqErr.StatusCode(), reqErr.Message(),
					awsErr.Code(), reqErr.RequestID())
				return r
			}
		} else {
			// Generic AWS Error with Code, Message, and original error (if any)
			s3Log.Errorf("code=%v msg=%v, err=%v\n", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			return ErrnoToReply(unix.EIO)
		}
	} else {
		return FuseReplyErrBase + int32(err.(unix.Errno))
	}
}

var PageSize = int64(os.Getpagesize())

type RaftBasicReply struct {
	reply      int32
	leaderId   uint32
	leaderAddr common.NodeAddrInet4
}

func NewRaftBasicReply(Status int32, leader *common.LeaderNodeMsg) RaftBasicReply {
	r := RaftBasicReply{reply: Status, leaderId: leader.GetNodeId(), leaderAddr: common.NodeAddrInet4{Port: int(leader.GetPort())}}
	r.leaderAddr.Addr[0] = leader.Addr[0]
	r.leaderAddr.Addr[1] = leader.Addr[1]
	r.leaderAddr.Addr[2] = leader.Addr[2]
	r.leaderAddr.Addr[3] = leader.Addr[3]
	return r
}

func (r *RaftBasicReply) GetNodeMsg(groupId string) *common.NodeMsg {
	ret := &common.NodeMsg{Addr: make([]byte, 4), Port: int32(r.leaderAddr.Port), GroupId: groupId, NodeId: r.leaderId}
	copy(ret.Addr, r.leaderAddr.Addr[:])
	return ret
}

func (r *RaftBasicReply) GetApiNodeMsg(groupId string) *api.ApiNodeMsg {
	ret := &api.ApiNodeMsg{Addr: make([]byte, 4), Port: int32(r.leaderAddr.Port), GroupId: groupId, NodeId: r.leaderId}
	copy(ret.Addr, r.leaderAddr.Addr[:])
	return ret
}

func (r *RaftBasicReply) GetLeaderNodeMsg() *common.LeaderNodeMsg {
	ret := &common.LeaderNodeMsg{Addr: make([]byte, 4), Port: int32(r.leaderAddr.Port), NodeId: r.leaderId}
	copy(ret.Addr, r.leaderAddr.Addr[:])
	return ret
}

type MyRandString struct {
	xorState uint64
}

var RandString = MyRandString{xorState: uint64(time.Now().Nanosecond())}

// https://en.wikipedia.org/wiki/Xorshift
func (r *MyRandString) xorShift() uint64 {
	for {
		old := atomic.LoadUint64(&r.xorState)
		var x = old
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		if atomic.CompareAndSwapUint64(&r.xorState, old, x) {
			return x
		}
	}
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func (r *MyRandString) Get(digit int64) string {
	b := make([]byte, digit)
	for i := int64(0); i < digit; i++ {
		b[i] = letters[r.xorShift()%uint64(len(letters))]
	}
	return string(b)
}
