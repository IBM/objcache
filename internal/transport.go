/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	SizeOfUint32 = unsafe.Sizeof(uint32(0))
	SizeOfUint64 = unsafe.Sizeof(uint64(0))
)

type ReadRpcMsgState struct {
	cmdId               uint8
	off                 uint16
	optHeaderLength     uint16
	optOff              uint16
	optOffForSplice     uint16
	bufOffForSplice     int32
	fileOffsetForSplice int64
	optComplete         int
	abortFile           bool
	completed           bool
	failed              error
}

// ReadDataToRaftLog
// Note: msg must be already filled by ReadRpcMsg
func ReadDataToRaftLog(fd int, msg *RpcMsg, r *ReadRpcMsgState, files *RaftFiles, pipeFds [2]int) {
	if r.cmdId != AppendEntriesCmdId {
		r.completed = true
		return
	}
	for r.optOffForSplice < r.optHeaderLength {
		extCmdId, extEntryPayload, nextOff := msg.GetAppendEntryExtHeader(r.optOffForSplice)
		if extCmdId&DataCmdIdBit == 0 {
			extEntryPayload = nil
			r.optOffForSplice = nextOff
			r.bufOffForSplice = 0
			continue
		}
		bodyFileId, bodyLength, bodyOffset := GetAppendEntryFileArgs(extEntryPayload)
		var rFd int
		var err error
		if r.abortFile {
			rFd, err = unix.Open("/dev/null", unix.O_WRONLY, 0644)
		} else {
			rFd, err = files.Open(bodyFileId, unix.O_WRONLY|unix.O_CREAT)
		}
		if err != nil {
			r.failed = err
			log.Errorf("Failed: ReadDataToRaftLog, Open, bodyFileId=%v, err=%v", bodyFileId, err)
			return
		}
		var count int64
		for r.bufOffForSplice < bodyLength {
			var nRead int64
			nRead, err = unix.Splice(fd, nil, pipeFds[1], nil, int(bodyLength-r.bufOffForSplice), unix.SPLICE_F_MOVE|unix.SPLICE_F_NONBLOCK)
			if err == unix.EAGAIN || nRead == 0 {
				if count == 0 {
					_ = unix.Close(rFd)
					return
				}
				break
			} else if err != nil {
				r.failed = err
				_ = unix.Close(rFd)
				log.Errorf("Failed: ReadDataToRaftLog, Splice (0), nfd=%v, pipeFds[1]=%v, bodyLength=%v, err=%v", fd, pipeFds[1], bodyLength, err)
				return
			}
			bodyOff := bodyOffset + int64(r.bufOffForSplice)
			for nRead > 0 {
				var l2 = int(nRead & int64(math.MaxInt))
				var nWrite int64
				nWrite, err = unix.Splice(pipeFds[0], nil, rFd, &bodyOff, l2, unix.SPLICE_F_MOVE)
				if err != nil {
					r.failed = err
					_ = unix.Close(rFd)
					log.Errorf("Failed: ReadDataToRaftLog, Splice (1), pipeFds[0]=%v, rFd=%v, l2=%v, offset=%v, err=%v", pipeFds[0], rFd, l2, bodyOff, err)
					return
				}
				nRead -= nWrite
				r.bufOffForSplice += int32(nWrite)
				count += nWrite
			}
		}
		if !r.abortFile {
			files.SeekRange(bodyFileId, bodyOffset+count)
		}
		if err2 := unix.Fsync(rFd); err2 != nil {
			log.Warnf("Failed (ignore): ReadDataToRaftLog, Fsync, rFd=%v, err=%v", rFd, err2)
		}
		/*if err2 := unix.Fadvise(rFd, bodyOffset, int64(bodyLength), unix.FADV_DONTNEED); err2 != nil {
			log.Warnf("Failed (ignore): ReadDataToRaftLog, Fadvise, rFd=%v, err=%v", rFd, err2)
		}*/
		if err2 := unix.Close(rFd); err2 != nil {
			log.Warnf("Failed (ignore): ReadDataToRaftLog, Close, rFd=%v, err=%v", rFd, err2)
		}
		if r.bufOffForSplice == bodyLength {
			r.optOffForSplice = nextOff
			r.bufOffForSplice = 0
		}
	}
	r.completed = r.optOff >= r.optHeaderLength
}

func ReadDataToBuffer(fd int, msg *RpcMsg, r *ReadRpcMsgState, data []byte) {
	payload := msg.GetCmdPayload()
	rpcId := msg.GetExecProtoBufRpcId(payload)
	if rpcId != RpcDownloadChunkViaRemoteCmdId {
		r.completed = true
		return
	}
	dataLength, _ := msg.GetOptControlHeader()
	if dataLength != uint32(len(data)) {
		if dataLength < uint32(len(data)) {
			data = data[:dataLength]
		} else {
			log.Warnf("ReadDataToBuffer, dataLength (%v) > len(data) (%v). you likely need to set chunkSize >= rpcChunkSize", dataLength, len(data))
			data = append(data, make([]byte, dataLength-uint32(len(data)))...)
		}
	}
	for uint32(r.bufOffForSplice) < dataLength {
		nRead, err := unix.Read(fd, data[r.bufOffForSplice:dataLength])
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			break
		} else if err != nil {
			log.Errorf("Failed: ReadDataToBuffer, Pread, err=%v", err)
			return
		}
		if nRead == 0 {
			break
		}
		r.bufOffForSplice += int32(nRead)
	}
	r.completed = uint32(r.bufOffForSplice) >= dataLength
}

func ReadDataToFd(fd int, msg *RpcMsg, r *ReadRpcMsgState, toFd int, pipeFds [2]int) {
	payload := msg.GetCmdPayload()
	rpcId := msg.GetExecProtoBufRpcId(payload)
	if rpcId != RpcDownloadChunkViaRemoteCmdId {
		r.completed = true
		return
	}
	dataLength, _ := msg.GetOptControlHeader()
	var nRead int64
	var err error
	for uint32(r.bufOffForSplice) < dataLength {
		nRead, err = unix.Splice(fd, nil, pipeFds[1], nil, int(dataLength-uint32(r.bufOffForSplice)), unix.SPLICE_F_MOVE|unix.SPLICE_F_NONBLOCK)
		if err == unix.EAGAIN || nRead == 0 {
			break
		} else if err != nil {
			r.failed = err
			log.Errorf("Failed: ReadDataToTempFile, Splice (0), nfd=%v, pipeFds[1]=%v, bodyLength=%v, err=%v", fd, pipeFds[1], dataLength, err)
			return
		}
		bodyOff := r.fileOffsetForSplice + int64(r.bufOffForSplice)
		for nRead > 0 {
			var l2 = int(nRead & int64(math.MaxInt))
			var nWrite int64
			nWrite, err = unix.Splice(pipeFds[0], nil, toFd, &bodyOff, l2, unix.SPLICE_F_MOVE)
			if err != nil {
				r.failed = err
				log.Errorf("Failed: ReadDataToTempFile, Splice (1), pipeFds[0]=%v, toFd=%v, l2=%v, offset=%v, err=%v", pipeFds[0], toFd, l2, bodyOff, err)
				return
			}
			nRead -= nWrite
			r.bufOffForSplice += int32(nWrite)
		}
	}
	r.completed = uint32(r.bufOffForSplice) >= dataLength
}

func ReadRpcMsg(fd int, msg *RpcMsg, r *ReadRpcMsgState) {
	var headerLength = uint16(RpcMsgMaxLength)
	var lengthIdentified = r.off >= uint16(RpcControlHeaderLength)
	for r.off < headerLength {
		c, err := unix.Read(fd, msg.buf[r.off:headerLength])
		if err == unix.EAGAIN || c == 0 {
			return
		} else if err != nil {
			r.failed = err
			log.Errorf("Failed: ReadRpcMsg, Read, fd=%v, off=%v, err=%v", fd, r.off, err)
			return
		}
		r.off += uint16(c)
		if !lengthIdentified {
			r.cmdId = msg.GetCmdId()
			lengthIdentified = msg.GetArrivingMsgLengths(r.off, &r.optHeaderLength)
		}
	}
	if !lengthIdentified {
		return
	}
	if r.optHeaderLength > 0 && msg.optBuf == nil {
		msg.optBuf = make([]byte, r.optHeaderLength)
	}
	for r.optOff < r.optHeaderLength {
		c, err := unix.Read(fd, msg.optBuf[r.optOff:r.optHeaderLength])
		if err == unix.EAGAIN || c == 0 {
			break
		} else if err != nil {
			r.failed = err
			log.Errorf("Failed: EpollReader.ReadExtHeader, Read, fd=%v, optOff=%v, optHeaderLength=%v, err=%v", fd, r.optOff, r.optHeaderLength, err)
			break
		}
		r.optOff += uint16(c)
	}
	if r.off == headerLength && r.optOff == r.optHeaderLength {
		r.optComplete += 1
	}
}

type WriteRpcState struct {
	headerSentBytes    uint16
	optHeaderSentBytes uint16
	optOff             uint16
	optBufOff          int32
	dataBufOff         int
	msg                RpcMsg
	completed          bool
	failed             error
	optHeaderLength    uint16
}

// __writeRpcMsg: if files == nil, skip sending files
func __writeRpcMsg(ev unix.EpollEvent, fd int, state *WriteRpcState, files *RaftFiles, dataBuf [][]byte, debug bool) error {
	if state.completed || state.failed != nil {
		return nil
	}
	var sendFileBytes int
	var writeTime time.Duration
	var writeOptTime time.Duration
	var writeDataTime time.Duration
	var sendFileTime time.Duration
	cmdId := state.msg.GetCmdId()
	if ev.Events&unix.EPOLLERR != 0 || ev.Events&unix.EPOLLHUP != 0 || ev.Events&unix.EPOLLIN != 0 {
		// a peer closed the socket (EPOLLHUP), something bad happened (EPOLLERR), or unexpected events are reported. delete it from the epoll list
		log.Errorf("Failed: __writeRpcMsg, EpollWait returned events=%v", ev.Events)
		return unix.EINVAL
	}
	for state.headerSentBytes < uint16(RpcMsgMaxLength) {
		writeBegin := time.Now()
		c, err := unix.Write(fd, state.msg.buf[state.headerSentBytes:RpcMsgMaxLength])
		writeTime += time.Since(writeBegin)
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return nil
		} else if err != nil {
			log.Errorf("Failed: __writeRpcMsg, Write (0), fd=%v, sendBytes=%v, RpcMsgMaxLength=%v, err=%v", fd, state.headerSentBytes, RpcMsgMaxLength, err)
			return err
		}
		state.headerSentBytes += uint16(c)
	}
	for state.optHeaderSentBytes < state.optHeaderLength {
		writeBegin := time.Now()
		c, err := unix.Write(fd, state.msg.optBuf[state.optHeaderSentBytes:state.optHeaderLength])
		writeOptTime += time.Since(writeBegin)
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return nil
		} else if err != nil {
			log.Errorf("Failed: __writeRpcMsg, Write (1), fd=%v, sendBytes=%v, RpcMsgMaxLength=%v, err=%v", fd, state.optHeaderSentBytes, RpcMsgMaxLength, err)
			return err
		}
		state.optHeaderSentBytes += uint16(c)
	}
	if dataBuf != nil { // for Chunk RPC
		var off = 0
		for i := 0; i < len(dataBuf); i++ {
			for state.dataBufOff < off+len(dataBuf[i]) {
				writeBegin := time.Now()
				c, err := unix.Write(fd, dataBuf[i][state.dataBufOff-off:])
				writeDataTime += time.Since(writeBegin)
				if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
					return nil
				} else if err != nil {
					log.Errorf("Failed: __writeRpcMsg, Write (2), fd=%v, sendBytes=%v, len(dataBuf)=%v, err=%v", fd, state.dataBufOff, len(dataBuf[i]), err)
					return err
				}
				state.dataBufOff += c
			}
			off += len(dataBuf[i])
		}
		if state.dataBufOff == off {
			state.completed = true
			state.failed = nil
		}
		if state.completed && debug {
			log.Debugf("Success: __writeRpcMsg, writeTime=%v (bytes=%v), writeOptTime=%v (bytes=%v), sendFileTime=%v (bytes=%v)",
				writeTime, state.headerSentBytes, writeOptTime, state.optHeaderSentBytes, sendFileTime, sendFileBytes)
		}
		return nil
	}
	for state.optOff < state.optHeaderLength {
		if cmdId != AppendEntriesCmdId {
			state.optOff = state.optHeaderLength
			state.optBufOff = 0
			continue
		}
		extCmdId, extEntryPayload, nextOff := state.msg.GetAppendEntryExtHeader(state.optOff)
		if files != nil && extCmdId&DataCmdIdBit != 0 {
			bodyFileId, bodyLength, bodyFileOffset := GetAppendEntryFileArgs(extEntryPayload)
			files.lock.Lock()
			rFd, reply := files.openCacheNoLock(bodyFileId)
			if reply != RaftReplyOk {
				files.lock.Unlock()
				log.Errorf("Failed: __writeRpcMsg, openCacheNoLock, bodyFileId=%v, reply=%v", bodyFileId, reply)
				return ReplyToFuseErr(reply)
			}
			var fileOff = bodyFileOffset + int64(state.optBufOff)
			for state.optBufOff < bodyLength {
				sendFileBegin := time.Now()
				n, err := unix.Sendfile(fd, rFd, &fileOff, int(bodyLength-state.optBufOff))
				sendFileTime += time.Since(sendFileBegin)
				if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
					files.lock.Unlock()
					return nil
				} else if err != nil {
					files.lock.Unlock()
					log.Errorf("Failed: __writeRpcMsg, Sendfile, fd=%v, fileOff=%v, extOffPair[1]=%v, bodyLength=%v, errr=%v", fd, fileOff, state.optBufOff, bodyLength, err)
					return err
				}
				state.optBufOff += int32(n)
				sendFileBytes += n
			}
			files.lock.Unlock()
			if state.optBufOff == bodyLength {
				state.optBufOff = 0
				state.optOff = nextOff
			}
		} else {
			state.optOff = nextOff
			state.optBufOff = 0
		}
	}
	if state.optOff >= state.optHeaderLength {
		state.completed = true
		state.failed = nil
	}
	if state.completed && debug {
		log.Debugf("Success: __writeRpcMsg, writeTime=%v (bytes=%v), writeOptTime=%v (bytes=%v), sendFileTime=%v (bytes=%v)",
			writeTime, state.headerSentBytes, writeOptTime, state.optHeaderSentBytes, sendFileTime, sendFileBytes)
	}
	return nil
}

/*************************************
 **************************************/

type EpollHandler struct {
	epollFd int
}

func NewEpollHandler() (*EpollHandler, error) {
	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		log.Errorf("Failed: NewEpollHandler, EpollCreate1, err=%v", err)
		return nil, err
	}
	return &EpollHandler{epollFd: epollFd}, nil
}

func (r *EpollHandler) AddFd(fd int, events uint32) (err error) {
	if err := unix.SetNonblock(fd, true); err != nil {
		log.Errorf("Failed: AddFd, SetNonblock, fd=%v, err=%v", fd, err)
		return err
	}
	// level-triggered
	if err = unix.EpollCtl(r.epollFd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: events, Fd: int32(fd)}); err != nil {
		log.Errorf("Failed: AddFd, EpollCtl, fd=%v, err=%v", fd, err)
		return
	}
	return
}

func (r *EpollHandler) RemoveFd(fd int) (err error) {
	if err = unix.EpollCtl(r.epollFd, unix.EPOLL_CTL_DEL, fd, nil); err != nil {
		log.Errorf("Failed: EpollHandler.RemoveFd, EpollCtl (EPOLL_CTL_DEL), epollFd=%v, fd=%v, err=%v", r.epollFd, fd, err)
	}
	return
}

func (r *EpollHandler) Close() (err error) {
	if err = unix.Close(r.epollFd); err != nil {
		log.Errorf("Failed: EpollHandler.Close, Close, fd=%v, err=%v", r.epollFd, err)
	}
	return
}

type RpcSeqNumArgs struct {
	data       []byte
	fd         int
	fileOffset int64
}

type RpcClient struct {
	h                  *EpollHandler
	fds                map[int]common.NodeAddrInet4
	sas                map[common.NodeAddrInet4]int
	fdLock             *sync.RWMutex
	connectFailedCount map[common.NodeAddrInet4]uint32
	r                  *EpollHandler
	pipeFds            [2]int
	boundDev           string
}

func NewRpcClient(boundDev string) (*RpcClient, error) {
	pipeFds := [2]int{}
	if err := unix.Pipe(pipeFds[:]); err != nil {
		log.Errorf("Failed: NewRpcClient, Pipe, err=%v", err)
		return nil, err
	}
	var h *EpollHandler
	r, err := NewEpollHandler()
	if err != nil {
		log.Errorf("Failed: NewRpcClient, NewEpollReader, err=%v", err)
		goto closePipes
	}

	h, err = NewEpollHandler()
	if err != nil {
		log.Errorf("Failed: NewRpcClient, NewEpollHandler, err=%v", err)
		goto closePipes
	}
	return &RpcClient{
		h: h, fds: make(map[int]common.NodeAddrInet4), sas: make(map[common.NodeAddrInet4]int), r: r, pipeFds: pipeFds,
		connectFailedCount: make(map[common.NodeAddrInet4]uint32), fdLock: new(sync.RWMutex), boundDev: boundDev,
	}, nil
closePipes:
	if err2 := unix.Close(pipeFds[0]); err2 != nil {
		log.Warnf("Failed (ignore): NewRpcClient, Close (0), pipeFds[0]=%v, err=%v", pipeFds[0], err2)
	}
	if err2 := unix.Close(pipeFds[1]); err2 != nil {
		log.Warnf("Failed (ignore): NewRpcClient, Close (1), pipeFds[1]=%v, err=%v", pipeFds[1], err2)
	}
	return nil, err
}

func (w *RpcClient) AddFd(na common.NodeAddrInet4, fd int) error {
	w.fdLock.RLock()
	_, ok := w.sas[na]
	w.fdLock.RUnlock()
	if ok {
		return nil
	}
	w.fdLock.Lock()
	if _, ok = w.sas[na]; ok {
		return nil
	}
	if err := w.h.AddFd(fd, unix.EPOLLOUT); err != nil {
		w.fdLock.Unlock()
		log.Errorf("Failed: RpcClient.AddFd writer.AddFd, fd=%v, na=%v, err=%v", fd, na, err)
		return err
	}
	if err := w.r.AddFd(fd, unix.EPOLLIN); err != nil {
		log.Errorf("Failed: RpcClient.AddFd, reader.AddFd, fd=%v, na=%v, err=%v", fd, na, err)
		if err2 := w.h.RemoveFd(fd); err2 != nil {
			log.Errorf("Failed (ignore): RpcClient.AddFd, writer.RemoveFd, fd=%v, err=%v", fd, err2)
		}
		w.fdLock.Unlock()
		return err
	}
	w.fds[fd] = na
	w.sas[na] = fd
	w.fdLock.Unlock()
	log.Debugf("Success: RpcClient.AddFd, fd=%v, na=%v", fd, na)
	return nil
}

func (w *RpcClient) Connect(na common.NodeAddrInet4) (fd int, err error) {
	var ok bool
	w.fdLock.RLock()
	fd, ok = w.sas[na]
	w.fdLock.RUnlock()
	if ok {
		return fd, nil
	}
	w.fdLock.Lock()
	fd, ok = w.sas[na]
	if ok {
		goto unlock
	}
	fd, err = unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Errorf("Failed: RpcClient.Connect, Socket, na=%v, err=%v", na, err)
		goto unlock
	}
	if w.boundDev != "" {
		if err = unix.SetsockoptString(fd, unix.SOL_SOCKET, unix.SO_BINDTODEVICE, w.boundDev); err != nil {
			log.Errorf("Failed: NewRpcThreads, SetsockoptString, fd=%v, boundDev=%v, err=%v", fd, w.boundDev, err)
			goto unlock
		}
	}
	if err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		log.Errorf("Failed: RpcClient.Connect, SetsockoptInt(fd, SOL_SOCKET, SO_REUSEADDR, 1), fd=%v, err=%v", fd, err)
		goto unlock
	}
	if err = unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_NODELAY, 1); err != nil {
		log.Errorf("Failed: RpcClient.Connect, SetsockoptInt(fd, SOL_TCP, TCP_NODELAY, 1), fd=%v, err=%v", fd, err)
		goto unlock
	}
	if err = unix.Connect(fd, &unix.SockaddrInet4{Addr: na.Addr, Port: na.Port}); err != nil {
		if failedCount := w.connectFailedCount[na]; failedCount < 3 {
			log.Errorf("Failed: RpcClient.Connect, Connect, sa=%v, err=%v", na, err)
			w.connectFailedCount[na] = failedCount + 1
		}
		goto close
	}
	if err = w.h.AddFd(fd, unix.EPOLLOUT); err != nil {
		log.Errorf("Failed: RpcClient.Connect writer.AddFd, fd=%v, na=%v, err=%v", fd, na, err)
		goto close
	}
	if err = w.r.AddFd(fd, unix.EPOLLIN); err != nil {
		log.Errorf("Failed: RpcClient.Connect, reader.AddFd, fd=%v, na=%v, err=%v", fd, na, err)
		if err2 := w.h.RemoveFd(fd); err2 != nil {
			log.Errorf("Failed (ignore): RpcClient.Connect, writer.RemoveFd, fd=%v, err=%v", fd, err2)
		}
		goto close
	}
	w.fds[fd] = na
	w.sas[na] = fd
	delete(w.connectFailedCount, na)
	w.fdLock.Unlock()
	log.Debugf("Success: RpcClient.Connect, fd=%v, na=%v", fd, na)
	return
close:
	if err2 := unix.Close(fd); err2 != nil {
		log.Errorf("Failed (ignore): RpcClient.Connect, Close, fd=%v, err=%v", fd, err2)
	}
unlock:
	w.fdLock.Unlock()
	return
}

func (w *RpcClient) RemoveFd(fd int) {
	w.fdLock.Lock()
	sa, ok := w.fds[fd]
	if !ok {
		log.Warnf("RpcClientEpollWriter.RemoveFd, fd is not added, fd=%v", fd)
		w.fdLock.Unlock()
		return
	}
	if err := w.h.RemoveFd(fd); err != nil {
		log.Errorf("Failed (ignore): RpcClientEpollWriter.RemoveFd, h.RemoveFd, fd=%v, err=%v", fd, err)
	}
	if err := w.r.RemoveFd(fd); err != nil {
		log.Errorf("Failed (ignore): RpcClientEpollWriter.RemoveFd, r.RemoveFd, fd=%v, err=%v", fd, err)
	}
	if err := unix.Close(fd); err != nil {
		log.Errorf("Failed (ignore): RpcClientEpollWriter.RemoveFd, Close, fd=%v, err=%v", fd, err)
	}
	delete(w.fds, fd)
	delete(w.sas, sa)
	w.fdLock.Unlock()
	log.Debugf("Success: RpcClient.RemoveFd, fd=%v, sa=%v", fd, sa)
}

func (w *RpcClient) Close() (err error) {
	if err2 := unix.Close(w.pipeFds[1]); err2 != nil {
		log.Warnf("Failed (ignore): RpcClientEpollWriter.Close, Close (0), pipeFds[1]=%v, err=%v", w.pipeFds[1], err2)
	} else {
		w.pipeFds[1] = -1
	}
	if err2 := unix.Close(w.pipeFds[0]); err2 != nil {
		log.Warnf("Failed (ignore): RpcClientEpollWriter.Close, Close (1), pipeFds[0]=%v, err=%v", w.pipeFds[0], err2)
	} else {
		w.pipeFds[0] = -1
	}
	if err = w.h.Close(); err != nil {
		log.Warnf("Failed (ignore): RpcClientEpollWriter.Close, h.Close, err=%v", err)
	} else {
		w.h = nil
	}
	if err = w.r.Close(); err != nil {
		log.Warnf("Failed (ignore): RpcClientEpollWriter.Close, r.Close, err=%v", err)
	} else {
		w.r = nil
	}
	w.fdLock.Lock()
	for fd := range w.fds {
		if err = unix.Close(fd); err != nil {
			log.Warnf("Failed (ignore): RpcClientEpollWriter.Close, unix.Close, fd=%v, err=%v", fd, err)
		}
	}
	w.fds = make(map[int]common.NodeAddrInet4)
	w.sas = make(map[common.NodeAddrInet4]int)
	w.connectFailedCount = make(map[common.NodeAddrInet4]uint32)
	w.fdLock.Unlock()
	return
}

func (w *RpcClient) CheckReset() (ok bool) {
	if ok = w.fdLock.TryLock(); !ok {
		log.Errorf("Failed: RpcClient.CheckReset, w.fdLock is taken")
		return
	}
	if ok2 := len(w.fds) == 0; !ok2 {
		log.Errorf("Failed: RpcClient.CheckReset, len(w.fds) != 0")
		ok = false
	}
	if ok2 := len(w.sas) == 0; !ok2 {
		log.Errorf("Failed: RpcClient.CheckReset, len(w.sas) != 0")
		ok = false
	}
	if ok2 := len(w.connectFailedCount) == 0; !ok2 {
		log.Errorf("Failed: RpcClient.CheckReset, len(w.connectFailedCount) != 0")
		ok = false
	}
	w.fdLock.Unlock()
	if ok2 := w.h == nil; !ok2 {
		log.Errorf("Failed: RpcClient.CheckReset, w.h != nil")
		ok = false
	}
	if ok2 := w.r == nil; !ok2 {
		log.Errorf("Failed: RpcClient.CheckReset, w.r != nil")
		ok = false
	}
	if ok2 := w.pipeFds[0] == -1; !ok2 {
		log.Errorf("Failed: RpcClient.CheckReset, w.pipeFds[0] != -1")
		ok = false
	}
	if ok2 := w.pipeFds[1] == -1; !ok2 {
		log.Errorf("Failed: RpcClient.CheckReset, w.pipeFds[1] != -1")
		ok = false
	}
	return
}

func (w *RpcClient) SendAndWait(msg RpcMsg, sa common.NodeAddrInet4, files *RaftFiles, timeout time.Duration) (replyMsg RpcMsg, err error) {
	// must be in an atomic context (i.e., holding raft.hbLock)
	var fd int
	begin := time.Now()
	fd, err = w.Connect(sa)
	if err != nil {
		log.Errorf("Failed: SendAndWait, Connect, sa=%v, err=%v", sa, err)
		return RpcMsg{}, err
	}
	err = w.UnicastRpcMsg(msg, sa, files, timeout, nil, false)
	msg.optBuf = nil
	if err != nil {
		if err != unix.ETIMEDOUT {
			w.RemoveFd(fd)
		}
		log.Errorf("Failed: SendAndWait, UnicastRpcMsg, sa=%v, err=%v", sa, err)
		return RpcMsg{}, err
	} else {
		log.Debugf("Success: SendAndWait, UnicastRpcMsg, fd=%v, sa=%v", fd, sa)
	}
	timeout -= time.Since(begin)

	var reply int32
	replyMsg, reply = w.WaitAndGetRpcReply(fd, timeout)
	if reply != RaftReplyOk {
		if reply != ErrnoToReply(unix.ETIMEDOUT) {
			w.RemoveFd(fd)
		}
		log.Errorf("Failed: SendAndWait, WaitAndGetRpcReply, sa=%v, reply=%v", sa, reply)
		return RpcMsg{}, ReplyToFuseErr(reply)
	}
	return replyMsg, nil
}

func (w *RpcClient) UnicastRpcMsg(msg RpcMsg, sa common.NodeAddrInet4, files *RaftFiles, timeout time.Duration, dataBuf [][]byte, debug bool) (err error) {
	begin := time.Now()
	err = nil
	w.fdLock.RLock()
	fd, ok := w.sas[sa]
	w.fdLock.RUnlock()
	if !ok {
		log.Errorf("Failed: UnicastRpcMsg, fd is not found, sa=%v", sa)
		return unix.EINVAL
	}
	info := WriteRpcState{
		headerSentBytes: 0, optHeaderSentBytes: 0, optOff: uint16(RpcOptControlHeaderLength), optBufOff: 0, dataBufOff: 0, msg: msg, completed: false, failed: nil,
		optHeaderLength: msg.GetOptHeaderLength(),
	}

	epollTime := [4]time.Duration{}
	writeTime := [4]time.Duration{}
	events := make([]unix.EpollEvent, len(w.sas))
	var i = 0
	for ; ; i++ {
		epollBegin := time.Now()
		elapsed := epollBegin.Sub(begin)
		if elapsed > timeout {
			err = unix.ETIMEDOUT
			log.Errorf("Timeout: UnicastRpcMsg, elapsed=%v, timeout=%v, nrLoop=%v", elapsed, timeout, i)
			break
		}
		nrEvents, err2 := unix.EpollWait(w.h.epollFd, events, int((timeout - elapsed).Milliseconds()))
		writeBegin := time.Now()
		epollTime[i%4] = writeBegin.Sub(epollBegin)
		if err2 == unix.EINTR {
			continue
		} else if err2 == unix.EBADFD || err2 == unix.EFAULT || err2 == unix.EINVAL { // EBADF, EFAULT, EINVAL. all of them should close the connection
			log.Errorf("Failed: UnicastRpcMsg, EpollWait, err=%v", err2)
			err = unix.EPIPE
			break
		}
		for j := 0; j < nrEvents; j++ {
			ev := events[j]
			if fd == int(ev.Fd) {
				if ev.Events&unix.EPOLLERR != 0 || ev.Events&unix.EPOLLHUP != 0 || ev.Events&unix.EPOLLIN != 0 {
					// a peer closed the socket (EPOLLHUP), something bad happened (EPOLLERR), or unexpected events are reported. delete it from the epoll list
					log.Errorf("Failed: UnicastRpcMsg, EpollWait returned events=%v, fd=%v", ev.Events, ev.Fd)
					info.failed = unix.EPIPE
					break
				}
				if err3 := __writeRpcMsg(ev, fd, &info, files, dataBuf, debug); err3 != nil {
					info.failed = err3
					break
				}
			}
		}
		writeTime[i%4] = time.Since(writeBegin)
		if info.completed || info.failed != nil {
			err = info.failed
			break
		}
	}
	if err == nil && debug {
		log.Debugf("Success: UnicastRpcMsg, epollTime=%v, writeTime=%v, nrLoop=%v", epollTime, writeTime, i)
	}
	return
}

func (w *RpcClient) BroadcastAndWaitRpcMsg(messages map[int]RpcMsg, raft *RaftInstance, timeout time.Duration, debug bool) (nrSuccess int) {
	// must be in an atomic context (i.e., holding raft.hbLock)
	if len(messages) == 0 {
		return 0
	}
	begin := time.Now()
	info := make(map[int]*WriteRpcState)
	for fd, msg := range messages {
		info[fd] = &WriteRpcState{
			headerSentBytes: 0, optHeaderSentBytes: 0, optOff: uint16(RpcOptControlHeaderLength), optBufOff: 0, dataBufOff: 0, msg: msg, completed: false, failed: nil,
			optHeaderLength: msg.GetOptHeaderLength(),
		}
	}

	connect := time.Since(begin)
	var epollTime time.Duration
	var writeRpcTime time.Duration
	events := make([]unix.EpollEvent, len(messages))
	var j = 0
	var elapsed time.Duration
	for ; ; j++ {
		epollBegin := time.Now()
		elapsed = epollBegin.Sub(begin)
		if elapsed > timeout {
			for sFd, s := range info {
				if !s.completed && s.failed == nil {
					log.Errorf("Timeout: BroadcastAndWaitRpcMsg, fd=%v, elapsed=%v, timeout=%v", sFd, elapsed, timeout)
					s.completed = false
					s.failed = unix.ETIMEDOUT
					//w.RemoveFd(sFd)
				}
			}
			break
		}
		nrEvents, err := unix.EpollWait(w.h.epollFd, events, int((timeout - elapsed).Milliseconds()))
		writeBegin := time.Now()
		epollTime += writeBegin.Sub(epollBegin)
		if err == unix.EINTR {
			continue
		} else if err == unix.EBADFD || err == unix.EFAULT || err == unix.EINVAL { // EBADF, EFAULT, EINVAL. all of them should close the connection
			for sFd, s := range info {
				if !s.completed && s.failed == nil {
					log.Errorf("Failed: BroadcastAndWaitRpcMsg, EpollWait, fd=%v, err=%v", sFd, err)
					s.completed = false
					s.failed = unix.EPIPE
					w.RemoveFd(sFd)
				}
			}
			break
		} else {
			for i := 0; i < nrEvents; i++ {
				fd := int(events[i].Fd)
				fdInfo, ok := info[fd]
				if !ok {
					log.Errorf("Failed (ignore): BroadcastAndWaitRpcMsg, fd is not registered, fd=%v", fd)
					w.RemoveFd(fd)
					continue
				}
				if err3 := __writeRpcMsg(events[i], fd, fdInfo, raft.files, nil, debug); err3 != nil {
					fdInfo.failed = err3
					fdInfo.completed = false
					w.RemoveFd(fd)
				}
			}
			writeRpcTime += time.Since(writeBegin)
		}
		var nrCompleted = 0
		for _, fdInfo := range info {
			if fdInfo.completed || fdInfo.failed != nil {
				nrCompleted += 1
			}
		}
		if nrCompleted >= len(info) {
			break
		}
	}

	var nrSent = 0
	var nrFailed = 0
	for fd, fdInfo := range info {
		if fdInfo.failed != nil {
			nrFailed += 1
			delete(info, fd)
		} else if fdInfo.completed {
			nrSent += 1
			// NOTE: we must wait for all replies for succeeded writes even if nrSent < len(messages) / 2
		}
	}
	if len(info) == 0 {
		log.Errorf("Failed: BroadcastAndWaitRpcMsg, all sockets are disconnected")
		return 0
	} else if debug {
		log.Debugf("Success: BroadcastAndWaitRpcMsg, connect=%v, nrSent=%v, nrFailed=%v, nrLoop=%v, epollTime=%v, writeRpcTime=%v",
			connect, nrSent, nrFailed, j, epollTime, writeRpcTime)
	}
	return w.WaitAndCheckRaftReply(raft, info, timeout, debug)
}

func (w *RpcClient) WaitAndCheckRaftReply(raft *RaftInstance, servers map[int]*WriteRpcState, timeout time.Duration, debug bool) (nrSuccess int) {
	begin := time.Now()
	nrSuccess = 0
	messages := [4]RpcMsg{}
	states := [4]ReadRpcMsgState{}
	for i := 0; i < 4; i++ {
		states[i].optOffForSplice = uint16(RpcOptControlHeaderLength)
	}
	fds := [4]int{-1, -1, -1, -1}
	sas := [4]common.NodeAddrInet4{}
	var nrFds = 0
	w.fdLock.RLock()
	events := make([]unix.EpollEvent, 4)
	for fd, info := range servers {
		if s, ok := w.fds[fd]; ok {
			info.completed = false
			info.failed = nil
			fds[nrFds] = fd
			sas[nrFds] = s
			nrFds += 1
		}
	}
	w.fdLock.RUnlock()
	var prepareTime = time.Since(begin)
	var epollTime [4]time.Duration
	var readTime time.Duration
	var spliceTime time.Duration
	var responseHandlingTime time.Duration
	var i = 0
	var nrFailed = 0
	for ; nrSuccess+nrFailed < nrFds; i++ {
		epollBegin := time.Now()
		elapsed := epollBegin.Sub(begin)
		if i > 0 && elapsed > timeout {
			for j := 0; j < nrFds; j++ {
				if !states[j].completed && states[j].failed == nil {
					states[j].failed = unix.ETIMEDOUT
					log.Errorf("Timeout: WaitAndCheckRaftReply, fd=%v, sa=%v, elapsed=%v, timeout=%v", fds[j], sas[j], elapsed, timeout)
					nrFailed += 1
				}
			}
			break
		}
		nrEvents, err := unix.EpollWait(w.r.epollFd, events, int((timeout - elapsed).Milliseconds()))
		readBegin := time.Now()
		epollTime[i%4] += readBegin.Sub(epollBegin)
		if err != nil && err == unix.EINTR {
			continue
		} else if err == unix.EBADFD || err == unix.EFAULT || err == unix.EINVAL { // EBADF, EFAULT, EINVAL. all of them should close the connection
			for j := 0; j < nrFds; j++ {
				if !states[j].completed && states[j].failed == nil {
					states[j].failed = unix.EPIPE
					log.Errorf("Failed: WaitAndCheckRaftReply, EpollWait, sa=%v, err=%v", sas[j], err)
					nrFailed += 1
				}
			}
			break
		} else {
			for i := 0; i < nrEvents; i++ {
				ev := events[i]
				fd := int(ev.Fd)
				var j = 0
				for ; j < nrFds; j++ {
					if fd == fds[j] {
						break
					}
				}
				if j == nrFds {
					log.Errorf("Failed (ignore): ReadAllFd, unexpected fd triggered event, ev=%v", ev)
					w.RemoveFd(fd)
					continue
				}
				if ev.Events&unix.EPOLLERR != 0 || ev.Events&unix.EPOLLHUP != 0 || ev.Events&unix.EPOLLOUT != 0 {
					// a peer closed the socket (EPOLLHUP), something bad happened (EPOLLERR), or unexpected events are reported. delete it from the epoll list
					states[j].failed = unix.EPIPE
					log.Errorf("Failed: WaitAndCheckRaftReply, EpollWait returned events=%v, sa=%v", ev.Events, sas[j])
					w.RemoveFd(fd)
					nrFailed += 1
					continue
				}
				if states[j].completed {
					// never read fd for completed requests
					continue
				}
				msg := messages[j]
				ReadRpcMsg(fd, &msg, &states[j])
				if states[j].failed != nil {
					log.Errorf("Failed: WaitAndCheckRaftReply, ReadRpcMsg, sa=%v, err=%v", sas[j], states[j].failed)
					w.RemoveFd(fd)
					nrFailed += 1
					continue
				}
				if states[j].optComplete == 0 {
					continue
				}
				spliceBegin := time.Now()
				readTime += spliceBegin.Sub(readBegin)
				ReadDataToRaftLog(fd, &msg, &states[j], raft.files, w.pipeFds)
				beginResponseHandling := time.Now()
				spliceTime += beginResponseHandling.Sub(spliceBegin)
				if states[j].completed {
					cmdId := msg.GetCmdId()
					var reply int32
					switch cmdId {
					case AppendEntriesResponseCmdId:
						reply = raft.HandleAppendEntriesResponse(msg, sas[j])
					case RequestVoteResponseCmdId:
						reply = raft.HandleRequestVoteResponse(msg, sas[j])
					default:
						log.Errorf("Failed (ignore): WaitAndCheckRaftReply, unknown cmdId, cmdId=%v, sa=%v", cmdId, sas[j])
					}
					if reply == RaftReplyOk {
						nrSuccess += 1
					} else {
						nrFailed += 1
					}
				}
				responseHandlingTime += time.Since(beginResponseHandling)
			}
		}
	}
	if debug {
		log.Debugf("Success: WaitAndCheckRaftReply, prepareTime=%v, nrLoop=%v, epollTime=%v, readTime=%v, spliceTime=%v, responseHandligTime=%v",
			prepareTime, i, epollTime, readTime, spliceTime, responseHandlingTime)
	}
	return
}

func (w *RpcClient) WaitAndGetRpcReply(fd int, timeout time.Duration) (msg RpcMsg, reply int32) {
	begin := time.Now()
	state := ReadRpcMsgState{optOffForSplice: uint16(RpcOptControlHeaderLength)}
	events := make([]unix.EpollEvent, len(w.sas))
	for {
		elapsed := time.Since(begin)
		if elapsed > timeout {
			reply = ErrnoToReply(unix.ETIMEDOUT)
			log.Errorf("Timeout: WaitAndGetRpcReply, elapsed=%v, timeout=%v", elapsed, timeout)
			return
		}
		nrEvents, err := unix.EpollWait(w.r.epollFd, events, int((timeout - elapsed).Milliseconds()))
		if err != nil && err == unix.EINTR {
			continue
		} else if err == unix.EBADFD || err == unix.EFAULT || err == unix.EINVAL { // EBADF, EFAULT, EINVAL. all of them should close the connection
			reply = ErrnoToReply(err)
			log.Errorf("Failed: WaitAndGetRpcReply, EpollWait, err=%v", err)
			return
		}
		for i := 0; i < nrEvents; i++ {
			ev := events[i]
			rFd := int(ev.Fd)
			if fd != rFd {
				//log.Errorf("Failed (ignore): WaitAndGetRpcReply, unexpected fd triggered event, rFd=%v (!=%v), ev=%v", rFd, fd, ev)
				continue
			}
			if ev.Events&unix.EPOLLERR != 0 || ev.Events&unix.EPOLLHUP != 0 || ev.Events&unix.EPOLLOUT != 0 {
				// a peer closed the socket (EPOLLHUP), something bad happened (EPOLLERR), or unexpected events are reported. delete it from the epoll list
				log.Errorf("Failed: WaitAndGetRpcReply, EpollWait returned events=%v", ev.Events)
				reply = ErrnoToReply(unix.EPIPE)
				return
			}
			ReadRpcMsg(fd, &msg, &state)
			if state.failed != nil {
				log.Errorf("Failed: WaitAndGetRpcReply, ReadRpcMsg, err=%v", state.failed)
				reply = ErrnoToReply(state.failed)
				return
			}
			if state.optComplete > 0 {
				state.completed = true
				reply = RaftReplyOk
				return
			}
		}
	}
}

/**********************/
/**********************/

type RpcClientConnectionV2 struct {
	fd          int32
	sa          common.NodeAddrInet4
	receiveCond *sync.Cond
	receiving   int32
	sendLock    *sync.Mutex
	rpcArgs     map[uint64]RpcSeqNumArgs
	rpcArgsLock *sync.Mutex
	received    map[uint64]RpcMsg
	aborted     []uint64
	pipeFds     [2]int
	sender      *EpollHandler
	receiver    *EpollHandler
	refCount    int32
}

func NewRpcClientConnectionV2(na common.NodeAddrInet4, connectErrLog bool, boundDev string) (ret *RpcClientConnectionV2, err error) {
	var sender, receiver *EpollHandler
	pipeFds := [2]int{}
	var fd int
	fd, err = unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2, Socket, na=%v, err=%v", na, err)
		return
	}
	if boundDev != "" {
		if err = unix.SetsockoptString(fd, unix.SOL_SOCKET, unix.SO_BINDTODEVICE, boundDev); err != nil {
			log.Errorf("Failed: NewRpcClientConnectionV2, SetsockoptString, fd=%v, boundDev=%v, err=%v", fd, boundDev, err)
			goto close
		}
	}
	if err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2, SetsockoptInt(fd, SOL_SOCKET, SO_REUSEADDR, 1), fd=%v, err=%v", fd, err)
		goto close
	}
	if err = unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_NODELAY, 1); err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2, SetsockoptInt(fd, SOL_TCP, TCP_NODELAY, 1), fd=%v, err=%v", fd, err)
		goto close
	}
	if err = unix.Connect(fd, &unix.SockaddrInet4{Addr: na.Addr, Port: na.Port}); err != nil {
		if connectErrLog {
			log.Errorf("Failed: NewRpcClientConnectionV2, Connect, sa=%v, err=%v", na, err)
		}
		goto close
	}
	receiver, err = NewEpollHandler()
	if err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2, NewEpollReader, err=%v", err)
		goto close
	}
	sender, err = NewEpollHandler()
	if err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2, NewEpollHandler, err=%v", err)
		goto closeReceiver
	}
	if err = sender.AddFd(fd, unix.EPOLLOUT); err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2, sender.AddFd, fd=%v, na=%v, err=%v", fd, na, err)
		goto closeSender
	}
	if err = receiver.AddFd(fd, unix.EPOLLIN); err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2, receiver.AddFd, fd=%v, na=%v, err=%v", fd, na, err)
		goto removeSender
	}
	if err = unix.Pipe(pipeFds[:]); err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2, Pipe, err=%v", err)
		goto removeReceiver
	}
	ret = &RpcClientConnectionV2{
		fd: int32(fd), sa: na, rpcArgs: make(map[uint64]RpcSeqNumArgs), receiveCond: sync.NewCond(new(sync.Mutex)), sendLock: new(sync.Mutex), rpcArgsLock: new(sync.Mutex),
		pipeFds: pipeFds, sender: sender, receiver: receiver, received: make(map[uint64]RpcMsg), aborted: nil,
	}
	log.Debugf("Success: NewRpcClientConnectionV2, fd=%v, na=%v", fd, na)
	return
removeReceiver:
	if err2 := receiver.RemoveFd(fd); err2 != nil {
		log.Errorf("Failed (ignore): NewRpcClientConnectionV2, receiver.RemoveFd, fd=%v, err=%v", fd, err2)
	}
removeSender:
	if err2 := sender.RemoveFd(fd); err2 != nil {
		log.Errorf("Failed (ignore): NewRpcClientConnectionV2, sender.RemoveFd, fd=%v, err=%v", fd, err2)
	}
closeSender:
	if err2 := sender.Close(); err2 != nil {
		log.Errorf("Failed (ignore): NewRpcClientConnectionV2, sender.Close, fd=%v, err=%v", fd, err2)
	}
closeReceiver:
	if err2 := receiver.Close(); err2 != nil {
		log.Errorf("Failed (ignore): NewRpcClientConnectionV2, receiver.Close, fd=%v, err=%v", fd, err2)
	}
close:
	if err2 := unix.Close(fd); err2 != nil {
		log.Errorf("Failed (ignore): NewRpcClientConnectionV2, Close, fd=%v, err=%v", fd, err2)
	}
	return
}

func NewRpcClientConnectionV2FromFd(fd int, na common.NodeAddrInet4) (ret *RpcClientConnectionV2, err error) {
	var sender *EpollHandler
	pipeFds := [2]int{}
	sender, err = NewEpollHandler()
	if err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2FromFd, NewEpollHandler, err=%v", err)
		return
	}
	if err = sender.AddFd(fd, unix.EPOLLOUT); err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2FromFd, sender.AddFd, fd=%v, na=%v, err=%v", fd, na, err)
		goto closeSender
	}
	if err = unix.Pipe(pipeFds[:]); err != nil {
		log.Errorf("Failed: NewRpcClientConnectionV2FromFd, Pipe, err=%v", err)
		goto removeSender
	}
	ret = &RpcClientConnectionV2{
		fd: int32(fd), sa: na, rpcArgs: make(map[uint64]RpcSeqNumArgs), receiveCond: sync.NewCond(new(sync.Mutex)), sendLock: new(sync.Mutex), rpcArgsLock: new(sync.Mutex),
		pipeFds: pipeFds, sender: sender, receiver: nil, received: make(map[uint64]RpcMsg),
	}
	log.Debugf("Success: NewRpcClientConnectionV2FromFd, fd=%v, na=%v", fd, na)
	return
removeSender:
	if err2 := sender.RemoveFd(fd); err2 != nil {
		log.Errorf("Failed (ignore): NewRpcClientConnectionV2FromFd, sender.RemoveFd, fd=%v, err=%v", fd, err2)
	}
closeSender:
	if err2 := sender.Close(); err2 != nil {
		log.Errorf("Failed (ignore): NewRpcClientConnectionV2FromFd, sender.Close, fd=%v, err=%v", fd, err2)
	}
	return
}

func (w *RpcClientConnectionV2) StoreRpcArgs(seqNum uint64, args *RpcSeqNumArgs) {
	if args != nil {
		w.rpcArgsLock.Lock()
		w.rpcArgs[seqNum] = *args
		w.rpcArgsLock.Unlock()
	}
}

func (w *RpcClientConnectionV2) SendRpcMsg(msg RpcMsg, files *RaftFiles, wakeUpInterval time.Duration, dataBuf [][]byte, doTimeout bool) (err error) {
	w.sendLock.Lock()
	if w.sender == nil {
		w.sendLock.Unlock()
		log.Errorf("Failed: SendRpcMsg, sender is already closed")
		return unix.EPIPE
	}
	begin := time.Now()
	err = nil
	info := WriteRpcState{
		headerSentBytes: 0, optHeaderSentBytes: 0, optOff: uint16(RpcOptControlHeaderLength), optBufOff: 0, dataBufOff: 0, msg: msg, completed: false, failed: nil,
		optHeaderLength: msg.GetOptHeaderLength(),
	}

	epollTime := [4]time.Duration{}
	writeTime := [4]time.Duration{}
	events := [1]unix.EpollEvent{}
	var i = 0
	var fd = 0
	var startedSend = false
	// CallObjcacheRpc calls this with doTimeout==true.
	// we do not check timeout here since heavy requests may slow down all other requests and cause timeout even if the target node is fine.
	// timeout is evaluated only when we start epoll to wait for the reply later
	for ; ; i++ {
		epollBegin := time.Now()
		if !startedSend && doTimeout {
			elapsed := epollBegin.Sub(begin)
			if elapsed > wakeUpInterval {
				log.Errorf("Failed: SendRpcMsg, timed out, elapsed=%v", elapsed)
				err = unix.ETIMEDOUT
				break
			}
		}
		nrEvents, err2 := unix.EpollWait(w.sender.epollFd, events[:], int(wakeUpInterval.Milliseconds()))
		writeBegin := time.Now()
		epollTime[i%4] = writeBegin.Sub(epollBegin)
		if err2 == unix.EINTR {
			continue
		} else if err2 == unix.EBADFD || err2 == unix.EFAULT || err2 == unix.EINVAL { // EBADF, EFAULT, EINVAL. all of them should close the connection
			err = unix.EPIPE
			info.completed = false
			log.Errorf("Failed: SendRpcMsg, EpollWait, err=%v", err2)
			break
		}
		for j := 0; j < nrEvents; j++ {
			ev := events[j]
			fd = int(ev.Fd)
			if ev.Fd != w.fd {
				log.Debugf("Failed (ignore): SendRpcMsg, EpollWait returned unknown fd, ev.Fd=%v, fd=%v", ev.Fd, fd)
				continue
			}
			if ev.Events&unix.EPOLLERR != 0 || ev.Events&unix.EPOLLHUP != 0 || ev.Events&unix.EPOLLIN != 0 {
				// a peer closed the socket (EPOLLHUP), something bad happened (EPOLLERR), or unexpected events are reported. delete it from the epoll list
				log.Errorf("Failed: SendRpcMsg, EpollWait returned events=%v, fd=%v", ev.Events, ev.Fd)
				info.failed = unix.EPIPE
				info.completed = false
				break
			}
			if err3 := __writeRpcMsg(ev, fd, &info, files, dataBuf, false); err3 != nil {
				info.failed = err3
				info.completed = false
			}
			startedSend = true
		}
		writeTime[i%4] = time.Since(writeBegin)
		if info.completed || info.failed != nil {
			err = info.failed
			break
		}
	}
	w.sendLock.Unlock()
	return
}

func (w *RpcClientConnectionV2) WaitAndGetRpcReply(seqNum uint64, timeout time.Duration) (msg RpcMsg, err error) {
	begin := time.Now()
	w.receiveCond.L.Lock()
	var ok bool
	if msg, ok = w.received[seqNum]; ok {
		delete(w.received, seqNum)
		w.receiveCond.L.Unlock()
		return
	}
	state := ReadRpcMsgState{optOffForSplice: uint16(RpcOptControlHeaderLength)}
	events := [1]unix.EpollEvent{}
	newMsg := RpcMsg{}
	var fd = 0
	var recvStarted = false

	for !atomic.CompareAndSwapInt32(&w.receiving, 0, 1) {
		w.receiveCond.Wait()
		if msg, ok = w.received[seqNum]; ok {
			delete(w.received, seqNum)
			w.receiveCond.L.Unlock()
			return
		}
		elapsed := time.Since(begin)
		if elapsed > timeout {
			w.receiveCond.L.Unlock()
			err = unix.ETIMEDOUT
			log.Errorf("Timeout: WaitAndGetRpcReply, seqNum=%v, elapsed=%v, timeout=%v", seqNum, elapsed, timeout)
			return
		}
	}

	// now this thread becomes an atomic receiver context until atomic.Storeint32(&w.receiving, 0)
	if len(w.aborted) > 0 {
		for _, abSeqNum := range w.aborted {
			delete(w.received, abSeqNum)
		}
		w.aborted = nil
	}
	if w.receiver == nil {
		atomic.StoreInt32(&w.receiving, 0)
		w.receiveCond.Broadcast()
		w.receiveCond.L.Unlock()
		log.Errorf("Failed: WaitAndGetRpcReply, receiver is already closed")
		err = unix.EPIPE
		return
	}
	w.receiveCond.L.Unlock()

	for {
		elapsed := time.Since(begin)
		if !recvStarted && elapsed > timeout {
			err = unix.ETIMEDOUT
			log.Errorf("Timeout: WaitAndGetRpcReply, seqNum=%v, elapsed=%v, timeout=%v", seqNum, elapsed, timeout)
			goto out
		}
		var nrEvents int
		nrEvents, err = unix.EpollWait(w.receiver.epollFd, events[:], int((timeout - elapsed).Milliseconds()))
		if err == unix.EINTR {
			continue
		} else if err == unix.EBADFD || err == unix.EFAULT || err == unix.EINVAL { // EBADF, EFAULT, EINVAL. all of them should close the connection
			log.Errorf("Failed: WaitAndGetRpcReply, EpollWait, err=%v", err)
			goto out
		}
		for i := 0; i < nrEvents; i++ {
			ev := events[i]
			if ev.Fd != w.fd {
				log.Debugf("Failed (ignore): WaitAndGetRpcReply, EpollWait returned unknown fd, ev.Fd=%v, fd=%v", ev.Fd, fd)
				//w.receiver.RemoveFd(int(ev.Fd))
				continue
			}
			fd = int(ev.Fd)
			if ev.Events&unix.EPOLLERR != 0 || ev.Events&unix.EPOLLHUP != 0 || ev.Events&unix.EPOLLOUT != 0 {
				// a peer closed the socket (EPOLLHUP), something bad happened (EPOLLERR), or unexpected events are reported. delete it from the epoll list
				log.Errorf("Failed: WaitAndGetRpcReply, EpollWait returned events=%v", ev.Events)
				err = unix.EPIPE
				goto out
			}
			ReadRpcMsg(fd, &newMsg, &state)
			if state.failed != nil {
				log.Errorf("Failed: WaitAndGetRpcReply, ReadRpcMsg, err=%v", state.failed)
				err = state.failed
				goto out
			}
			recvStarted = true
			if state.optComplete > 0 {
				payload := newMsg.GetCmdPayload()
				msgSeqNum := newMsg.GetExecProtoBufRpcSeqNum(payload)
				w.rpcArgsLock.Lock()
				args, ok2 := w.rpcArgs[msgSeqNum]
				w.rpcArgsLock.Unlock()
				if !ok2 {
					// when a request was canceled but has arrived (e.g., timed out), we need to use a dummy buffer
					dataLength, _ := msg.GetOptControlHeader()
					if dataLength > 0 {
						args = RpcSeqNumArgs{data: make([]byte, dataLength)}
					}
				}
				if args.data != nil {
					ReadDataToBuffer(fd, &newMsg, &state, args.data)
				} else if args.fd > 0 {
					state.fileOffsetForSplice = args.fileOffset
					ReadDataToFd(fd, &newMsg, &state, args.fd, w.pipeFds)
				} else {
					state.completed = true
				}
				if state.failed != nil {
					log.Errorf("Failed: WaitAndGetRpcReply, ReadDataToTempFile, err=%v", state.failed)
					err = state.failed
					goto out
				}
				if state.completed {
					err = nil
					recvStarted = false
					if seqNum == msgSeqNum {
						msg = newMsg
						goto out
					} else {
						w.receiveCond.L.Lock()
						w.received[msgSeqNum] = newMsg
						if msg, ok = w.received[seqNum]; ok {
							delete(w.received, seqNum)
							w.receiveCond.L.Unlock()
							goto out
						}
						w.receiveCond.Broadcast()
						w.receiveCond.L.Unlock()
						newMsg = RpcMsg{}
						state = ReadRpcMsgState{optOffForSplice: uint16(RpcOptControlHeaderLength)}
					}
				}
			}
		}
	}
out:
	w.receiveCond.L.Lock()
	if err != nil {
		w.aborted = append(w.aborted, seqNum)
	}
	atomic.StoreInt32(&w.receiving, 0)
	w.receiveCond.Broadcast()
	w.receiveCond.L.Unlock()
	return
}

func (w *RpcClientConnectionV2) RemoveRpcArgs(seqNum uint64) {
	w.rpcArgsLock.Lock()
	delete(w.rpcArgs, seqNum)
	w.rpcArgsLock.Unlock()
}

func (w *RpcClientConnectionV2) Close() bool {
	fd := int(atomic.LoadInt32(&w.fd))
	if !atomic.CompareAndSwapInt32(&w.fd, int32(fd), 0) {
		fd = 0
		return false
	}
	w.sendLock.Lock()
	if w.sender != nil {
		if fd > 0 {
			if err := w.sender.RemoveFd(fd); err != nil {
				log.Errorf("Failed (ignore): RpcClientConnectionV2.Close, sender.RemoveFd, fd=%v, err=%v", fd, err)
			}
		}
		if err := w.sender.Close(); err != nil {
			log.Errorf("Failed (ignore): RpcClientV2.Close, sender.Close, err=%v", err)
		}
		w.sender = nil
	}
	w.sendLock.Unlock()
	w.receiveCond.L.Lock()
	if w.receiver != nil {
		if fd > 0 {
			if err := w.receiver.RemoveFd(fd); err != nil {
				log.Errorf("Failed (ignore): RpcClientConnectionV2.closeReceiverNoLock, r.RemoveFd, fd=%v, err=%v", fd, err)
			}
		}
		if err := w.receiver.Close(); err != nil {
			log.Errorf("Failed (ignore): RpcClientV2.closeSenderNoLock, receiver.Close, err=%v", err)
		}
		w.receiver = nil
	}
	atomic.StoreInt32(&w.receiving, 0)
	w.receiveCond.Broadcast()
	w.receiveCond.L.Unlock()
	if fd > 0 {
		if err := unix.Close(fd); err != nil {
			log.Errorf("Failed (ignore): RpcClientConnectionV2.Close, Close, fd=%v, err=%v", fd, err)
		}
	}
	atomic.StoreInt32(&w.refCount, 0)
	log.Debugf("Success: RpcClientConnectionV2.Close, fd=%v", fd)
	return true
}

func (w *RpcClientConnectionV2) Up() {
	atomic.AddInt32(&w.refCount, 1)
}

func (w *RpcClientConnectionV2) Down() int32 {
	return atomic.AddInt32(&w.refCount, -1)
}

func (w *RpcClientConnectionV2) IsFree() bool {
	return atomic.LoadInt32(&w.refCount) == 0
}

func (w *RpcClientConnectionV2) CallObjcacheRpc(extCmdId uint16, seqNum uint64, args proto.Message, timeout time.Duration, files *RaftFiles, dataBuf [][]byte, rpcArgs *RpcSeqNumArgs, ret proto.Message) (reply int32) {
	begin := time.Now()
	msg := RpcMsg{}
	bufLen := 0
	for _, b := range dataBuf {
		bufLen += len(b)
	}
	if reply = msg.FillExecProtoBufArgs(extCmdId, seqNum, args, bufLen, false); reply != RaftReplyOk {
		log.Errorf("Failed: CallObjcacheRpc, FillExecProtoBufArgs, reply=%v", reply)
		return reply
	}
	fill := time.Now()
	w.StoreRpcArgs(seqNum, rpcArgs)
	err := w.SendRpcMsg(msg, files, timeout, dataBuf, false)
	msg.optBuf = nil
	if err != nil {
		w.RemoveRpcArgs(seqNum)
		log.Errorf("Failed: CallObjcacheRpc, SendRpcMsg, sa=%v, err=%v", w.sa, err)
		return ErrnoToReply(err)
	}
	send := time.Now()

	replyMsg, err2 := w.WaitAndGetRpcReply(seqNum, timeout)
	w.RemoveRpcArgs(seqNum)
	if err2 != nil {
		log.Errorf("Failed: CallObjcacheRpc, WaitAndGetRpcReply, sa=%v, err2=%v", w.sa, err2)
		return ErrnoToReply(err2)
	}
	recv := time.Now()
	if reply = replyMsg.ParseExecProtoBufMessage(ret); reply != RaftReplyOk {
		log.Errorf("Failed: CallObjcacheRpc, ParseExecProtoBufMessage, reply=%v", reply)
		return reply
	}
	parse := time.Now()
	log.Debugf("Success: CallObjcacheRpc, extCmdId=%v, seqNum=%v, len(dataBuf)=%v, sa=%v, fill=%v, send=%v, recv=%v, parse=%v",
		extCmdId, seqNum, len(dataBuf), w.sa, fill.Sub(begin), send.Sub(fill), recv.Sub(send), parse.Sub(recv))
	return RaftReplyOk
}

func (w *RpcClientConnectionV2) CallObjcacheRpcNoTimeout(extCmdId uint16, seqNum uint64, args proto.Message, files *RaftFiles, dataBuf [][]byte, rpcArgs *RpcSeqNumArgs, ret proto.Message) (reply int32) {
	begin := time.Now()
	msg := RpcMsg{}
	bufLen := 0
	for _, b := range dataBuf {
		bufLen += len(b)
	}
	if reply = msg.FillExecProtoBufArgs(extCmdId, seqNum, args, bufLen, false); reply != RaftReplyOk {
		log.Errorf("Failed: CallObjcacheRpcNoTimeout, FillExecProtoBufArgs, reply=%v", reply)
		return reply
	}
	fill := time.Now()
	w.StoreRpcArgs(seqNum, rpcArgs)
	lastWarn := time.Now()
	for {
		err := w.SendRpcMsg(msg, files, time.Millisecond, dataBuf, false)
		msg.optBuf = nil
		if err == unix.ETIMEDOUT {
			if time.Since(lastWarn).Minutes() >= 1 {
				log.Warnf("CallObjcacheRpcNoTimeout, blocking at SendRpcMsg, seqNum=%v, elapsed=%v", seqNum, time.Since(begin))
				lastWarn = time.Now()
			}
			continue
		} else if err != nil {
			w.RemoveRpcArgs(seqNum)
			log.Errorf("Failed: CallObjcacheRpcNoTimeout, SendRpcMsg, sa=%v, err=%v", w.sa, err)
			return ErrnoToReply(err)
		}
		break
	}
	send := time.Now()

	var replyMsg RpcMsg
	lastWarn = send
	for {
		var err error
		replyMsg, err = w.WaitAndGetRpcReply(seqNum, time.Millisecond*10)
		if err == unix.ETIMEDOUT {
			if time.Since(lastWarn).Minutes() >= 1 {
				log.Warnf("CallObjcacheRpcNoTimeout, blocking at WaitAndGetRpcReply, seqNum=%v, elapsed=%v", seqNum, time.Since(begin))
				lastWarn = time.Now()
			}
			continue
		} else if err != nil {
			w.RemoveRpcArgs(seqNum)
			log.Errorf("Failed: CallObjcacheRpcNoTimeout, WaitAndGetRpcReply, sa=%v, err=%v", w.sa, err)
			return ErrnoToReply(err)
		}
		w.RemoveRpcArgs(seqNum)
		break
	}
	recv := time.Now()
	if reply = replyMsg.ParseExecProtoBufMessage(ret); reply != RaftReplyOk {
		log.Errorf("Failed: CallObjcacheRpcNoTimeout, ParseExecProtoBufMessage, reply=%v", reply)
		return reply
	}
	parse := time.Now()
	log.Debugf("Success: CallObjcacheRpcNoTimeout, extCmdId=%v, seqNum=%v, len(dataBuf)=%v, sa=%v, fill=%v, send=%v, recv=%v, parse=%v",
		extCmdId, seqNum, len(dataBuf), w.sa, fill.Sub(begin), send.Sub(fill), recv.Sub(send), parse.Sub(recv))
	return RaftReplyOk
}

func (w *RpcClientConnectionV2) AsyncObjcacheRpc(extCmdId uint16, seqNum uint64, args proto.Message, sa common.NodeAddrInet4, files *RaftFiles, dataBuf [][]byte, rpcArgs *RpcSeqNumArgs) (reply int32) {
	begin := time.Now()
	msg := RpcMsg{}
	bufLen := 0
	for _, b := range dataBuf {
		bufLen += len(b)
	}
	if reply = msg.FillExecProtoBufArgs(extCmdId, seqNum, args, bufLen, false); reply != RaftReplyOk {
		log.Errorf("Failed: AsyncObjcacheRpc, FillExecProtoBufArgs, reply=%v", reply)
		return reply
	}
	w.StoreRpcArgs(seqNum, rpcArgs)
	lastWarn := time.Now()
	for {
		err := w.SendRpcMsg(msg, files, time.Millisecond, dataBuf, false)
		msg.optBuf = nil
		if err == unix.ETIMEDOUT {
			if time.Since(lastWarn).Minutes() >= 1 {
				log.Warnf("AsyncObjcacheRpc, blocking at SendRpcMsg, seqNum=%v, elapsed=%v", seqNum, time.Since(begin))
				lastWarn = time.Now()
			}
			continue
		} else if err != nil {
			w.RemoveRpcArgs(seqNum)
			log.Errorf("Failed: AsyncObjcacheRpc, SendRpcMsg, sa=%v, err=%v", w.sa, err)
			return ErrnoToReply(err)
		}
		break
	}
	return RaftReplyOk
}

type RpcClientV2 struct {
	sas  map[common.NodeAddrInet4]*RpcClientConnectionV2
	lock *sync.RWMutex

	boundDev           string
	connectFailedCount map[common.NodeAddrInet4]uint32
	nextSeqNum         uint64
}

func NewRpcClientV2(boundDev string) (*RpcClientV2, error) {
	return &RpcClientV2{
		boundDev:           boundDev,
		sas:                make(map[common.NodeAddrInet4]*RpcClientConnectionV2),
		connectFailedCount: make(map[common.NodeAddrInet4]uint32), nextSeqNum: 0,
		lock: new(sync.RWMutex),
	}, nil
}

func (w *RpcClientV2) Close() {
	w.lock.Lock()
	for _, con := range w.sas {
		con.Close()
	}
	w.sas = make(map[common.NodeAddrInet4]*RpcClientConnectionV2)
	w.connectFailedCount = make(map[common.NodeAddrInet4]uint32)
	w.lock.Unlock()
}

func (w *RpcClientV2) CheckReset() (ok bool) {
	if ok = w.lock.TryLock(); !ok {
		log.Errorf("Failed: RpcClientV2.CheckReset, w.lock is taken")
		return
	}
	if ok2 := len(w.sas) == 0; !ok2 {
		log.Errorf("Failed: RpcClientV2.CheckReset, len(w.sas) != 0")
		ok = false
	}
	if ok2 := len(w.connectFailedCount) == 0; !ok2 {
		log.Errorf("Failed: RpcClientV2.CheckReset, len(w.connectFailedCount) != 0")
		ok = false
	}
	w.lock.Unlock()
	return
}

func (w *RpcClientV2) AsyncObjcacheRpc(extCmdId uint16, args proto.Message, sa common.NodeAddrInet4, files *RaftFiles, dataBuf [][]byte, rpcArgs *RpcSeqNumArgs) (con *RpcClientConnectionV2, seqNum uint64, reply int32) {
	w.lock.RLock()
	var ok bool
	con, ok = w.sas[sa]
	if ok {
		con.Up()
	}
	w.lock.RUnlock()
	if !ok {
		w.lock.Lock()
		if con, ok = w.sas[sa]; !ok {
			failed := w.connectFailedCount[sa]
			var err error
			con, err = NewRpcClientConnectionV2(sa, failed >= 3, w.boundDev)
			if err != nil {
				w.connectFailedCount[sa] = failed + 1
				w.lock.Unlock()
				log.Errorf("Failed: AsyncObjcacheRpc, Connect, sa=%v, err=%v", sa, err)
				return nil, 0, ErrnoToReply(err)
			}
			w.sas[sa] = con
			delete(w.connectFailedCount, sa)
		}
		con.Up()
		w.lock.Unlock()
	}
	seqNum = atomic.AddUint64(&w.nextSeqNum, 1)
	reply = con.AsyncObjcacheRpc(extCmdId, seqNum, args, sa, files, dataBuf, rpcArgs)
	if reply != RaftReplyOk && reply != ErrnoToReply(unix.ETIMEDOUT) {
		con.Down()
		w.lock.Lock()
		if con.IsFree() {
			// ignore an error if it occurs during concurrent RPCs.
			if c2, ok := w.sas[con.sa]; ok && c2 == con {
				delete(w.sas, con.sa)
			}
			con.Close()
		}
		w.lock.Unlock()
		return nil, seqNum, reply
	}
	return con, seqNum, reply
}

func (w *RpcClientV2) WaitAsyncObjcacheRpc(con *RpcClientConnectionV2, seqNum uint64, ret proto.Message) (reply int32) {
	var replyMsg RpcMsg
	begin := time.Now()
	lastWarn := begin
	for {
		var err error
		replyMsg, err = con.WaitAndGetRpcReply(seqNum, time.Millisecond*10)
		if err == unix.ETIMEDOUT {
			if time.Since(lastWarn).Minutes() >= 1 {
				log.Warnf("WaitAsyncObjcacheRpc, blocking at WaitAndGetRpcReply, seqNum=%v, elapsed=%v", seqNum, time.Since(begin))
				lastWarn = time.Now()
			}
			continue
		} else if err != nil {
			con.RemoveRpcArgs(seqNum)
			log.Errorf("Failed: WaitAsyncObjcacheRpc, WaitAndGetRpcReply, sa=%v, err=%v", con.sa, err)
			con.Down()
			w.lock.Lock()
			if con.IsFree() {
				// ignore an error if it occurs during concurrent RPCs.
				if c2, ok := w.sas[con.sa]; ok && c2 == con {
					delete(w.sas, con.sa)
				}
				con.Close()
			}
			w.lock.Unlock()
			return ErrnoToReply(err)
		}
		con.RemoveRpcArgs(seqNum)
		break
	}
	if reply = replyMsg.ParseExecProtoBufMessage(ret); reply != RaftReplyOk {
		log.Errorf("Failed: WaitAsyncObjcacheRpc, ParseExecProtoBufMessage, reply=%v", reply)
	}
	con.Down()
	return reply
}

func (w *RpcClientV2) CallObjcacheRpc(extCmdId uint16, args proto.Message, sa common.NodeAddrInet4, timeout time.Duration, files *RaftFiles, dataBuf [][]byte, rpcArgs *RpcSeqNumArgs, ret proto.Message) (reply int32) {
	w.lock.RLock()
	var ok bool
	con, ok := w.sas[sa]
	if ok {
		con.Up()
	}
	w.lock.RUnlock()
	if !ok {
		w.lock.Lock()
		if con, ok = w.sas[sa]; !ok {
			failed := w.connectFailedCount[sa]
			var err error
			con, err = NewRpcClientConnectionV2(sa, failed >= 3, w.boundDev)
			if err != nil {
				w.connectFailedCount[sa] = failed + 1
				w.lock.Unlock()
				log.Errorf("Failed: CallObjcacheRpc, Connect, sa=%v, err=%v", sa, err)
				return ErrnoToReply(err)
			}
			w.sas[sa] = con
			delete(w.connectFailedCount, sa)
		}
		con.Up()
		w.lock.Unlock()
	}
	seqNum := atomic.AddUint64(&w.nextSeqNum, 1)
	if timeout.Nanoseconds() <= 0 {
		reply = con.CallObjcacheRpcNoTimeout(extCmdId, seqNum, args, files, dataBuf, rpcArgs, ret)
	} else {
		reply = con.CallObjcacheRpc(extCmdId, seqNum, args, timeout, files, dataBuf, rpcArgs, ret)
	}
	con.Down()
	if reply != RaftReplyOk && reply != ErrnoToReply(unix.ETIMEDOUT) {
		w.lock.Lock()
		if con.IsFree() {
			// ignore an error if it occurs during concurrent RPCs.
			if c2, ok := w.sas[sa]; ok && c2 == con {
				delete(w.sas, sa)
			}
			con.Close()
		}
		w.lock.Unlock()
	}
	return reply
}

type RpcReplyClient struct {
	fds  map[int]*RpcClientConnectionV2
	lock *sync.RWMutex
}

func NewRpcReplyClient() (*RpcReplyClient, error) {
	return &RpcReplyClient{
		fds:  make(map[int]*RpcClientConnectionV2),
		lock: new(sync.RWMutex),
	}, nil
}

func (w *RpcReplyClient) Register(fd int, sa common.NodeAddrInet4) (err error) {
	w.lock.Lock()
	if _, ok := w.fds[fd]; !ok {
		var con *RpcClientConnectionV2
		if con, err = NewRpcClientConnectionV2FromFd(fd, sa); err == nil {
			w.fds[fd] = con
		}
	}
	w.lock.Unlock()
	return
}

func (w *RpcReplyClient) Close() {
	w.lock.Lock()
	for _, con := range w.fds {
		con.Close()
	}
	w.fds = make(map[int]*RpcClientConnectionV2)
	w.lock.Unlock()
}

func (w *RpcReplyClient) CheckReset() (ok bool) {
	if ok = w.lock.TryLock(); !ok {
		log.Errorf("Failed: RpcReplyClient.CheckReset, w.lock is taken")
		return
	}
	if ok2 := len(w.fds) == 0; !ok2 {
		log.Errorf("Failed: RpcReplyClient.CheckReset, len(w.fds) != 0")
		ok = false
	}
	w.lock.Unlock()
	return
}

func (w *RpcReplyClient) ReplyRpcMsg(msg RpcMsg, fd int, sa common.NodeAddrInet4, files *RaftFiles, timeout time.Duration, dataBuf [][]byte) (reply int32) {
	w.lock.RLock()
	con, ok := w.fds[fd]
	if ok {
		con.Up()
	}
	w.lock.RUnlock()
	if !ok {
		log.Errorf("Failed: ReplyRpcMsg, connection is closed, sa=%v", sa)
		return ErrnoToReply(unix.EPIPE)
	}
	err := con.SendRpcMsg(msg, files, timeout, dataBuf, true)
	con.Down()
	if err != nil && err != unix.ETIMEDOUT {
		log.Errorf("Failed: ReplyRpcMsg, SendRpcMsg, sa=%v, err=%v", sa, err)
		w.lock.Lock()
		if con.IsFree() {
			// ignore an error if it occurs during concurrent RPCs.
			if c2, ok := w.fds[fd]; ok && con == c2 {
				delete(w.fds, fd)
			}
			con.Close()
		}
		w.lock.Unlock()
	}
	return ErrnoToReply(err)
}

// CmdIds for RpcMsg
// NoOpCmdId = uint16(0)
const (
	AppendEntriesCmdId         = uint8(1)
	AppendEntriesResponseCmdId = uint8(2)
	RequestVoteCmdId           = uint8(3)
	RequestVoteResponseCmdId   = uint8(4)
	ExecProtoBufCmdId          = uint8(5)
	ExecProtoBufResponseCmdId  = uint8(6)
)

const (
	RpcMsgMaxLength           = int32(32)
	RpcControlHeaderLength    = int32(3)
	RpcOptControlHeaderLength = int32(8)

	RequestVotePayloadSize           = uint8(20)
	RequestVoteResponsePayloadSize   = uint8(9)
	ExecProtoBufPayloadSize          = uint8(10)
	AppendEntriesResponsePayloadSize = uint8(17)
	AppendEntryPayloadSize           = uint8(28)
)

// AppendEntryHeader also has an extended header to implement AddServer, RemoveServer, AppendCommitLog, and other Log replication
// AppendEntryHeader without extended headers mean a heart beat message
// Raft may batch to send multiple entries of AppendEntryHeader.
// In this case, it puts extended AppendEntry headers (i.e., AppendEntry*ExtHeader) at RpcMsg.optBuf.

// AppendEntryHeartBeatCmdId    = uint16(0)
const (
	DataCmdIdBit = uint16(1 << 15)

	AppendEntryNoOpCmdId                      = uint16(0)
	AppendEntryAddServerCmdId                 = uint16(1)
	AppendEntryRemoveServerCmdId              = uint16(2)
	AppendEntryCommitTxCmdId                  = uint16(3)
	AppendEntryResetExtLogCmdId               = uint16(4)
	AppendEntryFillChunkCmdId                 = DataCmdIdBit | uint16(1)
	AppendEntryUpdateChunkCmdId               = DataCmdIdBit | uint16(2)
	AppendEntryUpdateMetaCmdId                = DataCmdIdBit | uint16(3)
	AppendEntryUpdateMetaCoordinatorCmdId     = DataCmdIdBit | uint16(4)
	AppendEntryCommitChunkCmdId               = DataCmdIdBit | uint16(5)
	AppendEntryAbortTxCmdId                   = DataCmdIdBit | uint16(6)
	AppendEntryPersistChunkCmdId              = DataCmdIdBit | uint16(7)
	AppendEntryBeginPersistCmdId              = DataCmdIdBit | uint16(8)
	AppendEntryPersistCmdId                   = DataCmdIdBit | uint16(9)
	AppendEntryUpdateNodeListCoordinatorCmdId = DataCmdIdBit | uint16(10)
	AppendEntryUpdateNodeListCmdId            = DataCmdIdBit | uint16(11)
	AppendEntryUpdateNodeListLocalCmdId       = DataCmdIdBit | uint16(12)
	AppendEntryCommitMigrationCmdId           = DataCmdIdBit | uint16(13)
	AppendEntryCreateMetaCoordinatorCmdId     = DataCmdIdBit | uint16(14)
	AppendEntryUpdateMetaKeyCmdId             = DataCmdIdBit | uint16(15)
	AppendEntryRenameCoordinatorCmdId         = DataCmdIdBit | uint16(16)
	AppendEntryCreateMetaCmdId                = DataCmdIdBit | uint16(17)
	AppendEntryDeleteMetaCmdId                = DataCmdIdBit | uint16(18)
	AppendEntryDeleteMetaCoordinatorCmdId     = DataCmdIdBit | uint16(19)
	AppendEntryDeletePersistCmdId             = DataCmdIdBit | uint16(20)
	AppendEntryUpdateParentMetaCmdId          = DataCmdIdBit | uint16(21)
	AppendEntryAddInodeFileMapCmdId           = DataCmdIdBit | uint16(22)
	AppendEntryDropLRUChunksCmdId             = DataCmdIdBit | uint16(23)
	AppendEntryCreateChunkCmdId               = DataCmdIdBit | uint16(24)
	AppendEntryUpdateMetaAttrCmdId            = DataCmdIdBit | uint16(25)
	AppendEntryDeleteInodeFileMapCmdId        = DataCmdIdBit | uint16(26)
	AppendEntryRemoveNonDirtyChunksCmdId      = DataCmdIdBit | uint16(27)
	AppendEntryForgetAllDirtyLogCmdId         = DataCmdIdBit | uint16(28)

	RpcGetMetaInClusterCmdId       = uint16(1)
	RpcGetMetaCmdId                = uint16(2)
	RpcDownloadChunkViaRemoteCmdId = uint16(3)
	RpcPrefetchChunkCmdId          = uint16(4)
	RpcRestoreDirtyMetasCmdId      = uint16(5)
	RpcGetApiIpAndPortCmdId        = uint16(6)

	RpcUpdateChunkCmdId        = DataCmdIdBit | uint16(10)
	RpcRestoreDirtyChunksCmdId = DataCmdIdBit | uint16(11)

	RpcCommitParticipantCmdId          = uint16(20)
	RpcCommitMigrationParticipantCmdId = uint16(21)
	RpcAbortParticipantCmdId           = uint16(22)

	RpcCreateMetaCmdId         = uint16(30)
	RpcLinkMetaCmdId           = uint16(31)
	RpcTruncateMetaCmdId       = uint16(32)
	RpcUpdateMetaSizeCmdId     = uint16(33)
	RpcDeleteMetaCmdId         = uint16(34)
	RpcUnlinkMetaCmdId         = uint16(35)
	RpcRenameMetaCmdId         = uint16(36)
	RpcCommitUpdateChunkCmdId  = uint16(37)
	RpcCommitDeleteChunkCmdId  = uint16(38)
	RpcCommitExpandChunkCmdId  = uint16(39)
	RpcCommitPersistChunkCmdId = uint16(40)
	RpcUpdateNodeListCmdId     = uint16(41)
	RpcInitNodeListCmdId       = uint16(42)
	RpcMpuAddCmdId             = uint16(43)
	RpcJoinMigrationCmdId      = uint16(44)
	RpcLeaveMigrationCmdId     = uint16(45)
	RpcCreateChildMetaCmdId    = uint16(46)
	RpcUpdateMetaKeyCmdId      = uint16(47)
	RpcUpdateMetaAttrCmdId     = uint16(48)
	RpcGetApiPortCmdId         = uint16(49)

	RpcCoordinatorUpdateNodeListCmdId = uint16(50)
	RpcCoordinatorAbortTxCmdId        = uint16(51)
	RpcCoordinatorFlushObjectCmdId    = uint16(52)
	RpcCoordinatorTruncateObjectCmdId = uint16(53)
	RpcCoordinatorDeleteObjectCmdId   = uint16(54)
	RpcCoordinatorHardLinkObjectCmdId = uint16(55)
	RpcCoordinatorRenameObjectCmdId   = uint16(56)
	RpcCoordinatorCreateObjectCmdId   = uint16(57)
	RpcCoordinatorPersistCmdId        = uint16(58)
	RpcCoordinatorDeletePersistCmdId  = uint16(59)
)

type RpcMsg struct {
	buf [RpcMsgMaxLength]byte
	/**
	  RpcMsg buffer format (control header):
	  ------------------------------------------------
	  | Bytes|| 0 - 0 | 1 - 2           | 3 - 31     |
	  | Name || cmdId | optHeaderLength | cmdPayload |
	  ------------------------------------------------
	  cmdId + optHeaderLength = controlHeader
	  cmdId: AppendEntriesCmdId, AppendEntriesResponseCmdId, RequestVoteCmdId, RequestVoteResponseCmdId, ExecProtoBufCmdId, ExecProtoBufResponseCmdId
	*/

	optBuf []byte
	/**
	  RpcMsg optional buffer format (optHeaderLength bytes)
	  -----------------------------------------------------------------
	  | Bytes|| 0 - 3           | 4 - 7     | 8 - (optHeaderLength-1) |
	  | Name || totalDataLength | nrEntries | optHeaderPayload        |
	  -----------------------------------------------------------------
	  optControlHeader: 0 - 7

	  Data is transmitted with sendfile (file) or write (byte array) and received with splice
	*/
}

func (d *RpcMsg) GetCmdId() uint8 {
	return d.buf[0]
}
func (d *RpcMsg) GetOptHeaderLength() uint16 {
	return binary.LittleEndian.Uint16(d.buf[1:3])
}
func (d *RpcMsg) GetCmdPayload() []byte {
	return d.buf[3:]
}
func (d *RpcMsg) SetCmdControlHeader(cmdId uint8, optHeaderLength uint16) {
	d.buf[0] = cmdId
	d.SetOptHeaderLength(optHeaderLength)
}
func (d *RpcMsg) SetOptHeaderLength(optHeaderLength uint16) {
	binary.LittleEndian.PutUint16(d.buf[1:3], optHeaderLength)
}
func (d *RpcMsg) GetOptControlHeader() (totalFileLength uint32, nrEntries uint32) {
	optHeaderLength := d.GetOptHeaderLength()
	if optHeaderLength >= 4 {
		totalFileLength = binary.LittleEndian.Uint32(d.optBuf[0:4])
	}
	if optHeaderLength >= 8 {
		nrEntries = binary.LittleEndian.Uint32(d.optBuf[4:8])
	}
	return
}
func (d *RpcMsg) GetOptHeaderPayload() []byte {
	optHeaderLength := d.GetOptHeaderLength()
	for int(optHeaderLength) > cap(d.optBuf) {
		d.optBuf = append(d.optBuf, make([]byte, 4096)...)
	}
	if optHeaderLength < uint16(RpcOptControlHeaderLength) {
		return nil
	}
	return d.optBuf[RpcOptControlHeaderLength:optHeaderLength]
}
func (d *RpcMsg) SetTotalFileLength(totalFileLength uint32) {
	binary.LittleEndian.PutUint32(d.optBuf[0:4], totalFileLength)
}
func (d *RpcMsg) SetNrEntries(nrEntries uint32) {
	binary.LittleEndian.PutUint32(d.optBuf[4:8], nrEntries)
}
func (d *RpcMsg) CreateOptControlHeader(totalFileLength uint32, nrEntries uint32, entryPayloadLength uint16) {
	buf := make([]byte, entryPayloadLength)
	binary.LittleEndian.PutUint32(buf[0:4], totalFileLength)
	binary.LittleEndian.PutUint32(buf[4:8], nrEntries)
	d.optBuf = buf
}
func (d *RpcMsg) FillRequestVoteArgs(term uint32, candidateId uint32, lastLogTerm uint32, lastLogIndex uint64) {
	d.SetCmdControlHeader(RequestVoteCmdId, 0)
	/**
	CmdPayload for RequestVote (cf. RequestVote RPC Arguments in Raft paper):
	------------------------------------------------------------
	| Bytes || 0 - 3 | 4 - 7       | 8 - 11      | 12 - 20      |
	| Name || term  | candidateId | lastLogTerm | lastLogIndex |
	------------------------------------------------------------
	*/
	payload := d.GetCmdPayload()
	binary.LittleEndian.PutUint32(payload[0:4], term)
	binary.LittleEndian.PutUint32(payload[4:8], candidateId)
	binary.LittleEndian.PutUint32(payload[8:12], lastLogTerm)
	binary.LittleEndian.PutUint64(payload[12:RequestVotePayloadSize], lastLogIndex)
}
func (d *RpcMsg) GetRequestVoteArgs() (term uint32, candidateId uint32, lastLogTerm uint32, lastLogIndex uint64) {
	payload := d.GetCmdPayload()
	term = binary.LittleEndian.Uint32(payload[0:4])
	candidateId = binary.LittleEndian.Uint32(payload[4:8])
	lastLogTerm = binary.LittleEndian.Uint32(payload[8:12])
	lastLogIndex = binary.LittleEndian.Uint64(payload[12:RequestVotePayloadSize])
	return
}
func (d *RpcMsg) FillRequestVoteResponseArgs(term uint32, voteGranted bool, reply int32) {
	d.SetCmdControlHeader(RequestVoteResponseCmdId, 0)
	/**
	CmdPayload for RequestVoteResponse (cf. RequestVote RPC Results in Raft paper):
	---------------------------------------
	| Bytes || 0 - 3 | 4 - 4       | 5 - 8 |
	| Name || term  | voteGranted | error |
	---------------------------------------
	Error bytes are used to inform critical errors such as failures during file writes
	*/
	payload := d.GetCmdPayload()
	binary.LittleEndian.PutUint32(payload[0:4], term)
	if voteGranted {
		payload[4] = 1
	} else {
		payload[4] = 0
	}
	binary.LittleEndian.PutUint32(payload[5:RequestVoteResponsePayloadSize], uint32(reply))
}
func (d *RpcMsg) GetRequestVoteResponseArgs() (term uint32, voteGranted bool, reply int32) {
	payload := d.GetCmdPayload()
	term = binary.LittleEndian.Uint32(payload[0:4])
	voteGranted = payload[4] == 1
	reply = int32(binary.LittleEndian.Uint32(payload[5:RequestVoteResponsePayloadSize]))
	return
}
func (d *RpcMsg) FillExecProtoBufArgs(execId uint16, seqNum uint64, m proto.Message, dataBufLen int, isResponse bool) int32 {
	buf, err := proto.Marshal(m)
	if err != nil {
		log.Errorf("Failed: FillExecProtoBufArgs, Marshal, err=%v", err)
		return ErrnoToReply(err)
	}
	if len(buf)+8 > math.MaxUint16 {
		log.Errorf("Failed: FillExecProtoBufArgs, message size (%v) is larger than sizeof(uint16)=%v", len(buf), math.MaxUint16)
		return ErrnoToReply(unix.EOVERFLOW)
	}
	entryPayloadLength := uint16(RpcOptControlHeaderLength) + uint16(len(buf))
	if isResponse {
		d.SetCmdControlHeader(ExecProtoBufResponseCmdId, entryPayloadLength)
	} else {
		d.SetCmdControlHeader(ExecProtoBufCmdId, entryPayloadLength)
	}
	/**
	CmdPayload for Extended RPC:
	---------------------------
	| Bytes || 0 - 1  | 2 - 9  |
	| Name || execId | seqNum |
	---------------------------

	OptHeaderPayload for Extended RPC:
	---------------------------------------------
	| Bytes || 0 - optHeaderLength               |
	| Name || ProtoBufPayload (protocol buffer) |
	---------------------------------------------
	*/
	payload := d.GetCmdPayload()
	binary.LittleEndian.PutUint16(payload[0:2], execId)
	binary.LittleEndian.PutUint64(payload[2:ExecProtoBufPayloadSize], seqNum)
	if d.optBuf == nil || len(d.optBuf) < int(entryPayloadLength) {
		d.CreateOptControlHeader(uint32(dataBufLen), 1, entryPayloadLength)
	} else {
		d.SetTotalFileLength(uint32(dataBufLen))
		d.SetNrEntries(1)
	}
	copy(d.GetOptHeaderPayload(), buf) // TODO: can we eliminate copy?
	return RaftReplyOk
}
func (d *RpcMsg) GetExecProtoBufRpcId(payload []byte) (rpcId uint16) {
	rpcId = binary.LittleEndian.Uint16(payload[0:2])
	return
}
func (d *RpcMsg) GetExecProtoBufRpcSeqNum(payload []byte) (seqNum uint64) {
	seqNum = binary.LittleEndian.Uint64(payload[2:ExecProtoBufPayloadSize])
	return
}
func (d *RpcMsg) ParseExecProtoBufMessage(m proto.Message) (reply int32) {
	payload := d.GetOptHeaderPayload()
	if err := proto.Unmarshal(payload, m); err != nil {
		log.Errorf("Failed: ParseExecProtoBufMessage, Unmarshal, err=%v", err)
		return ErrnoToReply(err)
	}
	return RaftReplyOk
}
func (d *RpcMsg) FillAppendEntriesResponseArgs(term uint32, success bool, logLength uint64, reply int32) {
	d.SetCmdControlHeader(AppendEntriesResponseCmdId, 0)
	/**
	CmdPayload for AppendEntriesResponse (cf. AppendEntries Results in Raft paper):
	------------------------------------------------
	| Bytes | 0 - 3 | 4 - 4   | 5 - 12    | 13 - 17 |
	| Name | term  | success | logLength | error   |
	------------------------------------------------
	*/
	payload := d.GetCmdPayload()
	binary.LittleEndian.PutUint32(payload[0:4], term)
	if success {
		payload[4] = 1
	} else {
		payload[4] = 0
	}
	binary.LittleEndian.PutUint64(payload[5:13], logLength)
	binary.LittleEndian.PutUint32(payload[13:AppendEntriesResponsePayloadSize], uint32(reply))
}
func (d *RpcMsg) GetAppendEntriesResponseArgs() (term uint32, success bool, logLength uint64, reply int32) {
	payload := d.GetCmdPayload()
	term = binary.LittleEndian.Uint32(payload[0:4])
	success = payload[4] == 1
	logLength = binary.LittleEndian.Uint64(payload[5:13])
	reply = int32(binary.LittleEndian.Uint32(payload[13:AppendEntriesResponsePayloadSize]))
	return
}
func (d *RpcMsg) FillAppendEntryArgs(term uint32, prevTerm uint32, prevIndex uint64, leaderCommit uint64, leaderId uint32) {
	d.SetCmdControlHeader(AppendEntriesCmdId, 0)
	/**
	CmdPayload for AppendEntry (cf. AppendEntries Results in Raft paper):
	-----------------------------------------------------------------
	| Bytes | 0 - 3 | 4 - 7    | 8 - 15    | 16 - 23      | 24 - 27  |
	| Name | term  | prevTerm | prevIndex | leaderCommit | leaderId |
	-----------------------------------------------------------------

	OptHeaderPayload for AppendEntry
	---------------------------------------------
	| Bytes | 0 - x    | x+1 - y  | y+1 - z  | ...
	| Name | entry #1 | entry #2 | entry #3 | ...
	---------------------------------------------
	Each entry contains its entryLength. see the comment in __appendExtPayloadArgs
	*/
	payload := d.GetCmdPayload()
	binary.LittleEndian.PutUint32(payload[0:4], term)
	binary.LittleEndian.PutUint32(payload[4:8], prevTerm)
	binary.LittleEndian.PutUint64(payload[8:16], prevIndex)
	binary.LittleEndian.PutUint64(payload[16:24], leaderCommit)
	binary.LittleEndian.PutUint32(payload[24:AppendEntryPayloadSize], leaderId)
}
func (d *RpcMsg) GetAppendEntryArgs() (term uint32, prevTerm uint32, prevIndex uint64, leaderCommit uint64, leaderId uint32) {
	payload := d.GetCmdPayload()
	term = binary.LittleEndian.Uint32(payload[0:4])
	prevTerm = binary.LittleEndian.Uint32(payload[4:8])
	prevIndex = binary.LittleEndian.Uint64(payload[8:16])
	leaderCommit = binary.LittleEndian.Uint64(payload[16:24])
	leaderId = binary.LittleEndian.Uint32(payload[24:AppendEntryPayloadSize])
	return
}
func (d *RpcMsg) GetAppendEntryNrEntries() (nrEntries uint32) {
	_, nrEntries = d.GetOptControlHeader()
	return
}
func (d *RpcMsg) GetAppendEntryExtHeader(off uint16) (extCmdId uint16, extEntryPayload []byte, nextOff uint16) {
	d.GetOptHeaderPayload()
	/**
	ExtPayload (identical to AppendEntryCommand format):
	---------------------------------------------------------------------------
	| Bytes | 0 - 15   | 16 - 16     | 17 - 20 | 21 - 22    | 23 - entryLength |
	| Name | checksum | entryLength | term    | extCmdId   | extEntryPayload  |
	---------------------------------------------------------------------------
	*/
	if off == 0 {
		off = uint16(RpcOptControlHeaderLength)
	}
	entryLength := d.optBuf[off+crc32.Size]
	extCmdId = binary.LittleEndian.Uint16(d.optBuf[off+crc32.Size+5 : off+uint16(AppendEntryCommandLogBaseSize)])
	extEntryPayload = d.optBuf[off+uint16(AppendEntryCommandLogBaseSize) : off+uint16(entryLength)]
	nextOff = off + uint16(entryLength)
	return
}
func GetAppendEntryFileArgs(extEntryPayload []byte) (fileId FileIdType, fileLength int32, fileOffset int64) {
	fileId = NewFileIdTypeFromBuf(extEntryPayload[0:16])
	fileLength = int32(binary.LittleEndian.Uint32(extEntryPayload[16:20]))
	fileOffset = int64(binary.LittleEndian.Uint64(extEntryPayload[20:28]))
	return
}
func (d *RpcMsg) GetAppendEntryCommandDiskFormat(off uint16) (cmd AppendEntryCommand, nextOff uint16) {
	entryLength := d.optBuf[off+crc32.Size]
	extEntryPayload := d.optBuf[off : off+uint16(entryLength)]
	nextOff = off + uint16(entryLength)
	copy(cmd.buf[:], extEntryPayload)
	return
}

func (d *RpcMsg) GetArrivingMsgLengths(off uint16, optHeaderLength *uint16) (complete bool) {
	if off < uint16(RpcControlHeaderLength) {
		return false
	}
	*optHeaderLength = d.GetOptHeaderLength()
	return true
}

/************************************************************
 ************************************************************/

type EpollReader struct {
	h        *EpollHandler
	sa       map[int]common.NodeAddrInet4
	saLock   *sync.RWMutex
	nrThread int32
}

func NewEpollReader() (*EpollReader, error) {
	h, err := NewEpollHandler()
	if err != nil {
		log.Errorf("Failed: NewEpollReader, NewEpollReader, err=%v", err)
		return nil, err
	}
	return &EpollReader{h: h, sa: make(map[int]common.NodeAddrInet4), saLock: new(sync.RWMutex)}, nil
}

func (r *EpollReader) AddFd(fd int, sa common.NodeAddrInet4) (err error) {
	r.saLock.Lock()
	if old, ok := r.sa[fd]; ok {
		log.Warnf("EpollReader.AddFd, fd is already added but overwritten with new sa, fd=%v, sa=%v->%v", fd, old, sa)
	}
	if err = r.h.AddFd(fd, unix.EPOLLIN); err != nil {
		r.saLock.Unlock()
		log.Errorf("Failed: EpollReader.AddFd, AddFd, fd=%v, err=%v", fd, err)
		return
	}
	r.sa[fd] = sa
	r.saLock.Unlock()
	return err
}

func (r *EpollReader) RemoveFd(fd int) (err error) {
	r.saLock.Lock()
	sa, ok := r.sa[fd]
	if ok {
		if err = r.h.RemoveFd(fd); err != nil {
			log.Errorf("Failed: EpollReader.RemoveFd, RemoveFd, fd=%v, err=%v", fd, err)
		} else {
			delete(r.sa, fd)
			log.Debugf("Success: EpollHandler.RemoveFd, fd=%v, sa=%v", fd, sa)
		}
	} else {
		log.Warnf("Failed: EpollReader.RemoveFd, fd is not addded, fd=%v", fd)
		err = unix.ENOENT
	}
	r.saLock.Unlock()
	return
}

func (r *EpollReader) RaftRpcThread(maxEvents int, n *NodeServer, pipeFds [2]int, raft *RaftInstance) {
	atomic.AddInt32(&r.nrThread, 1)
	var err error
	events := make([]unix.EpollEvent, maxEvents)

	msgs := make(map[int]*RpcMsg)
	states := make(map[int]*ReadRpcMsgState)
	var nrEvents int
	for atomic.LoadInt32(&raft.stopped) == 0 {
		var readTime time.Duration
		var spliceTime time.Duration
		var topHalfTime time.Duration
		var bottomHalfTime time.Duration
		var hadEntry = false
		nrEvents, err = unix.EpollWait(r.h.epollFd, events, int(raft.hbInterval.Milliseconds()))
		readBegin := time.Now()
		if err == unix.EINTR {
			continue
		} else if err == unix.EBADFD || err == unix.EFAULT || err == unix.EINVAL { // EBADF, EFAULT, EINVAL. all of them should close the connection
			log.Errorf("Failed: EpollReader.RaftRpcThread, EpollWait, err=%v", err)
			break
		}
		for i := 0; i < nrEvents; i++ {
			ev := events[i]
			fd := int(ev.Fd)
			r.saLock.RLock()
			sa, ok := r.sa[fd]
			r.saLock.RUnlock()
			if !ok {
				log.Errorf("BUG (ignore): EpollReader.RaftRpcThread, EpollWait returned fd that is not listed in given fdMsgs, epollFd=%v, fd=%d", r.h.epollFd, fd)
				continue
			}
			msg, ok := msgs[fd]
			if !ok {
				msg = &RpcMsg{}
				msgs[fd] = msg
			}
			state, ok := states[fd]
			if !ok {
				state = &ReadRpcMsgState{optOffForSplice: uint16(RpcOptControlHeaderLength)}
				states[fd] = state
			}
			var cmdId uint8
			if ev.Events&unix.EPOLLERR != 0 || ev.Events&unix.EPOLLHUP != 0 || ev.Events&unix.EPOLLOUT != 0 {
				// a peer closed the socket (EPOLLHUP), something bad happened (EPOLLERR), or unexpected events are reported. delete it from the epoll list
				log.Errorf("Failed: EpollReader.RaftRpcThread, EpollWait returned events=%v", ev.Events)
				goto abort
			}
			ReadRpcMsg(fd, msg, state)
			if state.failed != nil {
				goto abort
			}
			if state.optComplete > 0 {
				cmdId = msg.GetCmdId()
				switch cmdId {
				case RequestVoteCmdId:
					e := raft.replyClient.Register(fd, sa)
					if e != nil {
						log.Errorf("Failed (ignore): EpollReader.RaftRpcThread, Register, fd=%v, sa=%v, err=%v", fd, sa, err)
						goto abort
					}
					n.raft.RequestVoteRpc(*msg, sa, fd)
					goto waitNext
				case AppendEntriesCmdId:
					if state.optComplete == 1 {
						topHalfBegin := time.Now()
						readTime += topHalfBegin.Sub(readBegin)
						success, cancel := raft.AppendEntriesRpcTopHalf(*msg, sa, fd)
						topHalfTime += time.Since(topHalfBegin)
						if success {
							state.abortFile = false
						} else if cancel {
							goto abort
						} else {
							state.abortFile = true
						}
					}
					spliceBegin := time.Now()
					ReadDataToRaftLog(fd, msg, state, raft.files, pipeFds)
					bottomHalfBegin := time.Now()
					spliceTime += bottomHalfBegin.Sub(spliceBegin)
					if state.failed != nil {
						goto abort
					}
					if state.completed {
						if !state.abortFile {
							e := raft.replyClient.Register(fd, sa)
							if e != nil {
								log.Errorf("Failed (ignore): EpollReader.RaftRpcThread, Register, fd=%v, sa=%v, err=%v", fd, sa, err)
								goto abort
							}
							hadEntry = raft.AppendEntriesRpcBottomHalf(*msg, sa, fd)
							bottomHalfTime += time.Since(bottomHalfBegin)
						}
						goto waitNext
					}
				case ExecProtoBufCmdId:
					e := n.replyClient.Register(fd, sa)
					if e != nil {
						log.Errorf("Failed (ignore): EpollReader.RaftRpcThread, Register, fd=%v, sa=%v, err=%v", fd, sa, err)
						goto abort
					}
					if noData := n.ExecDataRpc(*msg, sa, fd, pipeFds, state); noData {
						go n.ExecRpcThread(*msg, sa, fd)
						state.completed = true
					}
					if state.completed {
						goto waitNext
					}
				default:
					log.Errorf("Failed (ignore): EpollReader.RaftRpcThread, unknown cmdId, cmdId=%v, sa=%v", cmdId, sa)
					goto abort
				}
			}
			continue
		abort:
			_ = r.RemoveFd(fd) // drop all remaining buffers. accept() will add a recovered connection
			delete(msgs, fd)
			delete(states, fd)
			continue
		waitNext:
			msgs[fd] = &RpcMsg{}
			states[fd] = &ReadRpcMsgState{optOffForSplice: uint16(RpcOptControlHeaderLength)}
			if hadEntry {
				log.Debugf("EpollReader.RaftRpcThread, readTime=%v, topHalfTime=%v, spliceTime=%v, bottomHalfTime=%v",
					readTime, topHalfTime, spliceTime, bottomHalfTime)
				hadEntry = false
			}
			continue
		}
	}
	if err2 := unix.Close(pipeFds[1]); err2 != nil {
		log.Warnf("Failed (ignore): EpollReader.RaftRpcThread, Close (0), pipeFds[1]=%v, err=%v", pipeFds[1], err2)
	}
	if err2 := unix.Close(pipeFds[0]); err2 != nil {
		log.Warnf("Failed (ignore): EpollReader.RaftRpcThread, Close (1), pipeFds[0]=%v, err=%v", pipeFds[0], err2)
	}
	atomic.AddInt32(&r.nrThread, -1)
}

func (r *EpollReader) Close() error {
	var i = 0
	for atomic.LoadInt32(&r.nrThread) > 0 {
		time.Sleep(time.Millisecond)
		i += 1
		if i > 10000 {
			log.Errorf("Failed: EpollReader.Close, RaftRpcThread does not stop > 10 sec")
			break
		}
	}
	err := r.h.Close()
	if err != nil {
		log.Errorf("Failed: EpollReader.Close, EpollHandler.Close, err=%v", err)
	} else {
		r.h = nil
	}
	r.saLock.Lock()
	r.sa = make(map[int]common.NodeAddrInet4)
	r.saLock.Unlock()
	return err
}

func (r *EpollReader) CheckReset() (ok bool) {
	if ok = r.h == nil; !ok {
		log.Errorf("Failed: EpollReader.CheckReset, r.h != nil")
	}
	if ok = r.saLock.TryLock(); !ok {
		log.Errorf("Failed: EpollReader.CheckReset, r.lock is taken")
		return
	}
	if ok2 := len(r.sa) == 0; !ok2 {
		log.Errorf("Failed: EpollReader.CheckReset, len(r.sa) != 0")
		ok = false
	}
	r.saLock.Unlock()
	if ok2 := atomic.LoadInt32(&r.nrThread) == 0; !ok2 {
		log.Errorf("Failed: EpollReader.CheckReset, nrThread > 0")
		ok = false
	}
	return
}

type RpcThreads struct {
	reader    *EpollReader
	fd        int
	nrThreads int32
}

func NewRpcThreads(sa common.NodeAddrInet4, boundDev string) (ret RpcThreads, err error) {
	ret.fd, err = unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Errorf("Failed: NewRpcThreads, Socket, sa=%v, err=%v", sa, err)
		return
	}
	if boundDev != "" {
		if err = unix.SetsockoptString(ret.fd, unix.SOL_SOCKET, unix.SO_BINDTODEVICE, boundDev); err != nil {
			log.Errorf("Failed: NewRpcThreads, SetsockoptString, fd=%v, boundDev=%v, err=%v", ret.fd, boundDev, err)
			return
		}
	}
	if err = unix.SetsockoptInt(ret.fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		log.Errorf("Failed: NewRpcThreads, SetsockoptInt(fd, SOL_SOCKET, SO_REUSEADDR, 1), fd=%v, err=%v", ret.fd, err)
		return
	}
	if err = unix.SetsockoptInt(ret.fd, unix.SOL_TCP, unix.TCP_NODELAY, 1); err != nil {
		log.Errorf("Failed: NewRpcThreads, SetsockoptInt(fd, SOL_TCP, TCP_NODELAY, 1), fd=%v, err=%v", ret.fd, err)
		return
	}
	if err = unix.Bind(ret.fd, &unix.SockaddrInet4{Addr: sa.Addr, Port: sa.Port}); err != nil {
		log.Errorf("Failed: NewRpcThreads, Bind, sa=%v, err=%v", sa, err)
		return
	}
	if err = unix.Listen(ret.fd, 1024); err != nil {
		log.Errorf("Failed: NewRpcThreads, Listen, sa=%v, err=%v", sa, err)
		return
	}
	if err = unix.SetNonblock(ret.fd, true); err != nil {
		log.Warnf("Failed: NewRpcThreads, SetNonblock, fd=%v, err=%v", ret.fd, err)
		return
	}
	if ret.reader, err = NewEpollReader(); err != nil {
		log.Errorf("Failed: NewRpcThreads, NewEpollReader, err=%v", err)
		if err2 := unix.Close(ret.fd); err2 != nil {
			log.Warnf("Failed (ignore): NewRpcThreads, Close (2), err=%v", err2)
		}
		return
	}
	log.Debugf("Success: NewRpcThreads, sa=%v", sa)
	return
}

func (f *RpcThreads) Start(maxEvents int, n *NodeServer, raft *RaftInstance) error {
	signal.Ignore(unix.SIGPIPE)
	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		log.Errorf("Failed: RpcThreads.Start, EpollCreate1, err=%v", err)
		return err
	}
	// level-triggered
	if err = unix.EpollCtl(epollFd, unix.EPOLL_CTL_ADD, f.fd, &unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(f.fd)}); err != nil {
		log.Errorf("Failed: RpcThreads.Start, EpollCtl, err=%v", err)
		return err
	}
	pipeFds := [2]int{}
	if err = unix.Pipe(pipeFds[:]); err != nil {
		log.Errorf("Failed: RpcThreads.Start, Pipe, err=%v", err)
		return err
	}
	go f.AcceptThread(epollFd, time.Duration(raft.electTimeout), &raft.stopped)
	go f.reader.RaftRpcThread(maxEvents, n, pipeFds, raft)
	return nil
}

func (f *RpcThreads) AcceptThread(epollFd int, acceptTimeout time.Duration, stopFlag *int32) {
	atomic.AddInt32(&f.nrThreads, 1)
	events := [1]unix.EpollEvent{}
	for atomic.LoadInt32(stopFlag) == 0 {
		nrEvents, err := unix.EpollWait(epollFd, events[:], int(acceptTimeout.Milliseconds()))
		if err != nil && err == unix.EINTR {
			continue
		} else if err == unix.EBADFD || err == unix.EFAULT || err == unix.EINVAL { // EBADF, EFAULT, EINVAL. all of them should close the connection
			log.Errorf("Failed: RpcThreads.Accept, EpollWait, err=%v", err)
			break
		}
		for i := 0; i < nrEvents; i++ {
			ev := events[i]
			if ev.Events&unix.EPOLLERR != 0 || ev.Events&unix.EPOLLHUP != 0 || ev.Events&unix.EPOLLOUT != 0 {
				// a peer closed the socket (EPOLLHUP), something bad happened (EPOLLERR), or unexpected events are reported. delete it from the epoll list
				log.Errorf("Failed: RpcThreads.Accept, EpollWait returned events=%v", ev.Events)
				err = unix.EIO
				break
			}
			var fd int
			var sa unix.Sockaddr
			fd, sa, err = unix.Accept(int(ev.Fd))
			if err == unix.EAGAIN {
				continue
			} else if err != nil {
				log.Errorf("Failed (ignore): RpcThreads.Accept, Accept, fd=%v, err=%v", ev.Fd, err)
				continue
			}
			saInet4, ok := sa.(*unix.SockaddrInet4)
			if ok {
				if err = f.reader.AddFd(fd, common.NodeAddrInet4{Addr: saInet4.Addr, Port: saInet4.Port}); err != nil {
					log.Errorf("Failed: RpcThreads.Accept, AddFd, fd=%v, saInet4=%v, err=%v", fd, *saInet4, err)
					ok = false
				} else {
					log.Debugf("Success: RpcThreads.Accept, fd=%v, saInet4=%v", fd, *saInet4)
				}
			} else {
				log.Errorf("Failed: RpcThreads.Accept, cannot convert sa to saInet4, sa=%v", sa)
			}
			if !ok {
				if err2 := unix.Close(fd); err2 != nil {
					log.Errorf("Failed (ignore): RpcThreads.Accept, Close, fd=%v, err=%v", fd, err2)
				}
			}
		}
	}
	if err2 := unix.Close(epollFd); err2 != nil {
		log.Warnf("Failed: RpcThreads.Accept, Close (0), sendFd=%v, err=%v", epollFd, err2)
	}
	atomic.AddInt32(&f.nrThreads, -1)
}

func (f *RpcThreads) Close() {
	var i = 0
	for atomic.LoadInt32(&f.nrThreads) > 0 {
		time.Sleep(time.Millisecond)
		i += 1
		if i > 10000 {
			log.Errorf("Failed: RpcThreads.Close, Accepter thread does not stop > 10 sec")
			break
		}
	}
	if f.fd > 0 {
		if err2 := unix.Close(f.fd); err2 != nil {
			log.Warnf("Failed: RpcThreads.Close, Close(fd), fd=%v, err=%v", f.fd, err2)
		} else {
			f.fd = -1
		}
	}
	if f.reader != nil {
		if err2 := f.reader.Close(); err2 != nil {
			log.Warnf("Failed: RpcThread.Close, reader.Close(), err=%v", err2)
		} else {
			f.reader = nil
		}
	}
}

func (f *RpcThreads) CheckReset() (ok bool) {
	if ok = f.fd == -1; !ok {
		log.Errorf("Failed: RpcThreads.CheckReset, f.fd != -1")
	}
	if ok2 := f.reader == nil; !ok2 {
		log.Errorf("Failed: RpcThreads.CheckReset, f.reader != nil")
		ok = false
	}
	if ok2 := atomic.LoadInt32(&f.nrThreads) == 0; !ok2 {
		log.Errorf("Failed: RpcThreads.CheckReset, nrThreads != 0")
		ok = false
	}
	return
}
