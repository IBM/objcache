/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"golang.org/x/sys/unix"
)

const (
	OnDiskStateReset = uint32(math.MaxUint32)
)

type OnDiskState struct {
	fileName string
	fd       int
	offset   int64
	value    uint32
	lock     *sync.RWMutex
}

func NewOnDiskState(rootDir string, selfId uint32, stateName string) (*OnDiskState, int32) {
	fileName := fmt.Sprintf("%s/%d-%s.log", rootDir, selfId, stateName)
	ret := &OnDiskState{lock: new(sync.RWMutex), value: OnDiskStateReset, fileName: fileName}
	st := unix.Stat_t{}
	errno := unix.Stat(fileName, &st)
	if errno == nil {
		var fd int
		fd, errno = unix.Open(fileName, unix.O_RDWR, 0644)
		if errno != nil {
			log.Errorf("Failed: NewOnDiskState, Open (0), fileName=%v, errno=%v", fileName, errno)
			return nil, ErrnoToReply(errno)
		}
		var restore = false
		if st.Size >= int64(SizeOfUint32) {
			buf := [SizeOfUint32]byte{}
			if _, errno = unix.Pread(fd, buf[:], st.Size-int64(SizeOfUint32)); errno != nil {
				log.Errorf("Failed: NewOnDiskState, Pread, fileName=%v, at=%v, err=%v", fileName, st.Size-int64(SizeOfUint32), errno)
				return nil, ErrnoToReply(errno)
			}
			restore = true
			ret.value = binary.LittleEndian.Uint32(buf[:])
		}
		ret.fd = fd
		if restore {
			log.Debugf("Restore: NewOnDiskState, h=%v, selfId=%v, value=%v", stateName, selfId, ret.value)
		}
	} else if errno != unix.ENOENT {
		log.Errorf("Failed: NewOnDiskState, Stat, fileName=%v, errno=%v", fileName, errno)
		return nil, ErrnoToReply(errno)
	} else {
		fd, err := unix.Open(fileName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0644)
		if err != nil {
			log.Errorf("Failed: NewOnDiskState, Open, fileName=%v, err=%v", fileName, err)
			return nil, ErrnoToReply(errno)
		}
		ret.fd = fd
	}
	return ret, RaftReplyOk
}

func (s *OnDiskState) __setNoLock(value uint32) int32 {
	if value == s.value {
		return RaftReplyOk
	}
	buf := [SizeOfUint32]byte{}
	binary.LittleEndian.PutUint32(buf[:], value)
	var bufOff = 0
	for bufOff < len(buf) {
		c, errno := unix.Pwrite(s.fd, buf[bufOff:], s.offset+int64(bufOff))
		if errno != nil {
			log.Errorf("Failed: OnDiskState.__setNoLock, Pwrite, fileName=%v, offset=%v, bufOff=%v, errno=%v", s.fileName, s.offset, bufOff, errno)
			return ErrnoToReply(errno)
		}
		bufOff += c
	}
	s.offset += int64(bufOff)
	log.Debugf("OnDiskState, %v: %v -> %v", s.fileName, s.value, value)
	s.value = value
	return RaftReplyOk
}

func (s *OnDiskState) Set(value uint32) int32 {
	s.lock.Lock()
	reply := s.__setNoLock(value)
	s.lock.Unlock()
	return reply
}

func (s *OnDiskState) Increment() int32 {
	s.lock.Lock()
	s.__setNoLock(s.value + 1)
	s.lock.Unlock()
	return RaftReplyOk
}

func (s *OnDiskState) Reset() int32 {
	return s.Set(OnDiskStateReset)
}

func (s *OnDiskState) Get() uint32 {
	s.lock.RLock()
	v := s.value
	s.lock.RUnlock()
	return v
}

func (s *OnDiskState) Clean() {
	s.lock.Lock()
	if s.fd > 0 {
		if err := unix.Close(s.fd); err != nil {
			log.Errorf("Failed: OnDiskState.Clean, Close, fileName=%v, fd=%v, err=%v", s.fileName, s.fd, err)
		} else {
			s.fd = -1
		}
	}
	if s.fileName != "" {
		if err := unix.Unlink(s.fileName); err != nil {
			log.Errorf("Failed: OnDiskState, Unlink, fileName=%v, err=%v", s.fileName, err)
		} else {
			s.fileName = ""
		}
	}
	s.lock.Unlock()
}

func (s *OnDiskState) CheckReset() (ok bool) {
	if ok = s.lock.TryLock(); !ok {
		log.Errorf("Failed: OnDiskState.CheckReset, s.lock is taken")
		return
	}
	if ok2 := s.fd == -1; !ok2 {
		log.Errorf("Failed: OnDiskState.CheckReset, s.fd != -1")
		ok = false
	}
	if ok2 := s.fileName == ""; !ok2 {
		log.Errorf("Failed: OnDiskState.CheckReset, s.fileName != \"\"")
		ok = false
	}
	s.lock.Unlock()
	return
}

const (
	DiskWriteBegin   int32 = -1
	DiskWritePrepare int32 = 0
)

type DiskWriteVector struct {
	buf       []byte
	logOffset int64
	size      int64

	// for Splice
	srcFd int

	state  int32 // DiskWriteBegin/DiskWritePrepare/ErrnoToReply(err)
	isHead bool  // true: fsync, commit, and abort
}

func (b *DiskWriteVector) Less(i btree.Item) bool {
	c := i.(*DiskWriteVector)
	return b.logOffset < c.logOffset
}

type OnDiskLog struct {
	fd       int
	filePath string
	size     int64
	caches   *btree.BTree
	writers  *btree.BTree
	cond     *sync.Cond
	lock     *sync.RWMutex
	syncing  int

	pages map[int64]*PageBuffer // key: logOffset

	maxNrCache int
	maxWriters int
}

func NewOnDiskLog(filePath string, maxNrCache int, maxWriters int) *OnDiskLog {
	if maxNrCache < 0 {
		log.Warnf("Failed: NewOnDiskLog, maxNrCache must be >= 1. use maxNrCache=0.")
		maxNrCache = 0
	}
	if maxWriters <= 0 {
		log.Errorf("Failed: NewOnDiskLog, maxWriters must be >= 1. use maxWriters=1")
		maxWriters = 1
	}
	lock := new(sync.RWMutex)
	ret := &OnDiskLog{
		fd: -1, filePath: filePath, caches: btree.New(3), writers: btree.New(3), cond: sync.NewCond(lock), lock: lock, syncing: 0,
		maxNrCache: maxNrCache, maxWriters: maxWriters, size: 0, pages: make(map[int64]*PageBuffer),
	}
	return ret
}

func (d *OnDiskLog) InitSize(fileSize int64) {
	d.size = fileSize
}

func (d *OnDiskLog) __open() error {
	if d.fd <= 0 {
		fd, err := unix.Open(d.filePath, unix.O_WRONLY|unix.O_CREAT, 0644)
		if err != nil {
			log.Errorf("Failed: OnDiskLog.__open, Open, filePath=%v, err=%v", d.filePath, err)
			return err
		}
		d.fd = fd
	}
	return nil
}

func (d *OnDiskLog) WaitWrites() (size int64) {
	// wait commits
	d.lock.Lock()
	for d.writers.Len() > 0 || d.syncing != 0 {
		d.cond.Wait()
	}
	size = d.size
	d.lock.Unlock()
	return
}

func (d *OnDiskLog) Clear() (size int64) {
	d.lock.Lock()
	for d.writers.Len() > 0 || d.syncing != 0 {
		d.cond.Wait()
	}
	if d.fd <= 0 {
		d.lock.Unlock()
		return 0
	}
	if err := unix.Unlink(d.filePath); err != nil {
		log.Errorf("Failed (ignore): OnDiskLog.Clear, Unlink, filePath=%v, err=%v", d.filePath, err)
	}
	if err := unix.Close(d.fd); err != nil {
		log.Errorf("Failed (ignore): OnDiskLog.Clear, Close, fd=%v, err=%v", d.fd, err)
	}
	d.fd = -1
	size = d.size
	d.size = 0
	d.writers.Clear(true)
	d.caches.Clear(true)
	d.pages = nil
	d.lock.Unlock()
	return
}

func (d *OnDiskLog) CheckReset() (ok bool) {
	if ok = d.lock.TryLock(); !ok {
		log.Errorf("Failed: OnDiskLog.CheckReset, d.lock is taken")
		return
	}
	if ok2 := d.writers.Len() == 0; !ok2 {
		log.Errorf("Failed: OnDiskLog.CheckReset, d.writers.Len() != 0")
		ok = false
	}
	if ok2 := d.caches.Len() == 0; !ok2 {
		log.Errorf("Failed: OnDiskLog.CheckReset, d.caches.Len() != 0")
		ok = false
	}
	if ok2 := d.syncing == 0; !ok2 {
		log.Errorf("Failed: OnDiskLog.CheckReset, d.syncing != 0")
		ok = false
	}
	if ok2 := d.fd == -1; !ok2 {
		log.Errorf("Failed: OnDiskLog.CheckReset, d.fd != -1")
		ok = false
	}
	if ok2 := d.filePath == ""; !ok2 {
		log.Errorf("Failed: OnDiskLog.CheckReset, d.filePath != \"\"")
		ok = false
	}
	if ok2 := len(d.pages) == 0; !ok2 {
		log.Errorf("Failed: OnDiskLog.CheckReset, len(d.pages) != 0")
		ok = false
	}
	d.lock.Unlock()
	return
}

func (d *OnDiskLog) Shrink(newSize int64) (oldSize int64, err error) {
	d.lock.Lock()
	if err = d.__open(); err != nil {
		log.Errorf("Failed: OnDiskLog.Shrink, __open, err=%v", err)
		d.lock.Unlock()
		return
	}
	for d.writers.Len() > 0 {
		d.cond.Wait()
	}
	size := d.size
	if size == newSize {
		d.lock.Unlock()
		return size, nil
	} else if size < newSize {
		log.Warnf("Failed: OnDiskLog.Shrink, no supports to expand larger sizes, size=%v, newSize=%v", size, newSize)
		d.lock.Unlock()
		return size, unix.ENOTSUP
	}
	if err = unix.Ftruncate(d.fd, newSize); err != nil {
		log.Errorf("Failed: OnDiskLog.Shrink, Ftruncate, filePath=%v, size=%v->%v, err=%v", d.filePath, size, newSize, err)
		d.lock.Unlock()
		return size, err
	}
	target := &DiskWriteVector{logOffset: newSize}
	d.caches.DescendLessOrEqual(target, func(i btree.Item) bool {
		bv := i.(*DiskWriteVector)
		if bv.logOffset <= newSize && newSize < bv.logOffset+bv.size {
			d.caches.Delete(i)
		}
		return false
	})
	for {
		var found = false
		d.caches.AscendGreaterOrEqual(target, func(i btree.Item) bool {
			d.caches.Delete(i)
			found = true
			return false
		})
		if !found {
			break
		}
	}
	d.size = newSize
	d.lock.Unlock()
	return size, err
}

func (d *OnDiskLog) ReserveAppendWrite(size int64) (logOffset int64) {
	d.lock.Lock()
	for d.writers.Len() > 0 {
		d.cond.Wait()
	}
	logOffset = d.size
	d.size = logOffset + size
	d.lock.Unlock()
	return
}

func (d *OnDiskLog) __beginRandomWrite(buf []byte, srcFd int, logOffset int64, size int64) (*DiskWriteVector, error) {
	d.lock.Lock()
	if err := d.__open(); err != nil {
		log.Errorf("Failed: OnDiskLog.__beginRandomWrite, __open, err=%v", err)
		d.lock.Unlock()
		return nil, err
	}
	for d.syncing > 0 || d.writers.Len() >= d.maxWriters {
		d.cond.Wait()
	}
	if buf != nil {
		size = int64(len(buf))
		srcFd = -1
	}
	vec := &DiskWriteVector{
		buf: buf, logOffset: logOffset, size: size, srcFd: srcFd, state: DiskWriteBegin, isHead: d.writers.Len() == 0,
	}
	d.writers.ReplaceOrInsert(vec)
	d.lock.Unlock()
	return vec, nil
}

func (d *OnDiskLog) BeginRandomWrite(buf []byte, logOffset int64) (*DiskWriteVector, error) {
	return d.__beginRandomWrite(buf, -1, logOffset, 0)
}

func (d *OnDiskLog) BeginRandomSplice(srcFd int, size int64, logOffset int64) (*DiskWriteVector, error) {
	return d.__beginRandomWrite(nil, srcFd, logOffset, size)
}

func (d *OnDiskLog) __beginAppendWrite(buf []byte, srcFd int, size int64) (*DiskWriteVector, error) {
	d.lock.Lock()
	if err := d.__open(); err != nil {
		log.Errorf("Failed: OnDiskLog.__beginAppendWrite, __open, err=%v", err)
		d.lock.Unlock()
		return nil, err
	}
	for d.syncing > 0 || d.writers.Len() >= d.maxWriters {
		d.cond.Wait()
	}
	var logOffset = int64(0)
	isHead := true
	if max := d.writers.Max(); max != nil {
		tail := max.(*DiskWriteVector)
		logOffset = tail.logOffset + tail.size
		isHead = false
	} else {
		logOffset = d.size
	}
	if buf != nil {
		size = int64(len(buf))
		srcFd = -1
	}
	vec := &DiskWriteVector{
		buf: buf, logOffset: logOffset, size: size, srcFd: srcFd, state: DiskWriteBegin, isHead: isHead,
	}
	d.writers.ReplaceOrInsert(vec)
	d.lock.Unlock()
	return vec, nil
}

func (d *OnDiskLog) BeginAppendWrite(buf []byte) (*DiskWriteVector, error) {
	return d.__beginAppendWrite(buf, -1, 0)
}

func (d *OnDiskLog) BeginAppendSplice(srcFd int, size int64) (*DiskWriteVector, error) {
	return d.__beginAppendWrite(nil, srcFd, size)
}

func (d *OnDiskLog) Write(vec *DiskWriteVector) error {
	count := int64(0)
	for count < vec.size {
		c, err := unix.Pwrite(d.fd, vec.buf[count:], vec.logOffset+count)
		if c == 0 {
			err = unix.EIO
			log.Errorf("Failed: OnDiskLog.Write, Pwrite, filePath=%v, fd=%v, count=%v, logOffset=%v, size=%v, err=incomplete write",
				d.filePath, d.fd, count, vec.logOffset, vec.size)
			d.__abortWrite(vec, err)
			return err
		}
		if err != nil {
			log.Errorf("Failed: OnDiskLog.Write, Pwrite, filePath=%v, fd=%v, count=%v, logOffset=%v, size=%v, err=%v",
				d.filePath, d.fd, count, vec.logOffset, vec.size, err)
			d.__abortWrite(vec, err)
			return err
		}
		count += int64(c)
	}
	atomic.CompareAndSwapInt32(&vec.state, DiskWriteBegin, DiskWritePrepare)
	d.lock.RLock()
	if d.syncing > 0 {
		d.cond.Broadcast()
	}
	d.lock.RUnlock()
	return nil
}

func (d *OnDiskLog) Splice(vec *DiskWriteVector, pipeFds [2]int, bufOff *int32) (err error) {
	var count = int64(0)
	for int64(*bufOff) < vec.size {
		var nRead int64
		nRead, err = unix.Splice(vec.srcFd, nil, pipeFds[1], nil, int(vec.size-int64(*bufOff)), unix.SPLICE_F_MOVE|unix.SPLICE_F_NONBLOCK)
		if nRead == 0 || err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			err = nil
			break
		} else if err != nil {
			log.Errorf("Failed: OnDiskLog.Splice, Splice (0), srcFd=%v, pipeFds[1]=%v, size=%v, err=%v", vec.srcFd, pipeFds[1], vec.size, err)
			d.__abortWrite(vec, err)
			return
		}
		bodyOff := vec.logOffset + int64(*bufOff)
		for nRead > 0 {
			var l2 = int(nRead & int64(math.MaxInt))
			var nWrite int64
			nWrite, err = unix.Splice(pipeFds[0], nil, d.fd, &bodyOff, l2, unix.SPLICE_F_MOVE)
			if err != nil {
				log.Errorf("Failed: OnDiskLog.Splice, Splice (1), pipeFds[0]=%v, toFd=%v, l2=%v, offset=%v, err=%v", pipeFds[0], d.fd, l2, bodyOff, err)
				d.__abortWrite(vec, err)
				return
			}
			if nWrite == 0 {
				break
			}
			nRead -= nWrite
			*bufOff += int32(nWrite)
			count += nWrite
		}
	}
	atomic.CompareAndSwapInt32(&vec.state, DiskWriteBegin, DiskWritePrepare)
	d.lock.RLock()
	if d.syncing > 0 {
		d.cond.Broadcast()
	}
	d.lock.RUnlock()
	return
}

func (d *OnDiskLog) __abortWrite(vec *DiskWriteVector, err error) {
	d.lock.Lock()
	if !vec.isHead {
		atomic.CompareAndSwapInt32(&vec.state, DiskWriteBegin, ErrnoToReply(err))
		if d.syncing > 0 {
			d.cond.Broadcast()
		}
		d.lock.Unlock()
		return
	}
	d.writers.Ascend(func(i btree.Item) bool {
		bv := i.(*DiskWriteVector)
		atomic.CompareAndSwapInt32(&bv.state, DiskWriteBegin, ErrnoToReply(err))
		return true
	})
	d.writers.Clear(true)
	d.cond.Broadcast()
	d.lock.Unlock()
}

func (d *OnDiskLog) __abortSync(err error) {
	d.writers.Ascend(func(i btree.Item) bool {
		bv := i.(*DiskWriteVector)
		atomic.CompareAndSwapInt32(&bv.state, DiskWriteBegin, ErrnoToReply(err))
		return true
	})
	// TODO: should we ftruncate?
	d.writers.Clear(true)
	d.syncing -= 1
	d.cond.Broadcast()
}

func (d *OnDiskLog) __commitSync() int64 {
	newSize := d.size
	d.writers.Ascend(func(i btree.Item) bool {
		bv := i.(*DiskWriteVector)
		atomic.StoreInt32(&bv.state, RaftReplyOk)
		if d.maxNrCache > 0 {
			d.caches.ReplaceOrInsert(bv)
			if d.caches.Len() > d.maxNrCache {
				d.caches.DeleteMin()
			}
		}
		if newSize < bv.logOffset+bv.size {
			newSize = bv.logOffset + bv.size
		}
		return true
	})
	d.writers.Clear(true)
	sizeIncrease := newSize - d.size
	if sizeIncrease < 0 {
		sizeIncrease = 0
	}
	d.size = newSize
	d.syncing -= 1
	d.cond.Broadcast()
	return sizeIncrease
}

func (d *OnDiskLog) EndWrite(vec *DiskWriteVector) (sizeIncrease int64, err error) {
	d.lock.Lock()
	if !vec.isHead {
		for vec.state <= DiskWritePrepare {
			d.cond.Wait()
		}
		d.lock.Unlock()
		return 0, ReplyToFuseErr(vec.state)
	}
	d.syncing += 1
	err = nil
	d.writers.Ascend(func(i btree.Item) bool {
		bv := i.(*DiskWriteVector)
		for {
			state := atomic.LoadInt32(&bv.state)
			if state == DiskWriteBegin {
				d.cond.Wait()
				continue
			} else if state == DiskWritePrepare {
				break
			} else {
				err = ReplyToFuseErr(state)
				break
			}
		}
		return err == nil
	})
	if err != nil {
		d.__abortSync(err)
		d.lock.Unlock()
		return 0, err
	}
	err = unix.Fsync(d.fd)
	if err != nil {
		d.__abortSync(err)
		log.Errorf("Failed: OnDiskLog.CommitWrite, Fsync, filePath=%v, logOffset=%v, size=%v, err=%v", d.filePath, vec.logOffset, vec.size, err)
		return 0, err
	}
	err2 := unix.Fadvise(d.fd, 0, 0, unix.FADV_DONTNEED)
	if err2 != nil {
		log.Errorf("Failed (ignore): OnDiskLog.CommitWrite, Fadvise, filePath=%v, err=%v", d.filePath, err2)
	}
	sizeIncrease = d.__commitSync()
	d.lock.Unlock()
	return sizeIncrease, err
}

func (d *OnDiskLog) AppendSingleBuffer(buf []byte) (vec *DiskWriteVector, sizeIncrease int64, err error) {
	if len(buf) == 0 {
		log.Warnf("BUG: AppendSingleBuffer, len(buf) == 0")
	}
	vec, err = d.BeginAppendWrite(buf)
	if err != nil {
		return
	}
	if err = d.Write(vec); err != nil {
		return
	}
	sizeIncrease, err = d.EndWrite(vec)
	return
}

func (d *OnDiskLog) WriteSingleBuffer(buf []byte, logOffset int64) (vec *DiskWriteVector, sizeIncrease int64, err error) {
	if len(buf) == 0 {
		log.Warnf("BUG: WriteSingleBuffer, len(buf) == 0")
	}
	vec, err = d.BeginRandomWrite(buf, logOffset)
	if err != nil {
		return
	}
	if err = d.Write(vec); err != nil {
		return
	}
	sizeIncrease, err = d.EndWrite(vec)
	return
}

func (d *OnDiskLog) ReadNoCache(buf []byte, logOffset int64, dataLen int64, directIo bool) (count int64, err error) {
	mode := unix.O_RDONLY
	if directIo {
		mode |= unix.O_DIRECT
	}
	// d.fd is only for writes. we need to open a new fd for read
	fd, errno := unix.Open(d.filePath, mode, 0644)
	if errno != nil {
		log.Errorf("Failed: OnDiskLog.ReadNoCache, Open, filePath=%v, errno=%v", d.filePath, errno)
		return -1, errno
	}
	if dataLen > int64(len(buf)) {
		dataLen = int64(len(buf))
	}
	count = 0
	for count < dataLen {
		c, errno := unix.Pread(fd, buf[count:dataLen], logOffset+count)
		if errno != nil {
			log.Errorf("Failed: OnDiskLog.ReadNoCache, Pread, filePath=%v, count=%v, errno=%v", d.filePath, count, errno)
			return -1, errno
		}
		if c == 0 {
			break
		}
		count += int64(c)
	}
	if !directIo {
		if err2 := unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED); err2 != nil {
			log.Errorf("Failed (ignore): OnDiskLog.ReadNoCache, Fadvise, filePath=%v, logOffset=%v, err=%v", d.filePath, logOffset, err2)
		}
	}
	if err2 := unix.Close(fd); err2 != nil {
		log.Errorf("Failed (ignore): OnDiskLog.ReadNoCache, Close, filePath=%v, logOffset=%v, err=%v", d.filePath, logOffset, err2)
	}
	if count == 0 {
		return 0, io.EOF
	}
	return
}

func (d *OnDiskLog) Read(logOffset int64, size int64) (bufs [][]byte, count int64, err error) {
	count = int64(0)
	d.lock.RLock()
	bufs = make([][]byte, 0)
	target := &DiskWriteVector{logOffset: logOffset}
	var needDiskRead = true
	d.caches.DescendLessOrEqual(target, func(i btree.Item) bool {
		bv := i.(*DiskWriteVector)
		if bv.buf == nil {
			return false // appended by splice
		}
		if bv.logOffset+bv.size <= logOffset {
			return false // overflow. SimpleReadAll should return EOVERFLOW
		}
		if bv.logOffset < logOffset { // AscendGreaterOrEqual should copy in the case of bv.logOffset == logOffset
			var appendBuf []byte
			if count+int64(len(bv.buf))-(logOffset-bv.logOffset) < size {
				appendBuf = bv.buf[logOffset-bv.logOffset:]
			} else {
				appendBuf = bv.buf[logOffset-bv.logOffset : logOffset-bv.logOffset+size]
			}
			bufs = append(bufs, appendBuf)
			count += int64(len(appendBuf))
		}
		needDiskRead = false
		return false
	})
	if needDiskRead {
		d.lock.RUnlock()
		newBuf := make([]byte, size-count)
		c, err := d.ReadNoCache(newBuf, logOffset+count, size-count, false)
		if err == nil {
			bufs = append(bufs, newBuf[:c])
			count += c
		}
		return bufs, count, err
	}
	d.caches.AscendGreaterOrEqual(target, func(i btree.Item) bool {
		bv := i.(*DiskWriteVector)
		if count == size {
			return false
		}
		if bv.buf == nil {
			needDiskRead = true // appended by splice
			return false
		}
		var appendBuf = bv.buf
		if count+int64(len(bv.buf)) > size {
			appendBuf = bv.buf[:size-count]
		}
		bufs = append(bufs, appendBuf)
		count += int64(len(appendBuf))
		return true
	})
	d.lock.RUnlock()
	if needDiskRead || size > count {
		newBuf := make([]byte, size-count)
		c, err := d.ReadNoCache(newBuf, logOffset+count, size-count, false)
		if err == nil {
			bufs = append(bufs, newBuf[:c])
			count += c
		}
		return bufs, count, err
	}
	return bufs, count, nil
}

func (d *OnDiskLog) SendZeroCopy(toFd int, logOffset int64, size int32, bufOff *int32) (err error) {
	var rFd int
	rFd, err = unix.Open(d.filePath, unix.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("Failed: OnDiskLog.SendZeroCopy, Open, filePath=%v, err=%v", d.filePath, err)
		return err
	}
	var fileOff = logOffset + int64(*bufOff)
	for *bufOff < size {
		var n int
		n, err = unix.Sendfile(toFd, rFd, &fileOff, int(size-*bufOff))
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			break
		} else if err != nil {
			log.Errorf("Failed: OnDiskLog.SendZeroCopy, Sendfile, toFd=%v, fileOff=%v, bufOff=%v, size=%v, errr=%v", toFd, fileOff, *bufOff, size, err)
			break
		}
		*bufOff += int32(n)
	}
	_ = unix.Close(rFd)
	return err
}

func (d *OnDiskLog) GetSize() int64 {
	d.lock.RLock()
	size := d.size
	d.lock.RUnlock()
	return size
}

type LogIdType struct {
	lower uint64
	upper uint32
}

func (f LogIdType) Put(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:SizeOfUint64], f.lower)
	binary.LittleEndian.PutUint32(buf[SizeOfUint64:SizeOfUint64+SizeOfUint32], f.upper)
}

func (f LogIdType) IsValid() bool {
	return f.upper > 0 || f.lower > 0
}

func NewLogIdTypeFromInodeKey(inodeKey InodeKeyType, offset int64, chunkSize int64) LogIdType {
	return LogIdType{lower: uint64(inodeKey), upper: uint32(offset / chunkSize)}
}

func NewLogIdTypeFromBuf(buf []byte) LogIdType {
	lower := binary.LittleEndian.Uint64(buf[0:SizeOfUint64])
	upper := binary.LittleEndian.Uint32(buf[SizeOfUint64 : SizeOfUint64+SizeOfUint32])
	return LogIdType{lower: lower, upper: upper}
}

type OnDiskLogger struct {
	lock           *sync.RWMutex
	filePrefix     string
	disks          map[LogIdType]*OnDiskLog
	refCount       map[LogIdType]int
	totalDiskUsage int64 //Note: currentTerm and votedFor are not counted

	snapshot LogIdType
}

func NewOnDiskLogger(filePrefix string) *OnDiskLogger {
	ret := &OnDiskLogger{
		lock:           new(sync.RWMutex),
		filePrefix:     filePrefix,
		disks:          make(map[LogIdType]*OnDiskLog),
		refCount:       make(map[LogIdType]int),
		totalDiskUsage: 0,
		snapshot:       LogIdType{lower: 0, upper: 0},
	}
	ret.Reset()
	return ret
}

func (c *OnDiskLogger) Reset() {
	baseName := filepath.Base(c.filePrefix)
	re := regexp.MustCompile(fmt.Sprintf("%s-([0-9-]+)-([0-9-]+).log", baseName))
	totalDiskUsage := int64(0)
	c.lock.Lock()
	c.clearNoLock()
	if dirs, err2 := os.ReadDir(filepath.Dir(c.filePrefix)); err2 == nil {
		for _, file := range dirs {
			f, err3 := file.Info()
			if err3 != nil || f.IsDir() {
				continue
			}
			found := re.FindAllStringSubmatch(f.Name(), 1)
			if len(found) <= 0 || len(found[0]) <= 1 {
				continue
			}
			lower, err := strconv.ParseUint(found[0][1], 10, 64)
			if err != nil {
				log.Errorf("Failed: OnDiskLogger.Reset, ParseUint (64 bit), malformed log file, file=%v, err=%v", file.Name(), err)
				continue
			}
			upper, err := strconv.ParseUint(found[0][2], 10, 32)
			if err != nil {
				log.Errorf("Failed: OnDiskLogger.Reset, ParseUint (32 bit), malformed log file, file=%v, err=%v", file.Name(), err)
				continue
			}
			logId := LogIdType{lower: uint64(lower), upper: uint32(upper)}
			disk := NewOnDiskLog(file.Name(), 32, 1)
			disk.InitSize(f.Size())
			c.disks[logId] = disk
			totalDiskUsage += f.Size()
			log.Infof("OnDiskLogger.Reset, found existing log file, file=%v, logId=%v, size=%v", file.Name(), logId, f.Size())
		}
	}
	c.totalDiskUsage = totalDiskUsage
	c.lock.Unlock()
	log.Infof("Success: OnDiskLogger.Reset, totalDiskUsage=%v", totalDiskUsage)
}

func (c *OnDiskLogger) GetSize(logId LogIdType) (size int64) {
	size = 0
	c.lock.RLock()
	d, ok := c.disks[logId]
	if ok {
		size = d.GetSize()
	}
	c.lock.RUnlock()
	return size
}

func (c *OnDiskLogger) GetDiskUsage() int64 {
	return atomic.LoadInt64(&c.totalDiskUsage)
}

func (c *OnDiskLogger) AddDiskUsage(size int64) int64 {
	return atomic.AddInt64(&c.totalDiskUsage, size)
}

func (c *OnDiskLogger) Freeze(logId LogIdType) {
	c.lock.Lock()
	c.snapshot = logId
	for {
		total := 0
		for _, c := range c.refCount {
			total += c
		}
		if total == 0 {
			break
		}
		c.lock.Unlock()
		time.Sleep(time.Millisecond)
		c.lock.Lock()
	}
	c.lock.Unlock()
}

func (c *OnDiskLogger) IsFreezed(logId LogIdType) bool {
	c.lock.RLock()
	ok := c.snapshot.upper != 0 && c.snapshot.upper != logId.upper
	c.lock.RUnlock()
	return ok
}

func (c *OnDiskLogger) GetDiskLog(logId LogIdType, maxNrCache int, maxWriters int) (disk *OnDiskLog) {
	c.lock.Lock()
	var ok bool
	disk, ok = c.disks[logId]
	if !ok {
		fileName := fmt.Sprintf("%s-%d-%d.log", c.filePrefix, logId.lower, logId.upper)
		disk = NewOnDiskLog(fileName, maxNrCache, maxWriters)
		c.disks[logId] = disk
		c.refCount[logId] = 0
	}
	c.refCount[logId] += 1
	c.lock.Unlock()
	return
}

func (c *OnDiskLogger) PutDiskLog(logId LogIdType, vec *DiskWriteVector, sizeIncrease int64) {
	c.lock.Lock()
	if r, ok := c.refCount[logId]; ok && r >= 1 {
		c.refCount[logId] = r - 1
	}
	c.lock.Unlock()
	c.AddDiskUsage(sizeIncrease)
}

func (c *OnDiskLogger) WriteSingleBuffer(logId LogIdType, buf []byte, logOffset int64) error {
	if c.IsFreezed(logId) {
		return unix.EBUSY
	}
	disk := c.GetDiskLog(logId, 0, 32)
	vec, sizeIncrease, err := disk.WriteSingleBuffer(buf, logOffset)
	if err == nil {
		c.PutDiskLog(logId, vec, sizeIncrease)
	}
	return err
}

func (c *OnDiskLogger) AppendSingleBuffer(logId LogIdType, buf []byte) (logOffset int64, err error) {
	if c.IsFreezed(logId) {
		return 0, unix.EBUSY
	}
	disk := c.GetDiskLog(logId, 0, 32)
	vec, sizeIncrease, err := disk.AppendSingleBuffer(buf)
	if err == nil {
		c.PutDiskLog(logId, vec, sizeIncrease)
	}
	return vec.logOffset, err
}

func (c *OnDiskLogger) Read(logId LogIdType, logOffset int64, logBytes int64) (extBuf []byte, err error) {
	if c.IsFreezed(logId) {
		return nil, unix.EBUSY
	}
	bufs, count, err := c.ZeroCopyRead(logId, logOffset, logBytes)
	if err != nil {
		return nil, err
	}
	if len(bufs) == 1 {
		extBuf = bufs[0]
	} else {
		extBuf = make([]byte, count)
		var total = 0
		for _, b := range bufs {
			total += copy(extBuf[total:], b)
		}
	}
	return
}

func (c *OnDiskLogger) ReadNoCache(logId LogIdType, buf []byte, logOffset int64, logBytes int64, directIo bool) (count int64, err error) {
	if c.IsFreezed(logId) {
		return 0, unix.EBUSY
	}
	disk := c.GetDiskLog(logId, 0, 32)
	return disk.ReadNoCache(buf, logOffset, logBytes, directIo)
}

func (c *OnDiskLogger) SendZeroCopy(logId LogIdType, toFd int, logOffset int64, logBytes int32, bufOff *int32) (err error) {
	if c.IsFreezed(logId) {
		return unix.EBUSY
	}
	disk := c.GetDiskLog(logId, 0, 32)
	return disk.SendZeroCopy(toFd, logOffset, logBytes, bufOff)
}

func (c *OnDiskLogger) ZeroCopyRead(logId LogIdType, logOffset int64, logBytes int64) (bufs [][]byte, count int64, err error) {
	if c.IsFreezed(logId) {
		return nil, 0, unix.EBUSY
	}
	disk := c.GetDiskLog(logId, 32, 1)
	return disk.Read(logOffset, logBytes)
}

func (c *OnDiskLogger) ReserveRange(logId LogIdType, logBytes int64) (offset int64) {
	return c.GetDiskLog(logId, 32, 1).ReserveAppendWrite(logBytes)
}

func (c *OnDiskLogger) Remove(logId LogIdType) (size int64, err error) {
	c.lock.Lock()
	disk, ok := c.disks[logId]
	if ok {
		size = disk.Clear()
		newDiskUsage := c.AddDiskUsage(-size)
		delete(c.disks, logId)
		log.Debugf("Success: OnDiskLogger.Remove, logId=%v, newDiskUsage=%v, size=%v", logId, newDiskUsage, size)
	}
	c.lock.Unlock()
	return
}

func (c *OnDiskLogger) Clear() {
	c.lock.Lock()
	c.clearNoLock()
	c.lock.Unlock()
}

func (c *OnDiskLogger) clearNoLock() {
	for _, disk := range c.disks {
		disk.Clear()
	}
	c.disks = make(map[LogIdType]*OnDiskLog)
	//c.totalDiskUsage = 0 // intentionally avoid clearing this. bugs raise check errors later
}

func (c *OnDiskLogger) CheckReset() (ok bool) {
	if ok = c.lock.TryLock(); !ok {
		log.Errorf("Failed: OnDiskLogger.CheckReset, c.lock is taken")
		return
	}
	if ok2 := len(c.disks) == 0; !ok2 {
		log.Errorf("Failed: OnDiskLogger.CheckReset, len(c.disks) != 0")
		ok = false
	}
	if ok2 := c.totalDiskUsage == 0; !ok2 {
		log.Errorf("Failed: OnDiskLogger.CheckReset, c.totalDiskUsage != 0")
		ok = false
	}
	c.lock.Unlock()
	return
}
