/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
)

type MetaRWHandler struct {
	inodeKey  InodeKeyType
	fetchKey  string
	version   uint32
	size      int64
	chunkSize int64
	mTime     int64
	mode      uint32
}

func NewMetaRWHandlerFromMeta(meta *WorkingMeta) MetaRWHandler {
	return MetaRWHandler{
		inodeKey: meta.inodeKey, fetchKey: meta.fetchKey,
		version: meta.version, size: meta.size, chunkSize: meta.chunkSize, mTime: meta.mTime, mode: meta.mode,
	}
}

func NewMetaRWHandlerFromMsg(msg *common.MetaRWHandlerMsg) MetaRWHandler {
	return MetaRWHandler{
		inodeKey: InodeKeyType(msg.GetInodeKey()), fetchKey: msg.GetFetchKey(),
		version: msg.GetVersion(), size: msg.GetSize(), chunkSize: msg.GetChunkSize(), mTime: msg.GetMTime(), mode: msg.GetMode(),
	}
}

func (m MetaRWHandler) toMsg() *common.MetaRWHandlerMsg {
	return &common.MetaRWHandlerMsg{
		InodeKey: uint64(m.inodeKey), FetchKey: m.fetchKey,
		Version: m.version, Size: m.size, ChunkSize: m.chunkSize, MTime: m.mTime, Mode: m.mode,
	}
}

type FileHandle struct {
	h                            MetaRWHandler
	lock                         *sync.RWMutex
	records                      map[string][]*common.StagingChunkMsg
	recordLength                 int
	dirty                        int32 // to support empty writes
	isReadOnly                   bool
	disableLocalWriteBackCaching bool
	flying                       map[interface{}][]SlicedPageBuffer

	prefetchOffset  int64
	readAheadOffset int64
}

func NewFileHandle(h MetaRWHandler, isReadOnly bool, disableLocalWriteBackCaching bool) *FileHandle {
	var records map[string][]*common.StagingChunkMsg = nil
	if !isReadOnly {
		records = make(map[string][]*common.StagingChunkMsg)
	}
	return &FileHandle{
		h: h, lock: new(sync.RWMutex),
		records: records, recordLength: 0, dirty: 0, isReadOnly: isReadOnly, disableLocalWriteBackCaching: disableLocalWriteBackCaching,
	}
}

func (i *FileHandle) dropAllNoLock(n *NodeServer) {
	atomic.StoreInt32(&i.dirty, 0)
	n.remoteCache.Delete(i.h.inodeKey)
	n.localHistory.Delete(i.h.inodeKey)
}

func (i *FileHandle) ReadNoCache(offset int64, size int64, n *NodeServer, op interface{}) (data [][]byte, count int, errno error) {
	begin := time.Now()
	for atomic.LoadInt32(&i.dirty) != 0 {
		_, err := i.Flush(n)
		if err != nil {
			return nil, 0, err
		}
	}
	i.lock.RLock()
	h := i.h
	i.lock.RUnlock()
	count = 0
	var prefetchTime time.Duration
	flying := make([]SlicedPageBuffer, 0)
	for int64(count) < size && offset+int64(count) < h.size {
		chunkOff := (offset + int64(count)) % h.chunkSize
		if count == 0 && n.flags.CosPrefetchSizeBytes > 0 {
			lastPrefetchOff := atomic.LoadInt64(&i.prefetchOffset)
			lastOffset := (offset + int64(count)) - chunkOff + n.flags.CosPrefetchSizeBytes
			if lastPrefetchOff+n.flags.CosPrefetchSizeBytes/2 <= lastOffset && atomic.CompareAndSwapInt64(&i.prefetchOffset, lastPrefetchOff, lastOffset) {
				prefetchBegin := time.Now()
				n.PrefetchChunk(h, lastPrefetchOff, lastOffset-lastPrefetchOff)
				prefetchTime += time.Since(prefetchBegin)
			}
		}
		length := h.size - offset
		if length > size-int64(count) {
			length = size - int64(count)
		}
		buf, c, err := n.ReadChunk(h, offset+int64(count), int(length), true)
		if err == io.EOF || c == 0 {
			break
		} else if err != nil {
			// Slice() calls buf.Up(). So, revert it to enable auto-eviction.
			for _, b := range flying {
				b.SetEvictable()
			}
			log.Errorf("Failed: FileHandle.ReadNoCache, VectorReadChunk, offset=%v, length=%v, err=%v", offset, length, err)
			return nil, 0, err
		}
		flying = append(flying, buf...)
		for _, b := range buf {
			data = append(data, b.Buf)
			count += len(b.Buf)
		}
	}
	// auto-eviction of buf is blocked during copying it to kernel (Slice() calls buf.Up()). We need to track them to unblock it later
	i.lock.Lock()
	if i.flying == nil {
		i.flying = make(map[interface{}][]SlicedPageBuffer)
	}
	i.flying[op] = flying
	i.lock.Unlock()
	if offset+int64(count) < h.size && int64(count) != size {
		log.Warnf("ReadNoCache, Something bad happens!!!!, count (%v) != size (%v)", count, size)
	}
	log.Debugf("Success: FileHandle.ReadNoCache, fetchKey=%v, offset=%v, size=%v, len(data)=%v, prefetch=%v, elapsed=%v",
		h.fetchKey, offset, size, len(data), prefetchTime, time.Since(begin))
	return
}

func (i *FileHandle) Read(offset int64, size int64, n *NodeServer, op interface{}) (data [][]byte, count int, errno error) {
	begin := time.Now()
	for atomic.LoadInt32(&i.dirty) != 0 {
		_, err := i.Flush(n)
		if err != nil {
			return nil, 0, err
		}
	}
	i.lock.RLock()
	h := i.h
	i.lock.RUnlock()

	count = 0
	var prefetchTime time.Duration
	flying := make([]SlicedPageBuffer, 0)
	for int64(count) < size && offset+int64(count) < h.size {
		chunkOff := (offset + int64(count)) % h.chunkSize
		alignedOffset := (offset + int64(count)) - chunkOff
		if count == 0 && n.flags.CosPrefetchSizeBytes > 0 {
			lastPrefetchOff := atomic.LoadInt64(&i.prefetchOffset)
			lastOffset := alignedOffset + n.flags.CosPrefetchSizeBytes
			if lastPrefetchOff+n.flags.CosPrefetchSizeBytes/2 <= lastOffset && atomic.CompareAndSwapInt64(&i.prefetchOffset, lastPrefetchOff, lastOffset) {
				prefetchBegin := time.Now()
				n.PrefetchChunk(h, lastPrefetchOff, lastOffset-lastPrefetchOff)
				prefetchTime += time.Since(prefetchBegin)
			}
		}
		length := h.size - alignedOffset
		if length > h.chunkSize {
			length = h.chunkSize
		}
		blocking := len(flying) == 0 || int64(count) < size
		buf, c, err := n.ReadChunk(h, alignedOffset, int(length), blocking)
		if err == io.EOF || (!blocking && err == unix.EAGAIN) || c == 0 {
			break
		} else if err != nil {
			// Slice() calls buf.Up(). So, revert it to enable auto-eviction.
			for _, b := range flying {
				b.SetEvictable()
			}
			log.Errorf("Failed: FileHandle.Read, ReadChunk, alignedOffset=%v, length=%v, err=%v", alignedOffset, length, err)
			return nil, 0, err
		}
		flying = append(flying, buf...)

		length = int64(c) - chunkOff
		if length > size-int64(count) {
			length = size - int64(count)
		}

		var bufOff = int64(0)
		for i := 0; i < len(buf) && bufOff < int64(c) && int64(count) < size; i++ {
			bufLen := int64(len(buf[i].Buf))
			var target []byte
			if bufOff+bufLen <= chunkOff {
				bufOff += bufLen
				continue
			} else if bufOff < chunkOff && chunkOff < bufOff+bufLen {
				if chunkOff+length < bufOff+bufLen {
					target = buf[i].Buf[chunkOff-bufOff : chunkOff-bufOff+length]
				} else {
					target = buf[i].Buf[chunkOff-bufOff:]
				}
			} else if chunkOff <= bufOff && bufOff < chunkOff+length {
				if chunkOff+length < bufOff+bufLen {
					target = buf[i].Buf[:chunkOff+length-bufOff]
				} else {
					target = buf[i].Buf
				}
			} else {
				break
			}
			data = append(data, target)
			count += len(target)
			bufOff += bufLen
		}
	}

	// auto-eviction of buf is blocked during copying it to kernel (Slice() calls buf.Up()). We need to track them to unblock it later
	i.lock.Lock()
	if i.flying == nil {
		i.flying = make(map[interface{}][]SlicedPageBuffer)
	}
	i.flying[op] = flying
	i.lock.Unlock()
	if offset+int64(count) < h.size && int64(count) != size {
		log.Warnf("Read, Something bad happens!!!!, count (%v) != size (%v)", count, size)
	}
	log.Debugf("Success: FileHandle.Read, fetchKey=%v, offset=%v, size=%v, len(data)=%v, prefetch=%v, elapsed=%v",
		h.fetchKey, offset, size, len(data), prefetchTime, time.Since(begin))

	if !n.flags.DisableLocalWritebackCaching && n.flags.ClientReadAheadSizeBytes > 0 {
		lastReadAheadOff := atomic.LoadInt64(&i.readAheadOffset)
		lastOffset := offset + int64(count) + n.flags.ClientReadAheadSizeBytes
		if lastReadAheadOff+n.flags.ClientReadAheadSizeBytes/2 <= lastOffset && atomic.CompareAndSwapInt64(&i.readAheadOffset, lastReadAheadOff, lastOffset) {
			n.ReadAheadChunk(h, offset+int64(count), lastOffset-lastReadAheadOff)
		}
	}
	return
}

func (i *FileHandle) ReleaseFlyingBuffer(op interface{}) {
	i.lock.Lock()
	flying := i.flying[op]
	delete(i.flying, op)
	i.lock.Unlock()
	for _, buf := range flying {
		buf.SetEvictable() // unblock auto-eviction
	}
}

func (i *FileHandle) appendRecords(records []*common.UpdateChunkRecordMsg) {
	for _, record := range records {
		if _, ok := i.records[record.GetGroupId()]; !ok {
			i.records[record.GetGroupId()] = make([]*common.StagingChunkMsg, 0)
		}
		i.records[record.GetGroupId()] = append(i.records[record.GetGroupId()], record.GetStags()...)
	}
	i.recordLength += 1
}

func (i *FileHandle) abort(newRecords []*common.UpdateChunkRecordMsg, n *NodeServer) {
	i.lock.Lock()
	i.appendRecords(newRecords)
	atomic.StoreInt32(&i.dirty, 0)
	n.AbortWriteObject(i.records)
	i.records = make(map[string][]*common.StagingChunkMsg)
	i.recordLength = 0
	i.lock.Unlock()
}

func (i *FileHandle) Write(offset int64, data []byte, n *NodeServer) (meta *WorkingMeta, errno error) {
	begin := time.Now()
	i.lock.RLock()
	h := i.h
	i.lock.RUnlock()
	firstLock := time.Now()
	count := int64(len(data))
	records, reply := n.WriteChunk(h.inodeKey, h.chunkSize, offset, count, data)
	if reply != RaftReplyOk {
		log.Errorf("Failed: FileHandle.Write, WriteChunk, inodeKey=%v, reply=%v", h.inodeKey, reply)
		i.abort(records, n)
		return nil, ReplyToFuseErr(reply)
	}
	updateChunk := time.Now()
	i.lock.Lock()
	i.appendRecords(records)
	recordMap := i.records
	recordLength := i.recordLength
	if recordLength > 1024 || i.disableLocalWriteBackCaching {
		i.recordLength = 0
		i.records = make(map[string][]*common.StagingChunkMsg)
		i.dropAllNoLock(n)
		atomic.StoreInt32(&i.dirty, 0)
		i.lock.Unlock()
		beginFlush := time.Now()
		var err error
		meta, err = i.__flush(h.inodeKey, recordMap, h.mTime, h.mode, n)
		if err != nil {
			return nil, err
		}
		endFlush := time.Now()
		i.lock.Lock()
		i.h = NewMetaRWHandlerFromMeta(meta)
		i.lock.Unlock()
		endAll := time.Now()
		log.Debugf("Success: FileHandle.Write, inodeKey=%v, offset=%v, count=%v, firstLock=%v, updateChunk=%v, beginFlush=%v, endFlush=%v, unlock=%v, elapsed=%v",
			h.inodeKey, offset, count, firstLock.Sub(begin), updateChunk.Sub(firstLock), beginFlush.Sub(updateChunk), endFlush.Sub(beginFlush), endAll.Sub(endFlush), endAll.Sub(begin))
	} else {
		meta = nil
		atomic.StoreInt32(&i.dirty, 1)
		i.lock.Unlock()
		endAll := time.Now()
		log.Debugf("Success: FileHandle.Write, inodeKey=%v, offset=%v, count=%v, firstLock=%v, updateChunk=%v, unlock=%v, elapsed=%v",
			h.inodeKey, offset, count, firstLock.Sub(begin), updateChunk.Sub(firstLock), endAll.Sub(updateChunk), endAll.Sub(begin))
	}
	return meta, nil
}

func (i *FileHandle) __flush(inodeKey InodeKeyType, recordMap map[string][]*common.StagingChunkMsg, mTime int64, mode uint32, n *NodeServer) (*WorkingMeta, error) {
	records := make([]*common.UpdateChunkRecordMsg, 0)
	for groupId, stags := range recordMap {
		records = append(records, &common.UpdateChunkRecordMsg{GroupId: groupId, Stags: stags})
	}
	meta, err := n.FlushObject(inodeKey, records, mTime, mode)
	if err != nil {
		log.Errorf("Failed: FileHandle.__flush, CallCoordinatorMeta, inodeKey=%v, err=%v", i.h.inodeKey, err)
		i.abort(nil, n)
	}
	return meta, err
}

func (i *FileHandle) Flush(n *NodeServer) (meta *WorkingMeta, errno error) {
	i.lock.Lock()
	if !atomic.CompareAndSwapInt32(&i.dirty, 1, 0) {
		i.lock.Unlock()
		return nil, nil
	}
	recordMap := i.records
	h := i.h
	i.records = make(map[string][]*common.StagingChunkMsg)
	i.recordLength = 0
	i.dropAllNoLock(n)
	i.lock.Unlock()
	meta, errno = i.__flush(h.inodeKey, recordMap, h.mTime, h.mode, n)
	if errno != nil {
		return
	}
	i.lock.Lock()
	i.h = NewMetaRWHandlerFromMeta(meta)
	i.lock.Unlock()
	return
}

func (i *FileHandle) SetModeMTime(mode *os.FileMode, mTime *time.Time) {
	i.lock.Lock()
	if mode != nil {
		i.h.mode = uint32(*mode)
	}
	if mTime != nil {
		i.h.mTime = mTime.UnixNano()
	}
	i.lock.Unlock()
}

func (i *FileHandle) GetLength() int64 {
	i.lock.RLock()
	ret := i.h.size
	i.lock.RUnlock()
	return ret
}

func (i *FileHandle) SetMeta(meta *WorkingMeta) {
	i.lock.Lock()
	i.h = NewMetaRWHandlerFromMeta(meta)
	i.records = make(map[string][]*common.StagingChunkMsg)
	i.recordLength = 0
	i.lock.Unlock()
}
