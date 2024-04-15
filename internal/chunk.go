/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/takeshi-yoshimura/fuse"
	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
)

type Chunk struct {
	inodeKey InodeKeyType
	offset   int64
	chunkIdx uint32

	workingHead WorkingChunk
	lock        *sync.RWMutex
	fillLock    *sync.RWMutex

	lastAccess     time.Time
	accessLinkNext *Chunk
	accessLinkPrev *Chunk
}

func CreateNewChunk(inodeKey InodeKeyType, offset int64, chunkIdx uint32) *Chunk {
	c := &Chunk{
		inodeKey:    inodeKey,
		workingHead: WorkingChunk{},
		lock:        new(sync.RWMutex),
		fillLock:    new(sync.RWMutex),
		offset:      offset,
		chunkIdx:    chunkIdx,
	}
	c.workingHead.prevVer = &c.workingHead
	c.workingHead.nextVer = &c.workingHead
	log.Debugf("CreateNewChunk, inodeKey=%v, offset=%v, chunkIdx=%v", inodeKey, offset, chunkIdx)
	return c
}

func (c *Chunk) GetLogId() LogIdType {
	return LogIdType{lower: uint64(c.inodeKey), upper: c.chunkIdx}
}

func (c *Chunk) GetWorkingChunk(ver uint32, updateLRU bool) (*WorkingChunk, error) {
	for working := c.workingHead.prevVer; working != &c.workingHead; working = working.prevVer {
		if working.chunkVer <= ver {
			if updateLRU {
				working.chunkPtr.lastAccess = time.Now().UTC()
				working.chunkPtr.UpdateLRUList()
			}
			return working, nil
		}
	}
	return nil, fuse.ENOENT
}

func (c *Chunk) AddWorkingChunk(inodeMgr *InodeMgr, working *WorkingChunk, prev *WorkingChunk) {
	if prev == nil {
		prev = c.workingHead.prevVer
	}
	working.prevVer = prev
	working.nextVer = prev.nextVer
	prev.nextVer.prevVer = working
	prev.nextVer = working
	working.chunkPtr.UpdateLRUList()
	inodeMgr.readCache.Delete(c.GetLogId())
}

func (c *Chunk) Drop(inodeMgr *InodeMgr, raft *RaftInstance) {
	inodeMgr.readCache.Delete(c.GetLogId())
	logId := c.GetLogId()
	length, err := raft.extLogger.Remove(logId)
	if err != nil {
		log.Errorf("Failed: Drop, Remove, err=%v", err)
		return
	}

	AccessLinkLock.Lock()
	for p := c.workingHead.prevVer; p != &c.workingHead; p = p.prevVer {
		p.chunkPtr.DeleteFromLRUListNoLock()
	}
	AccessLinkLock.Unlock()

	c.workingHead.prevVer = &c.workingHead
	c.workingHead.nextVer = &c.workingHead
	log.Debugf("Success: Chunk.Drop, inodeKey=%v, offset=%v, logId=%v, length=%v, lastAccess=%v", c.inodeKey, c.offset, logId, length, c.lastAccess)
}

func (c *Chunk) NewWorkingChunk(chunkVer uint32) *WorkingChunk {
	working := &WorkingChunk{
		chunkVer: chunkVer,
		chunkPtr: c,
		stags:    btree.New(3),
	}
	c.lastAccess = time.Now()
	return working
}

type StagingChunkPart struct {
	length int64
	chunk  *StagingChunk
	slop   int64
}

func (p *StagingChunkPart) Less(b btree.Item) bool {
	return p.slop < b.(*StagingChunkPart).slop
}

func (p *StagingChunkPart) LastOffset() int64 {
	return p.slop + p.length
}

const (
	StagingChunkData   = byte(1)
	StagingChunkDelete = byte(2)
	StagingChunkBlank  = byte(3)
)

type StagingChunk struct {
	filled     int32
	slop       int64
	length     int64
	updateType byte
	logOffset  int64
	fetchKey   string //used to fetch contents from COS to fill staging chunks at local)
}

func NewStagingChunk(slop int64, length int64, updateType byte, logOffset int64, key string, filled int32) *StagingChunk {
	return &StagingChunk{
		filled:     filled,
		slop:       slop,
		length:     length,
		updateType: updateType,
		logOffset:  logOffset,
		fetchKey:   key,
	}
}

func NewStagingChunkFromAddMsg(msg *common.StagingChunkAddMsg) *StagingChunk {
	s := NewStagingChunk(msg.GetSlop(), msg.GetLength(), msg.GetUpdateType()[0], msg.GetLogOffset(), msg.GetFetchKey(), msg.GetFilled())
	return s
}

func (s *StagingChunk) ReadObject(inodeMgr *InodeMgr, reader *BufferedDiskPageReader) (err error) {
	alignedSize := reader.dataLen + reader.alignLeft
	if alignedSize%int64(SectorSize) != 0 {
		alignedSize = alignedSize - alignedSize%int64(SectorSize) + int64(SectorSize)
	}
	var count int64
	count, err = inodeMgr.raft.extLogger.ReadNoCache(reader.logId, reader.buf.Buf, reader.bufLogOffset, alignedSize, true)
	if err != nil {
		return err
	}
	/**
	 * Buffer layout:
	 *     bf     o <------ dataLen ------>
	 * ----|------|------------------------|--------|---
	 *     <- al ->                         <- ar ->
	 *     <------------- alignedSize -------------->
	 * al = r.alignedLeft = offset % SectorSize
	 * ar = SectorSize-(al+dataLen)%SectorSize
	 */
	if count <= reader.alignLeft {
		return io.EOF
	} else if count < reader.alignLeft+reader.dataLen {
		reader.buf.Buf = reader.buf.Buf[:count]
	} else {
		reader.buf.Buf = reader.buf.Buf[:reader.alignLeft+reader.dataLen]
	}
	return nil
}

func (s *StagingChunk) AppendToLog(inodeMgr *InodeMgr, reader *BufferedDiskPageReader, fetchKey string) (err error) {
	logOffset := reader.bufLogOffset + reader.alignLeft
	begin := time.Now()
	var lastLogIndex uint64
	var reply int32
	var write, append time.Time

	buf := reader.buf.Buf[reader.alignLeft:]
	var skipWrite = false
	var skipAppend = false
	for i := 0; ; i++ {
		if !skipWrite {
			if err := inodeMgr.raft.extLogger.WriteSingleBuffer(reader.logId, buf, logOffset); err != nil {
				log.Errorf("Failed: StagingChunk.AppendToLog, WriteSingleBuffer, fetchKey=%v, logId=%v, len(buf)=%v, logOffset=%v, err=%v",
					fetchKey, reader.logId, len(buf), logOffset, err)
				goto out
			}
			skipWrite = true
			write = time.Now()
		}
		if !skipAppend {
			var cmd AppendEntryCommand
			for offset := int64(0); offset < int64(len(buf)); {
				dataLen := int64(len(buf)) - offset
				if dataLen > int64(inodeMgr.raft.maxHBBytes) {
					dataLen = int64(inodeMgr.raft.maxHBBytes)
				}
				b := buf[offset : offset+dataLen]
				rc := NewExtLogCommandFromExtBuf(AppendEntryFillChunkCmdId, reader.logId, logOffset, b)
				cmd := GetAppendEntryCommand(inodeMgr.raft.currentTerm.Get(), rc)
				lastLogIndex, reply = inodeMgr.raft.log.AppendCommand(cmd)
				if reply != RaftReplyOk {
					log.Errorf("Failed: StagingChunk.AppendToLog, AppendCommand, reply=%v", reply)
					err = ReplyToFuseErr(reply)
					goto out
				}
				offset += int64(len(b))
			}
			append = time.Now()
			reply = inodeMgr.raft.ReplicateLog(lastLogIndex, nil, nil, nil, &cmd, nil)
			if reply != RaftReplyOk {
				log.Errorf("Failed: StagingChunk.AppendToLog, ReplicateLog, lastLogIndex=%v, reply=%v", lastLogIndex, reply)
				err = ReplyToFuseErr(reply)
				goto out
			}
			skipAppend = true
		}
	out:
		if err != nil {
			// s.filled remains 0 and with reader inevictable at a failure.
			// We keep retrying until it succeeds infinitely. Users can still access the buffer since we don't set it evictable.
			// We may see out of memory and deadlock at high load, but it is okay since disk, network or remote nodes have permanent troubles we should fail-stop.
			log.Warnf("StagingChunk.AppendToLog, retry after 1 sec, write success=%v, append success=%v, replication success=false retry=%v", skipWrite, skipAppend, i)
			time.Sleep(time.Second)
			continue
		}
		atomic.StoreInt32(&s.filled, 1)
		inodeMgr.readCache.SetEvictable(reader)
		log.Debugf("Success: StagingChunk.AppendToLog, index=%v, logId=%v, logOffset=%v, length=%v, write=%v, append=%v, replicate=%v, elapsed=%v",
			lastLogIndex, reader.logId, logOffset, len(buf), write.Sub(begin), append.Sub(write), time.Since(append), time.Since(begin))
		return
	}
}

func (s *StagingChunk) GetObject(inodeMgr *InodeMgr, reader *BufferedDiskPageReader, fetchOffset int64) error {
	begin := time.Now()
	var res *GetBlobOutput
	var err, awsErr error
	key := ""
	if s.fetchKey != "" && s.length > 0 {
		key = s.fetchKey
		param := &GetBlobInput{Key: key, Start: uint64(fetchOffset + s.slop), Count: uint64(s.length), IfMatch: nil}
		res, awsErr = inodeMgr.back.getBlob(param)
		err = mapAwsError(awsErr)
	} else {
		awsErr = unix.ENOENT
		err = unix.ENOENT
	}
	// NOTE: getBlob does not return nano-sec timestamp. use headBlob to get correct timestmap.
	get := time.Now()
	var copied = false

	// Note: reader.alignLeft should be zero at this moment, but we consider it just in case.
	// GetSlicedPageBufferAt will ignore [0, alignLeft]. so, all we need to do is just skip the range.
	if err != nil && err != fuse.ENOENT && err != unix.EFAULT {
		log.Debugf("Failed: GetObject, getBlob, fetchKey=%v, offset=%v, slop=%v, length=%v, awsErr=%v, err=%v",
			key, fetchOffset, s.slop, s.length, awsErr, err)
		return err
	} else if err == fuse.ENOENT || err == unix.EFAULT {
		log.Debugf("GetObject, fill zero, fetchKey=%v, offset=%v, slop=%v, length=%v, awsErr=%v, err=%v",
			key, fetchOffset, s.slop, s.length, awsErr, err)
		for i := reader.alignLeft; i < s.length && i < int64(len(reader.buf.Buf)); i++ {
			reader.buf.Buf[i] = 0
		}
	} else {
		lastOffset := uint64(reader.alignLeft) + res.Size
		if lastOffset > uint64(len(reader.buf.Buf)) {
			lastOffset = uint64(len(reader.buf.Buf))
		}
		var count = uint64(0)
		for count < lastOffset {
			c, err := res.Body.Read(reader.buf.Buf[uint64(reader.alignLeft)+count : lastOffset])
			if err != nil {
				if err == io.EOF {
					count += uint64(c) // Note: Body.Read copies and returns c on EOF error...
					break
				}
				log.Errorf("Failed: GetObject, Read, err=%v", err)
				return err
			}
			count += uint64(c)
		}
		reader.buf.Buf = reader.buf.Buf[:uint64(reader.alignLeft)+count]
		copied = true
	}
	reader.buf.Up() // block auto-eviction during logging and replication
	go s.AppendToLog(inodeMgr, reader, key)
	c := time.Now()
	if copied {
		log.Infof("Success: GetObject, fetchKey=%v, offset=%v, slop=%v, logOffset=%v, length=%v, get=%v, copy=%v",
			key, fetchOffset, s.slop, s.logOffset, s.length, get.Sub(begin), c.Sub(get))
	}
	return nil
}

type WorkingChunk struct {
	chunkVer uint32
	stags    *btree.BTree

	chunkPtr *Chunk
	prevVer  *WorkingChunk
	nextVer  *WorkingChunk
}

func (c *WorkingChunk) Copy(chunkVer uint32) *WorkingChunk {
	copied := c.chunkPtr.NewWorkingChunk(chunkVer)
	copied.stags = c.stags.Clone()
	//c.stags.DeletePool() // should we reuse old pools?
	return copied
}

func (c *WorkingChunk) AddNewStag(raft *RaftInstance, backingKey string, offset int64, updateType byte, objectSize int64, chunkSize int64) int32 {
	// must be protected by working.lock
	slop := offset % chunkSize
	length := objectSize - offset
	if length > chunkSize {
		length = chunkSize
	}
	logId := c.chunkPtr.GetLogId()
	logOffset := raft.extLogger.ReserveRange(logId, length)
	filled := int32(1)
	if backingKey != "" {
		filled = 0
	}
	newStag := NewStagingChunk(slop, length, updateType, logOffset, backingKey, filled)
	c.AddStag(newStag)
	log.Debugf("AddNewStag, fetchKey=%v, offset=%v, size=%v, chunkSize=%v, logId=%v, logOffset=%v, length=%v",
		backingKey, offset, objectSize, chunkSize, logId, logOffset, length)
	return RaftReplyOk
}

func (c *WorkingChunk) AddNewStagFromMsg(l *common.StagingChunkAddMsg) {
	newStag := NewStagingChunk(l.GetSlop(), l.GetLength(), l.GetUpdateType()[0], l.GetLogOffset(), l.GetFetchKey(), l.GetFilled())
	c.AddStag(newStag)
}

func (c *WorkingChunk) AddStagingChunkFromAddMsg(cLog *common.WorkingChunkAddMsg) {
	for _, stag := range cLog.Stagings {
		c.AddNewStagFromMsg(stag)
	}
}

func (c *WorkingChunk) validate(_ *StagingChunk, _ int) bool {
	/*	var i = 0
		s.stags.Ascend(func (b btree.Item) bool {
			p := b.(*StagingChunkPart)
			log.Infof("part %d: slop=%v, length=%v, lastOffset=%v, isDeleting=%v", i, p.slop, p.length, p.LastOffset(), p.working.isDeleting)
			i += 1
			return true
		})
		log.Infof("Insert %d: slop=%v, length=%v, lastOffset=%v", pathIndex, stag.slop, stag.length, stag.slop + stag.length)*/
	return true
}

// NOTE: try reusing old instances to reduce GC
func (c *WorkingChunk) addPartSlowPath(stag *StagingChunk) {
	newLast := stag.slop + stag.length
	tempPart := StagingChunkPart{slop: stag.slop}
	var nextSlop = stag.slop
	var p *StagingChunkPart = nil
	c.stags.DescendLessOrEqual(&tempPart, func(b btree.Item) bool {
		if p == nil {
			p = b.(*StagingChunkPart)
			return true
		}
		return false
	})
	var added = false
	if p != nil {
		pLast := p.LastOffset()
		if pLast == stag.slop && atomic.LoadInt32(&stag.filled) == 1 &&
			p.chunk.slop == p.slop && p.chunk.length == p.length && p.chunk.logOffset+p.length == stag.logOffset {
			stag.logOffset = p.chunk.logOffset
			stag.length += p.length
			stag.slop = p.slop
			added = true
		}
		if p.slop == stag.slop {
			oldChunk := p.chunk
			c.replace(stag, p)
			added = true
			if newLast < pLast {
				// pattern A
				// p:     --|===|--
				// stag:  --*=*----
				// split: ----|=|--
				c.addPart(c.NewStagingChunkPart(pLast-newLast, oldChunk, newLast))
				return
			}
			// pattern B
			// p:     --|===|--
			// stag:  --*======
			// split: ---------
			// do nothing here
			// TODO: add oldChunk to freeList
		} else if stag.slop < pLast {
			p.length = stag.slop - p.slop

			if newLast < pLast {
				// pattern C
				// p:     --|=======|--
				// stag:  ----*==*-----
				// split: --|=|--|==|--
				c.addPart(c.NewStagingChunkPart(pLast-newLast, p.chunk, newLast)) // new part == latter split
				c.addNewPart(stag)
				return
			}
			// pattern D
			// p:     --|====|--
			// stag:  ----*=====
			// split: --|=|-----
			// nothing to do
		}
		// pattern E
		// p:    --|===|------
		// stag: ---------*===
		// or pattern B or D
		nextSlop = pLast
	}

	for nextSlop < newLast {
		tempPart.slop = nextSlop
		p = nil
		c.stags.AscendGreaterOrEqual(&tempPart, func(b btree.Item) bool {
			p = b.(*StagingChunkPart)
			return false
		})
		if p == nil {
			break
		}
		pLast := p.LastOffset()
		if p.slop <= newLast {
			c.stags.Delete(p)
			if pLast <= newLast {
				// p:     --|===|---
				// stag:  =======*--
				// split: ----------
				// TODO: add p to freeList
			} else {
				// p:     --|====|--
				// stag:  =====*----
				// split: -----|=|-
				p.length = pLast - newLast
				p.slop = newLast
				c.addPart(p)
			}
		}
		nextSlop = pLast
	}
	if !added {
		c.addNewPart(stag)
	}
}

func (c *WorkingChunk) addPart(newPart *StagingChunkPart) {
	c.stags.ReplaceOrInsert(newPart) //replaced := c.stags.ReplaceOrInsert(newPart)
	//if replaced != nil {
	//TODO: add freeList
	//}
}

func (c *WorkingChunk) replace(stag *StagingChunk, old *StagingChunkPart) {
	if old.slop == stag.slop {
		old.chunk = stag
		old.length = stag.length
	} else {
		log.Warnf("BUG: replace with different slop, old=%v, stag=%v", *old, *stag)
		c.addNewPart(stag)
	}
}

func (c *WorkingChunk) addNewPart(stag *StagingChunk) {
	c.addPart(c.NewStagingChunkPart(stag.length, stag, stag.slop))
}

func (c *WorkingChunk) deletePartAll() {
	c.stags.Clear(true)
}

func (c *WorkingChunk) Head() *StagingChunkPart {
	return c.stags.Get(c.stags.Min()).(*StagingChunkPart)
}

func (c *WorkingChunk) Tail() *StagingChunkPart {
	return c.stags.Get(c.stags.Max()).(*StagingChunkPart)
}

func (c *WorkingChunk) NewStagingChunkPart(length int64, stag *StagingChunk, slop int64) *StagingChunkPart {
	part := &StagingChunkPart{}
	part.length = length
	part.chunk = stag
	part.slop = slop
	return part
}

// AddStag
// must be in a critical section
func (c *WorkingChunk) AddStag(stag *StagingChunk) {
	if c.stags.Len() > 0 && stag.length == 0 {
		return
	}
	if c.stags.Len() == 0 {
		c.addNewPart(stag)
		c.validate(stag, 0)
		return
	}
	head := c.Head()
	tail := c.Tail()
	if head.length == 0 {
		c.deletePartAll()
	}
	// fast paths
	stagLast := stag.slop + stag.length
	if stag.slop <= head.slop && tail.LastOffset() <= stagLast {
		c.deletePartAll()
	}
	if c.stags.Len() == 0 || tail.LastOffset() < stag.slop || stagLast < head.slop { // tail.LastOffset() == stag.slop is handled by slowPath to reuse tail object
		c.addNewPart(stag)
		c.validate(stag, 1)
		return
	}
	// slow path
	c.addPartSlowPath(stag)
	c.validate(stag, 2)
}

func (c *WorkingChunk) findPart(curSlop int64) *StagingChunkPart {
	var p *StagingChunkPart = nil
	tempPart3 := StagingChunkPart{slop: curSlop}
	c.stags.DescendLessOrEqual(&tempPart3, func(b btree.Item) bool {
		q := b.(*StagingChunkPart)
		if q.slop <= curSlop && curSlop < q.LastOffset() && q.chunk.updateType == StagingChunkData {
			p = q
		}
		return false
	})
	if p != nil {
		return p
	}
	c.stags.AscendGreaterOrEqual(&tempPart3, func(b btree.Item) bool {
		p = b.(*StagingChunkPart)
		return p.chunk.updateType != StagingChunkData
	})
	if p != nil && p.chunk.updateType != StagingChunkData {
		return nil
	}
	return p
}

func (c *WorkingChunk) getSlice(inodeMgr *InodeMgr, ptr *StagingChunkPart, offset int64, blocking bool) (slice SlicedPageBuffer, err error) {
	stag := ptr.chunk
	logId := c.chunkPtr.GetLogId()
	if blocking {
		if reader := inodeMgr.readCache.GetCacheWithFillWait(logId, ptr, offset); reader != nil {
			slice, err = reader.GetSlicedPageBufferAt(ptr, offset, inodeMgr.readCache.dec)
			if err != nil {
				inodeMgr.readCache.SetEvictable(reader)
			}
			return
		}
	} else {
		reader, beginFill := inodeMgr.readCache.GetCacheOrBeginFill(logId, ptr.chunk.logOffset)
		if reader != nil { // cache exists
			slice, err = reader.GetSlicedPageBufferAt(ptr, offset, inodeMgr.readCache.dec)
			if err != nil {
				inodeMgr.readCache.SetEvictable(reader)
			}
			return
		} else if !beginFill { // someone is filling
			return SlicedPageBuffer{}, unix.EAGAIN
		}
		// cache does not exist and no one is filling
		// this thread starts filling (probably give up at next page allocation due to insufficient memory)
	}

	var reader *BufferedDiskPageReader
	reader, err = inodeMgr.readCache.GetNewBufferedDiskPageReader(logId, stag, blocking)
	if err != nil {
		if blocking || (!blocking && err != unix.EAGAIN) {
			log.Errorf("Failed: WorkingChunk.getSlice, GetPageBuffer, length=%v, err=%v", stag.length, err)
		}
		inodeMgr.readCache.EndFill(logId, stag.logOffset)
		return SlicedPageBuffer{}, err
	}
	needDownload := atomic.LoadInt32(&stag.filled) == 0
	if needDownload {
		err = stag.GetObject(inodeMgr, reader, c.chunkPtr.offset)
	} else {
		err = stag.ReadObject(inodeMgr, reader)
	}
	if err != nil {
		inodeMgr.readCache.ReleaseInFlightBuffer(reader)
		inodeMgr.readCache.EndFill(logId, stag.logOffset)
		log.Errorf("Failed: WorkingChunk.getSlice, Get/ReadObject, download=%v, bsaeOffset=%v, reply=%v",
			needDownload, c.chunkPtr.offset, err)
		return SlicedPageBuffer{}, err
	}
	slice, err = reader.GetSlicedPageBufferAt(ptr, offset, inodeMgr.readCache.dec)
	if err != nil {
		inodeMgr.readCache.ReleaseInFlightBuffer(reader)
		inodeMgr.readCache.EndFill(logId, stag.logOffset)
	} else {
		reader.buf.Up()
		// Note: reader.buf.Buf.refCount = 1 or 2 at this moment.
		// So, reader will not be auto-evicted before 1) AppendToLog's SetEvictable() and 2) caller's slice.SetEvictable()
		inodeMgr.readCache.EndFillWithPut(reader)
	}
	return
}

func (c *WorkingChunk) WriteToNext(inodeMgr *InodeMgr, w io.Writer, offset int64, blocking bool) (int64, error) {
	ptr := c.findPart(offset)
	if ptr == nil {
		return 0, io.EOF
	}
	if offset < ptr.slop {
		buf := []byte{0}
		var i = int64(0)
		for i < ptr.slop-offset {
			c, err := w.Write(buf)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Errorf("Failed: WorkingChunk.WriteToNext, Write (0), err=%v", err)
				return i, err
			}
			i += int64(c)
		}
		log.Debugf("WorkingChunk.WriteToNext, skip, ptr.slop==%v, offset=%v, length=%v", ptr.slop, offset, i)
		return i, nil
	}
	slice, err := c.getSlice(inodeMgr, ptr, offset, blocking)
	if err != nil {
		if (blocking && err != io.EOF) || (!blocking && err != io.EOF && err != unix.EAGAIN) {
			log.Errorf("Failed: WorkingChunk.WriteToNext, getSlice, offset=%v, err=%v", offset, err)
		}
		return 0, err
	}
	copied, err := w.Write(slice.Buf)
	slice.SetEvictable()
	log.Debugf("WorkingChunk.WriteToNext, WriteTo, copied=%v, err=%v", copied, err)
	if err != nil {
		if err != io.EOF {
			log.Errorf("Failed: WorkingChunk.WriteToNext, WriteTo, err=%v", err)
		}
		return 0, err
	}
	return int64(copied), nil
}

func (c *WorkingChunk) ReadNext(inodeMgr *InodeMgr, p []byte, offset int64, blocking bool) (int64, error) {
	ptr := c.findPart(offset)
	if ptr == nil {
		return 0, io.EOF
	}
	if offset < ptr.slop {
		var i = int64(0)
		for ; i < ptr.slop-offset && i < int64(len(p)); i++ {
			p[i] = 0
		}
		log.Debugf("ReadNext, skip, ptr.slop==%v, offset=%v, length=%v", ptr.slop, offset, i)
		return i, nil
	}
	slice, err := c.getSlice(inodeMgr, ptr, offset, blocking)
	if err != nil {
		if (blocking && err != io.EOF) || (!blocking && err != io.EOF && err != unix.EAGAIN) {
			log.Errorf("Failed: ReadNext, getSlice, offset=%v, err=%v", offset, err)
		}
		return 0, err
	}
	copied := copy(p, slice.Buf)
	slice.SetEvictable()
	if copied == 0 {
		return 0, io.EOF
	}
	return int64(copied), err
}

func (c *WorkingChunk) GetNext(inodeMgr *InodeMgr, offset int64, blocking bool) (buf SlicedPageBuffer, err error) {
	ptr := c.findPart(offset)
	if ptr == nil {
		return SlicedPageBuffer{}, io.EOF
	}
	if offset < ptr.slop {
		log.Debugf("GetNext, skip, ptr.slop==%v, offset=%v, length=%v", ptr.slop, offset, ptr.slop-offset)
		return NewBlankSlicedPageBuffer(ptr.slop - offset), nil
	}
	ret, err := c.getSlice(inodeMgr, ptr, offset, blocking)
	if err != nil {
		if (blocking && err != io.EOF) || (!blocking && err != io.EOF && err != unix.EAGAIN) {
			log.Errorf("Failed: ReadNext, getSlice, offset=%v, err=%v", offset, err)
		}
		return SlicedPageBuffer{}, err
	}
	// ClientBufferMgr will release slice
	return ret, err
}

// Size
// must hold lock
func (c *WorkingChunk) Size() int64 {
	p := c.LastNonDeletedPtr()
	if p != nil {
		return p.LastOffset()
	}
	return 0
}

func (c *WorkingChunk) LastNonDeletedPtr() *StagingChunkPart {
	var p *StagingChunkPart
	c.stags.Descend(func(b btree.Item) bool {
		p = b.(*StagingChunkPart)
		return p.chunk.updateType == StagingChunkDelete
	})
	if p.chunk.updateType == StagingChunkDelete {
		return nil
	}
	return p
}

func (c *WorkingChunk) Prefetch(inodeMgr *InodeMgr) {
	logId := c.chunkPtr.GetLogId()
	c.stags.Ascend(func(b btree.Item) bool {
		stag := b.(*StagingChunkPart).chunk
		if atomic.LoadInt32(&stag.filled) != 0 {
			return true
		}
		if beginFill := inodeMgr.readCache.TryBeginFill(logId, stag.logOffset); !beginFill {
			// give up immediately if someone else already has filled or been filling the target chunk
			return true
		}
		reader, err := inodeMgr.readCache.GetNewBufferedDiskPageReader(logId, stag, false)
		if err != nil {
			// give up immediately if we have no memory
			inodeMgr.readCache.EndFill(logId, stag.logOffset)
			if err != unix.EAGAIN {
				log.Errorf("Failed: Prefetch, GetPageBufferNonBlocking, length=%v, err=%v", stag.length, err)
			}
			return false
		}
		err = stag.GetObject(inodeMgr, reader, c.chunkPtr.offset)
		if err != nil {
			inodeMgr.readCache.ReleaseInFlightBuffer(reader)
			inodeMgr.readCache.EndFill(logId, stag.logOffset)
			log.Errorf("Failed: Prefetch, GetObject, err=%v", err)
			return false
		}
		inodeMgr.readCache.EndFillWithPut(reader)
		return true
	})
}

func (c *WorkingChunk) toStagingChunkAddMsg() []*common.StagingChunkAddMsg {
	c.chunkPtr.fillLock.Lock()
	defer c.chunkPtr.fillLock.Unlock()
	ret := make([]*common.StagingChunkAddMsg, 0)
	c.stags.Ascend(func(b btree.Item) bool {
		chunk := b.(*StagingChunkPart).chunk
		ret = append(ret, &common.StagingChunkAddMsg{
			Slop:       chunk.slop,
			Length:     chunk.length,
			UpdateType: []byte{chunk.updateType},
			LogOffset:  chunk.logOffset,
			FetchKey:   chunk.fetchKey,
			Filled:     chunk.filled,
		})
		return true
	})
	return ret
}

var AccessLinkHead = Chunk{}
var AccessLinkLock sync.Mutex

func InitAccessLinkHead() {
	AccessLinkHead.accessLinkNext = &AccessLinkHead
	AccessLinkHead.accessLinkPrev = &AccessLinkHead
}

func (c *Chunk) UpdateLRUList() {
	AccessLinkLock.Lock()
	if c.accessLinkNext != nil {
		c.accessLinkPrev.accessLinkNext = c.accessLinkNext
		c.accessLinkNext.accessLinkPrev = c.accessLinkPrev
	}
	c.accessLinkPrev = &AccessLinkHead
	c.accessLinkNext = AccessLinkHead.accessLinkNext
	AccessLinkHead.accessLinkNext.accessLinkPrev = c
	AccessLinkHead.accessLinkNext = c
	AccessLinkLock.Unlock()
}

func (c *WorkingChunk) DeleteFromVersionListNoLock() {
	c.prevVer.nextVer = c.nextVer
	c.nextVer.prevVer = c.prevVer
	c.prevVer = c
	c.nextVer = c
}

func (c *Chunk) DeleteFromLRUListNoLock() {
	c.accessLinkPrev.accessLinkNext = c.accessLinkNext
	c.accessLinkNext.accessLinkPrev = c.accessLinkPrev
	c.accessLinkPrev = c
	c.accessLinkNext = c
}

func CollectLRUChunks(dirtyMgr *DirtyMgr, raft *RaftInstance, reclaimDiskBytes int64) (keys []uint64, offsets []int64) {
	total := int64(0)
	keys = make([]uint64, 0)
	offsets = make([]int64, 0)
	AccessLinkLock.Lock()
	for c := AccessLinkHead.accessLinkPrev; c != &AccessLinkHead && total < reclaimDiskBytes; c = c.accessLinkPrev {
		if !dirtyMgr.IsDirtyChunk(c) {
			total += raft.extLogger.GetSize(c.GetLogId())
			keys = append(keys, uint64(c.inodeKey))
			offsets = append(offsets, c.offset)
		}
	}
	AccessLinkLock.Unlock()
	return
}

func CollectLRUDirtyKeys(dirtyMgr *DirtyMgr, raft *RaftInstance, reclaimDiskBytes int64) (keys map[InodeKeyType]bool) {
	now := time.Now()
	total := int64(0)
	keys = make(map[InodeKeyType]bool)
	AccessLinkLock.Lock()
	for c := AccessLinkHead.accessLinkPrev; c != &AccessLinkHead && total < reclaimDiskBytes; c = c.accessLinkPrev {
		// TOFIX: regard writes within the last <100ms as on-going writes and ignore them to avoid thrashing
		if dirtyMgr.IsDirtyChunk(c) && now.Sub(c.lastAccess) > time.Millisecond*100 {
			size := raft.extLogger.GetSize(c.GetLogId())
			if size > 0 {
				total += size
				keys[c.inodeKey] = true
			}
		}
	}
	AccessLinkLock.Unlock()
	return keys
}

// ChunkReader
// must hold lock for stags
type ChunkReader struct {
	offset     int64
	lastOffset int64
	chunk      *WorkingChunk
	inodeMgr   *InodeMgr
	blocking   bool
}

func (c *WorkingChunk) GetReader(chunkSize int64, objectSize int64, offset int64, inodeMgr *InodeMgr, blocking bool) *ChunkReader {
	// cannot use working.stags.size and working.stags.chunkSize since working may be generated by an old *WorkingMeta
	// this happens if the old *WorkingMeta is deleted and/or skipped with randomly write at another working
	c.chunkPtr.lock.RLock()
	beginOffsetInChunk := offset % chunkSize
	lastOffset := objectSize - (offset - beginOffsetInChunk)
	if lastOffset > chunkSize {
		lastOffset = chunkSize
	}
	if beginOffsetInChunk > lastOffset {
		beginOffsetInChunk = lastOffset
	}
	r := &ChunkReader{
		offset:     beginOffsetInChunk,
		lastOffset: lastOffset,
		chunk:      c,
		inodeMgr:   inodeMgr,
		blocking:   blocking,
	}
	c.chunkPtr.lock.RUnlock()
	return r
}

// used when reading chunks
func (r *ChunkReader) Read(p []byte) (int, error) {
	var bufOff = int64(0)
	var err error
	rOffset := r.offset
	for bufOff < int64(len(p)) && rOffset < r.lastOffset {
		var copied int64
		copied, err = r.chunk.ReadNext(r.inodeMgr, p[bufOff:], rOffset, r.blocking)
		if err != nil {
			if err == io.EOF {
				break
			}
			if r.blocking || err != unix.EAGAIN {
				log.Errorf("Failed: ChunkReader.Read, ReadNext, offset=%v, err=%v", r.offset, err)
			}
			return 0, err
		}
		rOffset += copied
		bufOff += copied
	}
	//bufOff2 := bufOff
	for ; bufOff < int64(len(p)) && rOffset < r.lastOffset; bufOff++ {
		p[bufOff] = 0
		rOffset += 1
	}
	/*if bufOff2 != bufOff {
		log.Infof("ChunkReader.Read, fill zero, offset_from=%v, offset_to=%v, bufOff_from=%v, bufOff_to=%v, count=%v", r.offset, rOffset, bufOff2, bufOff, bufOff-bufOff2)
	}*/
	if bufOff == 0 {
		return 0, io.EOF
	}
	r.offset = rOffset
	return int(bufOff), nil
}

func (r *ChunkReader) GetBufferZeroCopy(size int) (bufs []SlicedPageBuffer, count int, err error) {
	rOffset := r.offset
	lastOffset := r.lastOffset
	if rOffset == lastOffset {
		return nil, 0, io.EOF
	} else if rOffset+int64(size) < lastOffset {
		lastOffset = rOffset + int64(size)
	}
	count = 0
	for rOffset < lastOffset {
		var buf SlicedPageBuffer
		buf, err = r.chunk.GetNext(r.inodeMgr, rOffset, r.blocking)
		if err != nil {
			if err == io.EOF {
				break
			}
			if r.blocking || err != unix.EAGAIN {
				log.Errorf("Failed: ChunkReader.GetBufferZeroCopy, GetNext, rOffset=%v, err=%v", rOffset, err)
			}
			for _, b := range bufs {
				b.SetEvictable()
			}
			return nil, 0, err
		}
		c := len(buf.Buf)
		if c == 0 {
			break
		}
		if int64(c)+rOffset >= lastOffset {
			c = int(lastOffset - rOffset)
			buf.Slice(0, lastOffset-rOffset)
		}
		rOffset += int64(c)
		bufs = append(bufs, buf)
		count += c
	}
	if rOffset < lastOffset {
		bufs = append(bufs, NewBlankSlicedPageBuffer(lastOffset-rOffset))
		count += int(lastOffset - rOffset)
	}
	if count == 0 {
		return nil, 0, io.EOF
	}
	r.offset = lastOffset
	return bufs, count, nil
}

// WriteTo
// used when persisting chunks
func (r *ChunkReader) WriteTo(w io.Writer) (n int64, err error) {
	var bufOff = int64(0)
	rOffset := r.offset
	for rOffset < r.lastOffset {
		var copied int64
		copied, err = r.chunk.WriteToNext(r.inodeMgr, w, rOffset, r.blocking)
		if err != nil {
			if err == io.EOF {
				break
			}
			if r.blocking || err != unix.EAGAIN {
				log.Errorf("Failed: ChunkReader.WriteTo, WriteToNext, rOffset=%v, err=%v", rOffset, err)
			}
			return 0, err
		}
		rOffset += copied
		bufOff += copied
	}
	if rOffset < r.lastOffset {
		buf := []byte{0}
		for rOffset < r.lastOffset {
			c, err2 := w.Write(buf)
			if err2 != nil {
				if err2 != io.EOF {
					log.Errorf("Failed: ChunkReader.WriteTo, Write, err=%v", err2)
				}
				break
			}
			bufOff += int64(c)
			rOffset += int64(c)
		}
	}
	//log.Infof("ChunkReader.WriteTo, r.offset=%v, r.lastOffset=%v, bufOff=%v, rOffset=%v", r.offset, r.lastOffset, bufOff, rOffset)
	if bufOff == 0 {
		return 0, io.EOF
	}
	r.offset = rOffset
	return bufOff, nil
}

func (r *ChunkReader) Seek(offset int64, whence int) (int64, error) {
	var newOff = r.offset
	if whence == io.SeekStart {
		newOff = offset
	} else if whence == io.SeekCurrent {
		newOff += offset
	} else if whence == io.SeekEnd {
		newOff = r.lastOffset + offset
	} else {
		return 0, fuse.EINVAL
	}
	if newOff < 0 {
		return 0, fuse.EINVAL
	}
	r.offset = newOff
	return newOff, nil
}

func (r *ChunkReader) Close() (err error) {
	return
}

func (r *ChunkReader) IsSeeker() bool {
	return true
}

func (r *ChunkReader) HasLen() (int, bool) {
	ret := int(r.lastOffset - r.offset)
	if ret > 0 {
		return ret, true
	}
	return 0, false
}

func (r *ChunkReader) GetLen() (int64, error) {
	ret := r.lastOffset - r.offset
	if ret >= 0 {
		return ret, nil
	}
	return -1, nil
}
