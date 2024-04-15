/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"container/list"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/IBM/objcache/common"
	"golang.org/x/sys/unix"
)

type PagePool struct {
	bufs  []*PageBuffer
	lock  *sync.RWMutex
	size  int
	index int
}

func NewPagePool(size int, maxNrElements int) *PagePool {
	if int64(size)%PageSize != 0 {
		log.Errorf("Failed: NewPagePool, size (%v) must be a multiple of page size (%v)", size, PageSize)
		return nil
	}
	return &PagePool{
		size: size, lock: new(sync.RWMutex), bufs: make([]*PageBuffer, int64(size)/PageSize),
	}
}

func (p *PagePool) Get() (*PageBuffer, error) {
	p.lock.Lock()
	if p.index == 0 {
		p.lock.Unlock()
		data, err := unix.Mmap(0, 0, p.size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
		if err != nil {
			log.Errorf("Failed: PagePool.Get, Mmap, size=%v, err=%v", p.size, err)
			return nil, err
		}
		return &PageBuffer{Buf: data}, nil
	}
	p.index -= 1
	buf := p.bufs[p.index]
	p.bufs[p.index] = nil
	p.lock.Unlock()
	return buf, nil
}

func (p *PagePool) Put(buf *PageBuffer) {
	if buf == nil || buf.Buf == nil {
		return
	}
	p.lock.Lock()
	if p.index == len(p.bufs) {
		m := buf.Buf
		buf.Buf = nil
		p.lock.Unlock()
		if err := unix.Munmap(m); err != nil {
			log.Errorf("Failed (ignore): PagePool.Put, Munmap, buf=%v, err=%v", m, err)
		}
		return
	}
	p.bufs[p.index] = buf
	if p.index > 0 && p.bufs[p.index-1] == buf {
		log.Errorf("Oops")
	}
	p.index += 1
	p.lock.Unlock()
}

func (p *PagePool) Reset() {
	p.lock.Lock()
	bufs := p.bufs
	index := p.index
	p.index = 0
	p.bufs = make([]*PageBuffer, len(bufs))
	p.lock.Unlock()
	for _, buf := range bufs[:index] {
		if err := unix.Munmap(buf.Buf); err != nil {
			log.Errorf("Failed (ignore): PagePool.Reset, Munmap, buf=%v, err=%v", buf.Buf, err)
		}
	}
}

func (p *PagePool) CheckReset() (ok bool) {
	if ok = p.lock.TryLock(); !ok {
		log.Errorf("PagePool.CheckReset: p.lock is taken")
		return
	}
	if ok2 := len(p.bufs) == 0; !ok2 {
		log.Errorf("PagePool.CheckReset: len(p.bufs) > 0")
		ok = false
	}
	if ok2 := p.index == 0; !ok2 {
		log.Errorf("PagePool.CheckReset: p.index > 0")
		ok = false
	}
	p.lock.Unlock()
	return
}

type MemoryPool struct {
	pages        map[int64]*PagePool
	lock         *sync.RWMutex
	getPageCount int64
	putPageCount int64
}

var MemPool *MemoryPool

func InitMemoryPool() {
	if MemPool != nil {
		MemPool.Reset()
	} else {
		MemPool = &MemoryPool{pages: make(map[int64]*PagePool), lock: new(sync.RWMutex)}
	}
}

func (m *MemoryPool) GetPages(nrPages int64) (buf *PageBuffer, err error) {
	if nrPages == 0 {
		log.Errorf("Failed: GetPages, nrPages must be > 0")
		return nil, unix.EINVAL
	}
	size := nrPages * PageSize
	m.lock.RLock()
	pool, ok := m.pages[size]
	m.lock.RUnlock()
	if ok {
		return pool.Get()
	}
	m.lock.Lock()
	pool, ok = m.pages[size]
	if ok {
		m.lock.Unlock()
		return pool.Get()
	}
	pool = NewPagePool(int(size), int((1024*1024*1024)/size))
	m.pages[size] = pool
	m.lock.Unlock()
	p, err := pool.Get()
	log.Debugf("MemoryPool.GetPages: new pages, size=%v", size)
	return p, err
}

func (m *MemoryPool) PutPages(buf *PageBuffer) {
	size := int64(cap(buf.Buf))
	if size == 0 {
		log.Errorf("Failed (ignore): PutPages, cap(buf.Buf) == 0, buf=%v, Buf=%x, refCount=%v", buf, uintptr(unsafe.Pointer(&buf.Buf)), atomic.LoadInt32(&buf.refCount))
		return
	}
	buf.Buf = buf.Buf[:size]
	m.lock.RLock()
	pool, ok := m.pages[size]
	m.lock.RUnlock()
	if ok {
		pool.Put(buf)
		return
	}
	m.lock.Lock()
	pool, ok = m.pages[size]
	if ok {
		m.lock.Unlock()
		pool.Put(buf)
		return
	}
	pool = NewPagePool(int(size), int((1024*1024*1024)/size))
	pool.Put(buf)
	m.pages[size] = pool
	log.Debugf("MemoryPool.PutPages: new pages, size=%v", size)
	m.lock.Unlock()
}

func (m *MemoryPool) Reset() {
	m.lock.Lock()
	pages := m.pages
	m.pages = make(map[int64]*PagePool)
	m.putPageCount = 0
	m.getPageCount = 0
	m.lock.Unlock()
	for _, pool := range pages {
		pool.Reset()
	}
}

func (m *MemoryPool) CheckReset() (ok bool) {
	if ok = m.lock.TryLock(); !ok {
		log.Errorf("Failed: MemoryPool.CheckReset, m.lock is taken")
		return
	}
	if ok2 := len(m.pages) == 0; !ok2 {
		log.Errorf("Failed: MemoryPool.CheckReset, len(m.pages) != 0")
		ok = false
	}
	if ok2 := m.putPageCount == 0; !ok2 {
		log.Errorf("Failed: MemoryPool.CheckReset, m.putPageCount != 0")
		ok = false
	}
	if ok2 := m.getPageCount == 0; !ok2 {
		log.Errorf("Failed: MemoryPool.CheckReset, m.getPageCount != 0")
		ok = false
	}
	m.lock.Unlock()
	return
}

func GetPageBuffer(size int64) (*PageBuffer, error) {
	nrPages := (size + PageSize - 1) / PageSize
	atomic.AddInt64(&MemPool.getPageCount, 1)
	buf, err := MemPool.GetPages(nrPages)
	if err != nil {
		return nil, err
	}
	buf.Buf = buf.Buf[:size]
	return buf, nil
}

var TotalDown int32

func ReturnPageBuffer(buf *PageBuffer) {
	atomic.AddInt64(&MemPool.putPageCount, 1)
	MemPool.PutPages(buf)
	atomic.AddInt32(&TotalDown, 1)
}

type PageBuffer struct {
	Buf      []byte
	refCount int32
}

var TotalUp int32

// Up increments refCount to prevent Buf from begin released unexpectedly.
func (p *PageBuffer) Up() {
	/*count := */
	atomic.AddInt32(&p.refCount, 1)
	atomic.AddInt32(&TotalUp, 1)
	//log.Debugf("PageBuffer.Up: buf=%v, count=%v->%v", unsafe.Pointer(p), count-1, count)
}

func (p *PageBuffer) Down() (count int32) {
	count = atomic.AddInt32(&p.refCount, -1)
	atomic.AddInt32(&TotalDown, 1)
	return count
}

func (p *PageBuffer) IsEvictable() bool {
	return atomic.LoadInt32(&p.refCount) == 0
}

func __returnPageBuffer(orig interface{}) {
	ReturnPageBuffer(orig.(*PageBuffer))
}

func (p *PageBuffer) AsSlice() SlicedPageBuffer {
	return SlicedPageBuffer{Buf: p.Buf, orig: p, dec: __returnPageBuffer}
}

type SlicedPageBuffer struct {
	Buf  []byte
	orig interface{}
	dec  func(orig interface{})
}

func (p *SlicedPageBuffer) SetEvictable() {
	p.dec(p.orig)
	p.Buf = nil
	p.dec = nil // cause crash at more than two calls of SetEvictable
	p.orig = nil
}

func (p *SlicedPageBuffer) Slice(begin int64, last int64) {
	p.Buf = p.Buf[begin:last]
}

func decNoOp(interface{}) {}

func NewBlankSlicedPageBuffer(size int64) SlicedPageBuffer {
	return SlicedPageBuffer{Buf: make([]byte, size), orig: nil, dec: decNoOp}
}

/**
 * Buffer layout:
 *     bf     o <------ dataLen ------>
 * ----|------|------------------------|
 *     <- al ->
 *     <------------ len(buf.Buf) ----->
 * original request is (offset o, dataLen) but we need to align SectorSize for offset and size.
 * BufferedDiskPageReader must still return the range [o, o + dataLen] regardless of reading more than it.
 * bf: bufLogOffset is calculated by offset o and dataLen at prepareBuffer, aligned to be a multiple of SectorSize
 * al: alignLeft to align offset to be a multiple of SectorSize
 */
type BufferedDiskPageReader struct {
	buf          *PageBuffer
	logId        LogIdType
	bufLogOffset int64
	alignLeft    int64
	dataLen      int64
	prev         *BufferedDiskPageReader
	next         *BufferedDiskPageReader
}

func CalcRequiredPageBufferSize(offset int64, dataLen int64) int64 {
	alignLeft := offset % PageSize
	alignedSize := dataLen + alignLeft
	if alignedSize%PageSize != 0 {
		alignedSize = alignedSize - alignedSize%PageSize + PageSize
	}
	return alignedSize
}

func NewBufferedDiskPageReaderFromStag(page *PageBuffer, logId LogIdType, logOffset int64, dataLen int64) *BufferedDiskPageReader {
	r := &BufferedDiskPageReader{buf: page, logId: logId}
	r.next = r
	r.prev = r
	r.dataLen = dataLen
	r.alignLeft = logOffset % int64(SectorSize)
	r.bufLogOffset = logOffset - r.alignLeft
	return r
}

// GetSlicedPageBufferAt returns slice of Buf with ref count incremented. user must call .Release() later.
func (r *BufferedDiskPageReader) GetSlicedPageBufferAt(stagPart *StagingChunkPart, offset int64, dec func(interface{})) (SlicedPageBuffer, error) {
	stagPartSlop := stagPart.slop
	stag := stagPart.chunk

	/*
	 * a working is physically mapped to a log with varied offsets to optimize random writes.
	 * Suppose offset=12K is passed. we want to know a region marked as '*' in the below figure
	 *    8K             16K
	 * ---F=======|***|====F---- : log, (|***|: a region (offsetInLog, offsetInLog + lengthInLog)
	 *     \       \   \    \
	 *      \       \   \    \
	 *  -----D---X===|===X----D- : offset within a working (D-D : original stag, X=X: shrunk one by other writes)
	 *       10K 11K 12K 14K 18K
	 * 12K is an offset within a working, so, offsetInLog= 12K - 10K + 8K == 10K, lengthInLog = 14K - 12K
	 * 8K = stag.offset, 16K = stag.offset + stag.length
	 * 10K = stag.slop 11K = stagPartSlop 14K = stagPartSlop + stagPart.length, 18K = stag.slop + stag.length
	 */
	offsetInLog := offset - stag.slop + stag.logOffset
	lengthInLog := stagPartSlop + stagPart.length - offset
	//log.Debugf("prepareRead: logId=%v, offsetInLog=%v, lengthInLog=%v", reader.logId, offsetInLog, lengthInLog)

	bufSize := int64(len(r.buf.Buf))
	if offsetInLog < r.bufLogOffset+r.alignLeft || r.bufLogOffset+bufSize <= offsetInLog {
		log.Errorf("GetSlicedPageBufferAt, offset and dataLen is out of range, offsetInLog=%v, lengthInLog=%v, c=%v", offsetInLog, lengthInLog, 0)
		return SlicedPageBuffer{}, io.EOF
	}
	bufOff := offsetInLog - r.bufLogOffset
	lastOffset := bufOff + lengthInLog
	if lastOffset > bufSize {
		lastOffset = bufSize
	}
	return SlicedPageBuffer{Buf: r.buf.Buf[bufOff:lastOffset], orig: r, dec: dec}, nil
}

type FillingKey2 struct {
	logId  LogIdType
	offset int64
}

type ReaderBufferCache struct {
	cond         *sync.Cond
	lock         *sync.RWMutex
	cache        map[LogIdType]map[int64]*BufferedDiskPageReader
	filling      map[FillingKey2]*FillingInfo
	currentSize  int64
	inFlightSize int64
	maxSize      int64
	accessHead   BufferedDiskPageReader
}

func NewReaderBufferCache(flags *common.ObjcacheConfig) *ReaderBufferCache {
	r := &ReaderBufferCache{
		lock: new(sync.RWMutex), cond: sync.NewCond(new(sync.Mutex)), cache: make(map[LogIdType]map[int64]*BufferedDiskPageReader),
		maxSize: flags.ChunkCacheSizeBytes, filling: make(map[FillingKey2]*FillingInfo),
	}
	r.accessHead.prev = &r.accessHead
	r.accessHead.next = &r.accessHead
	return r
}

func (c *ReaderBufferCache) TryBeginFill(logId LogIdType, offset int64) (beginFill bool) {
	key := FillingKey2{logId: logId, offset: offset}
	c.lock.Lock()
	if _, ok := c.filling[key]; ok {
		c.lock.Unlock()
		return false
	}
	if offsets, ok := c.cache[logId]; ok {
		if _, ok2 := offsets[key.offset]; ok2 {
			//Note: this does not call c.deleteFromLRUList()
			c.lock.Unlock()
			return false
		}
	}
	c.filling[key] = &FillingInfo{filled: false, cond: sync.NewCond(new(sync.Mutex))}
	c.lock.Unlock()
	return true
}

func (c *ReaderBufferCache) GetCacheOrBeginFill(logId LogIdType, offset int64) (reader *BufferedDiskPageReader, beginFill bool) {
	key := FillingKey2{logId: logId, offset: offset}
	c.lock.Lock()
	if _, ok := c.filling[key]; ok {
		c.lock.Unlock()
		return nil, false
	}
	if offsets, ok := c.cache[logId]; ok {
		if cache, ok2 := offsets[key.offset]; ok2 {
			c.deleteFromLRUList(cache)
			cache.buf.Up()
			c.lock.Unlock()
			return cache, false
		}
	}
	c.filling[key] = &FillingInfo{filled: false, cond: sync.NewCond(new(sync.Mutex))}
	c.lock.Unlock()
	return nil, true
}

func (c *ReaderBufferCache) GetCacheWithFillWait(logId LogIdType, stagPart *StagingChunkPart, offset int64) *BufferedDiskPageReader {
	stag := stagPart.chunk
	key := FillingKey2{logId: logId, offset: stag.logOffset}
	c.lock.Lock()
	for {
		filling, ok := c.filling[key]
		if ok {
			// someone is filling the target buffer
			c.lock.Unlock()
			filling.cond.L.Lock()
			for !filling.filled {
				filling.cond.Wait()
			}
			filling.cond.L.Unlock()
			c.lock.Lock()
		} else {
			break
		}
	}
	if offsets, ok := c.cache[logId]; ok {
		if cache, ok2 := offsets[stag.logOffset]; ok2 {
			c.deleteFromLRUList(cache)
			cache.buf.Up()
			c.lock.Unlock()
			return cache
		}
	}
	c.filling[key] = &FillingInfo{filled: false, cond: sync.NewCond(new(sync.Mutex))}
	c.lock.Unlock()
	return nil
}

func (c *ReaderBufferCache) EndFill(logId LogIdType, offset int64) {
	key := FillingKey2{logId: logId, offset: offset}
	c.lock.Lock()
	filling, ok := c.filling[key]
	if ok {
		delete(c.filling, key)
	}
	c.lock.Unlock()
	if ok {
		filling.cond.L.Lock()
		filling.filled = true
		filling.cond.Broadcast()
		filling.cond.L.Unlock()
	}
}

func (c *ReaderBufferCache) EndFillWithPut(reader *BufferedDiskPageReader) {
	logId := reader.logId
	logOffset := reader.bufLogOffset + reader.alignLeft
	key := FillingKey2{logId: logId, offset: logOffset}
	size := int64(cap(reader.buf.Buf))

	c.lock.Lock()
	filling, endFill := c.filling[key]
	if endFill {
		delete(c.filling, key)
	} else {
		log.Warnf("BUG: ReaderBufferCache.EndFillWithPut, no key for fillling, key=%v", key)
	}
	if offsets, ok := c.cache[logId]; ok {
		if _, ok2 := offsets[logOffset]; ok2 {
			log.Warnf("BUG: ReaderBufferCache.EndFillWithPut, overwrite cache")
		}
	} else {
		c.cache[logId] = make(map[int64]*BufferedDiskPageReader)
	}
	c.cache[logId][logOffset] = reader
	c.lock.Unlock()

	c.cond.L.Lock()
	c.inFlightSize -= size
	c.currentSize += size
	c.cond.Broadcast()
	c.cond.L.Unlock()

	if endFill {
		filling.cond.L.Lock()
		filling.filled = true
		filling.cond.Broadcast()
		filling.cond.L.Unlock()
	}
}

func (c *ReaderBufferCache) ReleaseInFlightBuffer(reader *BufferedDiskPageReader) {
	if reader.buf.IsEvictable() {
		size := int64(cap(reader.buf.Buf))
		ReturnPageBuffer(reader.buf)
		c.cond.L.Lock()
		c.inFlightSize -= size
		c.cond.Broadcast()
		c.cond.L.Unlock()
	} else {
		log.Warnf("BUG: ReleaseInFlightBuffer, not evictable, refCount=%v", atomic.LoadInt32(&reader.buf.refCount))
	}
}

func (c *ReaderBufferCache) SetEvictable(reader *BufferedDiskPageReader) {
	count := reader.buf.Down()
	if count == 0 {
		c.lock.Lock()
		evictable := reader.buf.IsEvictable()
		if evictable { // need this to handle racy GetCache
			c.updateLRUList(reader)
		}
		c.lock.Unlock()
		if evictable {
			c.cond.Broadcast()
		}
	} else if count < 0 {
		log.Warnf("BUG: SetEvictable, count < 0, reader=%v", reader)
	}
}

func (c *ReaderBufferCache) dec(orig interface{}) {
	c.SetEvictable(orig.(*BufferedDiskPageReader))
}

func (c *ReaderBufferCache) Delete(logId LogIdType) (size int64) {
	size = int64(0)
	c.lock.Lock()
	if offsets, ok := c.cache[logId]; ok {
		for _, reader := range offsets {
			delete(offsets, reader.bufLogOffset+reader.alignLeft)
			if len(offsets) == 0 {
				delete(c.cache, reader.logId)
			}
			if reader.buf.IsEvictable() {
				c.deleteFromLRUList(reader)
				size += int64(cap(reader.buf.Buf))
				ReturnPageBuffer(reader.buf)
			} else {
				key := LogIdType{lower: uint64(math.MaxUint64), upper: uint32(math.MaxUint32)}
				if _, ok3 := c.cache[key]; !ok3 {
					c.cache[key] = make(map[int64]*BufferedDiskPageReader)
				}
				c.cache[key][int64(len(c.cache[key]))] = reader
			}
		}
	}
	c.lock.Unlock()
	if size > 0 {
		c.cond.L.Lock()
		c.currentSize -= size
		c.cond.Broadcast()
		c.cond.L.Unlock()
	}
	return
}

func (c *ReaderBufferCache) GetNewBufferedDiskPageReader(logId LogIdType, stag *StagingChunk, blocking bool) (*BufferedDiskPageReader, error) {
	alignedSize := CalcRequiredPageBufferSize(stag.logOffset, stag.length)
	c.cond.L.Lock()
	for c.inFlightSize+c.currentSize+alignedSize > c.maxSize {
		exceed := c.inFlightSize + c.currentSize + alignedSize - c.maxSize
		c.reclaim(exceed)
		if c.inFlightSize+c.currentSize+alignedSize > c.maxSize {
			if blocking {
				c.cond.Wait()
			} else {
				c.cond.L.Unlock()
				return nil, unix.EAGAIN
			}
		} else {
			break
		}
	}
	c.inFlightSize += alignedSize
	c.cond.L.Unlock()
	page, err := GetPageBuffer(alignedSize)
	if err != nil {
		c.cond.L.Lock()
		c.inFlightSize -= alignedSize
		c.cond.L.Unlock()
		return nil, err
	}
	ret := NewBufferedDiskPageReaderFromStag(page, logId, stag.logOffset, stag.length)
	return ret, nil
}

func (c *ReaderBufferCache) updateLRUList(r *BufferedDiskPageReader) {
	if r.next != nil {
		r.prev.next = r.next
		r.next.prev = r.prev
	}
	r.prev = &c.accessHead
	r.next = c.accessHead.next
	c.accessHead.next.prev = r
	c.accessHead.next = r
}

func (c *ReaderBufferCache) deleteFromLRUList(r *BufferedDiskPageReader) {
	r.prev.next = r.next
	r.next.prev = r.prev
	r.prev = r
	r.next = r
}

func (c *ReaderBufferCache) reclaim(reclaimSize int64) {
	c.cond.L.Unlock()

	size := int64(0)
	c.lock.Lock()
	var i = 0
	for size < reclaimSize {
		reader := c.accessHead.prev
		if reader == &c.accessHead {
			break
		}
		if reader.buf.IsEvictable() {
			c.deleteFromLRUList(reader)
			size += int64(cap(reader.buf.Buf))
			ReturnPageBuffer(reader.buf)
			if offsets, ok := c.cache[reader.logId]; ok {
				delete(offsets, reader.bufLogOffset+reader.alignLeft)
				if len(offsets) == 0 {
					delete(c.cache, reader.logId)
				}
			}
		} else {
			c.deleteFromLRUList(reader) // TODO: delete this
			log.Warnf("BUG: ReaderBufferCache.reclaim, non-evictable buffer at LRU list, count=%v", atomic.LoadInt32(&reader.buf.refCount))
		}
	}
	c.lock.Unlock()

	c.cond.L.Lock()
	if size > 0 {
		c.currentSize -= size
		c.cond.Broadcast()
	}
	if i > 0 && i%100 == 0 {
		log.Debugf("ReaderBufferCache.reclaim, i=%v, size=%v, reclaimSize=%v", i, size, reclaimSize)
	}
	i += 1
}

func (c *ReaderBufferCache) DropAll() {
	for logId := range c.cache {
		c.Delete(logId)
	}
}

func (c *ReaderBufferCache) CheckReset() (ok bool) {
	if ok = c.lock.TryLock(); !ok {
		log.Errorf("Failed: ReaderBufferCache.CheckReset, c.lock is taken")
		return
	}
	if ok = c.cond.L.(*sync.Mutex).TryLock(); !ok {
		c.lock.Unlock()
		log.Errorf("Failed: ReaderBufferCache.CheckReset, c.cond.L is taken")
		return
	}
	if ok2 := len(c.cache) == 0; !ok2 {
		log.Errorf("Failed: ReaderBufferCache.CheckReset, len(c.cache) > 0")
		ok = false
	}
	if ok2 := len(c.filling) == 0; !ok2 {
		log.Errorf("Failed: ReaderBufferCache.CheckReset, len(c.filling) > 0")
		ok = false
	}
	if ok2 := c.currentSize == 0; !ok2 {
		log.Errorf("Failed: ReaderBufferCache.CheckReset, currentSize != 0")
		ok = false
	}
	if ok2 := c.inFlightSize == 0; !ok2 {
		log.Errorf("Failed: ReaderBufferCache.CheckReset, inFlightSize != 0")
		ok = false
	}
	if ok2 := c.accessHead.next == &c.accessHead; !ok2 {
		log.Errorf("Failed: ReaderBufferCache.CheckReset, c.accessHead.next != &c.accessHead")
		ok = false
	}
	c.cond.L.Unlock()
	c.lock.Unlock()
	return
}

type RemotePageBuffer struct {
	inode  InodeKeyType
	offset int64
	buf    *PageBuffer
	next   *RemotePageBuffer
	prev   *RemotePageBuffer
}

func (r *RemotePageBuffer) AsSlice(dec func(interface{})) SlicedPageBuffer {
	return SlicedPageBuffer{Buf: r.buf.Buf, orig: r, dec: dec}
}

type FillingKey struct {
	inodeKey InodeKeyType
	offset   int64
}
type FillingInfo struct {
	cond   *sync.Cond
	filled bool
}

type RemoteBufferCache struct {
	cond         *sync.Cond
	lock         *sync.RWMutex
	cache        map[InodeKeyType]map[int64]*RemotePageBuffer
	filling      map[FillingKey]*FillingInfo
	currentSize  int64
	inFlightSize int64
	maxSize      int64
	accessHead   RemotePageBuffer
}

func NewRemotePageBufferCache(maxSize int64) *RemoteBufferCache {
	r := &RemoteBufferCache{
		lock: new(sync.RWMutex), cond: sync.NewCond(new(sync.Mutex)), cache: make(map[InodeKeyType]map[int64]*RemotePageBuffer),
		maxSize: maxSize, filling: make(map[FillingKey]*FillingInfo),
	}
	r.accessHead.prev = &r.accessHead
	r.accessHead.next = &r.accessHead
	return r
}

func (c *RemoteBufferCache) Has(inodekey InodeKeyType, offset int64) bool {
	c.lock.RLock()
	_, ok2 := c.filling[FillingKey{inodeKey: inodekey, offset: offset}]
	if ok2 {
		c.lock.RUnlock()
		return true // we don't need prefetch or readAhead because someone is filling
	}
	if offsets, ok := c.cache[inodekey]; ok {
		_, ok2 = offsets[offset]
		c.lock.RUnlock()
		return ok2 // we don't need prefetch or readAhead because we already have cache
	}
	c.lock.RUnlock()
	return false
}

func (c *RemoteBufferCache) GetCache(inodeKey InodeKeyType, offset int64) (SlicedPageBuffer, bool) {
	c.lock.Lock()
	if offsets, ok := c.cache[inodeKey]; ok {
		if cache, ok2 := offsets[offset]; ok2 {
			c.deleteFromLRUList(cache)
			cache.buf.Up()
			c.lock.Unlock()
			return cache.AsSlice(c.dec), true
		}
	}
	c.lock.Unlock()
	return SlicedPageBuffer{}, false
}

func (c *RemoteBufferCache) GetCacheOrBeginFill(inodeKey InodeKeyType, offset int64) (p *RemotePageBuffer, beginFill bool) {
	key := FillingKey{inodeKey: inodeKey, offset: offset}
	c.lock.Lock()
	if _, ok := c.filling[key]; ok {
		c.lock.Unlock()
		return nil, false
	}
	if offsets, ok := c.cache[inodeKey]; ok {
		if cache, ok2 := offsets[offset]; ok2 {
			c.deleteFromLRUList(cache)
			cache.buf.Up()
			c.lock.Unlock()
			return cache, false
		}
	}
	c.filling[key] = &FillingInfo{filled: false, cond: sync.NewCond(new(sync.Mutex))}
	c.lock.Unlock()
	return nil, true
}

func (c *RemoteBufferCache) GetCacheWithFillWait(inodeKey InodeKeyType, offset int64, length int) (SlicedPageBuffer, bool) {
	key := FillingKey{inodeKey: inodeKey, offset: offset}
	c.lock.Lock()
	for {
		filling, ok := c.filling[key]
		if ok {
			// someone is filling the target buffer
			c.lock.Unlock()
			filling.cond.L.Lock()
			for !filling.filled {
				filling.cond.Wait()
			}
			filling.cond.L.Unlock()
			c.lock.Lock()
		} else {
			break
		}
	}
	if offsets, ok := c.cache[inodeKey]; ok {
		if cache, ok2 := offsets[offset]; ok2 && len(cache.buf.Buf) >= length {
			c.deleteFromLRUList(cache)
			cache.buf.Up()
			c.lock.Unlock()
			return cache.AsSlice(c.dec), true
		}
	}
	c.filling[key] = &FillingInfo{filled: false, cond: sync.NewCond(new(sync.Mutex))}
	c.lock.Unlock()
	return SlicedPageBuffer{}, false
}

func (c *RemoteBufferCache) EndFill(inodeKey InodeKeyType, offset int64) {
	key := FillingKey{inodeKey: inodeKey, offset: offset}
	c.lock.Lock()
	filling, ok := c.filling[key]
	if ok {
		delete(c.filling, key)
	}
	c.lock.Unlock()
	if ok {
		filling.cond.L.Lock()
		filling.filled = true
		filling.cond.Broadcast()
		filling.cond.L.Unlock()
	}
}

func (c *RemoteBufferCache) GetRemotePageBuffer(inodeKey InodeKeyType, offset int64, length int64, blocking bool) (*RemotePageBuffer, error) {
	alignedSize := length
	if length%PageSize != 0 {
		alignedSize = length - length%PageSize + PageSize
	}
	c.cond.L.Lock()
	for c.inFlightSize+c.currentSize+alignedSize > c.maxSize {
		exceed := c.inFlightSize + c.currentSize + alignedSize - c.maxSize
		c.reclaim(exceed)
		if c.inFlightSize+c.currentSize+alignedSize > c.maxSize {
			if blocking {
				c.cond.Wait()
			} else {
				c.cond.L.Unlock()
				return nil, unix.EAGAIN
			}
		} else {
			break
		}
	}
	c.inFlightSize += alignedSize
	//log.Debugf("RemoteBufferCache.GetRemotePageBuffer, inodeKey=%v, offset=%v, alignedSize=%v, current=%v",
	//	inodeKey, offset, alignedSize, c.currentSize)
	c.cond.L.Unlock()

	p, err := GetPageBuffer(alignedSize)
	if err != nil {
		c.cond.L.Lock()
		c.inFlightSize -= alignedSize
		c.cond.Broadcast()
		c.cond.L.Unlock()
		return nil, err
	}
	ret := &RemotePageBuffer{buf: p, inode: inodeKey, offset: offset}
	ret.next = ret
	ret.prev = ret
	return ret, err
}

func (c *RemoteBufferCache) EndFillWithPut(page *RemotePageBuffer) {
	inodeId := page.inode
	offset := page.offset
	size := int64(cap(page.buf.Buf))
	key := FillingKey{inodeKey: inodeId, offset: offset}

	c.lock.Lock()
	filling, endFill := c.filling[key]
	if endFill {
		delete(c.filling, key)
	}
	if offsets, ok := c.cache[inodeId]; ok {
		if _, ok2 := offsets[offset]; ok2 {
			log.Warnf("BUG: RemoteBufferCache.Put, overwrite cache")
		}
	} else {
		c.cache[inodeId] = make(map[int64]*RemotePageBuffer)
	}
	c.cache[inodeId][offset] = page
	c.lock.Unlock()

	c.cond.L.Lock()
	c.inFlightSize -= size
	c.currentSize += size
	c.cond.Broadcast()
	c.cond.L.Unlock()
	if endFill {
		filling.cond.L.Lock()
		filling.filled = true
		filling.cond.Broadcast()
		filling.cond.L.Unlock()
	}
}

func (c *RemoteBufferCache) updateLRUList(r *RemotePageBuffer) {
	if r.next != nil {
		r.prev.next = r.next
		r.next.prev = r.prev
	}
	r.prev = &c.accessHead
	r.next = c.accessHead.next
	c.accessHead.next.prev = r
	c.accessHead.next = r
}

func (c *RemoteBufferCache) deleteFromLRUList(r *RemotePageBuffer) {
	r.prev.next = r.next
	r.next.prev = r.prev
	r.prev = r
	r.next = r
}

func (c *RemoteBufferCache) reclaim(reclaimSize int64) {
	c.cond.L.Unlock()

	size := int64(0)
	c.lock.Lock()
	for size < reclaimSize {
		p := c.accessHead.prev
		if p == &c.accessHead {
			break
		}
		if p.buf.IsEvictable() {
			c.deleteFromLRUList(p)
			size += int64(cap(p.buf.Buf))
			ReturnPageBuffer(p.buf)
			if offsets, ok := c.cache[p.inode]; ok {
				delete(offsets, p.offset)
				if len(offsets) == 0 {
					delete(c.cache, p.inode)
				}
			}
		} else {
			c.deleteFromLRUList(p) // TODO: delete this
			log.Warnf("BUG: RemoteBufferCache.reclaim, non-evictable buffer at LRU list, count=%v", atomic.LoadInt32(&p.buf.refCount))
		}
	}
	c.lock.Unlock()

	c.cond.L.Lock()
	if size > 0 {
		c.currentSize -= size
		c.cond.Broadcast()
	}
}

func (c *RemoteBufferCache) ReleaseInFlightBuffer(p *RemotePageBuffer) {
	if p.buf.IsEvictable() {
		size := int64(cap(p.buf.Buf))
		ReturnPageBuffer(p.buf)
		c.cond.L.Lock()
		c.inFlightSize -= size
		c.cond.Broadcast()
		c.cond.L.Unlock()
	} else {
		log.Warnf("BUG: ReleaseInFlightBuffer, not evictable, refCount=%v", atomic.LoadInt32(&p.buf.refCount))
	}
}

func (c *RemoteBufferCache) SetEvictable(page *RemotePageBuffer) (size int64) {
	if count := page.buf.Down(); count == 0 {
		c.lock.Lock()
		evictable := page.buf.IsEvictable()
		if evictable { // need this to handle racy GetCache
			c.updateLRUList(page)
		}
		c.lock.Unlock()
		if evictable {
			c.cond.Broadcast()
		}
	}
	return
}

func (c *RemoteBufferCache) dec(orig interface{}) {
	c.SetEvictable(orig.(*RemotePageBuffer))
}

func (c *RemoteBufferCache) DropAll() {
	for inode := range c.cache {
		c.Delete(inode)
	}
}

func (c *RemoteBufferCache) Delete(inode InodeKeyType) {
	size := int64(0)
	c.lock.Lock()
	var p *RemotePageBuffer
	if offsets, ok := c.cache[inode]; ok {
		for _, p = range offsets {
			delete(offsets, p.offset)
			if len(offsets) == 0 {
				delete(c.cache, p.inode)
			}
			if p.buf.IsEvictable() {
				c.deleteFromLRUList(p)
				size += int64(cap(p.buf.Buf))
				ReturnPageBuffer(p.buf)
			} else {
				key := InodeKeyType(math.MaxUint64)
				if _, ok3 := c.cache[key]; !ok3 {
					c.cache[key] = make(map[int64]*RemotePageBuffer)
				}
				c.cache[key][int64(len(c.cache[key]))] = p
			}
		}
	}
	c.lock.Unlock()
	if size > 0 {
		c.cond.L.Lock()
		c.currentSize -= size
		c.cond.Broadcast()
		c.cond.L.Unlock()
	}
}

func (c *RemoteBufferCache) CheckReset() (ok bool) {
	if ok = c.lock.TryLock(); !ok {
		log.Errorf("Failed: RemoteBufferCache.CheckReset, c.lock is taken")
		return
	}
	if ok = c.cond.L.(*sync.Mutex).TryLock(); !ok {
		c.lock.Unlock()
		log.Errorf("Failed: RemoteBufferCache.CheckReset, c.cond.L is taken")
		return
	}
	if ok2 := len(c.cache) == 0; !ok2 {
		log.Errorf("Failed: RemoteBufferCache.CheckReset, len(c.cache) > 0")
		ok = false
	}
	if ok2 := len(c.filling) == 0; !ok2 {
		log.Errorf("Failed: RemoteBufferCache.CheckReset, len(c.filling) > 0")
		ok = false
	}
	if ok2 := c.currentSize == 0; !ok2 {
		log.Errorf("Failed: RemoteBufferCache.CheckReset, currentSize != 0")
		ok = false
	}
	if ok2 := c.inFlightSize == 0; !ok2 {
		log.Errorf("Failed: RemoteBufferCache.CheckReset, inFlightSize != 0")
		ok = false
	}
	if ok2 := c.accessHead.next == &c.accessHead; !ok2 {
		log.Errorf("Failed: RemoteBufferCache.CheckReset, c.accessHead.next != &c.accessHead")
		ok = false
	}
	c.cond.L.Unlock()
	c.lock.Unlock()
	return
}

type LogOffsetPair struct {
	inode  InodeKeyType
	offset int64
	size   int
	p      *list.Element
}

type LocalReadHistory struct {
	lock    *sync.RWMutex
	cache   map[InodeKeyType]map[int64]*LogOffsetPair
	history *list.List
}

func NewLocalBufferCacheHistory() *LocalReadHistory {
	return &LocalReadHistory{lock: new(sync.RWMutex), cache: make(map[InodeKeyType]map[int64]*LogOffsetPair), history: list.New()}
}

func (c *LocalReadHistory) Has(inodeKey InodeKeyType, offset int64) (int, bool) {
	c.lock.Lock()
	if offsets, ok := c.cache[inodeKey]; ok {
		if entry, ok2 := offsets[offset]; ok2 {
			c.history.MoveToFront(entry.p)
			c.lock.Unlock()
			return entry.size, true
		}
	}
	c.lock.Unlock()
	return 0, false
}

func (c *LocalReadHistory) Add(inodeKey InodeKeyType, offset int64, length int) {
	c.lock.Lock()
	if c.history.Len() > 1024 {
		back := c.history.Back()
		c.history.Remove(back)
		old := back.Value.(*LogOffsetPair)
		if offsets, ok := c.cache[old.inode]; ok {
			delete(offsets, old.offset)
			if len(offsets) == 0 {
				delete(c.cache, old.inode)
			}
		}
	}
	entry := &LogOffsetPair{inode: inodeKey, offset: offset, size: length}
	entry.p = c.history.PushFront(entry)
	if _, ok := c.cache[inodeKey]; !ok {
		c.cache[inodeKey] = make(map[int64]*LogOffsetPair)
	}
	c.cache[inodeKey][offset] = entry
	c.lock.Unlock()
}

func (c *LocalReadHistory) Delete(inodeKey InodeKeyType) {
	c.lock.Lock()
	if offsets, ok := c.cache[inodeKey]; ok {
		for _, entry := range offsets {
			c.history.Remove(entry.p)
		}
	}
	delete(c.cache, inodeKey)
	c.lock.Unlock()
}

func (c *LocalReadHistory) DropAll() {
	c.lock.Lock()
	c.history = list.New()
	c.cache = make(map[InodeKeyType]map[int64]*LogOffsetPair)
	c.lock.Unlock()
}

func (c *LocalReadHistory) CheckReset() (ok bool) {
	if ok = c.lock.TryLock(); !ok {
		log.Errorf("Failed: LocalReadHistory.CheckReset, c.lock is taken")
		return
	}
	if ok2 := len(c.cache) == 0; !ok2 {
		log.Errorf("Failed: LocalReadHistory.CheckReset, len(c.cache) > 0")
		ok = false
	}
	if ok2 := c.history.Len() == 0; !ok2 {
		log.Errorf("Failed: LocalReadHistory.CheckReset, c.history.Len() > 0")
		ok = false
	}
	c.lock.Unlock()
	return

}
