/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/fs"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/takeshi-yoshimura/fuse"

	"github.com/google/btree"
	"github.ibm.com/TYOS/objcache/api"
	"github.ibm.com/TYOS/objcache/common"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	AppendEntryCommandLogBaseSize = int32(crc32.Size + 7)
	AppendEntryCommandLogSize     = AppendEntryCommandLogBaseSize + int32(AppendEntryFileCmdLength)

	AppendEntryFileCmdLength         = uint8(28)
	AppendEntryNoOpCmdLength         = uint8(0)
	AppendEntryAddServerCmdLength    = uint8(10)
	AppendEntryRemoveServerCmdLength = uint8(4)
	AppendEntryCommitTxCmdLength     = uint8(16)
	AppendEntryResetExtLogCmdLength  = uint8(12)
)

type FileIdType struct {
	lower uint64
	upper uint64
}

func (f FileIdType) Put(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:SizeOfUint64], f.lower)
	binary.LittleEndian.PutUint64(buf[SizeOfUint64:SizeOfUint64*2], f.upper)
}

func (f FileIdType) IsValid() bool {
	return f.upper > 0 || f.lower > 0
}

func NewFileIdTypeFromInodeKey(inodeKey InodeKeyType, offset int64, chunkSize int64) FileIdType {
	return FileIdType{lower: uint64(inodeKey), upper: uint64(offset - offset%chunkSize)}
}

func NewFileIdTypeFromBuf(buf []byte) FileIdType {
	lower := binary.LittleEndian.Uint64(buf[0:SizeOfUint64])
	upper := binary.LittleEndian.Uint64(buf[SizeOfUint64 : SizeOfUint64*2])
	return FileIdType{lower: lower, upper: upper}
}

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

func (l *AppendEntryCommand) setControlBits(extPayloadLength uint8, term uint32, extCmdId uint16) {
	l.buf[crc32.Size] = uint8(AppendEntryCommandLogBaseSize) + extPayloadLength
	binary.LittleEndian.PutUint32(l.buf[crc32.Size+1:crc32.Size+5], term)
	binary.LittleEndian.PutUint16(l.buf[crc32.Size+5:AppendEntryCommandLogBaseSize], extCmdId)
}
func (l *AppendEntryCommand) GetExtPayload() []byte {
	return l.buf[AppendEntryCommandLogBaseSize:]
}

func (l *AppendEntryCommand) setChecksum(extBuf []byte) {
	chkSum := crc32.NewIEEE()
	entryLength := l.GetEntryLength()
	_, _ = chkSum.Write(l.buf[crc32.Size:entryLength])
	if extBuf != nil {
		_, _ = chkSum.Write(extBuf)
	}
	copy(l.buf[:crc32.Size], chkSum.Sum(nil))
}
func (l *AppendEntryCommand) setChecksumFromFile(fileName string, offset int64, dataLen uint32) error {
	chkSum := crc32.NewIEEE()
	entryLength := l.GetEntryLength()
	_, _ = chkSum.Write(l.buf[crc32.Size:entryLength])
	fd, errno := unix.Open(fileName, unix.O_RDONLY, 0644)
	if errno != nil {
		log.Errorf("Failed: RaftFiles.CalcCheckSum, openCacheNoLock, fileName=%v, errno=%v", fileName, errno)
		return errno
	}
	buf := make([]byte, 4096)
	var count = uint32(0)
	for count < dataLen {
		last := uint32(4096)
		if count+4096 > dataLen {
			last = dataLen - count
		}
		c, err := unix.Pread(fd, buf[:last], offset+int64(count))
		if err != nil {
			log.Errorf("Failed: CalcCheckSum, Pread, fd=%v, offset=%v, count=%v, err=%v", fd, offset, count, err)
			_ = unix.Close(fd)
			return err
		}
		if c == 0 {
			break
		}
		count += uint32(c)
		chkSum.Write(buf[:c])
	}
	_ = unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED)
	_ = unix.Close(fd)
	copy(l.buf[:crc32.Size], chkSum.Sum(nil))
	return nil
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

func NewAppendEntryNoOpCommandDiskFormat(term uint32) (cmd AppendEntryCommand) {
	cmd.setControlBits(AppendEntryNoOpCmdLength, term, AppendEntryNoOpCmdId)
	cmd.setChecksum(nil)
	return
}
func NewAppendEntryFileCommand(term uint32, extCmdId uint16, fileId FileIdType, fileOffset int64, fileContent []byte) (cmd AppendEntryCommand) {
	cmd.setControlBits(AppendEntryFileCmdLength, term, extCmdId|DataCmdIdBit)
	extEntryPayload := cmd.GetExtPayload()
	fileId.Put(extEntryPayload[0:16])
	binary.LittleEndian.PutUint32(extEntryPayload[16:20], uint32(len(fileContent)))
	binary.LittleEndian.PutUint64(extEntryPayload[20:AppendEntryFileCmdLength], uint64(fileOffset))
	cmd.setChecksum(fileContent)
	return
}
func NewAppendEntryFileCommandFromFile(term uint32, extCmdId uint16, fileId FileIdType, fileOffset int64, dataLen uint32, fileName string) (cmd AppendEntryCommand, err error) {
	cmd.setControlBits(AppendEntryFileCmdLength, term, extCmdId|DataCmdIdBit)
	extEntryPayload := cmd.GetExtPayload()
	fileId.Put(extEntryPayload[0:16])
	binary.LittleEndian.PutUint32(extEntryPayload[16:20], uint32(dataLen))
	binary.LittleEndian.PutUint64(extEntryPayload[20:AppendEntryFileCmdLength], uint64(fileOffset))
	err = cmd.setChecksumFromFile(fileName, fileOffset, dataLen)
	return
}
func (l *AppendEntryCommand) GetAsAppendEntryFile() (fileId FileIdType, fileLength int32, fileOffset int64) {
	extEntryPayload := l.GetExtPayload()
	fileId = NewFileIdTypeFromBuf(extEntryPayload[0:16])
	fileLength = int32(binary.LittleEndian.Uint32(extEntryPayload[16:20]))
	fileOffset = int64(binary.LittleEndian.Uint64(extEntryPayload[20:AppendEntryFileCmdLength]))
	return
}
func NewAppendEntryAddServerCommand(term uint32, serverId uint32, ip [4]byte, port uint16) (cmd AppendEntryCommand) {
	cmd.setControlBits(AppendEntryAddServerCmdLength, term, AppendEntryAddServerCmdId)
	extEntryPayload := cmd.GetExtPayload()
	binary.LittleEndian.PutUint32(extEntryPayload[0:4], serverId)
	for i := int32(0); i < int32(4); i++ {
		extEntryPayload[4+i] = ip[i]
	}
	binary.LittleEndian.PutUint16(extEntryPayload[8:AppendEntryAddServerCmdLength], port)
	cmd.setChecksum(nil)
	return
}
func (l *AppendEntryCommand) GetAsAddServer() (serverId uint32, ip [4]byte, port uint16) {
	extEntryPayload := l.GetExtPayload()
	serverId = binary.LittleEndian.Uint32(extEntryPayload[0:4])
	for i := int32(0); i < int32(4); i++ {
		ip[i] = extEntryPayload[4+i]
	}
	port = binary.LittleEndian.Uint16(extEntryPayload[8:AppendEntryAddServerCmdLength])
	return
}
func NewAppendEntryRemoveServerCommand(term uint32, serverId uint32) (cmd AppendEntryCommand) {
	cmd.setControlBits(AppendEntryRemoveServerCmdLength, term, AppendEntryRemoveServerCmdId)
	extEntryPayload := cmd.GetExtPayload()
	binary.LittleEndian.PutUint32(extEntryPayload[0:AppendEntryRemoveServerCmdLength], serverId)
	cmd.setChecksum(nil)
	return
}
func (l *AppendEntryCommand) GetAsRemoveServer() (serverId uint32) {
	extEntryPayload := l.GetExtPayload()
	serverId = binary.LittleEndian.Uint32(extEntryPayload[0:AppendEntryRemoveServerCmdLength])
	return
}
func NewAppendEntryResetExtLogCommand(term uint32, fileId uint64, nextSeqNum uint32) (cmd AppendEntryCommand) {
	cmd.setControlBits(AppendEntryResetExtLogCmdLength, term, AppendEntryResetExtLogCmdId)
	extEntryPayload := cmd.GetExtPayload()
	binary.LittleEndian.PutUint64(extEntryPayload[0:8], fileId)
	binary.LittleEndian.PutUint32(extEntryPayload[8:AppendEntryResetExtLogCmdLength], nextSeqNum)
	cmd.setChecksum(nil)
	return
}
func NewAppendEntryDeleteExtLogCommand(term uint32, fileId uint64) (cmd AppendEntryCommand) {
	cmd.setControlBits(AppendEntryResetExtLogCmdLength, term, AppendEntryResetExtLogCmdId)
	extEntryPayload := cmd.GetExtPayload()
	binary.LittleEndian.PutUint64(extEntryPayload[0:8], fileId)
	cmd.setChecksum(nil)
	return
}
func (l *AppendEntryCommand) GetAsResetExtLog() (fileId uint64, nextSeqNum uint32) {
	extEntryPayload := l.GetExtPayload()
	fileId = binary.LittleEndian.Uint64(extEntryPayload[0:8])
	nextSeqNum = binary.LittleEndian.Uint32(extEntryPayload[8:AppendEntryResetExtLogCmdLength])
	return
}
func NewAppendEntryCommitTxCommand(term uint32, txId *common.TxIdMsg) (cmd AppendEntryCommand) {
	cmd.setControlBits(AppendEntryCommitTxCmdLength, term, AppendEntryCommitTxCmdId)
	extEntryPayload := cmd.GetExtPayload()
	binary.LittleEndian.PutUint32(extEntryPayload[0:4], txId.ClientId)
	binary.LittleEndian.PutUint32(extEntryPayload[4:8], txId.SeqNum)
	binary.LittleEndian.PutUint64(extEntryPayload[8:AppendEntryCommitTxCmdLength], txId.TxSeqNum)
	return
}
func (l *AppendEntryCommand) GetAsCommitTx() (clientId uint32, seqNum uint32, txSeqNum uint64) {
	extEntryPayload := l.GetExtPayload()
	clientId = binary.LittleEndian.Uint32(extEntryPayload[0:4])
	seqNum = binary.LittleEndian.Uint32(extEntryPayload[4:8])
	txSeqNum = binary.LittleEndian.Uint64(extEntryPayload[8:AppendEntryCommitTxCmdLength])
	return
}

func (l *AppendEntryCommand) AppendToRpcMsg(d *RpcMsg) (newOptHeaderLength uint16, newTotalFileLength uint32) {
	var fileLength = int32(0)
	var entryLength = l.GetEntryLength()
	if l.GetExtCmdId()&DataCmdIdBit != 0 {
		_, fileLength, _ = l.GetAsAppendEntryFile()
	}
	off := d.GetOptHeaderLength()
	if off == 0 {
		off = uint16(RpcOptControlHeaderLength)
	}
	d.SetOptHeaderLength(off + uint16(entryLength))
	curFileLength, curNrEntries := d.GetOptControlHeader()
	if fileLength > 0 {
		d.SetTotalFileLength(curFileLength + uint32(fileLength))
	}
	d.SetNrEntries(curNrEntries + 1)
	copy(d.optBuf[off:off+uint16(entryLength)], l.buf[:entryLength])
	log.Debugf("AppendToRpcMsg: MyCopy(d.optBuf[%v+crc32.Size:%v] from buf=%v", off, off+uint16(entryLength), l.buf[:entryLength])
	return off + uint16(entryLength), curFileLength + uint32(fileLength)
}

type HeadIndexFilePair struct {
	headIndex uint64
	filePath  string
}

func (h HeadIndexFilePair) Less(item btree.Item) bool {
	r := item.(HeadIndexFilePair)
	return h.headIndex < r.headIndex
}

type CachedCommand struct {
	cmd   AppendEntryCommand
	index uint64
}

func (c CachedCommand) Less(i btree.Item) bool {
	return c.index < i.(CachedCommand).index
}

type CommandCache struct {
	recent    *btree.BTree
	lock      *sync.RWMutex
	maxLength int
}

func NewCommandCache(maxLength int) CommandCache {
	return CommandCache{recent: btree.New(3), lock: new(sync.RWMutex), maxLength: maxLength}
}

func (c CommandCache) Get(index uint64) (cmd AppendEntryCommand, ok bool) {
	c.lock.RLock()
	cached := c.recent.Get(CachedCommand{index: index})
	c.lock.RUnlock()
	if cached == nil {
		return
	}
	return cached.(CachedCommand).cmd, true
}

func (c CommandCache) Put(index uint64, cmd AppendEntryCommand) {
	c.lock.Lock()
	if c.recent.Len() >= c.maxLength {
		c.recent.DeleteMin()
	}
	c.recent.ReplaceOrInsert(CachedCommand{index: index, cmd: cmd})
	c.lock.Unlock()
}

func (c *CommandCache) Clean() {
	c.lock.Lock()
	c.recent = btree.New(3)
	c.lock.Unlock()
}

func (c CommandCache) CheckReset() (ok bool) {
	if ok = c.lock.TryLock(); !ok {
		log.Errorf("Failed: CommandCache.CheckReset, c.lock is taken")
		return
	}
	if ok2 := c.recent.Len() == 0; !ok2 {
		log.Errorf("Failed: CommandCache.CheckReset, c.recent.Len() != 0")
		ok = false
	}
	c.lock.Unlock()
	return
}

type LogFile struct {
	rootDir    string
	filePrefix string
	hIdxPath   *btree.BTree
	fd         int
	lock       *sync.RWMutex
	headIndex  uint64
	logLength  uint64

	cache CommandCache
}

func NewLogFile(rootDir string, filePrefix string) (ret *LogFile, reply int32) {
	stat := unix.Stat_t{}
	errno := unix.Stat(rootDir, &stat)
	if errno == unix.ENOENT {
		if errno = os.MkdirAll(rootDir, 0755); errno != nil {
			log.Errorf("Failed: NewLogFile, MkdirAll, dir=%v, errno=%v", rootDir, errno)
			return nil, ErrnoToReply(errno)
		}
	} else if errno != nil {
		log.Errorf("Failed: NewLogFile, Stat, dir=%v, err=%v", rootDir, errno)
		return nil, ErrnoToReply(errno)
	}

	ret = &LogFile{
		rootDir: rootDir, filePrefix: filePrefix, headIndex: 0, logLength: 1, lock: new(sync.RWMutex),
		hIdxPath: btree.New(3), cache: NewCommandCache(32),
	}
	err := filepath.WalkDir(rootDir, func(filePath string, info fs.DirEntry, err error) error {
		if err != nil || info.IsDir() || !strings.HasPrefix(info.Name(), filePrefix) {
			return nil
		}
		var s os.FileInfo
		s, errno = info.Info()
		if errno != nil {
			log.Errorf("Failed: NewLogFile, Info, filePath=%v, err=%v", filePath, errno)
			return errno
		}
		var fd int
		fd, errno = unix.Open(filePath, unix.O_RDONLY, 0644)
		if errno != nil {
			log.Errorf("Failed: NewLogFile, Open, filePath=%v, errno=%v", filePath, errno)
			return errno
		}
		buf := [8]byte{}
		var bufOff = 0
		for bufOff < 8 {
			var c int
			c, errno = unix.Pread(fd, buf[bufOff:], int64(bufOff))
			if errno != nil {
				log.Errorf("Failed: NewLogFile, Pread, filePath=%v, errno=%v", filePath, errno)
				return errno
			}
			if c == 0 {
				break
			}
			bufOff += c
		}
		index := binary.LittleEndian.Uint64(buf[:])
		if ret.headIndex < index {
			ret.headIndex = index
			ret.logLength = uint64(s.Size() / int64(AppendEntryCommandLogSize))
		}
		ret.hIdxPath.ReplaceOrInsert(HeadIndexFilePair{headIndex: index, filePath: filePath})
		if errno = unix.Close(fd); errno != nil {
			log.Warnf("Failed (ignore): NewLogFile, Close, fd=%v, erno=%v", fd, errno)
		}
		return nil
	})
	if err != nil {
		return nil, ErrnoToReply(err)
	}
	if ret.headIndex == 0 {
		reply = ret.CreateNewFile(1)
	} else {
		filePath := filepath.Join(rootDir, fmt.Sprintf("%s%d.log", filePrefix, ret.headIndex))
		ret.fd, errno = unix.Open(filePath, unix.O_RDWR, 0644)
		if errno != nil {
			log.Errorf("Failed: NewLogFile, Open, filePath=%v, errno=%v", filePath, errno)
			return nil, ErrnoToReply(errno)
		}
		reply = RaftReplyOk
	}
	return
}

func (f *LogFile) CreateNewFile(newHeadIndex uint64) (reply int32) {
	f.lock.Lock()
	f.headIndex = newHeadIndex
	if f.fd > 0 {
		if errno := unix.Close(f.fd); errno != nil {
			log.Warnf("Failed (ignore): CreateNewFile, Close, fd=%v, errno=%v", f.fd, errno)
		}
	}
	filePath := filepath.Join(f.rootDir, fmt.Sprintf("%s%d.log", f.filePrefix, newHeadIndex))
	var errno error
	f.fd, errno = unix.Open(filePath, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_SYNC, 0644)
	if errno != nil {
		f.lock.Unlock()
		log.Errorf("Failed: CreateNewFile, Open, filePath=%v, errno=%v", filePath, errno)
		return ErrnoToReply(errno)
	}
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], newHeadIndex)
	var bufOff = 0
	for bufOff < 8 {
		var c int
		c, errno = unix.Pwrite(f.fd, buf[bufOff:], int64(bufOff))
		if errno != nil {
			f.lock.Unlock()
			log.Errorf("Failed: CreateNewFile, Pwrite, errno=%v", errno)
			return ErrnoToReply(errno)
		}
		bufOff += c
	}
	if err := unix.Fsync(f.fd); err != nil {
		log.Errorf("Failed (ignore): CreateNewFile, Fsync, fd=%v, errno=%v", f.fd, err)
	}
	f.hIdxPath.ReplaceOrInsert(HeadIndexFilePair{headIndex: f.headIndex, filePath: filePath})
	f.cache.Clean()
	f.lock.Unlock()
	return RaftReplyOk
}

func (f *LogFile) LoadCommandAt(logIndex uint64) (cmd AppendEntryCommand, reply int32) {
	f.lock.RLock()
	fd := f.fd
	headIndex := f.headIndex
	if logIndex == 0 {
		f.lock.RUnlock()
		return NewAppendEntryNoOpCommandDiskFormat(math.MaxUint32), RaftReplyOk
	}
	if logIndex < headIndex {
		var filePath = ""
		f.hIdxPath.AscendGreaterOrEqual(HeadIndexFilePair{headIndex: logIndex}, func(i btree.Item) bool {
			filePath = i.(HeadIndexFilePair).filePath
			return false
		})
		if filePath == "" {
			f.lock.RUnlock()
			log.Errorf("Failed: LoadCommandAt, cannot find logIndex=%v", logIndex)
			return cmd, ErrnoToReply(unix.ENOENT)
		}
		var errno error
		fd, errno = unix.Open(filePath, unix.O_RDONLY, 0644)
		if errno != nil {
			f.lock.RUnlock()
			log.Errorf("Failed: LoadCommandAt, Open, filePath=%v, errno=%v", filePath, errno)
			return cmd, ErrnoToReply(errno)
		}
		var bufOff = 0
		buf := [8]byte{}
		for bufOff < 8 {
			var c int
			c, errno = unix.Pread(fd, buf[bufOff:], 0)
			if errno != nil {
				f.lock.RUnlock()
				log.Errorf("Failed: LoadCommandAt, Pread (0), fd=%v, bufOff=%v, errno=%v", fd, bufOff, errno)
				return cmd, ErrnoToReply(errno)
			}
			if c == 0 {
				break
			}
			bufOff += c
		}
		headIndex = binary.LittleEndian.Uint64(buf[:])
	}
	var ok bool
	cmd, ok = f.cache.Get(logIndex)
	if !ok {
		var bufOff = 0
		for bufOff < len(cmd.buf) {
			c, errno := unix.Pread(fd, cmd.buf[bufOff:], 8+int64(logIndex-headIndex+1)*int64(AppendEntryCommandLogSize)+int64(bufOff))
			if errno != nil {
				f.lock.RUnlock()
				log.Errorf("Failed: LoadCommandAt, Pread (1), errno=%v", errno)
				return cmd, ErrnoToReply(errno)
			}
			if c == 0 {
				break
			}
			bufOff += c
		}
	}
	if logIndex < f.headIndex {
		if errno := unix.Close(fd); errno != nil {
			log.Warnf("Failed (ignore): LoadCommandAt, Close, errno=%v", errno)
		}
	}
	f.lock.RUnlock()
	return cmd, RaftReplyOk
}

func (f *LogFile) AppendCommandAt(logIndex uint64, cmd AppendEntryCommand) (reply int32) {
	f.lock.Lock()
	if logIndex < f.logLength {
		f.lock.Unlock()
		log.Errorf("Failed: AppendCommandAt, Term=%v, Index=%v (log already exists)", cmd.GetTerm(), logIndex)
		return ErrnoToReply(unix.EEXIST)
	}
	f.logLength = logIndex + 1
	fileOffset := 8 + int64(logIndex-f.headIndex+1)*int64(AppendEntryCommandLogSize)
	fd := f.fd
	f.lock.Unlock()
	var bufOff = 0
	for bufOff < len(cmd.buf) {
		c, errno := unix.Pwrite(fd, cmd.buf[bufOff:], fileOffset+int64(bufOff))
		if errno != nil {
			log.Errorf("Failed: AppendCommandAt, Pwrite, fd=%v, fileOffset=%v, bufOff=%v, errno=%v", fd, fileOffset, bufOff, errno)
			reply = ErrnoToReply(errno)
			//TODO: handle corrupt log entry
			return
		}
		bufOff += c
	}
	f.cache.Put(logIndex, cmd)
	if err := unix.Fsync(f.fd); err != nil {
		log.Errorf("Failed (ignore): AppendCommandAt, Fsync, fd=%v, errno=%v", f.fd, err)
	}
	log.Debugf("Success: AppendCommandAt, Term=%v, Index=%v, ExtCmdId=%v", cmd.GetTerm(), logIndex, cmd.GetExtCmdId())
	return RaftReplyOk
}

func (f *LogFile) AppendCommand(cmd AppendEntryCommand) (logIndex uint64, reply int32) {
	reply = RaftReplyOk
	f.lock.Lock()
	logIndex = f.logLength
	f.logLength = logIndex + 1
	fileOffset := 8 + int64(logIndex-f.headIndex+1)*int64(AppendEntryCommandLogSize)
	fd := f.fd
	f.lock.Unlock()
	var bufOff = 0
	for bufOff < len(cmd.buf) {
		c, errno := unix.Pwrite(fd, cmd.buf[bufOff:], fileOffset+int64(bufOff))
		if errno != nil {
			log.Errorf("Failed: AppendCommand, Pwrite, fd=%v, fileOffset=%v, bufOff=%v, errno=%v", fd, fileOffset, bufOff, errno)
			reply = ErrnoToReply(errno)
			//TODO: handle corrupt log entry
			break
		}
		bufOff += c
	}
	f.cache.Put(logIndex, cmd)
	if err := unix.Fsync(f.fd); err != nil {
		log.Errorf("Failed (ignore): AppendCommandAt, Fsync, fd=%v, errno=%v", f.fd, err)
	}
	//log.Debugf("Success: AppendCommand, Term=%v, Index=%v", cmd.GetTerm(), logIndex)
	return logIndex, reply
}

func (f *LogFile) SeekLog(logIndex uint64) (reply int32) {
	f.lock.Lock()
	if logIndex < f.headIndex {
		var filePath = ""
		oldHeadIndex := f.headIndex
		f.hIdxPath.AscendGreaterOrEqual(HeadIndexFilePair{headIndex: logIndex}, func(i btree.Item) bool {
			fPath := i.(HeadIndexFilePair).filePath
			hIdx := i.(HeadIndexFilePair).headIndex
			if filePath == "" {
				filePath = fPath
				f.headIndex = hIdx
			} else {
				if errno := unix.Unlink(fPath); errno != nil {
					log.Warnf("Failed: SeekLog, Unlink, filePath=%v, errno=%v", fPath, errno)
				}
				f.hIdxPath.Delete(i)
			}
			return false
		})
		if filePath == "" {
			f.lock.Unlock()
			log.Errorf("Failed: SeekLog, cannot find logIndex=%v", logIndex)
			return ErrnoToReply(unix.ENOENT)
		}
		if f.fd != -1 {
			if errno := unix.Close(f.fd); errno != nil {
				log.Warnf("Failed (ignore): SeekLog, Close, fd=%v, errno=%v", f.fd, errno)
			}
		}
		var errno error
		f.fd, errno = unix.Open(filePath, unix.O_RDONLY, 0644)
		if errno != nil {
			f.lock.Unlock()
			log.Errorf("Failed: SeekLog, Open, filePath=%v, errno=%v", filePath, errno)
			return ErrnoToReply(errno)
		}
		oldLogLength := f.logLength
		f.logLength = logIndex + 1
		log.Debugf("Success: SeekLog, logIndex=%v->%v, headIndex=%v->%v", oldLogLength, f.logLength, oldHeadIndex, f.headIndex)
	} else {
		if f.logLength > logIndex+1 {
			length := 8 + int64(logIndex-f.headIndex+1)*int64(AppendEntryCommandLogSize)
			if errno := unix.Ftruncate(f.fd, length); errno != nil {
				f.lock.Unlock()
				log.Errorf("Failed: SeekLog, Ftruncate, length=%v, errno=%v", length, errno)
				return ErrnoToReply(errno)
			}
			oldLogLength := f.logLength
			f.logLength = logIndex + 1
			log.Debugf("Success: SeekLog, Ftruncate, logLength=%v->%v, headIndex=%v, FileLength=%v", oldLogLength, f.logLength, f.headIndex, length)
		}
	}
	f.cache.Clean()
	f.lock.Unlock()
	return RaftReplyOk
}

func (f *LogFile) GetCurrentLogLength() uint64 {
	f.lock.RLock()
	ret := f.logLength
	f.lock.RUnlock()
	return ret
}

func (f *LogFile) Clean() {
	f.lock.Lock()
	if f.fd != -1 {
		if errno := unix.Close(f.fd); errno != nil {
			log.Warnf("Failed (ignore): LogFile.Clean, Close, fd=%v, errno=%v", f.fd, errno)
		} else {
			f.fd = -1
		}
	}
	unlinked := make([]btree.Item, 0)
	f.hIdxPath.AscendGreaterOrEqual(HeadIndexFilePair{headIndex: 0}, func(i btree.Item) bool {
		filePath := i.(HeadIndexFilePair).filePath
		if err := unix.Unlink(filePath); err != nil {
			log.Errorf("Failed (ignore): LogFile.Clean, Unlink, filePath=%v, err=%v", filePath, err)
		} else {
			unlinked = append(unlinked, i)
		}
		return true
	})
	for _, item := range unlinked {
		f.hIdxPath.Delete(item)
	}
	f.cache.Clean()
	f.lock.Unlock()
}

func (f *LogFile) CheckReset() (ok bool) {
	if ok = f.lock.TryLock(); !ok {
		log.Errorf("Failed: LogFile.CheckReset, f.lock is taken")
		return
	}
	if ok2 := f.hIdxPath.Len() == 0; !ok2 {
		log.Errorf("Failed: LogFile.CheckReset, len(f.hIdxPath) != 0")
		ok = false
	}
	if ok2 := f.cache.CheckReset(); !ok2 {
		log.Errorf("Failed: LogFile.CheckReset, cache")
		ok = false
	}
	if ok2 := f.fd == -1; !ok2 {
		log.Errorf("Failed: LogFile.CheckReset, f.fd != -1")
		ok = false
	}
	f.lock.Unlock()
	return
}

const (
	RaftFileCacheLimit = 100
)

type DataCacheKey struct {
	id     FileIdType
	offset int64
	length int32
}

type DataCache struct {
	buffers        map[DataCacheKey][]byte
	order          *list.List
	lock           *sync.RWMutex
	maxKeys        int
	maxValueLength int
}

func NewDataCache(maxKeys int, maxValueLength int) *DataCache {
	return &DataCache{
		buffers: make(map[DataCacheKey][]byte), order: list.New(),
		lock: new(sync.RWMutex), maxKeys: maxKeys, maxValueLength: maxValueLength,
	}
}

func (c *DataCache) Get(id FileIdType, offset int64, length int32) ([]byte, bool) {
	c.lock.RLock()
	buf, ok := c.buffers[DataCacheKey{id: id, offset: offset, length: length}]
	c.lock.RUnlock()
	//log.Debugf("DataCache.Get, id=%v, offset=%v, length=%v, len(buf)=%v, ok=%v", id, offset, length, len(buf), ok)
	return buf, ok
}

func (c *DataCache) Put(id FileIdType, offset int64, buf []byte) {
	if len(buf) > c.maxValueLength {
		return
	}
	c.lock.Lock()
	if len(c.buffers) >= c.maxKeys {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.buffers, oldest.Value.(DataCacheKey))
	}
	key := DataCacheKey{id: id, offset: offset, length: int32(len(buf))}
	c.buffers[key] = buf
	c.order.PushFront(key)
	c.lock.Unlock()
	//log.Debugf("DataCache.Put, id=%v, offset=%v, len(data)=%v, copied=%v", id, offset, len(data), copied)
}

func (c *DataCache) Close() {
	c.lock.Lock()
	c.buffers = make(map[DataCacheKey][]byte)
	c.order = list.New()
	c.lock.Unlock()
}

func (c *DataCache) CheckReset() (ok bool) {
	if ok = c.lock.TryLock(); !ok {
		log.Errorf("Failed: DataCache.CheckReset, c.lock is taken")
		return
	}
	if ok2 := len(c.buffers) == 0; !ok2 {
		log.Errorf("Failed: DataCache.CheckReset, len(c.buffers) != 0")
		ok = false
	}
	if ok2 := c.order.Len() == 0; !ok2 {
		log.Errorf("Failed: DataCache.CheckReset, c.order.Len() != 0")
		ok = false
	}
	c.lock.Unlock()
	return
}

type CachedFd struct {
	id FileIdType
	fd int
}
type RaftFiles struct {
	lock           *sync.Mutex
	filePrefix     string
	nextOffset     map[FileIdType]int64
	fdCache        map[FileIdType]*list.Element
	fdList         *list.List
	cacheLimit     int
	dataCache      *DataCache
	totalDiskUsage int64 //Note: currentTerm and votedFor are not counted
}

func NewRaftFileCache(filePrefix string, cacheLimit int) *RaftFiles {
	ret := &RaftFiles{
		lock:           new(sync.Mutex),
		filePrefix:     filePrefix,
		nextOffset:     nil,
		fdCache:        nil,
		fdList:         nil,
		dataCache:      NewDataCache(32, 4096),
		cacheLimit:     cacheLimit,
		totalDiskUsage: 0,
	}
	ret.Reset()
	return ret
}

func (c *RaftFiles) Reset() {
	nextOffset := make(map[FileIdType]int64)
	baseName := filepath.Base(c.filePrefix)
	re := regexp.MustCompile(fmt.Sprintf("%s-([0-9-]+)-([0-9-]+).log", baseName))
	totalDiskUsage := int64(0)
	if dirs, err2 := os.ReadDir(filepath.Dir(c.filePrefix)); err2 == nil {
		for _, file := range dirs {
			f, err3 := file.Info()
			if err3 != nil {
				continue
			}
			if f.IsDir() {
				continue
			}
			found := re.FindAllStringSubmatch(f.Name(), 1)
			if len(found) <= 0 || len(found[0]) <= 1 {
				continue
			}
			lower, err := strconv.ParseUint(found[0][1], 10, 64)
			if err != nil {
				log.Errorf("Failed: Reset, ParseUint (0), malformed log file, file=%v, err=%v", file.Name(), err)
				continue
			}
			upper, err := strconv.ParseUint(found[0][2], 10, 64)
			if err != nil {
				log.Errorf("Failed: Reset, ParseUint (1), malformed log file, file=%v, err=%v", file.Name(), err)
				continue
			}
			fileId := FileIdType{lower: uint64(lower), upper: uint64(upper)}
			nextOffset[fileId] = f.Size()
			totalDiskUsage += f.Size()
			log.Infof("Reset, found existing log file, file=%v, fileId=%v, size=%v", file.Name(), fileId, f.Size())
		}
	}
	c.lock.Lock()
	c.nextOffset = nextOffset
	c.totalDiskUsage = totalDiskUsage
	c.clearNoLock()
	c.lock.Unlock()
	log.Infof("Success: Reset, totalDiskUsage=%v", totalDiskUsage)
}

func (c *RaftFiles) GetFileName(fileId FileIdType) string {
	return fmt.Sprintf("%s-%d-%d.log", c.filePrefix, fileId.lower, fileId.upper)
}

func (c *RaftFiles) GetFileLength(fileId FileIdType) int64 {
	c.lock.Lock()
	length := c.nextOffset[fileId]
	c.lock.Unlock()
	return length
}

func (c *RaftFiles) GetDiskUsage() int64 {
	return atomic.LoadInt64(&c.totalDiskUsage)
}

func (c *RaftFiles) AddDiskUsage(size int64) int64 {
	return atomic.AddInt64(&c.totalDiskUsage, size)
}

func (c *RaftFiles) Open(fileId FileIdType, mode int) (int, error) {
	fileName := c.GetFileName(fileId)
	fd, errno := unix.Open(fileName, mode, 0644)
	if errno != nil {
		log.Errorf("Failed: RaftFiles.Open, fileName=%v, mode=%v, errno=%v", fileName, mode, errno)
		return -1, errno
	}
	return fd, errno
}

func (c *RaftFiles) openCacheNoLock(fileId FileIdType) (int, int32) {
	if !fileId.IsValid() {
		log.Errorf("Failed: openCacheNoLock, fileId is invalid, fileId=%v", fileId)
		return -1, ErrnoToReply(unix.EINVAL)
	}
	element, ok := c.fdCache[fileId]
	if ok {
		c.fdList.MoveToFront(element)
		return element.Value.(*CachedFd).fd, RaftReplyOk
	}
	fileName := c.GetFileName(fileId)
	fd, errno := unix.Open(fileName, unix.O_RDWR|unix.O_CREAT|unix.O_SYNC, 0644)
	if errno != nil {
		log.Errorf("Failed: RaftFiles.Get, openCacheNoLock, fileName=%v, errno=%v", fileName, errno)
		return -1, ErrnoToReply(errno)
	}
	if c.cacheLimit <= c.fdList.Len()+1 {
		last := c.fdList.Back()
		c.fdList.Remove(last)
		entry := last.Value.(*list.Element).Value.(*CachedFd)
		_ = unix.Close(entry.fd)
		delete(c.fdCache, entry.id)
	}
	element = &list.Element{Value: &CachedFd{id: fileId, fd: fd}}
	c.fdList.PushFront(element)
	c.fdCache[fileId] = element
	return fd, RaftReplyOk
}

func (c *RaftFiles) Splice(pipeFds [2]int, fileId FileIdType, fd int, offset int64, dataLength int32, bufOff *int32) (err error) {
	fileName := c.GetFileName(fileId)
	var toFd int
	toFd, err = unix.Open(fileName, unix.O_WRONLY|unix.O_CREAT, 0644)
	if err != nil {
		log.Errorf("Failed: RaftFiles.Splice, Open, fileId=%v, err=%v", fileId, err)
		return
	}
	//beginOffset := offset + int64(*bufOff)
	var count = int64(0)
	//var newDiskUsage = int64(0)
	for *bufOff < dataLength {
		var nRead int64
		nRead, err = unix.Splice(fd, nil, pipeFds[1], nil, int(dataLength-*bufOff), unix.SPLICE_F_MOVE|unix.SPLICE_F_NONBLOCK)
		if nRead == 0 || err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			err = nil
			break
		} else if err != nil {
			log.Errorf("Failed: RaftFiles.Splice, Splice (0), nfd=%v, pipeFds[1]=%v, bodyLength=%v, err=%v", fd, pipeFds[1], dataLength, err)
			goto close
		}
		bodyOff := offset + int64(*bufOff)
		for nRead > 0 {
			var l2 = int(nRead & int64(math.MaxInt))
			var nWrite int64
			nWrite, err = unix.Splice(pipeFds[0], nil, toFd, &bodyOff, l2, unix.SPLICE_F_MOVE)
			if err != nil {
				log.Errorf("Failed: RaftFiles.Splice, Splice (1), pipeFds[0]=%v, toFd=%v, l2=%v, offset=%v, err=%v", pipeFds[0], toFd, l2, bodyOff, err)
				goto close
			}
			if nWrite == 0 {
				break
			}
			nRead -= nWrite
			*bufOff += int32(nWrite)
			count += nWrite
		}
	}
	if err2 := unix.Fsync(toFd); err2 != nil {
		log.Warnf("Failed (ignore): RaftFiles.Splice, Fsync, fd=%v, err=%v", fd, err2)
	}
	/*if err2 := unix.Fadvise(toFd, beginOffset, count, unix.FADV_DONTNEED); err2 != nil {
		log.Warnf("Failed (ignore): RaftFiles.Splice, Fadvise, fd=%v, err=%v", fd, err2)
	}*/
	//log.Debugf("Success: RaftFiles.Splice, newDiskUsage=%v, count=%v", newDiskUsage, count)
close:
	if err2 := unix.Close(toFd); err2 != nil {
		log.Warnf("Failed (ignore): RaftFiles.Splice, Close, toFd=%v, err=%v", toFd, err2)
	}
	return
}

func (c *RaftFiles) SyncWrite(fileId FileIdType, buf []byte) (offset int64, length int32, reply int32) {
	var begin, copy, write, fsync time.Time
	begin = time.Now()
	c.lock.Lock()
	var ok bool
	offset, ok = c.nextOffset[fileId]
	if !ok {
		offset = 0
	}
	c.nextOffset[fileId] = offset + int64(len(buf))
	c.AddDiskUsage(int64(length))
	c.lock.Unlock()

	fileName := c.GetFileName(fileId)
	bufSlice := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	mode := unix.O_WRONLY | unix.O_SYNC | unix.O_CREAT
	var dst []byte = buf
	//var pages *PageBuffer = nil
	bufAligned := bufSlice.Data%uintptr(SectorSize) == 0
	/*if !bufAligned {
		var err error
		pages, err = GetPageBuffer(int64(len(buf)))
		if err == nil {
			dst = pages.Buf
			bufAligned = true
			memcpy(dst, buf)
		}
	}*/
	copy = time.Now()
	if bufAligned && len(dst)%SectorSize == 0 && offset%int64(SectorSize) == 0 {
		mode |= unix.O_DIRECT
	}
	fd, err := unix.Open(fileName, mode, 0644)
	if err != nil {
		log.Errorf("Failed: RaftFiles.SyncWrite, fileName=%v, directIO=%v, errno=%v", fileName, mode&unix.O_DIRECT != 0, err)
		reply = ErrnoToReply(err)
		goto put
	}
	length = 0
	for length < int32(len(dst)) {
		var c int
		c, err = unix.Pwrite(fd, dst[length:], offset+int64(length))
		if err != nil {
			log.Errorf("Failed: RaftFiles.SyncWrite, Pwrite, fileId=%v, fd=%v, offset=%v, length=%v, directIO=%v, err=%v",
				fileId, fd, offset, length, mode&unix.O_DIRECT != 0, err)
			reply = ErrnoToReply(err)
			goto out
		}
		length += int32(c)
	}
	write = time.Now()
	if err = unix.Fsync(fd); err != nil {
		log.Errorf("Failed: RaftFiles.SyncWrite, Fsync, fileId=%v, fd=%v, offset=%v, length=%v, directIO=%v, err=%v",
			fileId, fd, offset, length, mode&unix.O_DIRECT != 0, err)
		reply = ErrnoToReply(err)
		goto out
	}
	if mode&unix.O_DIRECT == 0 {
		if err = unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED); err != nil {
			log.Errorf("Failed (ignore): RaftFiles.SyncWrite, Fadvise, fileId=%v, fd=%v, offset=%v, length=%v, directIO=%v, err=%v",
				fileId, fd, offset, length, mode&unix.O_DIRECT != 0, err)
		}
	}
	fsync = time.Now()
	reply = ErrnoToReply(err)
	log.Debugf("Success: RaftFiles.SyncWrite, fileId=%v, offset=%v, length=%v, directIO=%v (bufAligned == %v, len(dst)%%SectorSize == %v, offset%%SectorSize == %v), copy=%v, write=%v, fsync=%v, elapsed=%v",
		fileId, offset, length, mode&unix.O_DIRECT != 0, bufAligned, len(dst)%SectorSize, offset%int64(SectorSize),
		copy.Sub(begin), write.Sub(copy), fsync.Sub(write), fsync.Sub(begin),
	)
out:
	if err2 := unix.Close(fd); err2 != nil {
		log.Errorf("Failed (ignore): RaftFiles.SyncWrite, Close, fileId=%v, fd=%v, err=%v", fileId, fd, err2)
	}
put:
	/*if pages != nil {
		ReturnPageBuffer(pages)
	}*/
	return
}

func (c *RaftFiles) OpenAndWriteCache(fileId FileIdType, buf []byte) (offset int64, length int32, reply int32) {
	c.lock.Lock()
	var ok bool
	offset, ok = c.nextOffset[fileId]
	if !ok {
		offset = 0
	}
	c.nextOffset[fileId] = offset + int64(len(buf))
	var fd int
	fd, reply = c.openCacheNoLock(fileId)
	c.AddDiskUsage(int64(length))
	c.lock.Unlock()

	if reply != RaftReplyOk {
		log.Errorf("Failed: RaftFiles.OpenAndWriteCache, fileId=%v, reply=%v", fileId, reply)
		return
	}
	length = 0
	for length < int32(len(buf)) {
		c, errno := unix.Pwrite(fd, buf[length:], offset+int64(length))
		if errno != nil {
			log.Errorf("Failed: RaftFiles.OpenAndWriteCache, Pwrite, fileId=%v, fd=%v, offset=%v, length=%v, errno=%v",
				fileId, fd, offset, length, errno)
			reply = ErrnoToReply(errno)
			return
		}
		length += int32(c)
	}
	if errno := unix.Fsync(fd); errno != nil {
		log.Errorf("Failed: RaftFiles.OpenAndWriteCache, Fsync, fileId=%v, fd=%v, offset=%v, length=%v, errno=%v",
			fileId, fd, offset, length, errno)
	}
	//log.Debugf("Success: RaftFiles.OpenAndWriteCache, fileId=%v, newDiskUsage=%v, length=%v", fileId, newDiskUsage, length)
	return
}

func (c *RaftFiles) OpenAndReadCache(fileId FileIdType, offset int64, length int32) ([]byte, int32) {
	if buf, ok := c.dataCache.Get(fileId, offset, length); ok {
		return buf, RaftReplyOk
	}
	c.lock.Lock()
	fd, reply := c.openCacheNoLock(fileId)
	c.lock.Unlock()
	if reply != RaftReplyOk {
		log.Errorf("Failed: RaftFiles.OpenAndReadCache, openCacheNoLock, fileId=%v, reply=%v", fileId, reply)
		return nil, reply
	}
	buf := make([]byte, length)
	var bufOff = int32(0)
	for bufOff < length {
		r, errno := unix.Pread(fd, buf[bufOff:], offset+int64(bufOff))
		if errno != nil {
			log.Errorf("Failed: RaftFiles.OpenAndReadCache, ReadAt, offset=%v, bufOff=%v, length=%v, errno=%v", offset, bufOff, length, errno)
			return nil, ErrnoToReply(errno)
		}
		if r == 0 {
			break
		}
		bufOff += int32(r)
	}
	return buf, RaftReplyOk
}

func (c *RaftFiles) ReserveRange(fileId FileIdType, dataLen int64) (offset int64) {
	c.lock.Lock()
	offset = c.nextOffset[fileId]
	c.nextOffset[fileId] = offset + dataLen
	c.AddDiskUsage(dataLen)
	c.lock.Unlock()
	return offset
}

func (c *RaftFiles) SeekRange(fileId FileIdType, newNextOffset int64) {
	c.lock.Lock()
	offset := c.nextOffset[fileId]
	if offset < newNextOffset {
		c.nextOffset[fileId] = newNextOffset
		c.AddDiskUsage(newNextOffset - offset)
	}
	c.lock.Unlock()
}

func (c *RaftFiles) Remove(fileId FileIdType) (int64, error) {
	c.lock.Lock()
	if element, ok := c.fdCache[fileId]; ok {
		entry := element.Value.(*CachedFd)
		_ = unix.Close(entry.fd)
		c.fdList.Remove(element)
		delete(c.fdCache, fileId)
	}
	fileName := c.GetFileName(fileId)
	stat := unix.Stat_t{}
	err := unix.Stat(fileName, &stat)
	if err != nil {
		c.lock.Unlock()
		if err != unix.ENOENT {
			log.Errorf("Failed: RaftFiles.Remove, Stat, fileName=%v, err=%v", fileName, err)
			return 0, err
		}
		return 0, nil
	}
	err = unix.Unlink(fileName)
	if err != nil {
		c.lock.Unlock()
		log.Errorf("Failed: RaftFiles.Remove, Unlink, fileName=%v, err=%v", fileName, err)
		return 0, err
	}
	delete(c.nextOffset, fileId)
	newDiskUsage := c.AddDiskUsage(-stat.Size)
	c.lock.Unlock()
	log.Debugf("Success: RaftFiles.Remove, fileId=%v, newDiskUsage=%v, stat.Size=%v", fileId, newDiskUsage, stat.Size)
	return stat.Size, nil
}

func (c *RaftFiles) DontNeed(fileId FileIdType) {
	c.lock.Lock()
	fileName := c.GetFileName(fileId)
	offset, ok := c.nextOffset[fileId]
	if !ok || offset == 0 {
		c.lock.Unlock()
		return
	}
	fd, err := unix.Open(fileName, unix.O_RDONLY, 0644)
	if err != nil {
		c.lock.Unlock()
		log.Warnf("Failed: RaftFiles.DontNeed, OpenFile, fileName=%v, err=%v", fileName, err)
		return
	}
	if err = unix.Fadvise(fd, 0, offset, unix.FADV_DONTNEED); err != nil {
		log.Warnf("Failed: RaftFiles.DontNeed, Fadvise, fileId=%v, length=%v, err=%v", fileId, offset, err)
		// fall through to close
	}
	if err = unix.Close(fd); err != nil {
		log.Warnf("Failed: RaftFiles.DontNeed, Close, fileNaem=%v, err=%v", fileName, err)
	}
	c.lock.Unlock()
}

func (c *RaftFiles) Clear() {
	c.lock.Lock()
	c.clearNoLock()
	c.lock.Unlock()
}

func (c *RaftFiles) clearNoLock() {
	for _, element := range c.fdCache {
		entry := element.Value.(*CachedFd)
		_ = unix.Close(entry.fd)
	}
	c.fdCache = make(map[FileIdType]*list.Element)
	c.fdList = list.New()
	c.dataCache = NewDataCache(32, 4096)
	//c.totalDiskUsage = 0 // intentionally avoid clearing this. bugs raise check errors later
	c.nextOffset = make(map[FileIdType]int64)
}

func (c *RaftFiles) CheckReset() (ok bool) {
	if ok = c.lock.TryLock(); !ok {
		log.Errorf("Failed: RaftFiles.CheckReset, c.lock is taken")
		return
	}
	if ok2 := len(c.fdCache) == 0; !ok2 {
		log.Errorf("Failed: RaftFiles.CheckReset, len(c.fdCache) != 0")
		ok = false
	}
	if ok2 := c.fdList.Len() == 0; !ok2 {
		log.Errorf("Failed: RaftFiles.CheckReset, c.fdList.Len() != 0")
		ok = false
	}
	if ok2 := c.dataCache.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftFiles.CheckReset, c.dataCache")
		ok = false
	}
	if ok2 := c.totalDiskUsage == 0; !ok2 {
		log.Errorf("Failed: RaftFiles.CheckReset, c.totalDiskUsage != 0")
		ok = false
	}
	if ok2 := len(c.nextOffset) == 0; !ok2 {
		log.Errorf("Failed: RaftFiles.CheckReset, len(c.nextOffset) != 0")
		ok = false
	}
	c.lock.Unlock()
	return
}

const RaftPersistStateReset = uint32(math.MaxUint32)

func IsReset(value uint32) bool {
	return value == RaftPersistStateReset
}

type RaftPersistState struct {
	fileName string
	fd       int
	offset   int64
	value    uint32
	lock     *sync.RWMutex
}

func NewRaftPersistState(rootDir string, selfId uint32, stateName string) (*RaftPersistState, int32) {
	fileName := fmt.Sprintf("%s/%d-%s.log", rootDir, selfId, stateName)
	ret := &RaftPersistState{lock: new(sync.RWMutex), value: RaftPersistStateReset, fileName: fileName}
	st := unix.Stat_t{}
	errno := unix.Stat(fileName, &st)
	if errno == nil {
		var fd int
		fd, errno = unix.Open(fileName, unix.O_RDWR, 0644)
		if errno != nil {
			log.Errorf("Failed: NewRaftPersistState, OpenFileHandle (0), fileName=%v, errno=%v", fileName, errno)
			return nil, ErrnoToReply(errno)
		}
		var restore = false
		if st.Size >= int64(SizeOfUint32) {
			buf := [SizeOfUint32]byte{}
			if _, errno = unix.Pread(fd, buf[:], st.Size-int64(SizeOfUint32)); errno != nil {
				log.Errorf("Failed: NewRaftPersistState, ReadAt, fileName=%v, at=%v, err=%v", fileName, st.Size-int64(SizeOfUint32), errno)
				return nil, ErrnoToReply(errno)
			}
			restore = true
			ret.value = binary.LittleEndian.Uint32(buf[:])
		}
		ret.fd = fd
		if restore {
			log.Debugf("Restore: NewRaftPersistState, h=%v, selfId=%v, value=%v", stateName, selfId, ret.value)
		}
	} else if errno != unix.ENOENT {
		log.Errorf("Failed: NewRaftPersistState, Stat, fileName=%v, errno=%v", fileName, errno)
		return nil, ErrnoToReply(errno)
	} else {
		fd, err := unix.Open(fileName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0644)
		if err != nil {
			log.Errorf("Failed: NewRaftPersistState, OpenFileHandle, fileName=%v, err=%v", fileName, err)
			return nil, ErrnoToReply(errno)
		}
		ret.fd = fd
	}
	return ret, RaftReplyOk
}

func (s *RaftPersistState) __setNoLock(value uint32) int32 {
	if value == s.value {
		return RaftReplyOk
	}
	buf := [SizeOfUint32]byte{}
	binary.LittleEndian.PutUint32(buf[:], value)
	var bufOff = 0
	for bufOff < len(buf) {
		c, errno := unix.Pwrite(s.fd, buf[bufOff:], s.offset+int64(bufOff))
		if errno != nil {
			log.Errorf("Failed: RaftPersistState.__setNoLock, Pwrite, fileName=%v, offset=%v, bufOff=%v, errno=%v", s.fileName, s.offset, bufOff, errno)
			return ErrnoToReply(errno)
		}
		bufOff += c
	}
	s.offset += int64(bufOff)
	log.Debugf("RaftPersistState, %v: %v -> %v", s.fileName, s.value, value)
	s.value = value
	return RaftReplyOk
}

func (s *RaftPersistState) Set(value uint32) int32 {
	s.lock.Lock()
	reply := s.__setNoLock(value)
	s.lock.Unlock()
	return reply
}

func (s *RaftPersistState) Increment() int32 {
	s.lock.Lock()
	s.__setNoLock(s.value + 1)
	s.lock.Unlock()
	return RaftReplyOk
}

func (s *RaftPersistState) Reset() int32 {
	return s.Set(RaftPersistStateReset)
}

func (s *RaftPersistState) Get() uint32 {
	s.lock.RLock()
	v := s.value
	s.lock.RUnlock()
	return v
}

func (s *RaftPersistState) Clean() {
	s.lock.Lock()
	if s.fd > 0 {
		if err := unix.Close(s.fd); err != nil {
			log.Errorf("Failed: RaftPersistState.Clean, Close, fileName=%v, fd=%v, err=%v", s.fileName, s.fd, err)
		} else {
			s.fd = -1
		}
	}
	if s.fileName != "" {
		if err := unix.Unlink(s.fileName); err != nil {
			log.Errorf("Failed: RaftPersistState, Unlink, fileName=%v, err=%v", s.fileName, err)
		} else {
			s.fileName = ""
		}
	}
	s.lock.Unlock()
}

func (s *RaftPersistState) CheckReset() (ok bool) {
	if ok = s.lock.TryLock(); !ok {
		log.Errorf("Failed: RaftPersistState.CheckReset, s.lock is taken")
		return
	}
	if ok2 := s.fd == -1; !ok2 {
		log.Errorf("Failed: RaftPersistState.CheckReset, s.fd != -1")
		ok = false
	}
	if ok2 := s.fileName == ""; !ok2 {
		log.Errorf("Failed: RaftPersistState.CheckReset, s.fileName != \"\"")
		ok = false
	}
	s.lock.Unlock()
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
	resumeCallback func()
	applyCallback  func(*AppendEntryCommand) int32
	rpcClient      *RpcClient
	replyClient    *RpcReplyClient
	rpcServer      *RpcThreads
	serverIds      map[uint32]common.NodeAddrInet4
	sas            map[common.NodeAddrInet4]uint32

	// Extended log information
	files         *RaftFiles
	extNextSeqNum uint32
	extLogFileId  FileIdType
	extLock       *sync.RWMutex

	// Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	log         *LogFile          // log entries; each entry contains command for state machine, and Term when entry was received by leader (first reqId is 1)
	currentTerm *RaftPersistState // latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    *RaftPersistState // candidateId that received vote in current Term (or null if none) (NOTE: null -> uint32 MAX)

	// Volatile state on all servers:
	commitIndex uint64 // reqId of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied uint64 // reqId of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  map[uint32]uint64 // for each server, reqId of the next log entry to send to that server (initialized to leader last log reqId + 1)
	matchIndex map[uint32]uint64 // for each server, reqId of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

func (n *RaftInstance) IsLeader() (r RaftBasicReply) {
	n.lock.RLock()
	if n.leaderId == RaftPersistStateReset {
		r = RaftBasicReply{reply: RaftReplyVoting}
	} else if n.selfId != n.leaderId || n.state != RaftLeader {
		r = RaftBasicReply{reply: RaftReplyNotLeader, leaderId: n.leaderId, leaderAddr: n.serverIds[n.leaderId]}
	} else {
		r = RaftBasicReply{reply: RaftReplyOk, leaderId: n.selfId, leaderAddr: n.selfAddr}
	}
	n.lock.RUnlock()
	return r
}

func (n *RaftInstance) GetExtFileId() FileIdType {
	n.extLock.RLock()
	ret := n.extLogFileId
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

func (n *RaftInstance) SetExt(fileId uint64, seqNum uint32) {
	n.extLock.Lock()
	n.extLogFileId = FileIdType{lower: 0, upper: fileId}
	n.extNextSeqNum = seqNum
	n.extLock.Unlock()
}

func (n *RaftInstance) CleanExtFile() {
	n.extLock.Lock()
	if _, err := n.files.Remove(n.extLogFileId); err != nil {
		log.Errorf("Failed: CleanExtFile, Remove, extLogFileId=%v, err=%v", n.extLogFileId, err)
	} else {
		n.extLogFileId = FileIdType{lower: 0, upper: 0}
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
	if reply = n.replyClient.ReplyRpcMsg(msg, fd, sa, n.files, n.hbInterval, nil); reply != RaftReplyOk {
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
			if reply := n.log.SeekLog(newIndex); reply != RaftReplyOk {
				msg = RpcMsg{}
				currentTerm := n.currentTerm.Get()
				msg.FillAppendEntriesResponseArgs(currentTerm, true, currentLogLength, reply)
				log.Errorf("Failed: AppendEntriesRpcBottomHalf, SeekLog, newIndex=%v, reply=%v", newIndex, reply)
				if reply := n.replyClient.ReplyRpcMsg(msg, fd, sa, n.files, n.hbInterval, nil); reply != RaftReplyOk {
					log.Errorf("Failed: AppendEntriesRpcBottomHalf, ReplyRpcMsg, term=%v, logLength=%v, leaderCommit=%v, reply=%v", currentTerm, currentLogLength, leaderCommit, reply)
				}
				return
			}
			break
		}
	}
	seek := time.Now()

	// 4. Append any new entries not already in the log
	optHeaderLength := msg.GetOptHeaderLength()
	hadEntry = false
	var i = uint64(0)
	cmds := make([]*AppendEntryCommand, 0)
	if 0 < optHeaderLength {
		for off := uint16(RpcOptControlHeaderLength); off < optHeaderLength; {
			cmd, nextOff := msg.GetAppendEntryCommandDiskFormat(off)
			if reply := n.log.AppendCommandAt(prevLogIndex+1+i, cmd); reply != RaftReplyOk {
				msg = RpcMsg{}
				currentTerm := n.currentTerm.Get()
				msg.FillAppendEntriesResponseArgs(currentTerm, true, currentLogLength, reply)
				log.Errorf("Failed: AppendEntriesRpcBottomHalf, AppendCommandAt, Index=%v, extCmdId=%v, reply=%v", prevLogIndex+1+i, cmd.GetExtCmdId(), reply)
				if reply := n.replyClient.ReplyRpcMsg(msg, fd, sa, n.files, n.hbInterval, nil); reply != RaftReplyOk {
					log.Errorf("Failed: AppendEntriesRpcBottomHalf, ReplyRpcMsg, term=%v, logLength=%v, leaderCommit=%v, reply=%v", currentTerm, currentLogLength, leaderCommit, reply)
				}
				return
			}
			cmds = append(cmds, &cmd)
			log.Debugf("AppendEntriesRpcBottomHalf, AppendCommandAt, Index=%v, extCmdId=%v, off=%v, nextOff=%v", prevLogIndex+1+i, cmd.GetExtCmdId(), off, nextOff)
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
		currentLogLength = n.ApplyAll(cmds[j], prevLogIndex+1+j)
	}
	apply := time.Now()

	msg = RpcMsg{}
	currentTerm := n.currentTerm.Get()
	msg.FillAppendEntriesResponseArgs(currentTerm, true, currentLogLength, RaftReplyOk)
	if reply := n.replyClient.ReplyRpcMsg(msg, fd, sa, n.files, n.hbInterval, nil); reply != RaftReplyOk {
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
			n.leaderId = RaftPersistStateReset
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

func (n *RaftInstance) ReplicateLog(lastLogIndex uint64, added *uint32, addedSa *common.NodeAddrInet4, removedNodeId *uint32, cmd *AppendEntryCommand) (reply int32) {
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
	_ = n.ApplyAll(cmd, lastLogIndex)

	log.Debugf("Success: ReplicateLog, lastLogIndex=%v, hb=%v, apply=%v", lastLogIndex, endHb.Sub(begin), time.Since(endHb))
	return RaftReplyOk
}

func (n *RaftInstance) AppendEntriesLocal(cmd AppendEntryCommand) (ret interface{}, lastLogIndex uint64, reply int32) {
	if r := n.IsLeader(); r.reply != RaftReplyOk {
		return nil, 0, r.reply
	}
	//begin := time.Now()
	lastLogIndex, reply = n.log.AppendCommand(cmd)
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendEntriesLocal, AppendCommand, reply=%v", reply)
		return nil, 0, reply
	}
	endLocal := time.Now()
	reply = n.ReplicateLog(lastLogIndex, nil, nil, nil, &cmd)
	endAll := time.Now()
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendEntriesLocal, ReplicateLog, lastLogIndex=%v, replicate=%v, reply=%v", lastLogIndex, endAll.Sub(endLocal), reply)
		return nil, 0, reply
	}

	//log.Debugf("Success: AppendEntriesLocal, lastLogIndex=%v, writeLocal=%v, replicate=%v",
	//	lastLogIndex, endLocal.Sub(begin), endAll.Sub(endLocal))
	return ret, lastLogIndex, RaftReplyOk
}

func (n *RaftInstance) AppendExtendedLogEntry(extCmdId uint16, m proto.Message) int32 {
	if r := n.IsLeader(); r.reply != RaftReplyOk {
		return r.reply
	}
	begin := time.Now()
	buf, err := proto.Marshal(m)
	if err != nil {
		log.Errorf("Failed: AppendExtendedLogEntry, Marshal, err=%v", err)
		return RaftReplyFail
	}
	fileId := n.GetExtFileId()
	marshalTime := time.Now()
	fileOffset, _, reply := n.files.OpenAndWriteCache(fileId, buf)
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendExtendedLogEntry, OpenAndWriteCache, fileId=%v, reply=%v", fileId, reply)
		return reply
	}
	n.files.dataCache.Put(fileId, fileOffset, buf)
	writeTime := time.Now()
	_, _, reply = n.AppendEntriesLocal(NewAppendEntryFileCommand(n.currentTerm.Get(), extCmdId, fileId, fileOffset, buf))
	appendEntryTime := time.Now()
	if reply != RaftReplyOk {
		log.Errorf("Failed: AppendExtendedLogEntry, AppendEntriesLocal, reply=%v", reply)
		return reply
	}
	log.Debugf("Success: AppendExtendedLogEntry, fileId=%v, extCmdId=%v, marshalTime=%v, writeTime=%v, appendEntryTime=%v",
		fileId, extCmdId, marshalTime.Sub(begin), writeTime.Sub(marshalTime), appendEntryTime.Sub(writeTime))
	return RaftReplyOk
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
			optHeaderLength, totalFileLength := cmd.AppendToRpcMsg(&msg)
			appendedBytes = int(RpcMsgMaxLength) + int(optHeaderLength) + int(totalFileLength)
			log.Debugf("heartBeatOneRound, prepare: sa=%v, Term=%v, Index=%v, extCmdId=%v, optHeaderLength=%v, totalFileLength=%v",
				sa, cmd.GetTerm(), nextIndex+i, cmd.GetExtCmdId(), optHeaderLength, totalFileLength)
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

		indices = append(indices, n.log.GetCurrentLogLength()-1)
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
		votedFor = RaftPersistStateReset
	}

	// 1. Reply false if T < currentTerm (3.3)
	if T < currentTerm {
		log.Errorf("Failed: RequestVoteRpc, T (%v) < currentTerm (%v), candidateId=%v", T, currentTerm, candidateId)
		goto respond
	}
	// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (3.4, 3.6)
	if ok := IsReset(votedFor) || votedFor == candidateId; ok {
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
	if reply = n.replyClient.ReplyRpcMsg(msg, fd, sa, n.files, n.hbInterval, nil); reply != RaftReplyOk {
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
	if _, _, reply2 := n.AppendEntriesLocal(NewAppendEntryNoOpCommandDiskFormat(n.currentTerm.Get())); reply2 != RaftReplyOk {
		log.Warnf("Failed: requestVotesAny, AppendEntriesLocal (NoOp), reply=%v", reply2)
		return false
	}
	n.files.Reset()
	n.resumeCallback()
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
	n.leaderId = RaftPersistStateReset
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
	n.CleanExtFile()
	_ = n.rpcClient.Close()
	n.replyClient.Close()
	n.rpcServer.Close()
	n.files.Clear()
	n.log.Clean()
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
	if ok2 := n.files.CheckReset(); !ok2 {
		log.Errorf("Failed: RaftInstance.CheckReset, files")
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
		if n.extLogFileId.lower != 0 || n.extLogFileId.upper != 0 {
			log.Errorf("Failed: RaftInstance.CheckReset, n.extLogFileId != 0")
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
	updatedNextIndex := n.log.GetCurrentLogLength()

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
		extHeaderLength, totalFileLength := cmd.AppendToRpcMsg(&msg)
		appendedBytes = int(RpcMsgMaxLength) + int(extHeaderLength) + int(totalFileLength)
		log.Debugf("CatchUpLog, AppendToRpcMsg, sa=%v, Term=%v, Index=%v", sa, cmd.GetTerm(), logIndex)
	}

	if appendedBytes == int(RpcMsgMaxLength) {
		return lastErrReply
	}
	n.hbLock.Lock()
	_, err := n.rpcClient.SendAndWait(msg, sa, n.files, timeout)
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
	cmd := NewAppendEntryAddServerCommand(n.currentTerm.Get(), serverId, sa.Addr, uint16(sa.Port))
	lastLogIndex, reply := n.log.AppendCommand(cmd)
	if reply != RaftReplyOk {
		log.Errorf("Failed: AddServerLocal, AppendLocalLogFromExt, serverId=%v, reply=%v", serverId, reply)
		return reply
	}
	reply = n.ReplicateLog(lastLogIndex, &serverId, &sa, nil, &cmd)
	if reply != RaftReplyOk {
		log.Errorf("Failed: AddServerLocal, AppendReplicatedLog, serverId=%v, reply=%v", serverId, reply)
		return reply
	}

	// 5. Reply OK
	log.Debugf("Success: AddServerLocal, sa=%v", sa)
	return RaftReplyOk
}

func (n *RaftInstance) WaitPreviousCommits() {
	if n.log.GetCurrentLogLength() == 0 {
		return
	}
	for n.commitIndex < n.log.GetCurrentLogLength()-1 {
		n.lock.RUnlock()
		time.Sleep(n.hbInterval)
		n.lock.RLock()
	}
}

func (n *RaftInstance) AppendInitEntry(cmd AppendEntryCommand) int32 {
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
	_ = n.ApplyAll(&cmd, lastLogIndex)
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
	cmd := NewAppendEntryAddServerCommand(n.currentTerm.Get(), n.selfId, n.selfAddr.Addr, uint16(n.selfAddr.Port))
	if reply := n.AppendInitEntry(cmd); reply != RaftReplyOk {
		log.Errorf("Failed: AppendBootstrapLogs (AddServer), AppendInitEntry, reply=%v", reply)
		return reply
	}
	cmd2 := NewAppendEntryResetExtLogCommand(n.currentTerm.Get(), 100, 1)
	if reply := n.AppendInitEntry(cmd2); reply != RaftReplyOk {
		log.Errorf("Failed: AppendBootstrapLogs (ResetExtLog), AppendInitEntry, reply=%v", reply)
		return reply
	}
	id := n.GenerateCoordinatorId()
	txId := &common.TxIdMsg{ClientId: id.clientId, SeqNum: id.seqNum, TxSeqNum: 0}
	node := &common.NodeMsg{NodeId: n.selfId, Addr: make([]byte, 4), Port: int32(n.selfAddr.Port), GroupId: groupId}
	copy(node.Addr, n.selfAddr.Addr[:])
	msg := &common.UpdateNodeListMsg{Nodes: &common.MembershipListMsg{}, TxId: txId, IsAdd: true}
	msg.Nodes.Servers = append(msg.Nodes.Servers, node)
	if reply := n.AppendExtendedLogEntry(AppendEntryUpdateNodeListLocalCmdId, msg); reply != RaftReplyOk {
		log.Errorf("Failed: AppendBootstrapLogs, AppendExtendedLogEntry, txId=%v, reply=%v", txId, reply)
		return reply
	}
	log.Debugf("Success: AppendBootstrapLogs, selfId=%v, fileId=%v, nextSeqNum=%v", n.selfId, 100, 1)
	return RaftReplyOk
}

func (n *RaftInstance) ApplyAll(cmd *AppendEntryCommand, logIndex uint64) (currentLogLength uint64) {
	begin := time.Now()
	n.applyCond.L.Lock()
	for n.lastApplied+1 < logIndex {
		n.applyCond.Wait()
	}
	bug := n.lastApplied+1 > logIndex
	n.applyCond.L.Unlock()
	if bug {
		log.Warnf("BUG: RaftInstance.ApplyAll, some threads stealed processing commitIndex (%v)", logIndex)
		return n.log.GetCurrentLogLength()
	}
	// n.lastApplied+1 == logIndex

	applyBegin := time.Now()
	reply2 := n.applyCallback(cmd)
	if reply2 != RaftReplyOk {
		log.Errorf("Failed (ignore): RaftInstance.Apply, Index=%v, reply=%v", logIndex, reply2)
	} else {
		log.Debugf("Success: RaftInstance.ApplyAll, Apply, Index=%v, extCmdId=%v, reply=%v, lockTime=%v, applyTime=%v",
			logIndex, cmd.GetExtCmdId(), reply2, applyBegin.Sub(begin), time.Since(applyBegin))
	}
	n.applyCond.L.Lock()
	if n.lastApplied < logIndex {
		n.lastApplied = logIndex
	}
	n.applyCond.Broadcast()
	n.applyCond.L.Unlock()
	return n.log.GetCurrentLogLength()
}

func (n *RaftInstance) ReplayAll() int32 {
	var index = uint64(0)
	for ; index < n.log.GetCurrentLogLength(); index++ {
		cmd, reply := n.log.LoadCommandAt(index)
		if reply != RaftReplyOk {
			log.Errorf("Failed: ReplayAll, index=%v, reply=%v", index, reply)
			return reply
		}
		reply = n.applyCallback(&cmd)
		if reply != RaftReplyOk {
			log.Errorf("Failed: ReplayAll, applyCallback, index=%v, reply=%v", index, reply)
			return reply
		}
		log.Debugf("Success: RaftInstance.ApplyAll, Apply, Index=%v, extCmdId=%v", index, cmd.GetExtCmdId())
	}
	n.applyCond.L.Lock()
	if n.lastApplied < index {
		n.lastApplied = index
	}
	n.applyCond.Broadcast()
	n.applyCond.L.Unlock()
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
	cmd := NewAppendEntryRemoveServerCommand(n.currentTerm.Get(), serverId)
	lastLogIndex, reply := n.log.AppendCommand(cmd)
	if reply != RaftReplyOk {
		log.Errorf("Failed: RemoveServerLocal, AppendCommand, serverId=%v, reply=%v", serverId, reply)
		return reply
	}
	reply = n.ReplicateLog(lastLogIndex, nil, nil, &n.selfId, &cmd)
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
		cmd := NewAppendEntryRemoveServerCommand(n.currentTerm.Get(), serverId)
		last, reply := n.log.AppendCommand(cmd)
		if reply != RaftReplyOk {
			log.Errorf("Failed: RemoveServerLocal, AppendCommand, serverId=%v, reply=%v", serverId, reply)
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
		_ = n.ApplyAll(cmds[i], lastLogIndex-nrRemoved+1+i)
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
	logFile, reply := NewLogFile(dataDir, fmt.Sprintf("%d-raft-", server.args.ServerId))
	if reply != RaftReplyOk {
		log.Errorf("Failed: NewRaftInstance, NewLogFile, rootDir=%v, reply=%v", dataDir, reply)
		return nil, 0
	}
	logLength := logFile.GetCurrentLogLength() // get logLength here since we want to get this before recording no op at leader election
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
	currentTerm, reply := NewRaftPersistState(logFile.rootDir, selfId, "currentTerm")
	if reply != RaftReplyOk {
		log.Errorf("Failed: NewRaftInstance, NewRaftPersistState, rootDir=%v, selfId=%v, h=currentTerm, reply=%v", logFile.rootDir, selfId, reply)
		return nil, 0
	}
	if currentTerm.Get() == RaftPersistStateReset {
		if reply = currentTerm.Set(0); reply != RaftReplyOk {
			log.Errorf("Failed: NewRaftInstance, NewRaftPersistState, currentTerm.Get, reply=%v", reply)
		}
	}
	votedFor, reply := NewRaftPersistState(logFile.rootDir, selfId, "votedFor")
	if reply != RaftReplyOk {
		log.Errorf("Failed: NewRaftInstance, NewRaftPersistState, rootDir=%v, selfId=%v, h=votedFor, reply=%v", logFile.rootDir, selfId, reply)
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
		leaderId:       RaftPersistStateReset,
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
		log:            logFile,
		files:          NewRaftFileCache(fmt.Sprintf("%s/%d-ext", logFile.rootDir, selfId), RaftFileCacheLimit),
		currentTerm:    currentTerm,
		votedFor:       votedFor,
		maxHBBytes:     int(server.flags.RpcChunkSizeBytes),
		resumeCallback: server.ResumeCoordinatorCommit,
		applyCallback:  server.Apply,
		rpcClient:      rpcClient,
		replyClient:    replyClient,
		rpcServer:      &rpcServer,
		serverIds:      make(map[uint32]common.NodeAddrInet4),
		sas:            make(map[common.NodeAddrInet4]uint32),
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
