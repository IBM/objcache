/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"fmt"
	"os"
	"time"

	"github.com/takeshi-yoshimura/fuse/fuseops"
	"github.com/IBM/objcache/common"
)

type WorkingMeta struct {
	// inodeKey is the unique ID for an inode, which is equivalent to UNIX inode number.
	// owner node of an inode can be calculated with hash(inodeKey)
	inodeKey InodeKeyType

	// fetchKey is the original key at COS.
	// Rename and hard link operations may update the actual file path different from this value.
	// We can keep fetching the original content from remote as intended even after such operations.
	fetchKey string

	// version represent the unique number for a version of an inode.
	version uint32

	// chunkSize enables us to save memory space to remember maps for chunks.
	// We provide fixed-size of chunks for each inode (currently, we only support 5MiB chunks).
	// owner node of an offset in inode inodeKey can be calculated with hash(<inodeKey, (offset - offset % chunkSize)>)
	// Note: for a simple optimization, we define the owner node for offset 0 becomes same as its inode owner (i.e., hash(inodeKey))
	chunkSize int64

	nlink    uint32
	size     int64
	mTime    int64
	mode     uint32
	expireMs int32
	prevVer  *WorkingMeta
}

func NewWorkingMeta(inodeKey InodeKeyType, chunkSize int64, expireMs int32, mode uint32, nlink uint32, fetchKey string) *WorkingMeta {
	return &WorkingMeta{
		inodeKey:  inodeKey,
		version:   1,
		chunkSize: chunkSize,
		size:      int64(0),
		mTime:     time.Now().UnixNano(),
		expireMs:  expireMs,
		mode:      mode,
		nlink:     nlink,
		fetchKey:  fetchKey,
		prevVer:   nil,
	}
}

func NewWorkingMetaFromMsg(res *common.CopiedMetaMsg) *WorkingMeta {
	meta := &WorkingMeta{
		inodeKey:  InodeKeyType(res.GetInodeKey()),
		version:   res.GetVersion(),
		chunkSize: res.GetChunkSize(),
		size:      res.GetSize(),
		mode:      res.GetMode(),
		nlink:     res.GetNlink(),
		mTime:     res.GetLastModified(),
		expireMs:  res.GetExpireMs(),
		fetchKey:  res.GetFetchKey(),
		prevVer:   nil,
	}
	return meta
}

func (m *WorkingMeta) IsDir() bool {
	return os.FileMode(m.mode)&os.ModeDir != 0
}

func (m *WorkingMeta) IsDeleted() bool {
	return m.nlink == 0
}

func (m *WorkingMeta) toMsg() *common.CopiedMetaMsg {
	ret := &common.CopiedMetaMsg{
		InodeKey:     uint64(m.inodeKey),
		Version:      m.version,
		ChunkSize:    m.chunkSize,
		Size:         m.size,
		Mode:         m.mode,
		Nlink:        m.nlink,
		LastModified: m.mTime,
		ExpireMs:     m.expireMs,
		FetchKey:     m.fetchKey,
	}
	return ret
}

// DropPrev: Meta.lock must be held
func (m *WorkingMeta) DropPrev() {
	m.prevVer = nil // this is safe because readers are blocked or can access the pointer for dropped versions
}

func (m *WorkingMeta) GetAttr(uid uint32, gid uint32) fuseops.InodeAttributes {
	ts := time.Unix(0, m.mTime)
	return fuseops.InodeAttributes{
		Size: uint64(m.size), Nlink: m.nlink, Mode: os.FileMode(m.mode),
		Atime: ts, Mtime: ts, Ctime: ts, Crtime: ts, Uid: uid, Gid: gid,
	}
}

func (m *WorkingMeta) GetMetadata() map[string][]byte {
	ret := make(map[string][]byte)
	ret["user.chunk_size"] = []byte(fmt.Sprintf("%d", m.chunkSize))
	if m.expireMs > 0 {
		ret["user.dirty_expire"] = []byte(time.Duration(int64(m.expireMs) * 1000 * 1000).String())
	} else {
		ret["user.dirty_expire"] = []byte("disabled")
	}
	//TODO
	return ret
}
