/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/takeshi-yoshimura/fuse/fuseops"
	"github.ibm.com/TYOS/objcache/common"
)

type Meta struct {
	metaLock    *sync.RWMutex
	workingHead WorkingMeta
	lock        *sync.Mutex
	persistLock *sync.Mutex
}

func (b *Meta) GetLatestWorkingMeta() *WorkingMeta {
	return b.workingHead.prevVer
}

func (b *Meta) GetWorkingMetaEqual(version uint32) *WorkingMeta {
	for working := b.workingHead.prevVer; working != nil; working = working.prevVer {
		if working.version == version {
			return working
		}
	}
	return nil
}

func (b *Meta) AddWorkingMeta(working *WorkingMeta) {
	var prev = &b.workingHead
	for ; prev.prevVer != nil && prev.prevVer.prevVer != nil; prev = prev.prevVer {
		if prev.prevVer.version < working.version {
			break
		} else if prev.prevVer.version == working.version {
			prev.prevVer = prev.prevVer.prevVer
			break
		}
	}
	working.prevVer = prev.prevVer
	prev.prevVer = working
}

func (b *Meta) DropAll() {
	b.workingHead.prevVer = nil
}

type MetaAttributes struct {
	inodeKey InodeKeyType
	mode     uint32
}

func NewMetaAttributes(inodeKey InodeKeyType, mode uint32) MetaAttributes {
	return MetaAttributes{inodeKey: inodeKey, mode: mode}
}

func NewMetaAttributesFromMsg(msg *common.CopiedMetaChildMsg) MetaAttributes {
	return NewMetaAttributes(InodeKeyType(msg.GetInodeKey()), msg.GetMode())
}

func (m MetaAttributes) toMsg(name string) *common.CopiedMetaChildMsg {
	return &common.CopiedMetaChildMsg{Name: name, InodeKey: uint64(m.inodeKey), Mode: m.mode}
}

func (m MetaAttributes) IsDir() bool {
	return os.FileMode(m.mode)&os.ModeDir != 0
}

type WorkingMeta struct {
	// inodeKey is the unique ID for an inode, which is equivalent to UNIX inode number.
	// owner node of an inode can be calculated with hash(inodeKey)
	inodeKey InodeKeyType

	// parentKey is the inode ID for the original parent of this inode.
	// Hard links may add parent directories, but we need to know the original key for COS buckets to fetch correct data.
	// This number is passed during looking up the inode from the top directory, so this is safely set eventually
	parentKey InodeKeyType

	// fetchKey is the original key at COS.
	// Rename and hard link operations may update the actual file path different from this value.
	// We can keep fetching the original content from remote as intended even after such operations.
	fetchKey string

	// version represent the unique number for a version of an inode.
	// readers can keep reading the same version of an inode without writer's locking
	version uint32

	// chunkSize enables us to save memory space to remember maps for chunks.
	// We provide fixed-size of chunks for each inode (currently, we only support 5MiB chunks).
	// owner node of an offset in inode inodeKey can be calculated with hash(<inodeKey, (offset - offset % chunkSize)>)
	// Note: for a simple optimization, we define the owner node for offset 0 becomes same as its inode owner (i.e., hash(inodeKey))
	chunkSize int64

	size       int64
	childAttrs map[string]MetaAttributes //TODO: should be recorded separately like chunks

	// mutable entries:
	mTime    int64
	mode     uint32
	expireMs int32
	prevVer  *WorkingMeta
}

func NewWorkingMeta(inodeKey InodeKeyType, parent MetaAttributes, chunkSize int64, expireMs int32, mode uint32, fetchKey string) *WorkingMeta {
	working := &WorkingMeta{
		inodeKey:   inodeKey,
		parentKey:  parent.inodeKey,
		version:    1,
		chunkSize:  chunkSize,
		size:       int64(0),
		mTime:      time.Now().UnixNano(),
		expireMs:   expireMs,
		mode:       mode,
		fetchKey:   fetchKey,
		childAttrs: nil,
		prevVer:    nil,
	}
	if working.IsDir() {
		working.childAttrs = make(map[string]MetaAttributes)
		working.childAttrs["."] = NewMetaAttributes(inodeKey, working.mode)
		working.childAttrs[".."] = parent
	}
	return working
}

func NewWorkingMetaFromMsg(res *common.CopiedMetaMsg) *WorkingMeta {
	meta := &WorkingMeta{
		inodeKey:  InodeKeyType(res.GetInodeKey()),
		parentKey: InodeKeyType(res.GetParentKey()),
		version:   res.GetVersion(),
		chunkSize: res.GetChunkSize(),
		size:      res.GetSize(),
		mode:      res.GetMode(),
		mTime:     res.GetLastModified(),
		expireMs:  res.GetExpireMs(),
		fetchKey:  res.GetFetchKey(),
		prevVer:   nil,
	}

	if meta.IsDir() {
		meta.childAttrs = map[string]MetaAttributes{}
		for _, child := range res.GetChildren() {
			meta.childAttrs[child.GetName()] = NewMetaAttributesFromMsg(child)
		}
	}
	return meta
}

func (m *WorkingMeta) IsDir() bool {
	return os.FileMode(m.mode)&os.ModeDir != 0
}

func (m *WorkingMeta) toMsg() *common.CopiedMetaMsg {
	ret := &common.CopiedMetaMsg{
		InodeKey:     uint64(m.inodeKey),
		ParentKey:    uint64(m.parentKey),
		Version:      m.version,
		ChunkSize:    m.chunkSize,
		Size:         m.size,
		Mode:         m.mode,
		LastModified: m.mTime,
		ExpireMs:     m.expireMs,
		FetchKey:     m.fetchKey,
	}
	for name, child := range m.childAttrs {
		ret.Children = append(ret.Children, child.toMsg(name))
	}
	return ret
}

// DropPrev: Meta.lock must be held
func (m *WorkingMeta) DropPrev() {
	m.prevVer = nil // this is safe because readers are blocked or can access the pointer for dropped versions
}

func (m *WorkingMeta) toMetaAttr() MetaAttributes {
	return MetaAttributes{inodeKey: m.inodeKey, mode: m.mode}
}

func (m *WorkingMeta) GetAttr(uid uint32, gid uint32) fuseops.InodeAttributes {
	ts := time.Unix(0, m.mTime)
	nlink := uint32(1)
	mode := os.FileMode(m.mode)
	if m.IsDir() {
		nlink = 2
	}
	return fuseops.InodeAttributes{
		Size: uint64(m.size), Nlink: nlink, Mode: mode, Atime: ts, Mtime: ts, Ctime: ts, Crtime: ts, Uid: uid, Gid: gid,
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
