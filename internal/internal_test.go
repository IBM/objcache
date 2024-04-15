/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"math/rand"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func randString(size int, eol bool) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if eol {
		size -= 1
	}
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		log.Errorf("Failed: rand.Read at randString: err=%v", err)
		return ""
	}

	var ret string
	for _, v := range b {
		ret += string(letters[int(v)%len(letters)])
	}
	if eol {
		ret += "\n"
	}
	return ret
}

func TestAppendSingleBuffer(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), t.Name())
	disk := NewOnDiskLog(filePath, 64, 1)
	buf := []byte(randString(128, true))
	var total = int64(0)
	for i := 0; i < 128; i++ {
		vec, size, err := disk.AppendSingleBuffer(buf)
		if err != nil {
			t.Errorf("Failed: TestAppendSingleBuffer, AppendSingleBuffer, i=%v, err=%v", i, err)
		}
		if size != 128 {
			t.Errorf("BUG: TestAppendSingleBuffer, invalid size, expected=128, actual=%v, i=%v", size, i)
		}
		if vec.logOffset != total {
			t.Errorf("BUG: TestAppendSingleBuffer, invalid vec.logOffset, expected=%v, actual=%v, i=%v", total, vec.logOffset, i)
		}
		cacheLen := i
		if i > 64 {
			cacheLen = 64
		}
		if disk.caches.Len() != cacheLen {
			t.Errorf("BUG: TestAppendSingleBuffer, invalid disk.caches.Len(), expected=%v, actual=%v, i=%v", cacheLen, disk.caches.Len(), i)
		}
		total += size
	}
}

func TestAppendWrite(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), t.Name())
	disk := NewOnDiskLog(filePath, 64, 1)
	buf := []byte(randString(128, true))
	var total = int64(0)
	for i := 0; i < 128; i++ {
		logOffset := disk.ReserveAppendWrite(int64(len(buf)))
		if logOffset != total {
			t.Errorf("BUG: TestAppendWrite, ReserveAppendWrite, expected=%v, actual=%v, i=%v", total, logOffset, i)
		}
		vec, err := disk.BeginRandomWrite(buf, logOffset)
		if err != nil {
			t.Errorf("Failed: TestAppendWrite, BeginRandomWrite, i=%v, err=%v", i, err)
		}
		err = disk.Write(vec)
		if err != nil {
			t.Errorf("Failed: TestAppendWrite, Write, i=%v, err=%v", i, err)
		}
		sizeIncrese, err := disk.EndWrite(vec)
		if err != nil {
			t.Errorf("Failed: TestAppendWrite, EndWrite, i=%v, err=%v", i, err)
		}
		cacheLen := i
		if i > 64 {
			cacheLen = 64
		}
		if disk.caches.Len() != cacheLen {
			t.Errorf("BUG: invalid disk.caches.Len(), expected=%v, actual=%v, i=%v", cacheLen, disk.caches.Len(), i)
		}
		total += sizeIncrese
	}
}

func TestAppendSplice(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), t.Name())
	pipeFds := [2]int{}
	if err := unix.Pipe(pipeFds[:]); err != nil {
		t.Errorf("Failed: TestAppendSplice, Pipe, err=%v", err)
	}
	filePath2 := filepath.Join(t.TempDir(), "src.txt")
	fd, err := unix.Open(filePath2, unix.O_WRONLY|unix.O_CREAT, 0644)
	if err != nil {
		t.Errorf("Failed: TestAppendSplice, Open, filePath=%v, err=%v", filePath2, err)
	}
	buf := []byte(randString(128, true))
	bufOff := 0
	for bufOff < len(buf) {
		c, err := unix.Write(fd, buf[bufOff:])
		if err != nil {
			t.Errorf("Failed: TestAppendSplice, Write, err=%v", err)
		}
		if c == 0 {
			break
		}
		bufOff += c
	}
	unix.Close(fd)

	disk := NewOnDiskLog(filePath, 64, 1)
	var total = int64(0)
	for i := 0; i < 128; i++ {
		logOffset := disk.ReserveAppendWrite(int64(len(buf)))
		if logOffset != total {
			t.Errorf("BUG: TestAppendSplice, ReserveAppendWrite, expected=%v, actual=%v, i=%v", total, logOffset, i)
		}
		srcFd, err := unix.Open(filePath2, unix.O_RDONLY, 0644)
		if err != nil {
			t.Errorf("Failed: TestAppendSplice, Open, filePath=%v, err=%v", filePath2, err)
		}
		vec, err := disk.BeginRandomSplice(srcFd, int64(len(buf)), logOffset)
		if err != nil {
			t.Errorf("Failed: TestAppendSplice, BeginRandomWrite, i=%v, err=%v", i, err)
		}
		bufOff := int32(0)
		for bufOff < int32(len(buf)) {
			err = disk.Splice(vec, pipeFds, &bufOff)
			if err != nil {
				t.Errorf("Failed: TestAppendSplice, Write, i=%v, err=%v", i, err)
			}
		}
		unix.Close(srcFd)
		sizeIncrese, err := disk.EndWrite(vec)
		if err != nil {
			t.Errorf("Failed: TestAppendSplice, EndWrite, i=%v, err=%v", i, err)
		}
		cacheLen := i
		if i > 64 {
			cacheLen = 64
		}
		if disk.caches.Len() != cacheLen {
			t.Errorf("BUG: invalid disk.caches.Len(), expected=%v, actual=%v, i=%v", cacheLen, disk.caches.Len(), i)
		}
		total += sizeIncrese
	}
}
func TestAppendAndReadSingleBuffer(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), t.Name())
	disk := NewOnDiskLog(filePath, 64, 1)
	buf := []byte(randString(128, true))
	vec, _, err := disk.AppendSingleBuffer(buf)
	if err != nil {
		t.Errorf("Failed: TestAppendAndReadSingleBuffer, AppendSingleBuffer, err=%v", err)
	}
	readBufs, c, err := disk.Read(vec.logOffset, 128)
	if err != nil {
		t.Errorf("Failed: TestAppendAndReadSingleBuffer, Read, logOffset=%v, err=%v", vec.logOffset, err)
	}
	if c != 128 {
		t.Errorf("Failed: TestAppendAndReadSingleBuffer, Read returned < written bytes (128), c=%v", c)
	}
	readBuf := make([]byte, 0)
	for j := 0; j < len(readBufs); j++ {
		for k := 0; k < len(readBufs[j]); k++ {
			readBuf = append(readBuf, readBufs[j][k])
		}
	}
	for i := 0; i < len(readBuf); i++ {
		if buf[i] != readBuf[i] {
			t.Errorf("Failed: TestAppendAndReadSingleBuffer, Read returned corrupt data\n"+
				"expected: %s\n"+
				"actual: %s", buf, readBuf)
		}
	}
}
