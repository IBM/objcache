/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/serialx/hashring"
)

func TestGetGroupForChunk(t *testing.T) {
	// NOTE: to check hash distribution
	inodeKey := InodeKeyType(18446744065119617033)
	groupStr := []string{"0", "1", "2", "3"}
	r := MyRandString{xorState: uint64(time.Now().UnixNano())}
	for i := 0; i < len(groupStr); i++ {
		groupStr[i] = r.Get(64)
	}
	//ring := hashring.NewWithHash(groupStr, MyHashFunc64V2)
	//ring := hashring.New(groupStr)
	ring := hashring.NewWithHash(groupStr, MyHashFunc64)
	//ring := hashring.NewWithHash(groupStr, MyHashFunc)
	outStr := "\n"
	groups := make(map[string]int)
	for off := int64(0); off < 4*1024*1024*1024; off += 16777216 {
		groupId, ok := GetGroupForChunk(ring, inodeKey, off, 16777216)
		if !ok {
			t.Error("could not find groupId")
			return
		}
		if _, ok := groups[groupId]; !ok {
			groups[groupId] = 0
		}
		groups[groupId] += 1
	}
	for groupId, count := range groups {
		outStr += fmt.Sprintf("groupId=%v, count=%v\n", groupId, count)
	}
	t.Error(outStr)
}
