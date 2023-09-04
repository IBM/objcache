//go:build linux
// +build linux

/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package internal

import (
	// #cgo CFLAGS: -march=native
	// #include <string.h>
	"C"
	"reflect"
	"unsafe"
)

func memcpy(dst, src []byte) int {
	n := len(src)
	if len(dst) < len(src) {
		n = len(dst)
	}
	if n == 0 {
		return 0
	}
	dstSlice := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	srcSlice := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	C.memcpy(unsafe.Pointer(dstSlice.Data), unsafe.Pointer(srcSlice.Data), C.size_t(n))
	return n
}
