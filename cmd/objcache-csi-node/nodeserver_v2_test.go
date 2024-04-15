/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

var binFile = "/data/go/src/github.com/IBM/objcache/bin/objcache-fuse"
var testRootDir = "/data/objcache/test"
var nextPort = 5637

func GetStagingTargetPath(volumeId string) string {
	return filepath.Join(testRootDir, "stage", volumeId)
}

func GetTargetPath(volumeId string) string {
	return filepath.Join(testRootDir, "publish", volumeId)
}

func GetStageRequest(volumeId string, delay time.Duration) *csi.NodeStageVolumeRequest {
	ret := &csi.NodeStageVolumeRequest{
		VolumeId: volumeId, StagingTargetPath: GetStagingTargetPath(volumeId),
		VolumeContext: map[string]string{
			"objcache.io/headWorkerIp":   "localhost",
			"objcache.io/headWorkerPort": "8637",
			"objcache.io/unstageDelay":   delay.String(),
		},
	}
	nextPort += 2
	return ret
}

func GetUnstageRequest(volumeId string) *csi.NodeUnstageVolumeRequest {
	return &csi.NodeUnstageVolumeRequest{VolumeId: volumeId, StagingTargetPath: GetStagingTargetPath(volumeId)}
}

func GetPublishRequest(volumeId string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId: volumeId, StagingTargetPath: GetStagingTargetPath(volumeId), TargetPath: GetTargetPath(volumeId),
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeContext: map[string]string{
			"objcache.io/headWorkerIp":   "localhost",
			"objcache.io/headWorkerPort": "9637",
		},
	}
}

func GetUnpublishRequest(volumeId string) *csi.NodeUnpublishVolumeRequest {
	return &csi.NodeUnpublishVolumeRequest{VolumeId: volumeId, TargetPath: filepath.Join(testRootDir, "publish", volumeId)}
}

func TestUnstageThread001(t *testing.T) {
	ns := NewNodeServerV2(binFile, "", "", "", "", testRootDir, "127.0.0.1", 9638, 19638)
	req := GetStageRequest("0", time.Millisecond*500)
	if _, err := ns.NodeStageVolume(context.TODO(), req); err != nil {
		t.Errorf("Failed: NodeStageVolume, 0, err=%v", err)
		return
	}
	if _, err := ns.NodeUnstageVolume(context.TODO(), GetUnstageRequest(req.VolumeId)); err != nil {
		t.Errorf("Failed: NodeUnstageVolume, 0, err=%v", err)
		return
	}
	ns.lock.Lock()
	if _, ok := ns.stageInfos[req.VolumeId]; !ok {
		t.Errorf("Failed: volumeId=%v is deleted too early", req.VolumeId)
		ns.lock.Unlock()
		return
	}
	ns.lock.Unlock()
	time.Sleep(time.Second * 1)
	ns.lock.Lock()
	_, ok := ns.stageInfos[req.VolumeId]
	ns.lock.Unlock()
	if ok {
		t.Errorf("Failed: volumeId=%v is not deleted after specified delay", req.VolumeId)
	}
	ns.StopUnstageThread()
}

func TestUnstageThread002(t *testing.T) {
	ns := NewNodeServerV2(binFile, "", "", "", "", testRootDir, "127.0.0.1", 9638, 19638)
	req := GetStageRequest("0", time.Second)
	if _, err := ns.NodeStageVolume(context.TODO(), req); err != nil {
		t.Errorf("Failed: NodeStageVolume, 0, err=%v", err)
		return
	}
	if _, err := ns.NodeUnstageVolume(context.TODO(), GetUnstageRequest(req.VolumeId)); err != nil {
		t.Errorf("Failed: NodeUnstageVolume, 0, err=%v", err)
		return
	}
	if _, err := ns.NodeStageVolume(context.TODO(), req); err != nil {
		t.Errorf("Failed: NodeStageVolume, 1, err=%v", err)
		return
	}
	time.Sleep(time.Second * 1)
	ns.lock.Lock()
	if _, ok := ns.stageInfos[req.VolumeId]; !ok {
		t.Errorf("Failed: volumeId=%v is deleted too early", req.VolumeId)
		ns.lock.Unlock()
		return
	}
	ns.lock.Unlock()

	if _, err := ns.NodeUnstageVolume(context.TODO(), GetUnstageRequest(req.VolumeId)); err != nil {
		t.Errorf("Failed: NodeUnstageVolume, 0, err=%v", err)
		return
	}

	time.Sleep(time.Second * 2)
	ns.lock.Lock()
	_, ok := ns.stageInfos[req.VolumeId]
	ns.lock.Unlock()
	if ok {
		t.Errorf("Failed: volumeId=%v is not deleted after specified delay", req.VolumeId)
	}
	ns.StopUnstageThread()
}

func TestUnstageThread003(t *testing.T) {
	ns := NewNodeServerV2(binFile, "", "", "", "", testRootDir, "127.0.0.1", 9638, 19638)
	reqs := make([]*csi.NodeStageVolumeRequest, 0)
	for i := 0; i < 10; i++ {
		reqs = append(reqs, GetStageRequest(strconv.Itoa(i), time.Millisecond*500))
	}
	for i := 0; i < 10; i++ {
		if _, err := ns.NodeStageVolume(context.TODO(), reqs[i]); err != nil {
			t.Errorf("Failed: NodeStageVolume, i=%v, err=%v", i, err)
			return
		}
	}
	ns.lock.Lock()
	c := len(ns.stageInfos)
	ns.lock.Unlock()
	if c < 10 {
		t.Errorf("Failed: staged volumes are deleted too early, current c=%v, expected=10", c)
		return
	}

	for i := 0; i < 10; i++ {
		if _, err := ns.NodeUnstageVolume(context.TODO(), GetUnstageRequest(reqs[i].VolumeId)); err != nil {
			t.Errorf("Failed: NodeUnstageVolume, 0, err=%v", err)
			return
		}
	}

	time.Sleep(time.Second)
	ns.lock.Lock()
	c = len(ns.stageInfos)
	ns.lock.Unlock()
	if c != 0 {
		t.Errorf("Failed: some of staged volumes are not deleted after specified delay, c=%v", c)
	}
	ns.StopUnstageThread()
}
