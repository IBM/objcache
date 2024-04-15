/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/btree"
	"github.com/takeshi-yoshimura/fuse"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/mount-utils"
)

type StageInfo struct {
	cmd          *exec.Cmd
	unstageDelay time.Duration
	stageCount   int
	raftPort     int
	apiPort      int
}

type CancelUnstageMsg struct {
	volumeID string
}

type UnstageMsg struct {
	volumeID          string
	stagingTargetPath string
	unstageAt         time.Time
}

func (u *UnstageMsg) Less(i btree.Item) bool {
	i2 := i.(*UnstageMsg)
	if u.unstageAt.Equal(i2.unstageAt) {
		return u.volumeID < i2.volumeID
	}
	return u.unstageAt.Before(i2.unstageAt)
}

type nodeServerV2 struct {
	fuseBinPath   string
	goDebugEnvVar string
	goMemLimit    string
	configFile    string
	nodeId        string
	rootDir       string
	externalIp    string
	portBegin     int
	portEnd       int
	nextPort      int
	ports         map[int]bool
	stageInfos    map[string]*StageInfo
	lock          *sync.Mutex
	msgChan       chan interface{}
	termChan      chan struct{}
}

func NewNodeServerV2(fuseBinPath string, goDebugEnvVar string, goMemLimit string, configFile string, nodeId string, rootDir string, externalIp string, portBegin int, portEnd int) *nodeServerV2 {
	ret := &nodeServerV2{
		fuseBinPath:   fuseBinPath,
		goDebugEnvVar: goDebugEnvVar,
		goMemLimit:    goMemLimit,
		configFile:    configFile,
		nodeId:        nodeId,
		rootDir:       rootDir,
		externalIp:    externalIp,
		stageInfos:    make(map[string]*StageInfo),
		lock:          new(sync.Mutex),
		msgChan:       make(chan interface{}, 10),
		portBegin:     portBegin,
		portEnd:       portEnd,
		nextPort:      portBegin,
		ports:         make(map[int]bool),
		termChan:      make(chan struct{}),
	}
	go ret.UnstageThread()
	return ret
}

func (ns *nodeServerV2) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	stagingPath := req.GetStagingTargetPath()

	notMnt, err := checkMount(targetPath, true)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := unix.Mount(stagingPath, targetPath, "objcache-bind", unix.MS_BIND|unix.MS_PRIVATE, ""); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("bind mount failed, staging=%v, target=%v, err=%v", stagingPath, targetPath, err))
	}

	log.Printf("NodePublishVolume: stagingPath=%v, targetPath=%v, volumeID=%v, publishContex=%v, volumeContext=%v",
		stagingPath, targetPath, volumeID, req.GetPublishContext(), req.GetVolumeContext())
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServerV2) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	begin := time.Now()
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	notMnt, err := checkMount(targetPath, false)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if err := unix.Unmount(targetPath, 0); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Failed: NodeUnpublishVolume, Unmount, targetPath=%v, err=%v", targetPath, err))
	}
	log.Printf("Success: NodeUnpublishVolume, targetPath=%v, volumeID=%v, elapsed=%v", targetPath, volumeID, time.Since(begin))
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServerV2) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingTargetPath := req.GetStagingTargetPath()

	ns.lock.Lock()
	stagInfo, ok := ns.stageInfos[volumeID]
	if ok {
		stagInfo.stageCount += 1
		ns.lock.Unlock()
		ns.msgChan <- &CancelUnstageMsg{volumeID: volumeID}
		return &csi.NodeStageVolumeResponse{}, nil
	}
	defer ns.lock.Unlock()
	notMnt, err := checkMount(stagingTargetPath, true)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		return nil, status.Error(codes.Internal, fmt.Sprintf("StagingTargetPath is already mounted, stagingTargetPath=%v", stagingTargetPath))
	}

	volumeStateDir := filepath.Join(ns.rootDir, volumeID)
	err = os.MkdirAll(volumeStateDir, 0755)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// TODO: Implement readOnly & mountFlags
	params := req.GetVolumeContext()
	headWorkerIp, ok := params["objcache.io/headWorkerIp"]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "storage class does not contain \"objcache.io/headWorkerIp\" key in parameters")
	}
	headWorkerPort, ok := params["objcache.io/headWorkerPort"]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "storage class does not contain \"objcache.io/headWorkerPort\" key in parameters")
	}
	var raftPort, apiPort int
	p := ns.nextPort
	for ; p < ns.portEnd && (raftPort == 0 || apiPort == 0); p++ {
		if _, ok := ns.ports[p]; !ok {
			if raftPort == 0 {
				raftPort = p
			} else if apiPort == 0 {
				apiPort = p
			}
			ns.ports[p] = true
		}
	}
	p = ns.portBegin
	for ; p < ns.nextPort && (raftPort == 0 || apiPort == 0); p++ {
		if _, ok := ns.ports[p]; !ok {
			if raftPort == 0 {
				raftPort = p
			} else if apiPort == 0 {
				apiPort = p
			}
			ns.ports[p] = true
		}
	}
	if raftPort == 0 || apiPort == 0 {
		return nil, status.Error(codes.Internal, fmt.Sprintf("objcache allocated too many ports, configured port range=%v-%v", ns.portBegin, ns.portEnd))
	}
	ns.nextPort = apiPort + 1
	if ns.nextPort == ns.portEnd+1 {
		ns.nextPort = ns.portBegin
	}
	unstageDelay, ok := params["objcache.io/unstageDelay"]
	var delay time.Duration = 0
	if ok {
		delay, err = time.ParseDuration(unstageDelay)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("\"objcache.io/unstageDelay\" has invalid format, str=%v, err=%v", unstageDelay, err))
		}
	}
	envs := make([]string, 0)
	envs = append(envs, os.Environ()...)
	if ns.goDebugEnvVar != "" {
		envs = append(envs, "GODEBUG="+ns.goDebugEnvVar)
	}
	if ns.goMemLimit != "" {
		envs = append(envs, "GOMEMLIMIT="+ns.goMemLimit)
	}
	args := []string{
		"--clientMode=true",
		"--listenIp=0.0.0.0",
		"--serverId=k8s",
		fmt.Sprintf("--headWorkerIp=%v", headWorkerIp),
		fmt.Sprintf("--headWorkerPort=%v", headWorkerPort),
		fmt.Sprintf("--externalIp=%v", ns.externalIp),
		fmt.Sprintf("--raftPort=%v", raftPort),
		fmt.Sprintf("--apiPort=%v", apiPort),
		fmt.Sprintf("--mountPoint=%v", stagingTargetPath),
		fmt.Sprintf("--rootDir=%v", volumeStateDir),
		fmt.Sprintf("--logFile=%v/objcache.log", volumeStateDir),
	}
	if ns.configFile != "" {
		args = append(args, fmt.Sprintf("--configFile=%v", ns.configFile))
	}
	fuseLogPath := filepath.Join(volumeStateDir, "fuse.log")
	logF, err := os.OpenFile(fuseLogPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("OpenFile for fuse.log failed, fuseLogPath=%v, err=%v", fuseLogPath, err))
	}
	devF, err := os.OpenFile(os.DevNull, os.O_RDWR, 0644)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("OpenFile for /dev/null failed, devNull=%v, err=%v", os.DevNull, err))
	}
	cmd := exec.Command(ns.fuseBinPath, args...)
	cmd.Stderr = logF
	cmd.Stdout = logF
	cmd.Stdin = devF
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if len(envs) > 0 {
		cmd.Env = envs
	}
	err = cmd.Start()
	logF.Close() // parent does not need this any more
	devF.Close()
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("fuse start failed, fuseLogPath=%v, args=%v, err=%v", fuseLogPath, args, err))
	}
	// need to wait until the child creates a mount point to prevent from racy volume publish
	var success = false
	for mountBegin := time.Now(); time.Since(mountBegin).Seconds() < 10; {
		mountStr, err := os.ReadFile("/proc/mounts")
		if err == nil {
			if strings.Contains(string(mountStr), stagingTargetPath) {
				success = true
				break
			}
		}
		time.Sleep(time.Second)
	}
	if !success {
		return nil, status.Error(codes.Internal, fmt.Sprintf("fuse cannot find %v in /proc/mounts >10 secs. timeout", stagingTargetPath))
	}
	ns.stageInfos[volumeID] = &StageInfo{cmd: cmd, stageCount: 1, unstageDelay: delay, raftPort: raftPort, apiPort: apiPort}

	log.Printf("NodeStageVolume: volumeID=%v, stagingTargetPath=%v, PublishContext=%v, VolumeCapability=%v, volumeContexts=%v",
		volumeID, stagingTargetPath, req.GetPublishContext(), req.VolumeCapability, req.GetVolumeContext())
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *nodeServerV2) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingTargetPath := req.GetStagingTargetPath()
	ns.lock.Lock()
	var delay time.Duration
	if stagReq, ok := ns.stageInfos[volumeID]; ok {
		stagReq.stageCount -= 1
		delay = stagReq.unstageDelay
	} else {
		notMnt, err := checkMount(stagingTargetPath, false)
		if err != nil || notMnt {
			ns.lock.Unlock()
			return nil, err
		}
		if err = fuse.Unmount(stagingTargetPath); err != nil {
			ns.lock.Unlock()
			return nil, err
		}
		ns.lock.Unlock()
		log.Printf("NodeUnstageVolume (non-registered): volumeID=%v, stagingTargetPath=%v", volumeID, stagingTargetPath)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}
	ns.lock.Unlock()

	ua := time.Now().Add(delay)
	ns.msgChan <- &UnstageMsg{volumeID: volumeID, stagingTargetPath: stagingTargetPath, unstageAt: ua}
	log.Printf("NodeUnstageVolume: volumeID=%v, stagingTargetPath=%v, unstageAt=%v", volumeID, stagingTargetPath, ua)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServerV2) StopUnstageThread() {
	ns.msgChan <- struct{}{}
	<-ns.termChan
}

func (ns *nodeServerV2) UnstageThread() {
	nextWakeupTime := time.Now().Add(time.Hour)
	delayed := make(map[string]*UnstageMsg)
	wakeupTimes := btree.New(3)
	var stop = false
	for !stop {
		timer := time.NewTimer(time.Until(nextWakeupTime))
		select {
		case msg := <-ns.msgChan:
			if !timer.Stop() {
				<-timer.C
			}
			if cancel, ok := msg.(*CancelUnstageMsg); ok {
				if old, ok := delayed[cancel.volumeID]; ok {
					wakeupTimes.Delete(old)
					delete(delayed, old.volumeID)
				}
			} else if unstage, ok := msg.(*UnstageMsg); ok {
				if old, ok := delayed[unstage.volumeID]; ok {
					wakeupTimes.Delete(old)
				}
				delayed[unstage.volumeID] = unstage
				wakeupTimes.ReplaceOrInsert(unstage)
			} else {
				stop = true
			}
		case <-timer.C:
			consumed := make([]*UnstageMsg, 0)
			kills := make([]*exec.Cmd, 0)
			waits := make([]*exec.Cmd, 0)
			ns.lock.Lock()
			wakeupTimes.AscendLessThan(&UnstageMsg{unstageAt: time.Now()}, func(i btree.Item) bool {
				unstage := i.(*UnstageMsg)
				consumed = append(consumed, unstage)
				stagingTargetPath := unstage.stagingTargetPath
				if stagInfo, ok := ns.stageInfos[unstage.volumeID]; ok && stagInfo.stageCount <= 0 {
					delete(ns.stageInfos, unstage.volumeID)
					notMnt, err := checkMount(stagingTargetPath, false)
					if err == nil {
						if notMnt {
							err = unix.EINVAL
						} else {
							err = fuse.Unmount(stagingTargetPath)
						}
					}
					if err != nil {
						kills = append(kills, stagInfo.cmd)
					} else {
						waits = append(waits, stagInfo.cmd)
						log.Printf("UnstageThread: volumeID=%v, stagingTargetPath=%v, time=%v", unstage.volumeID, stagingTargetPath, time.Now())
					}
					delete(ns.ports, stagInfo.raftPort)
					delete(ns.ports, stagInfo.apiPort)
				}
				return true
			})
			ns.lock.Unlock()
			go func(kills []*exec.Cmd, waits []*exec.Cmd) {
				for _, cmd := range kills {
					cmd.Process.Kill()
					cmd.Wait()
				}
				for _, cmd := range waits {
					cmd.Wait()
				}
			}(kills, waits)
			for _, unstage := range consumed {
				wakeupTimes.Delete(unstage)
				delete(delayed, unstage.volumeID)
			}
		}
		if next := wakeupTimes.Min(); next != nil {
			nextWakeupTime = next.(*UnstageMsg).unstageAt
		} else {
			nextWakeupTime = time.Now().Add(time.Hour)
		}
	}
	ns.termChan <- struct{}{}
}

// NodeGetCapabilities returns the supported capabilities of the node server
func (ns *nodeServerV2) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// currently there is a single NodeServer capability according to the spec
	nscap := &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			nscap,
		},
	}, nil
}

func (ns *nodeServerV2) NodeExpandVolume(context.Context, *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return &csi.NodeExpandVolumeResponse{}, status.Error(codes.Unimplemented, "NodeExpandVolume is not implemented")
}

func (ns *nodeServerV2) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeId,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{*driverName + "/node": "true"},
		},
	}, nil
}

func (ns *nodeServerV2) NodeGetVolumeStats(ctx context.Context, in *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func checkMount(targetPath string, createDir bool) (bool, error) {
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if createDir {
				if err = os.MkdirAll(targetPath, 0750); err != nil {
					return false, err
				}
			}
			notMnt = true
		} else {
			return false, err
		}
	}
	return notMnt, nil
}
