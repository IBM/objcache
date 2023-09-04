/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"log"
	"os"
	"sync/atomic"

	"golang.org/x/net/context"
	"golang.org/x/sys/unix"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

type nodeServer struct {
	nodeId         string
	firstMountPath string
	isReady        int32
}

func NewNodeServer(nodeId string, mountPoint string) *nodeServer {
	return &nodeServer{
		nodeId:         nodeId,
		firstMountPath: mountPoint,
		isReady:        0,
	}
}

func (ns *nodeServer) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	stagingTargetPath := req.GetStagingTargetPath()

	// Check arguments
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(stagingTargetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging Target path missing in request")
	}
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}
	if !ns.IsReady() {
		return nil, status.Error(codes.Unavailable, "Retry: objcache is initializing....")
	}

	notMnt, err := checkMount(targetPath, true)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	deviceID := ""
	if req.GetPublishContext() != nil {
		deviceID = req.GetPublishContext()[deviceID]
	}

	// TODO: Implement readOnly & mountFlags
	readOnly := req.GetReadonly()
	// TODO: check if attrib is correct with context.
	attrib := req.GetVolumeContext()
	mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()

	log.Printf("NodePublishVolume: targetPath=%v, stagingTargetPath=%v, deviceID=%v, readonly=%v, volumeID=%v, attributes=%v, mountflags=%v, publishContex=%v, volumeContext=%v",
		targetPath, stagingTargetPath, deviceID, readOnly, volumeID, attrib, mountFlags, req.GetPublishContext(), req.GetVolumeContext())

	err = unix.Mount(ns.firstMountPath, targetPath, "objcache-bind", unix.MS_BIND, "")
	if err != nil {
		return nil, err
	}
	log.Printf("Success: NodePublishVolume, bind mount %v to %v", ns.firstMountPath, targetPath)

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	stat := unix.Stat_t{}
	if err := unix.Stat(targetPath, &stat); err != nil {
		if err != unix.ENOENT {
			log.Printf("Failed: NodeUnpublishVolume, Stat, path=%v, err=%v", targetPath, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		log.Printf("Failed (ignore): NodeUnpublishVolume, Stat, path does not exist, path=%v", targetPath)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}
	if err := unix.Unmount(targetPath, 0); err != nil {
		log.Printf("Failed: NodeUnpublishVolume, Unmount, path=%v, err=%v", targetPath, err)
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	log.Printf("Success: NodeUnpublishVolume, Unmount %v", targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingTargetPath := req.GetStagingTargetPath()

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if len(stagingTargetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "NodeStageVolume Volume Capability must be provided")
	}

	log.Printf("NodeStageVolume: volumeID=%v, stagingTargetPath=%v, PublishContext=%v, VolumeCapability=%v, volumeContexts=%v",
		volumeID, stagingTargetPath, req.GetPublishContext(), req.VolumeCapability, req.GetVolumeContext())

	notMnt, err := checkMount(stagingTargetPath, true)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		return &csi.NodeStageVolumeResponse{}, nil
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingTargetPath := req.GetStagingTargetPath()

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if len(stagingTargetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodeGetCapabilities returns the supported capabilities of the node server
func (ns *nodeServer) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
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

func (ns *nodeServer) NodeExpandVolume(context.Context, *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return &csi.NodeExpandVolumeResponse{}, status.Error(codes.Unimplemented, "NodeExpandVolume is not implemented")
}

func (ns *nodeServer) IsReady() bool {
	return atomic.LoadInt32(&ns.isReady) == 1
}

func (ns *nodeServer) SetReady() {
	atomic.StoreInt32(&ns.isReady, 1)
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

func (ns *nodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	log.Printf("Using default NodeGetInfo")

	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeId,
	}, nil
}

func (ns *nodeServer) NodeGetVolumeStats(ctx context.Context, in *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
