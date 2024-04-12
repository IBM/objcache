/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/IBM/objcache/api"
	"github.com/IBM/objcache/common"
	"github.com/IBM/objcache/internal"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/takeshi-yoshimura/fuse"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var args common.ObjcacheCmdlineArgs

var (
	nodeId            = flag.String("nodeId", "", "node ID")
	endpoint          = flag.String("endpoint", "unix://tmp/csi.sock", "endpoint")
	workerName        = flag.String("workerName", "objcache-worker", "")
	deploymentModel   = flag.String("deploymentModel", "monolithic", "version: monolithic or isolated")
	driverName        = flag.String("driverName", "", "CSI driver name")
	driverVersion     = flag.String("driverVersion", "", "CSI driver version")
	fuseBinPath       = flag.String("fuse.binPath", "/objcache-fuse", "binary file for FUSE")
	fuseGoDebugEnvVar = flag.String("fuse.goDebugEnvVar", "", "")
	fuseGoMemLimit    = flag.String("fuse.goMemLimit", "", "")
	fuseConfigFile    = flag.String("fuse.configFile", "", "")
	fusePortBegin     = flag.Int("fuse.portBegin", 9638, "")
	fusePortEnd       = flag.Int("fuse.portEnd", 19638, "")
)

func init() {
	args.SetCmdArgs()
	_ = flag.Set("logtostderr", "true")
}

func mainMonolithic() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, unix.SIGTERM)

	log := common.StdouterrLogger
	conf := args.GetObjcacheConfig()
	if *driverName == "" {
		*driverName = "objcache"
	}
	if args.ListenIp == "" || *driverName == "" || *driverVersion == "" {
		log.Fatal("Need to specify --listenIp --driverName, and --dirverVersion")
	}
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		log.Fatalf("Failed: CSIMain, Env POD_NAME is not set")
	}
	if !strings.HasPrefix(podName, *workerName+"-"+*nodeId+"-") {
		log.Fatalf("Failed: CSIMain, invalid workerName: %v, nodeName=%v, POD_NAME=%v", *workerName, *nodeId, podName)
	}

	s := common.NewNonBlockingGRPCServer()
	ns := NewNodeServer(*nodeId, args.MountPoint)
	s.Start(*endpoint, common.NewObjcacheIdentityServer(*driverName, *driverVersion), nil, ns)
	err := os.MkdirAll(args.MountPoint, 0755)
	if err != nil {
		log.Errorf("Failed: ObjCacheController.Mount, MkdirAll %v, err=%v", args.MountPoint, err)
		return
	}

	var fs *internal.ObjcacheFileSystem
	fs, err = internal.GetFSWithoutMount(&args, &conf)
	if err != nil {
		log.Errorf("Failed: GetFSWithoutMount, err=%v", err)
		return
	}
	target := fmt.Sprintf("%s:%d", args.ListenIp, args.ApiPort)
	for {
		time.Sleep(time.Second)
		conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Errorf("Failed: RequestJoin, Dial, selfAddr=%v, err=%v", target, err)
			continue
		}
		c := NewObjcacheApiClient(conn)
		var duration = 1 * time.Second
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			var ack *ApiRet
			ack, err = c.IsReady(ctx, &Void{})
			cancel()
			if err == nil && ack.Status == int32(Reply_Ok) {
				break
			}
			log.Infof("Waiting for target node %v to be ready... Retrying after %v.\n", target, duration)
			time.Sleep(duration)
			duration *= 2
		}
		_ = conn.Close()
		break
	}
	if args.HeadWorkerIp != "" {
		for {
			err = fs.RequestJoinLocal(args.HeadWorkerIp, args.HeadWorkerPort)
			if err == nil {
				log.Infof("Success: RequestJoin, selfAddr=%v, HeadWorkerIp=%v, port=%v", args.ListenIp, args.HeadWorkerIp, args.HeadWorkerPort)
				break
			}
			log.Warnf("Failed: RequestJoin, selfAddr=%v, HeadWorkerIp=%v, port=%v, err=%v", args.ListenIp, args.HeadWorkerIp, args.HeadWorkerPort, err)
			time.Sleep(time.Second * 3)
		}
	}
	mfs, err := fs.FuseMount(&args, &conf)
	if err != nil {
		log.Fatalf("Failed: FuseMount, err=%v", err)
	}
	ns.SetReady()
	go func() {
		for {
			sig := <-signalChan
			log.Infof("Received %v, attempting to unmount...", sig)

			err := fuse.Unmount(args.MountPoint)
			if err != nil {
				log.Errorf("Failed: Unmount, mountPoint=%v, sig=%v, err=%v", args.MountPoint, sig, err)
			} else {
				log.Infof("Success: Unmount, mountPoint=%v, sig=%v", args.MountPoint, sig)
				s.Stop()
				return
			}
		}
	}()
	log.Infof("Filesystem is now running.")

	err = mfs.Join(context.Background())
	if err != nil {
		log.Errorf("Failed: MountedFileSystem.Join, err=%v", err)
		return
	}
	s.Wait()
}

func mainIsolated() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, unix.SIGTERM)
	s := common.NewNonBlockingGRPCServer()
	ns := NewNodeServerV2(*fuseBinPath, *fuseGoDebugEnvVar, *fuseGoMemLimit, *fuseConfigFile, *nodeId, args.RootDir, args.ExternalIp, *fusePortBegin, *fusePortEnd)
	s.Start(*endpoint, common.NewObjcacheIdentityServer(*driverName, *driverVersion), nil, ns)
	<-signalChan
	s.Stop()
	s.Wait()
	ns.StopUnstageThread()
}

func testIsolated() {
	log := common.StdouterrLogger

	ns := NewNodeServerV2(*fuseBinPath, *fuseGoDebugEnvVar, *fuseGoMemLimit, *fuseConfigFile, *nodeId, args.RootDir, args.ExternalIp, *fusePortBegin, *fusePortEnd)
	params := make(map[string]string)
	params["objcache.io/headWorkerIp"] = args.HeadWorkerIp
	params["objcache.io/headWorkerPort"] = strconv.Itoa(args.HeadWorkerPort)
	params["objcache.io/godebug"] = "gctrace=1"
	params["objcache.io/gomemlimit"] = "12GiB"
	volumeId := "test"
	_, err := ns.NodeStageVolume(context.TODO(), &csi.NodeStageVolumeRequest{
		VolumeId: volumeId, StagingTargetPath: args.MountPoint,
		VolumeContext: params,
	})
	if err != nil {
		log.Fatalf("Failed: testIsolated: NodeStageVolume, err=%v", err)
	}
	targetPath := filepath.Join(args.RootDir, volumeId, "mount")
	_, err = ns.NodePublishVolume(context.TODO(),
		&csi.NodePublishVolumeRequest{
			VolumeId: volumeId, StagingTargetPath: args.MountPoint, TargetPath: targetPath,
			VolumeCapability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
			VolumeContext: params,
		})
	if err != nil {
		log.Fatalf("Faild: testIsolated: NodePublishVolume, err=%v", err)
	}
	fmt.Scanln()
	log.Infof("Attempting to NodeUnpublishVolume...")

	_, err = ns.NodeUnpublishVolume(context.TODO(), &csi.NodeUnpublishVolumeRequest{VolumeId: volumeId, TargetPath: targetPath})
	if err != nil {
		log.Fatalf("Faild: testIsolated: NodeUnpublishVolume, err=%v", err)
	}
	fmt.Scanln()
	log.Infof("Attempting to NodeUnstageVolume...")
	_, err = ns.NodeUnstageVolume(context.TODO(), &csi.NodeUnstageVolumeRequest{VolumeId: volumeId, StagingTargetPath: args.MountPoint})
	if err != nil {
		log.Fatalf("Faild: testIsolated: NodeUnstageVolume, err=%v", err)
	}
}

func main() {
	flag.Parse()
	args.FilllAutoConfig()
	switch *deploymentModel {
	case "monolithic":
		mainMonolithic()
	case "isolated":
		mainIsolated()
	case "test-isolated":
		testIsolated()
	default:
		log.Fatalf("supported deploymentModel=monolithic or isolated, invalid deploymentModel: %v", *deploymentModel)
	}
}
