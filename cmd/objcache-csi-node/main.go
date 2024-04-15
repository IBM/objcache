/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/IBM/objcache/common"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sys/unix"
)

var args common.ObjcacheCmdlineArgs

var (
	nodeId            = flag.String("nodeId", "", "node ID")
	endpoint          = flag.String("endpoint", "unix://tmp/csi.sock", "endpoint")
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

func main() {
	flag.Parse()
	args.FilllAutoConfig()
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
