/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"flag"
	"time"

	"github.com/IBM/objcache/common"
	"github.com/IBM/objcache/internal"
	_ "go.uber.org/automaxprocs"
)

var args common.ObjcacheCmdlineArgs

func init() {
	args.SetCmdArgs()
	_ = flag.Set("logtostderr", "true")
}

func main() {
	flag.Parse()
	args.FilllAutoConfig()
	args.MountPoint = ""
	log := internal.InitLog(&args)
	config, err := internal.GetServerConfig(&args, time.Millisecond*100)
	if err != nil {
		log.Errorf("Failed: GetServerConfig, err=%v", err)
		return
	}

	backend, err := internal.NewObjCache(args.SecretFile, config.DebugS3, int(config.ChunkSizeBytes))
	if err != nil {
		log.Errorf("Failed: NewObjCache, SecretFile=%v, rr=%v", args.SecretFile, err)
		return
	}
	node := internal.NewNodeServer(backend, &args, &config)
	if !args.ClientMode && args.HeadWorkerIp != "" {
		if err = node.RequestJoinLocal(args.HeadWorkerIp, args.HeadWorkerPort); err != nil {
			log.Errorf("Failed: RequestJoinLocal, headWorkerIp=%v, headWorkerPort=%v, err=%v", args.HeadWorkerIp, args.HeadWorkerPort, err)
			return
		}
	}
	if args.ClientMode {
		if err = node.UpdateNodeListAsClient(); err != nil {
			log.Errorf("Failed: UpdateNodeListAsClient, headWorkerIp=%v, headWorkerPort=%v, err=%v", args.HeadWorkerIp, args.HeadWorkerPort, err)
			return
		}
	}

	log.Infof("Objcache instance is now running.")
	node.WaitShutdown()
}
