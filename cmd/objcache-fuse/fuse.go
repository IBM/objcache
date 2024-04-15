/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"flag"
	"time"

	"github.com/IBM/objcache/common"
	. "github.com/IBM/objcache/internal"
)

var args common.ObjcacheCmdlineArgs

func init() {
	args.SetCmdArgs()
}

func main() {
	flag.Parse()
	args.FilllAutoConfig()
	log := InitLog(&args)
	config, err := GetServerConfig(&args, time.Millisecond*100)
	if err != nil {
		log.Errorf("Failed: GetServerConfig, err=%v", err)
		return
	}

	fs, err := GetFSWithoutMount(&args, &config)
	if err != nil {
		log.Errorf("Failed: GetFSWithoutMount, err=%v", err)
		return
	}
	if !args.ClientMode && args.HeadWorkerIp != "" {
		if err = fs.RequestJoinLocal(args.HeadWorkerIp, args.HeadWorkerPort); err != nil {
			log.Errorf("Failed: RequestJoinLocal, headWorkerIp=%v, headWorkerPort=%v, err=%v", args.HeadWorkerIp, args.HeadWorkerPort, err)
			return
		}
	}
	if args.ClientMode {
		if err = fs.InitNodeListAsClient(); err != nil {
			log.Errorf("Failed: InitNodeListAsClient, headWorkerIp=%v, headWorkerPort=%v, err=%v", args.HeadWorkerIp, args.HeadWorkerPort, err)
			return
		}
	}
	if err = fs.FuseMount(&args, &config); err != nil {
		log.Errorf("Failed: FuseMount, err=%v", err)
	}
}
