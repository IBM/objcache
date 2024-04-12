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
	"golang.org/x/sys/unix"

	"context"
	"os"
	"os/signal"

	"github.com/takeshi-yoshimura/fuse"
)

var args common.ObjcacheCmdlineArgs

func init() {
	args.SetCmdArgs()
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, unix.SIGTERM)

	flag.Parse()
	args.FilllAutoConfig()
	log := InitLog(&args)
	config, err := GetServerConfig(&args, time.Millisecond*100)
	if err != nil {
		log.Fatalf("Failed: GetServerConfig, err=%v", err)
	}

	fs, err := GetFSWithoutMount(&args, &config)
	if err != nil {
		log.Fatalf("Failed: GetFSWithoutMount, err=%v", err)
	}
	if !args.ClientMode && args.HeadWorkerIp != "" {
		if err = fs.RequestJoinLocal(args.HeadWorkerIp, args.HeadWorkerPort); err != nil {
			log.Fatalf("Failed: RequestJoinLocal, headWorkerIp=%v, headWorkerPort=%v, err=%v", args.HeadWorkerIp, args.HeadWorkerPort, err)
		}
	}
	if args.ClientMode {
		if err = fs.InitNodeListAsClient(); err != nil {
			log.Fatalf("Failed: InitNodeListAsClient, headWorkerIp=%v, headWorkerPort=%v, err=%v", args.HeadWorkerIp, args.HeadWorkerPort, err)
		}
	}
	mfs, err := fs.FuseMount(&args, &config)
	if err != nil {
		log.Fatalf("Failed: FuseMount, err=%v", err)
	}

	go func() {
		for {
			s := <-signalChan
			log.Infof("Received %v, attempting to unmount...", s)

			err := fuse.Unmount(args.MountPoint)
			if err != nil {
				log.Errorf("Failed: Unmount, mountPoint=%v, s=%v, err=%v", args.MountPoint, s, err)
			} else {
				log.Infof("Success: Unmount, mountPoint=%v, s=%v", args.MountPoint, s)
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

	fs.Shutdown()
}
