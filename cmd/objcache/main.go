/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package main

import (
	"flag"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/IBM/objcache/common"
	"github.com/IBM/objcache/internal"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sys/unix"
)

var args common.ObjcacheCmdlineArgs

func init() {
	args.SetCmdArgs()
	_ = flag.Set("logtostderr", "true")
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, unix.SIGTERM)

	flag.Parse()
	args.FilllAutoConfig()
	args.MountPoint = ""
	log := internal.InitLog(&args)
	config, err := internal.GetServerConfig(&args, time.Millisecond*100)
	if err != nil {
		log.Fatalf("Failed: GetServerConfig, err=%v", err)
	}

	backend, err := internal.NewObjCache(args.SecretFile, config.DebugS3, int(config.ChunkSizeBytes))
	if err != nil {
		log.Fatalf("Failed: NewObjCache, SecretFile=%v, rr=%v", args.SecretFile, err)
	}
	node := internal.NewNodeServer(backend, &args, &config)
	if !args.ClientMode && args.HeadWorkerIp != "" {
		if err = node.RequestJoinLocal(args.HeadWorkerIp, args.HeadWorkerPort); err != nil {
			log.Fatalf("Failed: RequestJoinLocal, headWorkerIp=%v, headWorkerPort=%v, err=%v", args.HeadWorkerIp, args.HeadWorkerPort, err)
		}
	}
	if args.ClientMode {
		if err = node.UpdateNodeListAsClient(); err != nil {
			log.Fatalf("Failed: UpdateNodeListAsClient, headWorkerIp=%v, headWorkerPort=%v, err=%v", args.HeadWorkerIp, args.HeadWorkerPort, err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			recv := <-signalChan
			if recv == unix.SIGTERM {
				node.Shutdown(true)
				break
			} else {
				log.Debugf("Ignore signal, recv=%v", recv)
			}
		}
		wg.Done()
	}()
	log.Infof("Objcache instance is now running.")

	wg.Wait()
	signal.Reset(unix.SIGTERM)
	os.Exit(0)
}
