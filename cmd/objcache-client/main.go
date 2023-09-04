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
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/takeshi-yoshimura/fuse"
	"github.ibm.com/TYOS/objcache/api"
	"github.ibm.com/TYOS/objcache/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	clientArgs struct {
		cmd             string
		targetIp        string
		targetPort      int
		updatedNodeIp   string
		updatedNodePort int
		testReadiness   bool
		fileId          int64
		offset          int64
		length          int64
		stateFile       string
		mountPoint      string
		nrThread        int
	}
)

func init() {
	flag.StringVar(&clientArgs.cmd, "cmd", "", "isReady/join/quit/leave/dropCache/writeTest/readTest/addTest/removeTest/grpc")
	flag.StringVar(&clientArgs.targetIp, "targetIp", "", "target node to request cmd")
	flag.IntVar(&clientArgs.targetPort, "targetPort", 9638, "target port to request cmd")
	flag.StringVar(&clientArgs.updatedNodeIp, "updatedNodeIp", "", "(valid for --cmd join/leave/addTest/removeTest) node to request join/leave")
	flag.IntVar(&clientArgs.updatedNodePort, "updatedNodePort", 9639, "")
	flag.BoolVar(&clientArgs.testReadiness, "testReadiness", true, "test readiness before invoking cmd")
	flag.Int64Var(&clientArgs.fileId, "fileId", -1, "file Id for writeTest/readTest")
	flag.Int64Var(&clientArgs.offset, "offset", 0, "offset for readTest")
	flag.Int64Var(&clientArgs.length, "length", 0, "length for readTest")
	flag.StringVar(&clientArgs.stateFile, "stateFile", "", "state file for running objcache")
	flag.StringVar(&clientArgs.mountPoint, "mountPoint", "", "mount path for running objcache")
	flag.IntVar(&clientArgs.nrThread, "nrThread", 1, "nr thread for grpc testing")
}

func TestReadiness(c api.ObjcacheApiClient) error {
	var duration = 1 * time.Second
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		ack, err := c.IsReady(ctx, &api.Void{})
		cancel()
		if err == nil && ack.Status == int32(api.Reply_Ok) {
			break
		}
		log.Printf("Waiting for target node to be ready... Retrying after %v sec.\n", duration)
		time.Sleep(duration)
		duration *= 2
	}
	return nil
}

func GetClient(ip string, port int) (api.ObjcacheApiClient, *grpc.ClientConn) {
	addr := fmt.Sprintf("%s:%d", ip, port)
	log.Printf("Try to connect %v", addr)

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}

	c := api.NewObjcacheApiClient(conn)

	if clientArgs.testReadiness {
		if err := TestReadiness(c); err != nil {
			_ = conn.Close()
			log.Fatalf("Failed: TestReadiness, err=%v", err.Error())
		}
	}

	log.Printf("cmd=%v, targetAddr=%v\n", clientArgs.cmd, addr)
	return c, conn
}

func IsReady() {
	c, conn := GetClient(clientArgs.targetIp, clientArgs.targetPort)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	ack, err := c.IsReady(ctx, &api.Void{})
	cancel()
	_ = conn.Close()
	if err != nil || ack.Status != int32(api.Reply_Ok) {
		log.Printf("Failed: IsReady, %v", err)
	}
}

func Panic() {
	c, conn := GetClient(clientArgs.targetIp, clientArgs.targetPort)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	_, err := c.Panic(ctx, &api.Void{})
	cancel()
	_ = conn.Close()
	if err != nil {
		log.Printf("Failed: Panic, %v", err)
	}
}

func Join() {
	c, conn := GetClient(clientArgs.updatedNodeIp, clientArgs.updatedNodePort)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	_, err := c.RequestJoin(ctx, &api.RequestJoinArgs{TargetAddr: clientArgs.targetIp, TargetPort: int32(clientArgs.targetPort)})
	cancel()
	_ = conn.Close()
	if err != nil {
		log.Printf("Failed: Join, %v", err)
	}
}

func Leave() {
	c, conn := GetClient(clientArgs.updatedNodeIp, clientArgs.updatedNodePort)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	ip := net.ParseIP(clientArgs.targetIp)
	if ip == nil {
		log.Fatalf("Failed: Leave, ParseIpString, targetIp=%v", clientArgs.targetIp)
	}
	_, err := c.RequestRemoveNode(ctx, &api.RequestLeaveArgs{Ip: ip.To4()[:], Port: int32(clientArgs.targetPort)})
	cancel()
	_ = conn.Close()
	if err != nil {
		log.Printf("Failed: RequestRemoveNode, %v", err)
	}
}

func Quit() {
	c, conn := GetClient(clientArgs.targetIp, clientArgs.targetPort)
	_, err := c.Terminate(context.Background(), &api.Void{})
	_ = conn.Close()
	if err != nil {
		log.Printf("Failed: Quit, %v", err)
		return
	}
	if clientArgs.stateFile != "" {
		common.WaitShutdown(clientArgs.stateFile)
	}
	if clientArgs.mountPoint != "" {
		if err := fuse.Unmount(clientArgs.mountPoint); err != nil {
			log.Printf("Failed: Quit, Unmount, mountPath=%v, er=%v", clientArgs.mountPoint, err)
		}
	}
}

func CoreDump() {
	c, conn := GetClient(clientArgs.targetIp, clientArgs.targetPort)
	_, err := c.CoreDump(context.Background(), &api.Void{})
	_ = conn.Close()
	if err != nil {
		log.Printf("Failed: CoreDump, %v", err)
	}
}

func Rejuvenate() {
	c, conn := GetClient(clientArgs.targetIp, clientArgs.targetPort)
	_, err := c.Rejuvenate(context.Background(), &api.Void{})
	_ = conn.Close()
	if err != nil {
		log.Printf("Failed: Rejuvenate, %v", err)
	}
}

func DropCache() {
	c, conn := GetClient(clientArgs.targetIp, clientArgs.targetPort)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	_, err := c.DropCache(ctx, &api.Void{})
	cancel()
	_ = conn.Close()
	if err != nil {
		log.Printf("Failed: DropCache, %v", err)
	}
}

func MeasureGrpcLatency(nrThread int, spin bool, perThreadCon bool) {
	var wg sync.WaitGroup
	cs := make([]api.ObjcacheApiClient, nrThread)
	conns := make([]*grpc.ClientConn, nrThread)
	if perThreadCon {
		for i := 0; i < nrThread; i++ {
			cs[i], conns[i] = GetClient(clientArgs.targetIp, clientArgs.targetPort)
		}
	} else {
		c, conn := GetClient(clientArgs.targetIp, clientArgs.targetPort)
		for i := 0; i < nrThread; i++ {
			cs[i] = c
			conns[i] = conn
		}
	}
	start := int32(0)
	latency := make([]time.Duration, nrThread)
	locks := make([]int32, nrThread)
	for i := 0; i < nrThread; i++ {
		if spin {
			locks[i] = 1
		} else {
			wg.Add(1)
		}
		go func(i int) {
			for atomic.LoadInt32(&start) == 0 {
			}
			begin := time.Now()
			for j := 0; j < 10; j++ {
				_, err := cs[i].IsReady(context.Background(), &api.Void{})
				if err != nil {
					log.Printf("Error: IsReady, err=%v", err)
				}
			}
			elapsed := time.Since(begin) / 10
			latency[i] = elapsed
			if spin {
				atomic.StoreInt32(&locks[i], 0)
			} else {
				wg.Done()
			}
		}(i)
	}
	beginAll := time.Now()
	atomic.StoreInt32(&start, 1)
	if spin {
		var stop = false
		for !stop {
			stop = true
			for i := 0; i < nrThread; i++ {
				if atomic.LoadInt32(&locks[i]) != 0 {
					stop = false
					break
				}
			}
		}
	} else {
		wg.Wait()
	}
	elapsed := time.Since(beginAll)
	if perThreadCon {
		for i := 0; i < nrThread; i++ {
			_ = conns[i].Close()
		}
	} else {
		_ = conns[0].Close()
	}
	for i := 0; i < nrThread; i++ {
		log.Printf("Latency: thread=%v, time=%v", i, latency[i])
	}
	log.Printf("Throughput: total thread=%v, time=%v, threads/ms=%v", nrThread, elapsed, float64(nrThread)/elapsed.Seconds()/1000)
}

func main() {
	flag.Parse()

	if clientArgs.cmd == "" {
		log.Fatalln("Must specify --cmd")
	}
	if clientArgs.targetIp == "" {
		log.Fatalln("Must specify --targetIp")
	}
	if (clientArgs.cmd == "readTest" || clientArgs.cmd == "writeTest") && clientArgs.fileId < 0 {
		log.Fatalln("Specify fileId > 0 for readTest")
	}
	if (clientArgs.cmd == "join" || clientArgs.cmd == "leave" || clientArgs.cmd == "addTest" || clientArgs.cmd == "removeTest") && clientArgs.updatedNodeIp == "" {
		log.Fatalf("Specify --updatedNodeIp for --cmd %s", clientArgs.cmd)
	}

	if clientArgs.cmd == "isReady" {
		IsReady()
	} else if clientArgs.cmd == "join" {
		Join()
	} else if clientArgs.cmd == "leave" {
		Leave()
	} else if clientArgs.cmd == "quit" {
		Quit()
	} else if clientArgs.cmd == "dropCache" {
		DropCache()
	} else if clientArgs.cmd == "grpc" {
		MeasureGrpcLatency(clientArgs.nrThread, false, false)
	} else if clientArgs.cmd == "grpc2" {
		MeasureGrpcLatency(clientArgs.nrThread, false, true)
	} else if clientArgs.cmd == "coredump" {
		CoreDump()
	} else if clientArgs.cmd == "rejuvenate" {
		Rejuvenate()
	} else if clientArgs.cmd == "panic" {
		Panic()
	} else {
		log.Printf("Invalid cmd: %v", clientArgs.cmd)
	}
}
