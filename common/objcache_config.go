/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package common

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/api/resource"
)

type ObjcacheCmdlineArgs struct {
	// used for specifying per worker configurations
	// use ObjcacheConfig (generated from ConfigFile) for static configuration shared among cluster nodes
	// use SecretConfig (generated from SecretFile) for S3 credentials
	ServerId              uint
	ServerIdString        string
	RaftGroupId           string
	PassiveRaftInit       bool
	PassiveRaftInitString string
	ListenIp              string
	ExternalIp            string
	RaftPort              int
	ApiPort               int
	ProfilePort           int
	HeadWorkerIp          string
	HeadWorkerPort        int
	RootDir               string
	LogFile               string
	ClientMode            bool
	ConfigFile            string
	SecretFile            string
	MountPoint            string
	CacheCapacity         string
}

func (c *ObjcacheCmdlineArgs) SetCmdArgs() {
	flag.StringVar(&c.ServerIdString, "serverId", "1", "Identity number for nodes and local clocks")
	flag.StringVar(&c.RaftGroupId, "raftGroupId", "1", "Replication group ID (servers within the same ID replicate all logs)")
	flag.StringVar(&c.PassiveRaftInitString, "passiveRaftInit", "false", "Trying to deploy this instance as Raft followers")
	flag.StringVar(&c.ListenIp, "listenIp", "0.0.0.0", "listened Ip")
	flag.StringVar(&c.ExternalIp, "externalIp", "", "external Ip that remote servers access")
	flag.IntVar(&c.RaftPort, "raftPort", 9638, "port number for raft communication")
	flag.IntVar(&c.ApiPort, "apiPort", 8638, "communication port")
	flag.IntVar(&c.ProfilePort, "profilePort", 0, "gprof port")
	flag.StringVar(&c.HeadWorkerIp, "headWorkerIp", "", "primary IP to join")
	flag.IntVar(&c.HeadWorkerPort, "headWorkerPort", 0, "port for the primary address to join")
	flag.StringVar(&c.RootDir, "rootDir", "/var/lib/objcache/9638", "directory to store local data")
	flag.StringVar(&c.LogFile, "logFile", "", "log file path for debug (blank means stderr, default: blank)")
	flag.StringVar(&c.MountPoint, "mountPoint", "/var/lib/objcache/9638/mount", "mount path for FUSE/CSI")
	flag.StringVar(&c.ConfigFile, "configFile", "", "yaml file for ObjcacheConfig")
	flag.StringVar(&c.SecretFile, "secretFile", "", "yaml file for S3 credentials")
	flag.BoolVar(&c.ClientMode, "clientMode", false, "client mode")
	flag.StringVar(&c.CacheCapacity, "cacheCapacity", "30Gi", "capacity of the local volume")
}

func (c *ObjcacheCmdlineArgs) FilllAutoConfig() {
	if c.ServerIdString == "k8s" {
		addrIp := net.ParseIP(c.ExternalIp)
		if addrIp == nil {
			log.Fatalf("Failed: SetCmdArgs, ParseIP, ExternalIp=%v", c.ExternalIp)
		}
		ipV4 := addrIp.To4()
		if ipV4 == nil {
			log.Fatalf("Failed: SetCmdArgs, ParseIP, ExternalIp=%v, addrIp=%v", c.ExternalIp, addrIp)
		}
		c.ServerId = uint(binary.BigEndian.Uint32(ipV4))
		log.Infof("SetCmdArgs, set serverId=%v from ExternalIp=%v", c.ServerId, c.ExternalIp)
	} else {
		r, err := strconv.ParseUint(c.ServerIdString, 10, 32)
		if err != nil {
			log.Fatalf("Failed: SetCmdArgs, ParseUint, invalid integer for --serverId, ServerId=%v, err=%v", c.ServerIdString, err)
		}
		c.ServerId = uint(r)
	}
	if c.PassiveRaftInitString == "k8s" {
		// assume this instance runs as k8s statefulset
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("Failed: SetCmdArgs, Hostname, cannot get hostname to determine passiveRaftInit, string=%v, err=%v", c.PassiveRaftInitString, err)
		}
		split := strings.Split(hostname, ".")
		c.PassiveRaftInit = strings.HasPrefix(split[0], "-0")
		log.Infof("SetCmdArgs, regard --passiveRaftInit=%v according to hostname %v", c.PassiveRaftInit, hostname)
	} else {
		r, err := strconv.ParseBool(c.PassiveRaftInitString)
		if err != nil {
			log.Fatalf("Failed: SetCmdArgs, ParseBool, invalid integer for --passiveRaftInit, PassiveRaftInitString=%v, err=%v", c.PassiveRaftInitString, err)
		}
		c.PassiveRaftInit = r
	}
}

func (c *ObjcacheCmdlineArgs) GetObjcacheConfig() ObjcacheConfig {
	return NewConfig(c.ConfigFile)
}

type ObjcacheConfig struct {
	MountOptions map[string]string `yaml:"mountOptions"`
	DirMode      uint32            `yaml:"dirMode"`
	FileMode     uint32            `yaml:"fileMode"`
	Uid          uint32            `yaml:"uid"`
	Gid          uint32            `yaml:"gid"`

	Seed int64 `yaml:"seed"`

	DisableLocalWritebackCaching bool   `yaml:"disableLocalWriteBackCaching"`
	MaxRetry                     int    `yaml:"maxRetry"`
	CosPrefetchSize              string `yaml:"cosPrefetchSize"`
	ClientReadAheadSize          string `yaml:"clientReadAheadSize"`
	RemoteCacheSize              string `yaml:"remoteCacheSize"`
	ChunkCacheSize               string `yaml:"chunkCacheSize"`
	UploadParallel               int    `yaml:"uploadParallel" default:"16"`

	ChunkSize      string `yaml:"chunkSize"`
	RpcChunkSize   string `yaml:"rpcChunkSize"`
	UseNotifyStore bool   `yaml:"useNotifyStore" default:"false"`

	ChunkSizeBytes           int64 `yaml:"-"`
	RpcChunkSizeBytes        int64 `yaml:"-"`
	CosPrefetchSizeBytes     int64 `yaml:"-"`
	ChunkCacheSizeBytes      int64 `yaml:"-"`
	ClientReadAheadSizeBytes int64 `yaml:"-"`
	RemoteCacheSizeBytes     int64 `yaml:"-"`

	IfName           string `yaml:"ifName"`
	DebugFuse        bool   `yaml:"debugFuse"`
	DebugS3          bool   `yaml:"debugS3"`
	BlockProfileRate int    `yaml:"blockProfileRate"`
	MutexProfileRate int    `yaml:"mutexProfileRate"`

	HeartBeatInterval    string `yaml:"heartBeatInterval"`
	ElectTimeout         string `yaml:"electTimeout"`
	ShutdownTimeout      string `yaml:"shutdownTimeout"`
	EvictionInterval     string `yaml:"evictionInterval"`
	DirtyExpireInterval  string `yaml:"dirtyExpireInterval"`
	DirtyFlusherInterval string `yaml:"dirtyFlusherInterval"`
	RpcTimeout           string `yaml:"rpcTimeout"`
	ChunkRpcTimeout      string `yaml:"chunkRpcTimeout"`
	CommitRpcTimeout     string `yaml:"commitRpcTimeout"`
	CoordinatorTimeout   string `yaml:"coordinatorTimeout"`
	MpuTimeout           string `yaml:"mpuTimeout"`
	PersistTimeout       string `yaml:"persistTimeout"`

	HeartBeatIntervalDuration    time.Duration `yaml:"-"`
	ElectTimeoutDuration         time.Duration `yaml:"-"`
	ShutdownTimeoutDuration      time.Duration `yaml:"-"`
	EvictionIntervalDuration     time.Duration `yaml:"-"`
	DirtyExpireIntervalMs        int32         `yaml:"-"`
	DirtyFlusherIntervalDuration time.Duration `yaml:"-"`
	RpcTimeoutDuration           time.Duration `yaml:"-"`
	ChunkRpcTimeoutDuration      time.Duration `yaml:"-"`
	CommitRpcTimeoutDuration     time.Duration `yaml:"-"`
	CoordinatorTimeoutDuration   time.Duration `yaml:"-"`
	MpuTimeoutDuration           time.Duration `yaml:"-"`
	PersistTimeoutDuration       time.Duration `yaml:"-"`
}

func (c *ObjcacheConfig) MergeServerConfig(conf *ObjcacheConfig) (newConfig ObjcacheConfig) {
	new := *conf

	// keep some configurations so that admin can control client configurations
	new.DisableLocalWritebackCaching = c.DisableLocalWritebackCaching
	new.MaxRetry = c.MaxRetry
	new.CosPrefetchSize = c.CosPrefetchSize
	new.CosPrefetchSizeBytes = c.CosPrefetchSizeBytes
	new.RemoteCacheSize = c.RemoteCacheSize
	new.RemoteCacheSizeBytes = c.RemoteCacheSizeBytes
	new.ChunkCacheSize = c.ChunkCacheSize
	new.ChunkCacheSizeBytes = c.ChunkCacheSizeBytes
	new.UploadParallel = c.UploadParallel

	new.DebugFuse = c.DebugFuse
	new.DebugS3 = c.DebugS3
	new.BlockProfileRate = c.BlockProfileRate
	new.MutexProfileRate = c.MutexProfileRate
	return new
}

func setDefaultString(str *string, defaultStr string) {
	if str == nil || *str == "" {
		*str = defaultStr
	}
}

func NewConfig(yamlFile string) (c ObjcacheConfig) {
	var buf []byte = nil
	if yamlFile != "" {
		f, err := os.Open(yamlFile)
		if err != nil {
			log.Fatalf("Failed: NewConfig, yamlFile=%v, err=%v", yamlFile, err)
		}
		b, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			log.Fatalf("Failed: NewConfig, ReadAll, err=%v", err)
		}
		buf = b
	}
	var err error
	c, err = NewConfigFromByteArray(buf)
	if err != nil {
		log.Fatalf("Failed: NewConfig, NewConfigFromByteArray, err=%v", err)
	}
	return
}

func NewConfigFromByteArray(buf []byte) (c ObjcacheConfig, err error) {
	if buf != nil {
		if err = yaml.UnmarshalStrict(buf, &c); err != nil {
			log.Printf("Failed: NewConfig, UnmarshalStrict, err=%v", err)
			return
		}
	}

	//mountOptions can be omitted
	if c.DirMode == 0 {
		c.DirMode = 0755
	}
	if c.FileMode == 0 {
		c.FileMode = 0644
	}
	if c.Uid == 0 {
		c.Uid = uint32(os.Getuid())
	}
	if c.Gid == 0 {
		c.Gid = uint32(os.Getgid())
	}

	setDefaultString(&c.ChunkSize, "16Mi")
	var chunkSize resource.Quantity
	if chunkSize, err = resource.ParseQuantity(c.ChunkSize); err != nil {
		log.Printf("Failed: NewConfig, ParseQuantity, ChunkSize=%v, err=%v", c.ChunkSize, err)
		return
	}
	c.ChunkSizeBytes = chunkSize.Value()
	setDefaultString(&c.RpcChunkSize, "16Mi")
	var rpcSize resource.Quantity
	if rpcSize, err = resource.ParseQuantity(c.RpcChunkSize); err != nil {
		log.Printf("Failed: NewConfig, ParseQuantity, RpcChunkSize=%v, err=%v", c.RpcChunkSize, err)
		return
	}
	c.RpcChunkSizeBytes = rpcSize.Value()
	if c.MaxRetry <= 0 {
		c.MaxRetry = 100
	}
	setDefaultString(&c.CosPrefetchSize, "256Mi")
	var prefetchSize resource.Quantity
	if prefetchSize, err = resource.ParseQuantity(c.CosPrefetchSize); err != nil {
		log.Printf("Failed: NewConfig, ParseQuantity, CosPrefetchSize=%v, err=%v", c.CosPrefetchSize, err)
		return
	}
	c.CosPrefetchSizeBytes = prefetchSize.Value()

	setDefaultString(&c.ClientReadAheadSize, "16Mi")
	var readAheadSize resource.Quantity
	if readAheadSize, err = resource.ParseQuantity(c.ClientReadAheadSize); err != nil {
		log.Printf("Failed: NewConfig, ParseQuantity, ClientReadAheadSize=%v, err=%v", c.ClientReadAheadSize, err)
		return
	}
	c.ClientReadAheadSizeBytes = readAheadSize.Value()
	//UseNotifyStore
	if c.UploadParallel <= 0 {
		c.UploadParallel = 1
	}

	setDefaultString(&c.RemoteCacheSize, "256Mi")
	var remoteCacheSize resource.Quantity
	if remoteCacheSize, err = resource.ParseQuantity(c.RemoteCacheSize); err != nil {
		log.Printf("Failed: NewConfig, ParseQuantity, RemoteCacheSize=%v, err=%v", c.RemoteCacheSize, err)
		return
	}
	c.RemoteCacheSizeBytes = remoteCacheSize.Value()

	setDefaultString(&c.ChunkCacheSize, "256Mi")
	var chunkCacheSize resource.Quantity
	if chunkCacheSize, err = resource.ParseQuantity(c.ChunkCacheSize); err != nil {
		log.Printf("Failed: NewConfig, ParseQuantity, ChunkCacheSize=%v, err=%v", c.ChunkCacheSize, err)
		return
	}
	c.ChunkCacheSizeBytes = chunkCacheSize.Value()

	setDefaultString(&c.IfName, "")
	//DebugFuse
	//DebugS3
	//profilePort
	//BlockProfileRate
	//MutexProfileRate
	setDefaultString(&c.HeartBeatInterval, "15ms")
	if c.HeartBeatIntervalDuration, err = time.ParseDuration(c.HeartBeatInterval); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, HeartBeatInterval=%v, err=%v", c.HeartBeatInterval, err)
		return
	}
	setDefaultString(&c.ElectTimeout, "150ms")
	if c.ElectTimeoutDuration, err = time.ParseDuration(c.ElectTimeout); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, ElectTimeout=%v, err=%v", c.ElectTimeout, err)
		return
	}
	setDefaultString(&c.ShutdownTimeout, "1m")
	if c.ShutdownTimeoutDuration, err = time.ParseDuration(c.ShutdownTimeout); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, ShutdownTimeout=%v, err=%v", c.ShutdownTimeout, err)
		return
	}
	setDefaultString(&c.EvictionInterval, "1s")
	if c.EvictionIntervalDuration, err = time.ParseDuration(c.EvictionInterval); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, EvictionInterval=%v, err=%v", c.EvictionInterval, err)
		return
	}
	setDefaultString(&c.DirtyExpireInterval, "1m")
	var DirtyExpireIntervalDuration time.Duration
	if DirtyExpireIntervalDuration, err = time.ParseDuration(c.DirtyExpireInterval); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, DirtyExpireInterval=%v, err=%v", c.DirtyExpireInterval, err)
		return
	}
	if DirtyExpireIntervalDuration.Milliseconds() > int64(math.MaxInt32) {
		log.Printf("Failed: NewConfig, dirty expire interval must be < %d ms", math.MaxInt32)
		return
	}
	if DirtyExpireIntervalDuration.Milliseconds() < int64(math.MinInt32) {
		DirtyExpireIntervalDuration = time.Millisecond * -1
	}
	if DirtyExpireIntervalDuration.Nanoseconds() < time.Millisecond.Nanoseconds() {
		log.Infof("NewConfig, auto dirty flushing is disabled by default (NOTE: ignore micro-second level intervals for dirty expire), dirtyExpireInterval=%v", c.DirtyExpireInterval)
	}
	c.DirtyExpireIntervalMs = int32(DirtyExpireIntervalDuration.Milliseconds())
	setDefaultString(&c.DirtyFlusherInterval, "100ms")
	if c.DirtyFlusherIntervalDuration, err = time.ParseDuration(c.DirtyFlusherInterval); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, DirtyFlusherInterval=%v, err=%v", c.DirtyFlusherInterval, err)
		return
	}
	setDefaultString(&c.RpcTimeout, "15ms")
	if c.RpcTimeoutDuration, err = time.ParseDuration(c.RpcTimeout); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, RpcTimeout=%v, err=%v", c.RpcTimeout, err)
		return
	}
	setDefaultString(&c.ChunkRpcTimeout, "30ms")
	if c.ChunkRpcTimeoutDuration, err = time.ParseDuration(c.ChunkRpcTimeout); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, RpcChunkTimeout=%v, err=%v", c.ChunkRpcTimeout, err)
		return
	}
	setDefaultString(&c.CommitRpcTimeout, "15ms")
	if c.CommitRpcTimeoutDuration, err = time.ParseDuration(c.CommitRpcTimeout); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, CommitRpcTimeout=%v, err=%v", c.CommitRpcTimeout, err)
		return
	}
	setDefaultString(&c.CoordinatorTimeout, "50ms")
	if c.CoordinatorTimeoutDuration, err = time.ParseDuration(c.CoordinatorTimeout); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, CoordinatorTimeout=%v, err=%v", c.CoordinatorTimeout, err)
		return
	}
	setDefaultString(&c.MpuTimeout, "25s") // 1GB / 40MB/s = 25s
	if c.MpuTimeoutDuration, err = time.ParseDuration(c.MpuTimeout); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, MpuTimeout=%v, err=%v", c.MpuTimeout, err)
		return
	}
	setDefaultString(&c.PersistTimeout, "833s") // 100GB / 120MB/s = 833s
	if c.PersistTimeoutDuration, err = time.ParseDuration(c.PersistTimeout); err != nil {
		log.Printf("Failed: NewConfig, ParseDuration, PersistTimeout=%v, err=%v", c.PersistTimeout, err)
	}
	return
}

type NodeAddrInet4 struct {
	Port int
	Addr [4]byte
	// delete src info from unix.SockaddrInet4
}

func (n *NodeAddrInet4) String() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d", n.Addr[0], n.Addr[1], n.Addr[2], n.Addr[3], n.Port)
}

func (n *NodeAddrInet4) IpString() string {
	return fmt.Sprintf("%d.%d.%d.%d", n.Addr[0], n.Addr[1], n.Addr[2], n.Addr[3])
}

func WaitShutdown(stateFilePath string) {
	for {
		stateArray, err := ReadState(stateFilePath)
		if os.IsNotExist(err) {
			log.Printf("Success: WaitShutdown, state file is deleted. file=%v", stateFilePath)
			return
		} else if err != nil {
			log.Printf("Failed: WaitShutdown, other err is detected. file=%v, err=%v", stateFilePath, err)
			return
		}
		if len(stateArray) < 3 {
			log.Printf("WaitShutdown, objcache is still running, file=%v", stateFilePath)
			time.Sleep(time.Second)
			continue
		}
		if stateArray[2] == "Record" {
			log.Printf("Success: WaitShutdown, objcache (%v: %v) exited. file=%v", stateArray[0], stateArray[1], stateFilePath)
			return
		}
		log.Printf("Failed: WaitShutdown, stateArray is corrupt. file=%v, stateArray[2]=%v, err=%v", stateFilePath, stateArray[2], err)
		return
	}
}

func ReadState(stateFilePath string) ([]string, error) {
	f, err := os.Open(stateFilePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	ret := make([]string, 0)
	r := bufio.NewReader(f)
	for {
		str, err := r.ReadString(',')
		if err == io.EOF {
			ret = append(ret, strings.TrimSuffix(str, ","))
			break
		} else if err != nil {
			log.Printf("Failed: ReadState, ReadString, err=%v", err)
			return nil, err
		}
		ret = append(ret, strings.TrimSuffix(str, ","))
	}
	return ret, nil
}

func MarkExit(stateFilePath string) error {
	f, err := os.OpenFile(stateFilePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Failed: MarkExit, OpenFile at Run, path=%v, err=%v", stateFilePath, err)
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.WriteString(",Record")
	if err != nil {
		log.Printf("Failed: MarkExit, WriteString at Run, path=%v, err=%v", stateFilePath, err)
		return err
	}
	if err = f.Sync(); err != nil {
		log.Printf("Failed: MarkExit, Sync, raftF=%v, err=%v", f.Name(), err)
		return err
	}
	return nil
}

func MarkRunning(stateFilePath string, selfAddr string, mountPoint string) error {
	f, err := os.OpenFile(stateFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		log.Printf("Failed: instantiate, OpenFile at Run, path=%v, err=%v", stateFilePath, err)
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.WriteString(strings.Join([]string{selfAddr, mountPoint}, ","))
	if err != nil {
		log.Printf("Failed: instantiate, WriteString at Run, path=%v, err=%v", stateFilePath, err)
		return err
	}
	if err = f.Sync(); err != nil {
		log.Printf("Failed: instantiate, Sync, raftF=%v, err=%v", f.Name(), err)
		return err
	}
	return nil
}
