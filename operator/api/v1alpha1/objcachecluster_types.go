/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package v1alpha1

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ObjcacheClusterSpec defines the desired state of ObjcacheCluster
type ObjcacheClusterSpec struct {
	//+kubebuilder:default=0
	// Seed is used to generate group ID (default: 0 == epoch time at the first deploy)
	Seed int64 `json:"seed,omitempty"`

	//+kubebuilder:default="1s"
	// UnstageDelayPeriod is the period after which CSI node begins terminating unstaged FUSE processes to reduce cold startups
	UnstageDelayPeriod string `json:"unstageDelayPeriod,omitempty"`

	//+kubebuilder:default=1
	// Shards is the maximum number of groups to be deployed (Shards * (1 + RaftFollowers) pods are deployed at random nodes within given constraints)
	Shards *int `json:"shards"`

	//+kubebuilder:default=0
	//+kubebuilder:validation:Minimum=0
	//+kubebuilder:validation:Maximum=4
	// RaftFollowers is the maximum number of followers consisting a single replication group
	RaftFollowers int `json:"raftFollowers,omitempty"`

	// GeneratedStorageClassName is the generated storage class name for this objcache cluster
	GeneratedStorageClassName string `json:"generatedStorageClassName"`

	//+kubebuilder:default="docker-na.artifactory.swg-devops.com/res-cpe-team-docker-local/objcache:dev"
	// ObjcacheImage is the image name for objcache
	ObjcacheImage string `json:"objcacheImage,omitempty"`

	// ServerConfig is detailed configurations (e.g., timeouts) for servers in this cluster
	ServerConfig ObjcacheConfig `json:"serverConfig"`

	//+kubebuilder:default=9638
	//+kubebuilder:validation:Minimum=0
	//+kubebuilder:validation:Maximum=65535
	// Port is the port for Raft log replication and RPC
	Port int `json:"port,omitempty"`

	//+kubebuilder:default=8638
	//+kubebuilder:validation:Minimum=0
	//+kubebuilder:validation:Maximum=65535
	// ApiPort is the port for controller APIs such as termination
	ApiPort int `json:"apiPort,omitempty"`

	//+kubebuilder:default=""
	// Secret is the name of secrets storing S3 credentials and mapped directory names
	Secret string `json:"secret,omitempty"`

	// GoDebugEnvVar sets pods GODEBUG
	//+kubebuilder:default=""
	GoDebugEnvVar *string `json:"goDebugEnvVar,omitempty"`

	// GoMemLimit sets pods GOMEMLIMIT
	//+kubebuilder:default=""
	GoMemLimit *string `json:"goMemLimit,omitempty"`

	// MultiNicName adds multi nic annotations to worker pods
	MultiNicName *string `json:"multiNicName,omitempty"`

	// Resources defines the resource requirement for servers
	Resources *ResourceRequirementsApplyConfiguration `json:"resources,omitempty"`

	// LocalVolume is the configuration for local volumes managed by each worker
	LocalVolume ObjcacheLocalVolume `json:"localVolume,omitempty"`

	// OverridePodSpec is the detailed configurations for worker pods
	OverridePodSpec *PodSpecApplyConfiguration `json:"overridePodSpec,omitempty"`
}

type ObjcacheConfig struct {
	// copied from common/objcache_config.go

	//+kubebuilder:default={"allow_other":""}
	MountOptions map[string]string `json:"mountOptions,omitempty" yaml:"mountOptions,omitempty"`
	//+kubebuilder:default=511
	DirMode uint32 `json:"dirMode,omitempty" yaml:"dirMode,omitempty"`
	//+kubebuilder:default=420
	FileMode uint32 `json:"fileMode,omitempty" yaml:"fileMode,omitempty"`
	//+kubebuilder:default=1000
	Uid uint32 `json:"uid,omitempty" yaml:"uid,omitempty"`
	//+kubebuilder:default=1000
	Gid uint32 `json:"gid,omitempty" yaml:"gid,omitempty"`

	//+kubebuilder:default=100
	MaxRetry int `json:"maxRetry,omitempty" yaml:"maxRetry,omitempty"`
	//+kubebuilder:default="16Mi"
	ChunkSize string `json:"chunkSize,omitempty" yaml:"chunkSize,omitempty"`
	//+kubebuilder:default="16Mi"
	RpcChunkSize string `json:"rpcChunkSize,omitempty" yaml:"rpcChunkSize,omitempty"`
	//+kubebuilder:default="1Gi"
	CosPrefetchSize string `json:"cosPrefetchSize,omitempty" yaml:"cosPrefetchSize,omitempty"`
	//+kubebuilder:default="256Mi"
	ClientReadAheadSize string `json:"clientReadAheadSize,omitempty" yaml:"clientReadAheadSize,omitempty"`
	//+kubebuilder:default="24Gi"
	RemoteCacheSize string `json:"remoteCacheSize,omitempty" yaml:"remoteCacheSize,omitempty"`
	//+kubebuilder:default="24Gi"
	ChunkCacheSize string `json:"chunkCacheSize,omitempty" yaml:"chunkCacheSize,omitempty"`
	//+kubebuilder:default=16
	UploadParallel int `json:"uploadParallel,omitempty" yaml:"uploadParallel,omitempty"`

	//+kubebuilder:default=""
	IfName string `json:"ifName,omitempty" yaml:"ifName,omitempty"`
	//+kubebuilder:default=false
	DebugFuse bool `json:"debugFuse,omitempty" yaml:"debugFuse,omitempty"`
	//+kubebuilder:default=false
	DebugS3 bool `json:"debugS3,omitempty" yaml:"debugS3,omitempty"`
	//+kubebuilder:default=0
	BlockProfileRate int `json:"blockProfileRate,omitempty" yaml:"blockProfileRate,omitempty"`
	//+kubebuilder:default=0
	MutexProfileRate int `json:"mutexProfileRate,omitempty" yaml:"mutexProfileRate,omitempty"`

	//+kubebuilder:default="100ms"
	HeartBeatInterval string `json:"heartBeatInterval,omitempty" yaml:"heartBeatInterval,omitempty"`
	//+kubebuilder:default="500ms"
	ElectTimeout string `json:"electTimeout,omitempty" yaml:"electTimeout,omitempty"`
	//+kubebuilder:default="1m"
	ShutdownTimeout string `json:"shutdownTimeout,omitempty" yaml:"shutdownTimeout,omitempty"`
	//+kubebuilder:default="1s"
	EvictionInterval string `json:"evictionInterval,omitempty" yaml:"evictionInterval,omitempty"`
	//+kubebuilder:default="24h"
	DirtyExpireInterval string `json:"dirtyExpireInterval,omitempty" yaml:"dirtyExpireInterval,omitempty"`
	//+kubebuilder:default="10s"
	DirtyFlusherInterval string `json:"dirtyFlusherInterval,omitempty" yaml:"dirtyFlusherInterval,omitempty"`
	//+kubebuilder:default="10s"
	RpcTimeout string `json:"rpcTimeout,omitempty" yaml:"rpcTimeout,omitempty"`
	//+kubebuilder:default="20s"
	ChunkRpcTimeout string `json:"chunkRpcTimeout,omitempty" yaml:"chunkRpcTimeout,omitempty"`
	//+kubebuilder:default="20s"
	CommitRpcTimeout string `json:"commitRpcTimeout,omitempty" yaml:"commitRpcTimeout,omitempty"`
	//+kubebuilder:default="20s"
	CoordinatorTimeout string `json:"coordinatorTimeout,omitempty" yaml:"coordinatorTimeout,omitempty"`
	//+kubebuilder:default="25s"
	MpuTimeout string `json:"mpuTimeout,omitempty" yaml:"mpuTimeout,omitempty"`
	//+kubebuilder:default="833s"
	PersistTimeout string `json:"persistTimeout,omitempty" yaml:"persistTimeout,omitempty"`
}

type PodSpecApplyConfiguration corev1apply.PodSpecApplyConfiguration

func (in *PodSpecApplyConfiguration) DeepCopy() *PodSpecApplyConfiguration {
	out := new(PodSpecApplyConfiguration)
	bytes, err := json.Marshal(in)
	if err != nil {
		panic("Failed to marshal")
	}
	err = json.Unmarshal(bytes, out)
	if err != nil {
		panic("Failed to unmarshal")
	}
	return out
}

type ResourceRequirementsApplyConfiguration corev1apply.ResourceRequirementsApplyConfiguration

func (in *ResourceRequirementsApplyConfiguration) DeepCopy() *ResourceRequirementsApplyConfiguration {
	out := new(ResourceRequirementsApplyConfiguration)
	bytes, err := json.Marshal(in)
	if err != nil {
		panic("Failed to marshal")
	}
	err = json.Unmarshal(bytes, out)
	if err != nil {
		panic("Failed to unmarshal")
	}
	return out
}

// ObjcacheClusterStatus defines the observed state of ObjcacheCluster
type ObjcacheClusterStatus struct {
	StartedShards int   `json:"startedShards"`
	RaftFollowers int   `json:"raftFollowers"`
	Seed          int64 `json:"seed"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// ObjcacheCluster is the Schema for the objcacheclusters API
type ObjcacheCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ObjcacheClusterSpec   `json:"spec,omitempty"`
	Status ObjcacheClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ObjcacheClusterList contains a list of ObjcacheCluster
type ObjcacheClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ObjcacheCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ObjcacheCluster{}, &ObjcacheClusterList{})
}
