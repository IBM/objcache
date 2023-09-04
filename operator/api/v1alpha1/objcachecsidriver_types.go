/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */

package v1alpha1

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ObjcacheCsiDriverSpec defines the desired state of ObjcacheCsiDriver
type ObjcacheCsiDriverSpec struct {
	// ServiceAccount is the name of service account with roles for CSI-related resources
	ServiceAccount string `json:"serviceAccount"`

	//+kubebuilder:default={objcacheCsiController: "docker-na.artifactory.swg-devops.com/res-cpe-team-docker-local/objcache-csi-controller:dev", objcacheCsiNode: "docker-na.artifactory.swg-devops.com/res-cpe-team-docker-local/objcache-csi-node:dev", driverRegistrar: "registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.7.0", provisioner: "registry.k8s.io/sig-storage/csi-provisioner:v3.4.0", attacher: "registry.k8s.io/sig-storage/csi-attacher:v4.2.0"}
	// Images defines used images for CSI pods
	Images ObjcacheImages `json:"images,omitempty"`

	//+kubebuilder:default="ocp-4.10.20"
	// EnvStr is the string to describe OCP versions ("ocp-4.10.20" or "roks-4.8")
	EnvStr string `json:"envStr,omitempty"`

	//+kubebuilder:default={capacity:"40Gi", cacheReplacementThreashold:80,mountPath:"/var/lib/objcache"}
	// LocalVolume is the local volume configuration for objcache FUSE
	LocalVolume ObjcacheLocalVolume `json:"localVolume,omitempty"`

	// MultiNicName adds multi nic annotations to node pods
	MultiNicName *string `json:"multiNicName,omitempty"`

	// FuseGoDebugEnvVar sets FUSE process GODEBUG
	//+kubebuilder:default=""
	FuseGoDebugEnvVar *string `json:"fuseGoDebugEnvVar,omitempty"`
	// FuseGoMemLimit sets FUSE process GOMEMLIMIT
	//+kubebuilder:default="64GiB"
	FuseGoMemLimit *string `json:"fuseGoMemLimit,omitempty"`

	// FuseConfig is configuration for objcache-fuse
	FuseConfig FuseConfig `json:"fuseConfig"`

	// OverridePodTemplateSpec is the detailed configurations for CSI pods
	OverridePodTemplateSpec *PodTemplateSpecApplyConfiguration `json:"overridePodTemplateSpec,omitempty"`

	//+kubebuilder:default=9638
	FusePortBegin int `json:"fusePortBegin,omitempty"`

	//+kubebuilder:default=19638
	FusePortEnd int `json:"fusePortEnd,omitempty"`
}

type FuseConfig struct {
	//+kubebuilder:default=false
	DisableLocalWriteBackCaching bool `json:"disableLocalWriteBackCaching,omitempty" yaml:"disableLocalWriteBackCaching"`
	//+kubebuilder:default=100
	MaxRetry int `json:"maxRetry,omitempty" yaml:"maxRetry"`
	//+kubebuilder:deafult="1Gi"
	CosPrefetchSize string `json:"cosPrefetchSize,omitempty" yaml:"cosPrefetchSize"`
	//+kubebuilder:deafult="256Mi"
	ClientReadAheadSize string `json:"clientReadAheadSize,omitempty" yaml:"clientReadAheadSize"`
	//+kubebuilder:deafult="48Gi"
	RemoteCacheSize string `json:"remoteCacheSize,omitempty" yaml:"remoteCacheSize"`
	//+kubebuilder:deafult="48Gi"
	ChunkCacheSize string `json:"chunkCacheSize,omitempty" yaml:"chunkCacheSize"`
	//+kubebuilder:default=16
	UploadParallel int `json:"uploadParallel,omitempty" yaml:"uploadParallel"`

	//+kubebuilder:default=false
	DebugFuse bool `json:"debugFuse,omitempty" yaml:"debugFuse,omitempty"`
	//+kubebuilder:default=false
	DebugS3 bool `json:"debugS3,omitempty" yaml:"debugS3,omitempty"`
	//+kubebuilder:default=0
	BlockProfileRate int `json:"blockProfileRate,omitempty" yaml:"blockProfileRate,omitempty"`
	//+kubebuilder:default=0
	MutexProfileRate int `json:"mutexProfileRate,omitempty" yaml:"mutexProfileRate,omitempty"`
}

type ObjcacheLocalVolume struct {
	//+kubebuilder:default=""
	// StorageClass is the name of storage class for allocating an ephemeral volume
	StorageClass string `json:"storageClass,omitempty"`

	//+kubebuilder:default="40Gi"
	// Capacity is the size of the local volume that is requested to StorageClass
	Capacity resource.Quantity `json:"capacity,omitempty"`

	//+kubebuilder:default=80
	//+kubebuilder:validation:Minimum=20
	//+kubebuilder:validation:Maximum=100
	// CacheReplacementThreashold is the percentage to start cache replacement
	CacheReplacementThreashold int `json:"cacheReplacementThreashold,omitempty"`

	//+kubebuilder:default="/var/lib/objcache"
	// MountPath is the directory path for an epemeral volume
	MountPath string `json:"mountPath,omitempty"`

	// DebugLogPvc is the name of persistent volume claims for objcache logs
	DebugLogPvc *string `json:"debugLogPvc,omitempty"`
}

type ObjcacheImages struct {
	//+kubebuilder:default="docker-na.artifactory.swg-devops.com/res-cpe-team-docker-local/objcache-csi-controller:dev"
	// ObjcacheCsiController is the image name for objcache-csi-controller
	ObjcacheCsiController string `json:"objcacheCsiController,omitempty"`

	//+kubebuilder:default="docker-na.artifactory.swg-devops.com/res-cpe-team-docker-local/objcache-csi-node:dev"
	// ObjcacheCsiNode is the image name for objcache-csi-node
	ObjcacheCsiNode string `json:"objcacheCsiNode,omitempty"`

	//+kubebuilder:default="registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.7.0"
	// DriverRegistar is the image name of the CSI driver-registrar
	DriverRegistar string `json:"driverRegistrar,omitempty"`

	//+kubebuilder:default="registry.k8s.io/sig-storage/csi-provisioner:v3.4.0"
	// Provisioner is the image name of the CSI provisioner
	Provisioner string `json:"provisioner,omitempty"`

	//+kubebuilder:default="registry.k8s.io/sig-storage/csi-attacher:v4.2.0"
	// Attacher is the image name of the CSI attacher
	Attacher string `json:"attacher,omitempty"`
}

type PodTemplateSpecApplyConfiguration corev1apply.PodTemplateSpecApplyConfiguration

func (in *PodTemplateSpecApplyConfiguration) DeepCopy() *PodTemplateSpecApplyConfiguration {
	out := new(PodTemplateSpecApplyConfiguration)
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

// ObjcacheCsiDriverStatus defines the observed state of ObjcacheCsiDriver
type ObjcacheCsiDriverStatus struct {
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// ObjcacheCsiDriver is the Schema for the objcachecsidrivers API
type ObjcacheCsiDriver struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ObjcacheCsiDriverSpec   `json:"spec,omitempty"`
	Status ObjcacheCsiDriverStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ObjcacheCsiDriverList contains a list of ObjcacheCsiDriver
type ObjcacheCsiDriverList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ObjcacheCsiDriver `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ObjcacheCsiDriver{}, &ObjcacheCsiDriverList{})
}
