/*
 * Copyright 2023- IBM Inc. All rights reserved
 * SPDX-License-Identifier: Apache-2.0
 */
package internal

import (
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/takeshi-yoshimura/fuse"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v2"
)

type BucketSpec struct {
	backend  *S3Backend
	testFile string
}
type ObjCacheBackend struct {
	buckets map[string]*BucketSpec
}

type BucketCredential struct {
	DirName     string `yaml:"dirName"`
	BucketName  string `yaml:"bucketName"`
	BackendName string `yaml:"backendName"`
	Endpoint    string `yaml:"endpoint"`
	AccessKey   string `yaml:"accessKey"`
	SecretKey   string `yaml:"secretKey"`
	TestFile    string `yaml:"testFile"`
	Anonymous   bool   `yaml:"anonymous"`
}

type BucketCredentials struct {
	Buckets []BucketCredential `yaml:"buckets"`
}

func NewObjCacheFromSecrets(buckets []BucketCredential, debugS3 bool, bufferSize int) (*ObjCacheBackend, error) {
	bucketMap := make(map[string]*BucketSpec)
	for _, bucket := range buckets {
		conf := &S3Config{}
		conf.Init()
		if bucket.Anonymous {
			conf.Credentials = credentials.AnonymousCredentials
		}
		if bucket.AccessKey != "" {
			conf.AccessKey = bucket.AccessKey
		}
		if bucket.SecretKey != "" {
			conf.SecretKey = bucket.SecretKey
		}
		if bucket.Endpoint != "" {
			conf.StsEndpoint = bucket.Endpoint
		}
		dirName := bucket.DirName
		if dirName == "" {
			dirName = bucket.BucketName
		}
		if dirName == ObjcacheDirName {
			log.Errorf("Failed: specified directory name (%s) is reserved. plaese use other names", dirName)
			return nil, unix.EINVAL
		}

		var back *S3Backend
		if bucket.BackendName == "gcs" {
			var err error
			if back, err = NewGCS3(bucket.BucketName, debugS3, conf, bufferSize); err == nil {
				if conf.StsEndpoint == "" {
					back.Endpoint = "https://storage.googleapis.com"
					back.awsConfig.Endpoint = &back.Endpoint
				} else {
					back.Endpoint = conf.StsEndpoint
					back.awsConfig.Endpoint = &conf.StsEndpoint
				}
				log.Debugf("NewObjCacheFromSecrets, NewS3, dirName=%v, backendName=%v, bucketName=%v, anonymous=%v, endpoint=%v",
					dirName, bucket.BackendName, bucket.BucketName, bucket.Anonymous, bucket.Endpoint)
			} else {
				log.Errorf("Failed: NewObjCacheFromSecrets, NewS3, dirName=%v, backendName=%v, bucketName=%v, anonymous=%v, endpoint=%v, err=%v",
					dirName, bucket.BackendName, bucket.BucketName, bucket.Anonymous, bucket.Endpoint, err)
				continue
			}
		} else {
			bucket.BackendName = "s3"
			var err error
			if back, err = NewS3(bucket.BucketName, debugS3, conf, bufferSize); err == nil {
				if conf.StsEndpoint != "" {
					back.Endpoint = conf.StsEndpoint
					back.awsConfig.Endpoint = &conf.StsEndpoint
					//config.RegionSet = true
				}
				log.Debugf("NewObjCacheFromSecrets, NewS3, dirName=%v, backendName=%v, bucketName=%v, anonymous=%v, endpoint=%v",
					dirName, bucket.BackendName, bucket.BucketName, bucket.Anonymous, bucket.Endpoint)
			} else {
				log.Errorf("Failed: NewObjCacheFromSecrets, NewS3, dirName=%v, backendName=%v, bucketName=%v, anonymous=%v, endpoint=%v, err=%v",
					dirName, bucket.BackendName, bucket.BucketName, bucket.Anonymous, bucket.Endpoint, err)
				continue
			}
		}
		bucketMap[dirName] = &BucketSpec{backend: back, testFile: bucket.TestFile}
	}
	return &ObjCacheBackend{buckets: bucketMap}, nil
}

func NewObjCache(secretFile string, debugS3 bool, bufferSize int) (*ObjCacheBackend, error) {
	f, err := os.Open(secretFile)
	if err != nil {
		log.Fatalf("Failed: NewObjCache, secretFile=%v, err=%v", secretFile, err)
	}
	buf, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		log.Fatalf("Failed: NewObjCache, ReadAll, err=%v", err)
	}
	conf := BucketCredentials{}
	if err = yaml.UnmarshalStrict(buf, &conf); err != nil {
		log.Fatalf("Failed: NewObjCache, UnmarshalStrict, err=%v", err)
	}
	return NewObjCacheFromSecrets(conf.Buckets, debugS3, bufferSize)
}

func (s *ObjCacheBackend) Init(key string) error {
	var wg sync.WaitGroup
	var lastErr error = nil
	for _, spec := range s.buckets {
		wg.Add(1)
		go func(spec *BucketSpec) {
			testKey := key
			if spec.testFile != "" {
				testKey = spec.testFile
			}
			if spec.backend.gcs {
				gcs := GCS3{S3Backend: spec.backend}
				if err := gcs.Init(testKey); err != nil {
					lastErr = err
				}
			} else {
				if err := spec.backend.Init(testKey); err != nil {
					lastErr = err
				}
			}
			wg.Done()
		}(spec)
	}
	wg.Wait()
	return lastErr
}

func (s *ObjCacheBackend) GetBucketKey(path string) (string, string) {
	split := strings.Split(path, "/")
	return split[0], strings.Join(split[1:], "/")
}

func (s *ObjCacheBackend) headBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	dirName, key := s.GetBucketKey(param.Key)
	if key == "" {
		key = "/"
	}
	spec, ok := s.buckets[dirName]
	if !ok {
		return nil, unix.ENOENT
	}
	newParam := &HeadBlobInput{Key: key}
	if !spec.backend.gcs {
		return spec.backend.HeadBlob(newParam)
	}
	gcs := GCS3{S3Backend: spec.backend}
	return gcs.HeadBlob(param)
}

func (s *ObjCacheBackend) listBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	dirName, key := s.GetBucketKey(*param.Prefix)
	newParam := *param
	newParam.Prefix = &key
	if param.StartAfter != nil {
		newStartAfter := strings.TrimPrefix(*param.StartAfter, dirName)
		newParam.StartAfter = &newStartAfter
	} else if key == "/" {
		newParam.StartAfter = &key
	}

	spec, ok := s.buckets[dirName]
	if !ok {
		return nil, unix.ENOENT
	}
	var res *ListBlobsOutput = nil
	var err error = fuse.ENOENT
	if !spec.backend.gcs {
		res, err = spec.backend.ListBlobs(&newParam)
	} else {
		gcs := GCS3{S3Backend: spec.backend}
		res, err = gcs.ListBlobs(&newParam)
	}
	if res == nil || err != nil {
		return res, err
	}
	items := make([]BlobItemOutput, 0)
	prefixes := make([]BlobPrefixOutput, 0)
	for _, i := range res.Items {
		newKey := dirName + "/" + *i.Key
		items = append(items, BlobItemOutput{
			Key:          &newKey,
			ETag:         i.ETag,
			LastModified: i.LastModified,
			Size:         i.Size,
			StorageClass: i.StorageClass,
		})
	}
	for _, p := range res.Prefixes {
		newPrefix := dirName + "/" + *p.Prefix
		prefixes = append(prefixes, BlobPrefixOutput{Prefix: &newPrefix})
	}

	return &ListBlobsOutput{
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: res.NextContinuationToken,
		IsTruncated:           res.IsTruncated,
		RequestId:             res.RequestId,
	}, nil
}

func (s *ObjCacheBackend) deleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	dirName, key := s.GetBucketKey(param.Key)
	spec, ok := s.buckets[dirName]
	if !ok {
		return nil, unix.ENOENT
	}
	newParam := &DeleteBlobInput{Key: key}
	if !spec.backend.gcs {
		return spec.backend.DeleteBlob(newParam)
	}
	gcs := GCS3{S3Backend: spec.backend}
	return gcs.DeleteBlob(newParam)
}

func (s *ObjCacheBackend) getBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	dirName, path := s.GetBucketKey(param.Key)
	spec, ok := s.buckets[dirName]
	if !ok {
		return nil, unix.ENOENT
	}
	newParam := &GetBlobInput{Key: path, Start: param.Start, Count: param.Count, IfMatch: param.IfMatch}
	if !spec.backend.gcs {
		return spec.backend.GetBlob(newParam)
	}
	gcs := GCS3{S3Backend: spec.backend}
	return gcs.GetBlob(newParam)
}

func (s *ObjCacheBackend) putBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	dirName, key := s.GetBucketKey(param.Key)
	spec, ok := s.buckets[dirName]
	if !ok {
		return nil, unix.ENOENT
	}
	newParam := &PutBlobInput{
		Key:         key,
		Metadata:    param.Metadata,
		ContentType: param.ContentType,
		DirBlob:     param.DirBlob,
		Body:        param.Body,
		Size:        param.Size,
	}
	if !spec.backend.gcs {
		return spec.backend.PutBlob(newParam)
	}
	gcs := GCS3{S3Backend: spec.backend}
	return gcs.PutBlob(newParam)
}

func (s *ObjCacheBackend) multipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	dirName, key := s.GetBucketKey(param.Key)
	spec, ok := s.buckets[dirName]
	if !ok {
		return nil, unix.ENOENT
	}
	newParam := &MultipartBlobBeginInput{
		Key:         key,
		Metadata:    param.Metadata,
		ContentType: param.ContentType,
	}
	var origRes *MultipartBlobCommitInput = nil
	var err error
	if !spec.backend.gcs {
		origRes, err = spec.backend.MultipartBlobBegin(newParam)
	} else {
		gcs := GCS3{S3Backend: spec.backend}
		origRes, err = gcs.MultipartBlobBegin(newParam)
	}
	if err != nil {
		return nil, err
	}
	newKey := param.Key
	return &MultipartBlobCommitInput{
		Key:         &newKey,
		Metadata:    origRes.Metadata,
		UploadId:    origRes.UploadId,
		Parts:       origRes.Parts,
		NumParts:    origRes.NumParts,
		backendData: origRes.backendData,
	}, err
}

func (s *ObjCacheBackend) multipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	dirName, key := s.GetBucketKey(*param.Commit.Key)
	spec, ok := s.buckets[dirName]
	if !ok {
		return nil, unix.ENOENT
	}
	newParam := &MultipartBlobAddInput{
		Commit: &MultipartBlobCommitInput{
			Key:         &key,
			Metadata:    param.Commit.Metadata,
			UploadId:    param.Commit.UploadId,
			Parts:       param.Commit.Parts,
			NumParts:    param.Commit.NumParts,
			backendData: param.Commit.backendData,
		},
		PartNumber: param.PartNumber,
		Body:       param.Body,
		Size:       param.Size,
		Last:       param.Last,
		Offset:     param.Offset,
	}
	var origRes *MultipartBlobAddOutput = nil
	var err error
	atomic.AddUint32(&param.Commit.NumParts, 1) // NumParts need to update here to handle racy mpu add
	if !spec.backend.gcs {
		origRes, err = spec.backend.MultipartBlobAdd(newParam)
	} else {
		gcs := GCS3{S3Backend: spec.backend}
		origRes, err = gcs.MultipartBlobAdd(newParam)
	}
	if err != nil {
		return nil, err
	}
	// param.CommitPersistRecord is reused in the commit phase
	param.Commit.Parts = newParam.Commit.Parts
	param.Commit.UploadId = newParam.Commit.UploadId
	param.Commit.Metadata = newParam.Commit.Metadata
	return origRes, err
}

func (s *ObjCacheBackend) multipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	dirName, key := s.GetBucketKey(*param.Key)
	spec, ok := s.buckets[dirName]
	if !ok {
		return nil, unix.ENOENT
	}
	newParam := &MultipartBlobCommitInput{
		Key:         &key,
		Metadata:    param.Metadata,
		UploadId:    param.UploadId,
		Parts:       param.Parts,
		NumParts:    param.NumParts,
		backendData: param.backendData,
	}
	if !spec.backend.gcs {
		return spec.backend.MultipartBlobAbort(newParam)
	}
	gcs := GCS3{S3Backend: spec.backend}
	return gcs.MultipartBlobAbort(newParam)
}

func (s *ObjCacheBackend) multipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	dirName, key := s.GetBucketKey(*param.Key)
	spec, ok := s.buckets[dirName]
	if !ok {
		return nil, unix.ENOENT
	}
	newParam := &MultipartBlobCommitInput{
		Key:         &key,
		Metadata:    param.Metadata,
		UploadId:    param.UploadId,
		Parts:       param.Parts,
		NumParts:    param.NumParts,
		backendData: param.backendData,
	}
	if !spec.backend.gcs {
		return spec.backend.MultipartBlobCommit(newParam)
	}
	gcs := GCS3{S3Backend: spec.backend}
	return gcs.MultipartBlobCommit(newParam)
}
