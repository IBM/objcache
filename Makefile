#
# Copyright 2023- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache-2.0
#

.PHONY: test build container push clean

REGISTRY_NAME=docker-na.artifactory.swg-devops.com/res-cpe-team-docker-local
CONTROLLER_IMAGE_NAME=objcache-csi-controller
NODE_IMAGE_NAME=objcache-csi-node
NOFS_IMAGE_NAME=objcache
TEST_IMAGE_NAME=objcache-test
FUSE_IMAGE_NAME=objcache-fuse
OPERATOR_IMAGE_NAME=objcache-operator
VERSION ?= dev
CONTROLLER_IMAGE_TAG=$(REGISTRY_NAME)/$(CONTROLLER_IMAGE_NAME):$(VERSION)
NODE_IMAGE_TAG=$(REGISTRY_NAME)/$(NODE_IMAGE_NAME):$(VERSION)
NOFS_IMAGE_TAG=$(REGISTRY_NAME)/$(NOFS_IMAGE_NAME):$(VERSION)
TEST_IMAGE_TAG=$(REGISTRY_NAME)/$(TEST_IMAGE_NAME):$(VERSION)
FUSE_IMAGE_TAG=$(REGISTRY_NAME)/$(FUSE_IMAGE_NAME):$(VERSION)
OPERATOR_IMAGE_TAG=$(REGISTRY_NAME)/$(OPERATOR_IMAGE_NAME):0.0.1

CLIENT_GO_FILES = $(shell find ./cmd/objcache-client/ common/ api/ -type f -name '*.go')
CORE_FILES=$(shell find internal/ common/ api/ -type f -name '*.go')
NOFS_GO_FILES= $(shell find ./cmd/objcache -type f -name '*.go')

FUSE_GO_FILES = $(CORE_FILES) ./cmd/objcache-fuse/fuse.go
CSI_CONTROLLER_GO_FILES = $(shell find ./cmd/objcache-csi-controller -type f -name '*.go')
CSI_NODE_GO_FILES = $(shell find ./cmd/objcache-csi-node -type f -name '*.go')
PROTO_FILES = api/objcache.pb.go common/types.pb.go
OPERATOR_FILES = $(shell find operator/ -type f -name '*.go')

LD_FLAGS = -ldflags '-extldflags "-static"'
ifdef DEBUG
GC_FLAGS = -gcflags="all=-N -l"
else
GC_FLAGS =
endif

api/objcache.pb.go: api/objcache.proto
	protoc --go_out=./ --go-grpc_out=./api/ --go_opt=paths=source_relative api/objcache.proto
common/types.pb.go: common/types.proto
	protoc --go_out=./ --go-grpc_out=./common/ --go_opt=paths=source_relative common/types.proto
bin/objcache-client: $(CLIENT_GO_FILES) $(PROTO_FILES)
	CGO_ENABLED=0 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/objcache-client ./cmd/objcache-client/
bin/objcache-fuse: $(FUSE_GO_FILES) $(PROTO_FILES)
	CGO_ENABLED=1 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/objcache-fuse ./cmd/objcache-fuse/
bin/objcache-csi-controller: $(CSI_CTONROLLER_GO_FILES) $(CORE_FILES) $(PROTO_FILES)
	CGO_ENABLED=1 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/objcache-csi-controller ./cmd/objcache-csi-controller
bin/objcache-csi-node: $(CSI_NODE_GO_FILES) $(CORE_FILES) $(PROTO_FILES)
	CGO_ENABLED=1 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/objcache-csi-node ./cmd/objcache-csi-node
bin/objcache: $(NOFS_GO_FILES) $(CORE_FILES) $(PROTO_FILES)
	CGO_ENABLED=1 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/objcache ./cmd/objcache
bin/test: test/main.go
	CGO_ENABLED=1 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/test ./test/main.go

proto: api/objcache.pb.go common/types.pb.go
objcache: bin/objcache
fuse: bin/objcache-fuse
csi-controller: bin/objcache-csi-controller
csi-node: bin/objcache-csi-node bin/objcache-fuse
client: bin/objcache-client
operator: $(OPERATOR_FILES)
	cd operator && make generate manifests && go build -o bin/manager main.go
build: fuse csi-controller csi-node objcache client test
test: bin/test

c_csi-controller: csi-controller
	docker build --pull -t $(CONTROLLER_IMAGE_TAG) -f ./cmd/objcache-csi-controller/Dockerfile .
c_csi-node: csi-node client
	docker build --pull -t $(NODE_IMAGE_TAG) -f ./cmd/objcache-csi-node/Dockerfile .
c_objcache: objcache client
	docker build --pull -t $(NOFS_IMAGE_TAG) -f ./cmd/objcache/Dockerfile .
c_test: test
	docker build --pull -t $(TEST_IMAGE_TAG) -f test/Dockerfile .
c_operator: 
	docker build --pull -t $(OPERATOR_IMAGE_TAG) -f operator/Dockerfile operator/
crd: operator
	./operator/bin/kustomize build operator/config/crd > deploy/kubernetes/crd.yaml
operator-yaml: operator
	cd ./operator/config/manager && ../../bin/kustomize edit set image controller=${OPERATOR_IMAGE_TAG}
	./operator/bin/kustomize build operator/config/default > deploy/kubernetes/operator.yaml
container: c_csi-controller c_csi-node c_objcache c_test c_operator crd operator-yaml

p_csi-controller: c_csi-controller
	docker push $(CONTROLLER_IMAGE_TAG)
p_csi-node: c_csi-node
	docker push $(NODE_IMAGE_TAG)
p_objcache: c_objcache
	docker push $(NOFS_IMAGE_TAG)
p_test: c_test
	docker push $(TEST_IMAGE_TAG)
p_operator: c_operator
	make -C operator docker-push
p_fuse: c_fuse
	docker push $(FUSE_IMAGE_TAG)
push: p_csi-controller p_csi-node p_objcache p_test p_operator

clean:
	go clean -r -x
	rm -rf bin
