#
# Copyright 2023- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache-2.0
#

.PHONY: build container push clean

REGISTRY_NAME=docker-na.artifactory.swg-devops.com/res-cpe-team-docker-local
CONTROLLER_IMAGE_NAME=objcache-csi-controller
NODE_IMAGE_NAME=objcache-csi-node
NOFS_IMAGE_NAME=objcache
FUSE_IMAGE_NAME=objcache-fuse
OPERATOR_IMAGE_NAME=objcache-operator
VERSION ?= dev
CONTROLLER_IMAGE_TAG=$(REGISTRY_NAME)/$(CONTROLLER_IMAGE_NAME):$(VERSION)
NODE_IMAGE_TAG=$(REGISTRY_NAME)/$(NODE_IMAGE_NAME):$(VERSION)
NOFS_IMAGE_TAG=$(REGISTRY_NAME)/$(NOFS_IMAGE_NAME):$(VERSION)
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
	CGO_ENABLED=0 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/objcache-fuse ./cmd/objcache-fuse/
bin/objcache-csi-controller: $(CSI_CTONROLLER_GO_FILES) $(CORE_FILES) $(PROTO_FILES)
	CGO_ENABLED=0 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/objcache-csi-controller ./cmd/objcache-csi-controller
bin/objcache-csi-node: $(CSI_NODE_GO_FILES) $(CORE_FILES) $(PROTO_FILES)
	CGO_ENABLED=0 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/objcache-csi-node ./cmd/objcache-csi-node
bin/objcache: $(NOFS_GO_FILES) $(CORE_FILES) $(PROTO_FILES)
	CGO_ENABLED=0 GOOS=linux go build -v -a $(LD_FLAGS) $(GC_FLAGS) -o bin/objcache ./cmd/objcache

proto: api/objcache.pb.go common/types.pb.go
objcache: bin/objcache
fuse: bin/objcache-fuse
csi-controller: bin/objcache-csi-controller
csi-node: bin/objcache-csi-node bin/objcache-fuse
client: bin/objcache-client
operator/bin/kustomize:
	cd operator && make kustomize
operator: $(OPERATOR_FILES)
	cd operator && make generate manifests && go build -o bin/manager main.go
build: fuse csi-controller csi-node objcache client

c_csi-controller: csi-controller
	docker build --pull -t $(CONTROLLER_IMAGE_TAG) -f ./cmd/objcache-csi-controller/Dockerfile .
c_csi-node: csi-node client
	docker build --pull -t $(NODE_IMAGE_TAG) -f ./cmd/objcache-csi-node/Dockerfile .
c_objcache: objcache client
	docker build --pull -t $(NOFS_IMAGE_TAG) -f ./cmd/objcache/Dockerfile .
c_operator: 
	docker build --pull -t $(OPERATOR_IMAGE_TAG) -f operator/Dockerfile operator/
crd: operator operator/bin/kustomize deploy/kubernetes/crd.yaml
operator-yaml: deploy/kubernetes/crd.yaml deploy/kubernetes/operator.yaml deploy/kubernetes/namespace.yaml
container: c_csi-controller c_csi-node c_objcache c_operator crd operator-yaml

OPERATOR_NAMESPACE = objcache-operator-system
OPERATOR_NAMEPREFIX = objcache-operator-

deploy/kubernetes/operator-all.yaml: ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd operator/config/manager && cp kustomization.yaml .kustomization.yaml.orig && \
	../../bin/kustomize edit set image controller=${OPERATOR_IMAGE_TAG}
	./operator/bin/kustomize build operator/config/default > deploy/kubernetes/operator-all.yaml && \
	mv operator/config/manager/.kustomization.yaml.orig operator/config/manager/kustomization.yaml

deploy/kubernetes/crd.yaml:
	./operator/bin/kustomize build operator/config/crd > deploy/kubernetes/crd.yaml

deploy/kubernetes/operator.yaml:
	cd operator/config/manager-only && cp kustomization.yaml .kustomization.yaml.orig && \
	../../bin/kustomize edit set image controller=${OPERATOR_IMAGE_TAG} && \
	../../bin/kustomize edit set namespace $(OPERATOR_NAMESPACE) && \
	../../bin/kustomize edit set nameprefix ${OPERATOR_NAMEPREFIX} && \
	../../bin/kustomize build . > ../../../deploy/kubernetes/operator.yaml && \
	mv .kustomization.yaml.orig kustomization.yaml

deploy/kubernetes/namespace.yaml:
	cd operator/config/namespace && cp kustomization.yaml .kustomization.yaml.orig && \
	../../bin/kustomize edit set nameprefix ${OPERATOR_NAMEPREFIX} && \
	../../bin/kustomize edit set namespace $(OPERATOR_NAMESPACE) && \
	../../bin/kustomize build . > ../../../deploy/kubernetes/namespace.yaml && \
	mv .kustomization.yaml.orig kustomization.yaml

p_csi-controller: c_csi-controller
	docker push $(CONTROLLER_IMAGE_TAG)
p_csi-node: c_csi-node
	docker push $(NODE_IMAGE_TAG)
p_objcache: c_objcache
	docker push $(NOFS_IMAGE_TAG)
p_operator: c_operator
	make -C operator docker-push
p_fuse: c_fuse
	docker push $(FUSE_IMAGE_TAG)
push: p_csi-controller p_csi-node p_objcache p_operator

clean:
	go clean -r -x
	rm -rf bin
