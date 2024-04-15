Objcache is a distributed filesystem (DFS) over cloud object storage (COS) as durable storage with high performance and quick/zero scaling.
Objcache maps objects stored at COS buckets (e.g., s3://bucket/key) to files (e.g., /opt/bucket/key) with write-back cache, sharding, and replication at local storage in a cluster.

### How to deploy

1. Deploy CRD and Operator

```bash
$ kubectl apply -f deploy/kubernetes/crd.yaml \
                -f deploy/kubernetes/namespace.yaml \
                -f deploy/kubernetes/operator.yaml --server-side=true
```

2. Create a service account (and SCC) for the objcache CSI driver

```bash
$ kubectl apply -f deployment/kubernetes/serviceaccount.yaml
$ oc adm policy add-scc-to-user privileged system:serviceaccount:objcache-operator-system:objcache-csi-sa # only for OpenShift
```

3. Configure and deploy secret.yaml for bucket credentials

```bash
$ cp deployment/kubernetes/EXAMPLE-secret.yaml secret.yaml
$ vim secret.yaml
$ kubectl apply -f secret.yaml
```

4. Deploy objcache resources and its PVC

```bash
$ kubectl apply -f deploy/kubernetes/csidriver.yaml \
                -f deploy/kubernetes/objcache-sample.yaml
```

5. Test a pod with the PVC mounted

```bash
$ kubectl apply -f deploy/kubernetes/test.yaml
$ kubectl exec -it objcache-sample-test-0 -- bash

$ ls /objcache/test-bucket/
$ echo "test" > /objcache/test-bucket/test.txt
$ exit

$ kubectl exec -it objcache-sample-test-1 -- bash

$ cat /objcache/test-bucket/test.txt
$ sync /objcache/test-bucket/test.txt
$ exit

$ aws s3 cp s3://test-bucket/test.txt /tmp/test.txt
$ cat /tmp/test.txt
```

### How to build

1. Install Go 1.19

2. Install protocol buffers (v3.6.1) and Go/gRPC plugins

```bash
$ curl -L -O https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip
$ unzip protoc-3.6.1-linux-x86_64.zip -d protoc
$ sudo cp protoc/bin/protoc /usr/local/bin/
$ go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.25.0
$ go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
```

3. Build

```bash
$ mkdir -p $HOME/go/src/github.com/IBM/objcache; cd $HOME/go/src/github.com/IBM
$ git clone https://github.com/IBM/objcache.git
$ cd objcache
$ go get github.com/IBM/objcache
$ go mod download
$ vim Makefile # change REGISTRY_NAME if you need
$ make container
```

### License

This repository is distributed under the terms of the Apache 2.0 License.
Some of files in this repository are copied (and modified) from Goofys (https://github.com/kahing/goofys), see each file header for license details.
