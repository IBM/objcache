module github.com/IBM/objcache

go 1.21

toolchain go1.21.3

require (
	github.com/aws/aws-sdk-go v1.51.20
	github.com/container-storage-interface/spec v1.7.0
	github.com/golang/protobuf v1.5.4
	github.com/google/btree v1.1.2
	github.com/kubernetes-csi/csi-lib-utils v0.12.0
	github.com/serialx/hashring v0.0.0-20200727003509-22c0c7ab6b1b
	github.com/sirupsen/logrus v1.6.0
	github.com/takeshi-yoshimura/fuse v0.0.0-20230810022419-2eee45af00b5
	go.uber.org/automaxprocs v1.5.3
	golang.org/x/net v0.21.0
	golang.org/x/sys v0.19.0
	google.golang.org/grpc v1.63.2
	google.golang.org/protobuf v1.33.0
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/apimachinery v0.26.3
	k8s.io/mount-utils v0.29.3
)

require google.golang.org/genproto/googleapis/rpc v0.0.0-20240227224415-6ceb2ff114de // indirect

require (
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.3 // indirect
	github.com/moby/sys/mountinfo v0.6.2 // indirect
	github.com/spaolacci/murmur3 v1.1.0
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto v0.0.0-20240227224415-6ceb2ff114de // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/klog/v2 v2.110.1 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
)
