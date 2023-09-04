FROM golang:1.19
RUN apt-get update && \
    apt-get -y install --no-install-recommends ca-certificates fuse && \
    rm -rf /var/lib/apt/lists/*  && \
    go install github.com/go-delve/delve/cmd/dlv@latest

COPY ./bin/objcache-fuse     /objcache-fuse
COPY ./bin/objcache-csi-node /objcache-csi-node
COPY ./bin/objcache-client   /objcache-client
ENTRYPOINT ["/objcache-csi-node"]
