FROM ubuntu:22.04
RUN apt-get update && \
    apt-get -y install --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY ./bin/objcache /objcache
COPY ./bin/objcache-client   /objcache-client
ENTRYPOINT ["/objcache"]
