# csi-direct-iscsi image. Built and pushed by CI on merges to main:
#   docker build --build-arg VERSION=1.1.0.0 -t csi-direct-iscsi .
FROM golang:1.26.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY directiscsi ./directiscsi
COPY proxy ./proxy
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-X main.version=${VERSION}" -o /out/csi-direct-iscsi ./cmd/csi-direct-iscsi

# The node proxy formats, mounts and resizes direct volumes itself: the same
# filesystem tools as upstream's cinder-csi-plugin image. The node's iSCSI,
# multipath and sg3 tools run chrooted on the host (/host).
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates mount udev btrfs-progs e2fsprogs xfsprogs util-linux \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/csi-direct-iscsi /usr/local/bin/csi-direct-iscsi
ENTRYPOINT ["/usr/local/bin/csi-direct-iscsi"]
