# syntax=docker/dockerfile:1
# Copyright The Kubernetes Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG GOLANG_IMAGE=golang:1.26
# CGO is disabled, so the binary needs no libc; the static base carries only
# ca-certificates, tzdata and /etc/passwd.
ARG BASE_IMAGE=gcr.io/distroless/static-debian12

# The build stage runs on the builder's architecture and cross-compiles for
# the target, so the Go toolchain never runs under emulation.
FROM --platform=$BUILDPLATFORM $GOLANG_IMAGE AS builder

WORKDIR /go/src/app

# Dependencies first so a code-only change does not re-download them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Populated by buildx from --platform; they describe the target, which is
# what GOOS/GOARCH need.
ARG TARGETOS=linux
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w" -o /go/bin/dranet ./cmd/dranet

FROM $BASE_IMAGE
COPY --from=builder --chown=root:root /go/bin/dranet /dranet
CMD ["/dranet"]
