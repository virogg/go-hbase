#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0

# Regenerates Go code from proto/*.proto.
#
# Java code is regenerated automatically by the Maven build
# (protobuf-maven-plugin -> target/generated-sources/protobuf/java/), so
# this script only handles the Go side.
#
# Requirements: protoc (any 3.x) on PATH; protoc-gen-go on PATH (commonly
# at $(go env GOPATH)/bin). The script PATH-augments GOPATH/bin so a fresh
# checkout works without manual setup.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v protoc >/dev/null 2>&1; then
  echo "proto-gen: protoc not found on PATH" >&2
  echo "  install: apt-get install -y protobuf-compiler   (or your OS equivalent)" >&2
  exit 1
fi

# Ensure GOPATH/bin (where `go install` drops protoc-gen-go) is reachable.
if command -v go >/dev/null 2>&1; then
  GOBIN="$(go env GOPATH)/bin"
  case ":$PATH:" in
    *":$GOBIN:"*) ;;
    *) export PATH="$GOBIN:$PATH" ;;
  esac
fi

if ! command -v protoc-gen-go >/dev/null 2>&1; then
  echo "proto-gen: protoc-gen-go not found on PATH" >&2
  echo "  install: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" >&2
  exit 1
fi

cd "$ROOT"

# `module=` mode: protoc-gen-go emits files into a path derived from the
# go_package option, with the module prefix stripped. Our protos declare
# go_package = github.com/virogg/go-hbase/internal/wire/{wirepb,hookpb},
# so output lands at internal/wire/{wirepb,hookpb}/*.pb.go.
protoc \
  --proto_path=proto \
  --go_out=. \
  --go_opt=module=github.com/virogg/go-hbase \
  proto/wire.proto proto/hooks.proto

echo "proto-gen: Go code generated under internal/wire/{wirepb,hookpb}/"
