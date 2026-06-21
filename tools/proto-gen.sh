#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0

# Regenerates Go code from proto/*.proto.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v protoc >/dev/null 2>&1; then
  echo "proto-gen: protoc not found on PATH" >&2
  echo "  install: apt-get install -y protobuf-compiler   (or your OS equivalent)" >&2
  exit 1
fi

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

protoc \
  --proto_path=proto \
  --go_out=. \
  --go_opt=module=github.com/virogg/go-hbase \
  proto/wire.proto \
  proto/hooks.proto \
  proto/hbase/Cell.proto \
  proto/hbase/HBase.proto \
  proto/hbase/Client.proto

echo "proto-gen: Go code generated under internal/wire/{wirepb,hookpb,hbasepb}/"
