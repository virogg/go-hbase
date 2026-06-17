// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command endpoint-observer is the Go-side runtime of the TE22 endpoint
// round-trip integration test. It registers a no-op RegionObserver plus a
// Go Endpoint, so the stock GenericRegionObserver entrypoint can host the
// generic GoEndpointService: a client calls it via Table.coprocessorService
// and the call round-trips to Go and back.
//
// The endpoint implements "upper" (upper-cases its payload) and, for TE31,
// "get" (a reverse GET: it reads the row named by its payload from the region
// the client invoked the endpoint on, and returns the first cell's value). The
// no-op observer is required because GenericRegionObserver is a
// RegionCoprocessor: without a registered region observer the strict preOpen
// hook would dispatch to Go, find no observer, and abort region open. With
// UnimplementedRegionObserver every region hook is a no-op.
package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type upperEndpoint struct{}

// Call upper-cases its payload. Method "get" exercises the TE31 reverse-RPC GET.
// Two method names inject faults for the TE24 fault matrix: "panic" panics
// inside the handler (the SDK must recover it into a client error without
// killing the shared process), and "exit" crashes the whole Go process mid-call
// (the Java supervisor must fail the in-flight client call promptly and restart,
// rather than hang).
func (upperEndpoint) Call(ctx context.Context, method string, payload []byte) ([]byte, error) {
	slog.Info("endpoint-observer: call", "method", method, "bytes", len(payload))
	switch method {
	case "panic":
		panic("endpoint-observer: injected panic")
	case "exit":
		slog.Warn("endpoint-observer: injected os.Exit(1)")
		os.Exit(1)
		return nil, nil // unreachable
	case "get":
		return reverseGet(ctx, payload)
	default:
		return bytes.ToUpper(payload), nil
	}
}

// reverseGet reads the row named by payload from the invoking region via the
// TE31 reverse-RPC channel and returns the first cell's value (empty if the row
// has no cells). It builds a vendored HBase Get and parses the vendored Result
// the bridge ships back, exercising the full vendored-pb -> native -> vendored-pb
// conversion path end to end.
func reverseGet(ctx context.Context, row []byte) ([]byte, error) {
	getProto, err := proto.Marshal(&hbasepb.Get{Row: row})
	if err != nil {
		return nil, err
	}
	resultProto, err := hbasecop.ReverseGet(ctx, getProto)
	if err != nil {
		return nil, err
	}
	var result hbasepb.Result
	if err := proto.Unmarshal(resultProto, &result); err != nil {
		return nil, err
	}
	if len(result.Cell) == 0 {
		return nil, nil
	}
	slog.Info("endpoint-observer: reverse GET ok", "cells", len(result.Cell))
	return result.Cell[0].Value, nil
}

func main() {
	err := hbasecop.RunAll(hbasecop.UnimplementedRegionObserver{}, upperEndpoint{})
	if err != nil {
		slog.Error("endpoint-observer: fatal", "err", err)
		os.Exit(1)
	}
}
