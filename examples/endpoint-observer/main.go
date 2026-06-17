// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command endpoint-observer is the Go-side runtime of the TE22 endpoint
// round-trip integration test. It registers a no-op RegionObserver plus a
// Go Endpoint, so the stock GenericRegionObserver entrypoint can host the
// generic GoEndpointService: a client calls it via Table.coprocessorService
// and the call round-trips to Go and back.
//
// The endpoint implements one method, "upper", which upper-cases its request
// payload. The no-op observer is required because GenericRegionObserver is a
// RegionCoprocessor: without a registered region observer the strict preOpen
// hook would dispatch to Go, find no observer, and abort region open. With
// UnimplementedRegionObserver every region hook is a no-op.
package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type upperEndpoint struct{}

// Call upper-cases its payload. Two method names inject faults for the TE24
// fault matrix: "panic" panics inside the handler (the SDK must recover it into
// a client error without killing the shared process), and "exit" crashes the
// whole Go process mid-call (the Java supervisor must fail the in-flight client
// call promptly and restart, rather than hang).
func (upperEndpoint) Call(_ context.Context, method string, payload []byte) ([]byte, error) {
	slog.Info("endpoint-observer: call", "method", method, "bytes", len(payload))
	switch method {
	case "panic":
		panic("endpoint-observer: injected panic")
	case "exit":
		slog.Warn("endpoint-observer: injected os.Exit(1)")
		os.Exit(1)
		return nil, nil // unreachable
	default:
		return bytes.ToUpper(payload), nil
	}
}

func main() {
	err := hbasecop.RunAll(hbasecop.UnimplementedRegionObserver{}, upperEndpoint{})
	if err != nil {
		slog.Error("endpoint-observer: fatal", "err", err)
		os.Exit(1)
	}
}
