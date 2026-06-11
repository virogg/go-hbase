// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command noop-observer is the Go side of the T81 latency bench: a
// RegionObserver whose PrePut/PostPut do zero work, so a timed dispatch
// through it measures pure bridge overhead (proto build → wire encode →
// shmem ring → cpruntime dispatch → response path) and nothing else.
//
// Unlike examples/counter-observer it never logs from a hook — log I/O
// would pollute the latency distribution the bench exists to pin down.
// Built by `make go-build-bench-noop` into test/bench/bin/linux-amd64/
// noop-runtime and mapped onto the test classpath (resource path
// bench/linux-amd64/noop-runtime) by the root pom's testResources block.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type noopObserver struct {
	hbasecop.UnimplementedRegionObserver
}

func (noopObserver) PrePut(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hbasecop.MutationProto,
) (hbasecop.HookResult, error) {
	return hbasecop.HookResult{}, nil
}

func (noopObserver) PostPut(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hbasecop.MutationProto,
) error {
	return nil
}

func main() {
	if err := hbasecop.Run(noopObserver{}); err != nil {
		slog.Error("noop-observer: fatal", "err", err)
		os.Exit(1)
	}
}
