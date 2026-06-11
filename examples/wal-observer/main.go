// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command wal-observer is the Go-side runtime of the T82 WAL-throughput
// bench. The shaded coproc-jar embeds this ELF at
// bin/linux-amd64/hbasecop-runtime so the Java bridge supervisor extracts
// it from classpath resources and exec's it on the RegionServer.
//
// The observer is deliberately a no-op: preWALWrite/postWALWrite cross
// the shmem rings and return immediately, so the bench IT measures pure
// bridge overhead on the WAL append hot path against a baseline run
// without the coprocessor registered.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

// noopWALObserver answers every WAL hook with the zero result. The two
// hot-path hooks are overridden explicitly (rather than inherited from
// UnimplementedWALObserver) so the bench provably dispatches through the
// hook table; they must stay free of any work — no logging, no counters.
type noopWALObserver struct {
	hbasecop.UnimplementedWALObserver
}

func (noopWALObserver) PreWALWrite(context.Context, hbasecop.ObserverEnv, *hookpb.PreWALWriteRequest) (hbasecop.HookResult, error) {
	return hbasecop.HookResult{}, nil
}

func (noopWALObserver) PostWALWrite(context.Context, hbasecop.ObserverEnv, *hookpb.PostWALWriteRequest) error {
	return nil
}

func main() {
	slog.Info("wal-observer: starting")

	if err := hbasecop.RunWAL(noopWALObserver{}); err != nil {
		slog.Error("wal-observer: fatal", "err", err)
		os.Exit(1)
	}
}
