// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command fault-observer is the Go-side runtime of the T36 fault-injection
// coproc. It mirrors counter-observer's layout - the Java bridge extracts the
// embedded ELF from the shaded jar and exec's it on the RegionServer.
//
// The injected fault is selected by the HBASECOP_FAULT_MODE environment
// variable (counter-observer's defaults are reused for HBASECOP_SHMEM_* and
// HBASECOP_RING_*). Valid modes are documented on the [fault.Mode] type.
package main

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/virogg/go-hbase/examples/fault-observer/fault"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

const faultModeEnv = "HBASECOP_FAULT_MODE"

type defaultActions struct{}

func (defaultActions) Kill9() {
	pid := os.Getpid()
	slog.Warn("fault-observer: SIGKILL self", "pid", pid)
	_ = syscall.Kill(pid, syscall.SIGKILL)
	// Should be unreachable; sleep keeps the goroutine quiet if signal delivery is delayed.
	time.Sleep(time.Hour)
}

func (defaultActions) Exit1() {
	slog.Warn("fault-observer: os.Exit(1)")
	os.Exit(1)
}

func (defaultActions) Hang(ctx context.Context) {
	slog.Warn("fault-observer: hanging on prePut until context done")
	<-ctx.Done()
}

func (defaultActions) AllocateOOM() {
	slog.Warn("fault-observer: allocating to OOM")
	// Allocate 1 GiB at a time until the kernel OOM-killer reaps us. cgroup memory
	// limits on the docker dev cluster (T26) are well below this, so the process
	// dies within a few iterations. Hold references on the chunks so GC can't reclaim.
	const chunkBytes = 1 << 30
	var chunks [][]byte
	for {
		b := make([]byte, chunkBytes)
		// Touch every page to force resident allocation rather than over-commit.
		for i := 0; i < len(b); i += int(pageSize()) {
			b[i] = 1
		}
		chunks = append(chunks, b)
		slog.Warn("fault-observer: allocated chunk", "total_gib", len(chunks))
	}
}

func pageSize() uintptr {
	if ps := os.Getpagesize(); ps > 0 {
		return uintptr(ps)
	}
	return 4096
}

func main() {
	modeStr := os.Getenv(faultModeEnv)
	mode, err := fault.ParseMode(modeStr)
	if err != nil {
		slog.Error("fault-observer: invalid HBASECOP_FAULT_MODE", "value", modeStr, "err", err)
		os.Exit(2)
	}
	slog.Info("fault-observer: starting", "mode", mode.String(), "goVersion", runtime.Version())

	obs := fault.New(mode, defaultActions{})
	if err := hbasecop.Run(obs); err != nil {
		slog.Error("fault-observer: fatal", "err", err)
		os.Exit(1)
	}
}
