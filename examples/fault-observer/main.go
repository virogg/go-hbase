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
	// Make THIS process the kernel OOM-killer's preferred victim so memory pressure
	// reaps the Go child — never the co-resident RegionServer JVM. The dev cluster
	// runs no container mem_limit, so on a memory-constrained host (CI runners) the
	// kernel would otherwise be free to pick the RS (1 GiB heap, large RSS) instead,
	// killing the cluster and hanging the whole fault matrix (the T36 flake).
	// Best-effort: unsupported on non-Linux or without permission.
	_ = os.WriteFile("/proc/self/oom_score_adj", []byte("1000\n"), 0o644)

	// Allocate in chunks, touching every page (resident, not over-commit), holding
	// references so GC can't reclaim. A hard cap self-exits like an OOM kill (137) if
	// the kernel has not reaped us first, so the fault stays bounded and deterministic
	// regardless of host RAM while still exercising the supervisor's death-recovery.
	const chunkBytes = 1 << 28   // 256 MiB
	const hardCapBytes = 3 << 30 // 3 GiB backstop
	var chunks [][]byte
	for {
		b := make([]byte, chunkBytes)
		for i := 0; i < len(b); i += int(pageSize()) {
			b[i] = 1
		}
		// Retain every chunk so GC can't reclaim it (read via len below, which also
		// keeps the slice live for staticcheck).
		chunks = append(chunks, b)
		allocated := int64(len(chunks)) * chunkBytes
		slog.Warn("fault-observer: allocated chunk", "total_mib", allocated>>20)
		if allocated >= hardCapBytes {
			slog.Warn("fault-observer: hard cap reached without reap, exiting 137")
			os.Exit(137)
		}
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
