// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

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
	_ = os.WriteFile("/proc/self/oom_score_adj", []byte("1000\n"), 0o644)

	const chunkBytes = 1 << 28   // 256 MiB
	const hardCapBytes = 3 << 30 // 3 GiB backstop
	var chunks [][]byte
	for {
		b := make([]byte, chunkBytes)
		for i := 0; i < len(b); i += int(pageSize()) {
			b[i] = 1
		}
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
