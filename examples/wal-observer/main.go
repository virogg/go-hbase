// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

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
