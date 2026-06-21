// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

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
