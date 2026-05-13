// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command counter-observer is the Go-side runtime of the counter-observer
// coproc demo (T25). Built into examples/counter-observer/src/main/resources/
// bin/linux-amd64/hbasecop-runtime so the Maven shade picks it up; the Java
// bridge supervisor (GoProcess) extracts it from the deployed coproc-jar and
// exec's it on the RegionServer.
//
// The observer keeps an atomic counter of PrePut invocations and emits it to
// the JSON logger that the bridge tails into the RegionServer log.
package main

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type counterObserver struct {
	puts atomic.Uint64
}

func (c *counterObserver) PrePut(
	_ context.Context,
	env hbasecop.ObserverEnv,
	_ *hbasecop.MutationProto,
) (hbasecop.HookResult, error) {
	n := c.puts.Add(1)
	slog.Info("counter-observer: prePut",
		"table", env.TableName,
		"region", env.RegionName,
		"puts", n,
	)
	return hbasecop.HookResult{}, nil
}

func (c *counterObserver) PostPut(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hbasecop.MutationProto,
) error {
	return nil
}

func main() {
	if err := hbasecop.Run(&counterObserver{}); err != nil {
		slog.Error("counter-observer: fatal", "err", err)
		os.Exit(1)
	}
}
