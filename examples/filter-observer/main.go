// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log/slog"
	"os"

	"github.com/virogg/go-hbase/examples/filter-observer/filter"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

const (
	blockedPrefixEnv     = "HBASECOP_FILTER_BLOCKED_PREFIX"
	defaultBlockedPrefix = "block-"
)

func main() {
	prefix := os.Getenv(blockedPrefixEnv)
	if prefix == "" {
		prefix = defaultBlockedPrefix
	}
	slog.Info("filter-observer: starting", "blocked_prefix", prefix)

	obs := filter.New([]byte(prefix))
	if err := hbasecop.Run(obs); err != nil {
		slog.Error("filter-observer: fatal", "err", err)
		os.Exit(1)
	}
}
