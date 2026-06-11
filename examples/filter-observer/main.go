// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command filter-observer is the Go-side runtime of the T43 read-path
// integration test. The shaded coproc-jar embeds this ELF at
// bin/linux-amd64/hbasecop-runtime so the Java bridge supervisor extracts
// it from classpath resources and exec's it on the RegionServer.
//
// The blocked prefix is selected by the HBASECOP_FILTER_BLOCKED_PREFIX
// environment variable (defaults to "block-"); the Java side maps the
// hbase.virogg.filter.blocked_prefix table-level config to that env var.
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
