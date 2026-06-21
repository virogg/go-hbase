// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log/slog"
	"os"

	"github.com/virogg/go-hbase/examples/master-policy-observer/policy"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

const (
	blockedPrefixEnv     = "HBASECOP_POLICY_BLOCKED_PREFIX"
	defaultBlockedPrefix = "forbidden-"
)

func main() {
	prefix := os.Getenv(blockedPrefixEnv)
	if prefix == "" {
		prefix = defaultBlockedPrefix
	}
	slog.Info("master-policy-observer: starting", "blocked_prefix", prefix)

	obs := policy.New([]byte(prefix))
	if err := hbasecop.RunMaster(obs); err != nil {
		slog.Error("master-policy-observer: fatal", "err", err)
		os.Exit(1)
	}
}
