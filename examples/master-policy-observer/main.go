// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command master-policy-observer is the Go-side runtime of the T51
// MasterObserver integration test. The shaded coproc-jar embeds this
// ELF at bin/linux-amd64/hbasecop-runtime so the Java bridge supervisor
// extracts it from classpath resources and exec's it on the HMaster.
//
// The blocked prefix is selected by the HBASECOP_POLICY_BLOCKED_PREFIX
// environment variable (defaults to "forbidden-"); the Java side maps
// the hbasecop.policy.blocked_prefix coprocessor config to that env var.
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
