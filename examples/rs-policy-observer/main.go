// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command rs-policy-observer is the Go-side runtime of the T52
// RegionServerObserver integration test. The shaded coproc-jar embeds
// this ELF at bin/linux-amd64/hbasecop-runtime so the Java bridge
// supervisor extracts it from classpath resources and exec's it on the
// RegionServer.
//
// When the HBASECOP_RS_POLICY_VETO_WAL_ROLL environment variable is set
// to "true" the observer rejects every preRollWALWriterRequest hook; the
// Java side maps the hbasecop.policy.veto_wal_roll coprocessor config to
// that env var.
package main

import (
	"log/slog"
	"os"

	"github.com/virogg/go-hbase/examples/rs-policy-observer/policy"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

const vetoWalRollEnv = "HBASECOP_RS_POLICY_VETO_WAL_ROLL"

func main() {
	vetoWalRoll := os.Getenv(vetoWalRollEnv) == "true"
	slog.Info("rs-policy-observer: starting", "veto_wal_roll", vetoWalRoll)

	obs := policy.New(vetoWalRoll)
	if err := hbasecop.RunRegionServer(obs); err != nil {
		slog.Error("rs-policy-observer: fatal", "err", err)
		os.Exit(1)
	}
}
