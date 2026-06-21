// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

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
