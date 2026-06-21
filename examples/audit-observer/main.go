// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log/slog"
	"os"

	"github.com/virogg/go-hbase/examples/audit-observer/audit"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

func main() {
	slog.Info("audit-observer: starting")
	if err := hbasecop.Run(audit.New()); err != nil {
		slog.Error("audit-observer: fatal", "err", err)
		os.Exit(1)
	}
}
