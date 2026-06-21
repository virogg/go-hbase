// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log/slog"
	"os"

	"github.com/virogg/go-hbase/examples/ttl-validator/ttl"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

func main() {
	slog.Info("ttl-validator: starting")
	if err := hbasecop.Run(ttl.New()); err != nil {
		slog.Error("ttl-validator: fatal", "err", err)
		os.Exit(1)
	}
}
