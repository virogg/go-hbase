// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command audit-observer is the Go-side runtime of the T72 post-hook audit
// example. The coproc-jar embeds this ELF at bin/linux-amd64/hbasecop-runtime
// so the Java bridge supervisor extracts it from classpath resources and
// exec's it on the RegionServer.
//
// Every completed Put/Delete produces one structured JSON audit record on
// stderr (forwarded into the RegionServer log by the bridge). Records carry
// a SHA-256 row digest rather than the raw key - see the audit package doc.
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
