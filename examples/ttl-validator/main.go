// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command ttl-validator is the Go-side runtime of the T73 pre-hook
// validation example. The coproc-jar embeds this ELF at
// bin/linux-amd64/hbasecop-runtime so the Java bridge supervisor extracts
// it from classpath resources and exec's it on the RegionServer.
//
// PrePut rejects any Put whose cell values lack the "ttl=<seconds>;"
// envelope; under the default strict pre-hook policy the client receives
// an IOException and the write is aborted. See the ttl package doc.
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
