// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// wire-golden regenerates the byte-level .bin files under
// test/golden/wire/v1/ from the human-edited fixtures.tsv. Cross-language
// tests in internal/wire and com.virogg.hbasecop.bridge.wire verify both
// directions (encode→bytes, bytes→decode) against this corpus.
//
// Usage:
//
//	go run ./cmd/wire-golden -out test/golden/wire/v1
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wiregolden"
)

func main() {
	outDir := flag.String("out", "test/golden/wire/v1", "directory holding fixtures.tsv and *.bin")
	flag.Parse()

	tsvPath := filepath.Join(*outDir, "fixtures.tsv")
	tsv, err := os.Open(tsvPath)
	if err != nil {
		log.Fatalf("open %s: %v", tsvPath, err)
	}
	fixtures, err := wiregolden.Parse(tsv)
	closeErr := tsv.Close()
	if err != nil {
		log.Fatalf("parse %s: %v", tsvPath, err)
	}
	if closeErr != nil {
		log.Fatalf("close %s: %v", tsvPath, closeErr)
	}

	for _, fx := range fixtures {
		var buf bytes.Buffer
		opts := []wire.EncoderOption{}
		if fx.ChunkSize > 0 {
			opts = append(opts, wire.WithChunkSize(fx.ChunkSize))
		}
		enc := wire.NewEncoder(&buf, opts...)
		msg := fx.Message
		if err := enc.Encode(&msg); err != nil {
			log.Fatalf("%s: encode: %v", fx.Name, err)
		}
		binPath := filepath.Join(*outDir, fx.Name+".bin")
		if err := os.WriteFile(binPath, buf.Bytes(), 0o644); err != nil {
			log.Fatalf("%s: write: %v", binPath, err)
		}
		fmt.Printf("  %-30s %5d bytes\n", fx.Name, buf.Len())
	}
	fmt.Printf("wrote %d fixtures to %s\n", len(fixtures), *outDir)
}
