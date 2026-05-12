// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire_test

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wiregolden"
)

// TestGoldenCorpus is the Go half of the T13 cross-language verification.
// For every fixture in test/golden/wire/v1/fixtures.tsv we check that:
//
//  1. Encoding the logical Message produces byte-identical bytes to <name>.bin.
//  2. Decoding <name>.bin yields a Message equal to the logical Message.
//
// The Java half (WireGoldenTest) makes the same assertions against the same
// .bin files, so the corpus proves Go encode ↔ Java decode bit-for-bit.
func TestGoldenCorpus(t *testing.T) {
	dir := goldenDir(t)
	tsv, err := os.Open(filepath.Join(dir, "fixtures.tsv"))
	if err != nil {
		t.Fatalf("open fixtures.tsv: %v", err)
	}
	t.Cleanup(func() { _ = tsv.Close() })
	fixtures, err := wiregolden.Parse(tsv)
	if err != nil {
		t.Fatalf("parse fixtures.tsv: %v", err)
	}
	if len(fixtures) == 0 {
		t.Fatalf("no fixtures parsed")
	}

	for _, fx := range fixtures {
		t.Run(fx.Name, func(t *testing.T) {
			want, err := os.ReadFile(filepath.Join(dir, fx.Name+".bin"))
			if err != nil {
				t.Fatalf("read %s.bin: %v", fx.Name, err)
			}

			// (1) Encode → bytes
			var got bytes.Buffer
			opts := []wire.EncoderOption{}
			if fx.ChunkSize > 0 {
				opts = append(opts, wire.WithChunkSize(fx.ChunkSize))
			}
			msg := fx.Message
			if err := wire.NewEncoder(&got, opts...).Encode(&msg); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("encode bytes mismatch:\n  got  (%d): %x\n  want (%d): %x",
					got.Len(), got.Bytes(), len(want), want)
			}

			// (2) bytes → Decode
			decoded, err := wire.NewDecoder(bytes.NewReader(want)).Decode()
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if decoded.Type != fx.Message.Type ||
				decoded.ReqID != fx.Message.ReqID ||
				decoded.RegionID != fx.Message.RegionID ||
				decoded.HookID != fx.Message.HookID ||
				!bytes.Equal(decoded.Payload, normalizePayload(fx.Message.Payload)) {
				t.Fatalf("decoded mismatch:\n  got  %+v\n  want %+v", decoded, fx.Message)
			}
		})
	}
}

// goldenDir resolves the repo-rooted test/golden/wire/v1 directory from
// this test's source location. Runtime.Caller(0) is used because go test
// runs with the package directory as cwd, not the repo root.
func goldenDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "test", "golden", "wire", "v1")
}

// normalizePayload accounts for the Decoder always returning a non-nil
// (possibly empty) byte slice even when the encoded payload was nil.
func normalizePayload(p []byte) []byte {
	if p == nil {
		return []byte{}
	}
	return p
}
