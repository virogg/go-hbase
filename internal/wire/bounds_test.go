// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"testing"
)

// TestDecodeRejectsHugeChunkTotal pins the fix for the chunk_total OOM DoS:
// a single frame declaring chunk_total far above MaxChunks must be rejected
// with ErrTooManyChunks BEFORE the decoder allocates the chunk slice - never
// an out-of-memory from make([][]byte, chunk_total).
func TestDecodeRejectsHugeChunkTotal(t *testing.T) {
	for _, total := range []uint32{MaxChunks + 1, 1 << 20, 0xFFFFFFFF} {
		frame := craftHeader(TypeRequest, 1, 0, 7, 0, total, []byte("x"))
		_, err := NewDecoder(bytes.NewReader(frame)).Decode()
		if !errors.Is(err, ErrTooManyChunks) {
			t.Fatalf("chunk_total=%d: want ErrTooManyChunks, got %v", total, err)
		}
	}
}

// TestDecodeCapsPendingReassemblies pins the unbounded-pending-map fix: once
// MaxPendingReassemblies distinct multi-chunk req_ids are in flight (final
// chunk never sent), a further new req_id is rejected with ErrTooManyPending
// rather than growing the map without bound.
func TestDecodeCapsPendingReassemblies(t *testing.T) {
	var buf bytes.Buffer
	// Open MaxPendingReassemblies+1 distinct multi-chunk messages, each sending
	// only chunk 0 of 2 so none ever completes.
	for i := range uint64(MaxPendingReassemblies + 1) {
		buf.Write(craftHeader(TypeRequest, i+1, 0, 7, 0, 2, []byte("a")))
	}
	d := NewDecoder(&buf)
	var lastErr error
	for {
		msg, err := d.Decode()
		if err != nil {
			lastErr = err
			break
		}
		if msg == nil {
			break
		}
	}
	if !errors.Is(lastErr, ErrTooManyPending) {
		t.Fatalf("want ErrTooManyPending after exceeding cap, got %v", lastErr)
	}
}

// TestDecodeCapsPendingBytes pins the retained-BYTES bound: the entry-count
// cap alone still permits ~256 GiB of near-complete max-payload reassemblies
// (4096 × 67 MB), so a peer abandoning large reassemblies must hit
// ErrTooManyPendingBytes long before MaxPendingReassemblies entries exist.
func TestDecodeCapsPendingBytes(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), MaxPayloadBytes)
	// Each frame opens a distinct 2-chunk reassembly holding one max-size
	// chunk that never completes. Far fewer frames than the entry cap must
	// suffice - that is the point of the byte bound.
	framesToCap := MaxPendingBytes/MaxPayloadBytes + 2
	if framesToCap >= MaxPendingReassemblies {
		t.Fatalf("test premise broken: %d frames to byte cap >= %d entry cap", framesToCap, MaxPendingReassemblies)
	}
	var buf bytes.Buffer
	for i := range uint64(framesToCap) {
		buf.Write(craftHeader(TypeRequest, i+1, 0, 7, 0, 2, payload))
	}
	d := NewDecoder(&buf)
	var lastErr error
	for {
		msg, err := d.Decode()
		if err != nil {
			lastErr = err
			break
		}
		if msg == nil {
			break
		}
	}
	if !errors.Is(lastErr, ErrTooManyPendingBytes) {
		t.Fatalf("want ErrTooManyPendingBytes, got %v", lastErr)
	}
}
