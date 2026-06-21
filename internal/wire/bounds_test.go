// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestDecodeRejectsHugeChunkTotal(t *testing.T) {
	for _, total := range []uint32{MaxChunks + 1, 1 << 20, 0xFFFFFFFF} {
		frame := craftHeader(TypeRequest, 1, 0, 7, 0, total, []byte("x"))
		_, err := NewDecoder(bytes.NewReader(frame)).Decode()
		if !errors.Is(err, ErrTooManyChunks) {
			t.Fatalf("chunk_total=%d: want ErrTooManyChunks, got %v", total, err)
		}
	}
}

func TestDecodeCapsPendingReassemblies(t *testing.T) {
	var buf bytes.Buffer
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

func TestDecodeCapsPendingBytes(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), MaxPayloadBytes)
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
