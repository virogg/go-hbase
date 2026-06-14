// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"testing"
)

// TestEncodeRejectsMessageExceedingMaxChunks pins the producer-side cap:
// a payload that would split into more than MaxChunks chunks is rejected
// with ErrMessageTooLarge and NOTHING is written, rather than emitting a
// frame stream carrying chunk_total > MaxChunks that the matching Decoder
// is required to reject with ErrTooManyChunks (self-undecodable output).
func TestEncodeRejectsMessageExceedingMaxChunks(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, WithChunkSize(1)) // 1 byte per chunk
	payload := make([]byte, MaxChunks+1)      // needs MaxChunks+1 chunks

	err := enc.Encode(&Message{Type: TypeRequest, ReqID: 1, Payload: payload})
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Encode err = %v, want ErrMessageTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("Encode wrote %d bytes on rejection, want 0 (must fail before emitting any frame)", buf.Len())
	}
}

// TestEncodeAllowsExactlyMaxChunks confirms the cap is inclusive: a
// payload that splits into exactly MaxChunks chunks encodes and decodes
// cleanly (boundary just below the reject).
func TestEncodeAllowsExactlyMaxChunks(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, WithChunkSize(1))
	payload := make([]byte, MaxChunks)

	if err := enc.Encode(&Message{Type: TypeRequest, ReqID: 1, Payload: payload}); err != nil {
		t.Fatalf("Encode at exactly MaxChunks: %v", err)
	}
	msg, err := NewDecoder(&buf).Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(msg.Payload) != MaxChunks {
		t.Fatalf("round-trip payload len = %d, want %d", len(msg.Payload), MaxChunks)
	}
}
