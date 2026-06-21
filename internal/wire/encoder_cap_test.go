// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeRejectsMessageExceedingMaxChunks(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, WithChunkSize(1))
	payload := make([]byte, MaxChunks+1)

	err := enc.Encode(&Message{Type: TypeRequest, ReqID: 1, Payload: payload})
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Encode err = %v, want ErrMessageTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("Encode wrote %d bytes on rejection, want 0 (must fail before emitting any frame)", buf.Len())
	}
}

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
