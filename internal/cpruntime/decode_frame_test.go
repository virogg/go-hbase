// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime

import (
	"bytes"
	"testing"

	"github.com/virogg/go-hbase/internal/wire"
)

func encodeOne(t *testing.T, m *wire.Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := wire.NewEncoder(&buf).Encode(m); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return buf.Bytes()
}

func TestDecodeFrameSingleMessage(t *testing.T) {
	data := encodeOne(t, &wire.Message{Type: wire.TypeRequest, ReqID: 7, Payload: []byte("hi")})
	msg, err := decodeFrame(data)
	if err != nil {
		t.Fatalf("decodeFrame: %v", err)
	}
	if string(msg.Payload) != "hi" {
		t.Fatalf("payload = %q, want %q", msg.Payload, "hi")
	}
}

func TestDecodeFrameRejectsTrailingBytes(t *testing.T) {
	data := encodeOne(t, &wire.Message{Type: wire.TypeRequest, ReqID: 7, Payload: []byte("hi")})
	data = append(data, 0xFF) // junk trailing byte

	if _, err := decodeFrame(data); err == nil {
		t.Fatal("decodeFrame accepted a slot with trailing bytes; want error")
	}
}
