// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/virogg/go-hbase/internal/wire"
)

// TestRoundTripSingleChunk covers payloads that fit in one frame including
// the empty-payload edge case and the exact-MaxFrameSize boundary.
func TestRoundTripSingleChunk(t *testing.T) {
	cases := []struct {
		name string
		msg  wire.Message
	}{
		{
			name: "empty_heartbeat",
			msg:  wire.Message{Type: wire.TypeHeartbeat},
		},
		{
			name: "small_request",
			msg: wire.Message{
				Type: wire.TypeRequest, ReqID: 7, RegionID: 3, HookID: 12,
				Payload: []byte("hello"),
			},
		},
		{
			name: "exact_max_frame",
			msg: wire.Message{
				Type: wire.TypeRequest, ReqID: 1,
				Payload: bytes.Repeat([]byte{0xCC}, wire.MaxPayloadBytes),
			},
		},
		{
			name: "every_payload_type_response",
			msg:  wire.Message{Type: wire.TypeResponse, ReqID: 9, Payload: []byte{0x01, 0x02}},
		},
		{
			name: "every_payload_type_error",
			msg:  wire.Message{Type: wire.TypeError, ReqID: 10, Payload: []byte("boom")},
		},
		{
			name: "every_payload_type_shutdown",
			msg:  wire.Message{Type: wire.TypeShutdown, Payload: []byte("bye")},
		},
		{
			name: "every_payload_type_log",
			msg:  wire.Message{Type: wire.TypeLog, Payload: []byte("info")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := wire.NewEncoder(&buf).Encode(&tc.msg); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := wire.NewDecoder(&buf).Decode()
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			assertMessageEqual(t, got, &tc.msg)
		})
	}
}

// TestRoundTripMultiChunk drives the chunking path with payloads spanning
// 2 and 3 chunks at the default 64KB MaxFrameSize.
func TestRoundTripMultiChunk(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"two_chunks", makePayload(wire.MaxPayloadBytes + 1)},
		{"three_chunks", makePayload(wire.MaxPayloadBytes*2 + 5)},
		{"exact_two_chunks", makePayload(wire.MaxPayloadBytes * 2)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := wire.Message{
				Type: wire.TypeRequest, ReqID: 42, RegionID: 1, HookID: 5,
				Payload: tc.payload,
			}
			var buf bytes.Buffer
			if err := wire.NewEncoder(&buf).Encode(&msg); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := wire.NewDecoder(&buf).Decode()
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			assertMessageEqual(t, got, &msg)
		})
	}
}

// TestEncoderWithChunkSize lets the test driver use a tiny chunk window to
// exercise reassembly state without 64KB-per-test allocations.
func TestEncoderWithChunkSize(t *testing.T) {
	msg := wire.Message{
		Type: wire.TypeRequest, ReqID: 5, HookID: 1,
		Payload: []byte("abcdefghijklmnopqrstuvwxyz"), // 26 bytes
	}
	var buf bytes.Buffer
	enc := wire.NewEncoder(&buf, wire.WithChunkSize(10))
	if err := enc.Encode(&msg); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := wire.NewDecoder(&buf).Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	assertMessageEqual(t, got, &msg)
}

// TestDecodeOutOfOrderChunks exercises the reassembly buffer when chunks
// arrive in a non-monotonic chunk_idx sequence (idx=1 before idx=0). The
// decoder buffers until the final chunk arrives and concatenates in
// chunk_idx order regardless of arrival order.
func TestDecodeOutOfOrderChunks(t *testing.T) {
	// Build 3 chunks for req_id=11 in arrival order [2, 0, 1].
	type chunk struct {
		idx     uint32
		payload []byte
	}
	chunks := []chunk{
		{2, []byte("ccc")},
		{0, []byte("aaa")},
		{1, []byte("bbb")},
	}
	var buf bytes.Buffer
	for _, c := range chunks {
		writeRawChunk(t, &buf, rawChunk{
			Type: wire.TypeRequest, ReqID: 11, RegionID: 4, HookID: 2,
			ChunkIdx: c.idx, ChunkTotal: 3, Payload: c.payload,
		})
	}

	dec := wire.NewDecoder(&buf)
	msg, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	want := &wire.Message{
		Type: wire.TypeRequest, ReqID: 11, RegionID: 4, HookID: 2,
		Payload: []byte("aaabbbccc"),
	}
	assertMessageEqual(t, msg, want)
}

func TestDecodeRejectsOversizedLen(t *testing.T) {
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 1<<30)
	buf.Write(lenBuf[:])

	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, wire.ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
}

func TestDecodeRejectsTruncatedHeader(t *testing.T) {
	// Length field claims 23 bytes follow but we supply only 5.
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 23)
	buf.Write(lenBuf[:])
	buf.Write([]byte{1, 0, 0, 0, 0})

	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

func TestDecodeRejectsShorterThanHeader(t *testing.T) {
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 10) // header alone is 23 bytes
	buf.Write(lenBuf[:])

	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, wire.ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge for short frame, got %v", err)
	}
}

func TestDecodeRejectsUnknownType(t *testing.T) {
	var buf bytes.Buffer
	writeRawChunk(t, &buf, rawChunk{
		TypeByte: 99, ReqID: 1, ChunkTotal: 1, Payload: []byte("x"),
	})
	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, wire.ErrUnknownType) {
		t.Fatalf("want ErrUnknownType, got %v", err)
	}
}

// TestDecodeRejectsTypeOnePastCeiling pins the exact wire-type ceiling: byte 11
// is the value a future v3 frame type would occupy, so a contributor adding a
// type without raising Type.Valid() must trip this (and its Java twin) rather
// than silently decode. TypeRPCResponse=10 is the current top.
func TestDecodeRejectsTypeOnePastCeiling(t *testing.T) {
	var buf bytes.Buffer
	writeRawChunk(t, &buf, rawChunk{
		TypeByte: uint8(wire.TypeRPCResponse) + 1, ReqID: 1, ChunkTotal: 1, Payload: []byte("x"),
	})
	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, wire.ErrUnknownType) {
		t.Fatalf("want ErrUnknownType for type byte %d, got %v", uint8(wire.TypeRPCResponse)+1, err)
	}
}

func TestDecodeRejectsInvalidChunkBounds(t *testing.T) {
	cases := []struct {
		name string
		raw  rawChunk
	}{
		{"zero_total", rawChunk{Type: wire.TypeRequest, ReqID: 1, ChunkIdx: 0, ChunkTotal: 0}},
		{"idx_eq_total", rawChunk{Type: wire.TypeRequest, ReqID: 1, ChunkIdx: 2, ChunkTotal: 2}},
		{"idx_gt_total", rawChunk{Type: wire.TypeRequest, ReqID: 1, ChunkIdx: 5, ChunkTotal: 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writeRawChunk(t, &buf, tc.raw)
			_, err := wire.NewDecoder(&buf).Decode()
			if !errors.Is(err, wire.ErrInvalidChunk) {
				t.Fatalf("want ErrInvalidChunk, got %v", err)
			}
		})
	}
}

func TestDecodeRejectsDuplicateChunk(t *testing.T) {
	var buf bytes.Buffer
	writeRawChunk(t, &buf, rawChunk{
		Type: wire.TypeRequest, ReqID: 7, ChunkIdx: 0, ChunkTotal: 2, Payload: []byte("aa"),
	})
	writeRawChunk(t, &buf, rawChunk{
		Type: wire.TypeRequest, ReqID: 7, ChunkIdx: 0, ChunkTotal: 2, Payload: []byte("aa"),
	})
	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, wire.ErrInvalidChunk) {
		t.Fatalf("want ErrInvalidChunk on duplicate, got %v", err)
	}
}

func TestDecodeRejectsHeaderMismatchAcrossChunks(t *testing.T) {
	var buf bytes.Buffer
	writeRawChunk(t, &buf, rawChunk{
		Type: wire.TypeRequest, ReqID: 7, HookID: 1, ChunkIdx: 0, ChunkTotal: 2, Payload: []byte("aa"),
	})
	writeRawChunk(t, &buf, rawChunk{
		Type: wire.TypeRequest, ReqID: 7, HookID: 9, ChunkIdx: 1, ChunkTotal: 2, Payload: []byte("bb"),
	})
	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, wire.ErrInvalidChunk) {
		t.Fatalf("want ErrInvalidChunk on header drift, got %v", err)
	}
}

func TestDecodeRejectsControlMultiChunk(t *testing.T) {
	var buf bytes.Buffer
	writeRawChunk(t, &buf, rawChunk{
		Type: wire.TypeHeartbeat, ChunkIdx: 0, ChunkTotal: 2, Payload: []byte("a"),
	})
	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, wire.ErrControlMultiChunk) {
		t.Fatalf("want ErrControlMultiChunk, got %v", err)
	}
}

func TestDecodeMultiChunkRequiresReqID(t *testing.T) {
	var buf bytes.Buffer
	writeRawChunk(t, &buf, rawChunk{
		Type: wire.TypeRequest, ReqID: 0, ChunkIdx: 0, ChunkTotal: 2, Payload: []byte("a"),
	})
	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, wire.ErrInvalidChunk) {
		t.Fatalf("want ErrInvalidChunk for req_id=0 multi-chunk, got %v", err)
	}
}

func TestDecodeCleanEOF(t *testing.T) {
	var buf bytes.Buffer
	_, err := wire.NewDecoder(&buf).Decode()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF on empty stream, got %v", err)
	}
}

func TestEncodeRejectsUnknownType(t *testing.T) {
	var buf bytes.Buffer
	err := wire.NewEncoder(&buf).Encode(&wire.Message{Type: wire.TypeUnknown})
	if !errors.Is(err, wire.ErrUnknownType) {
		t.Fatalf("want ErrUnknownType, got %v", err)
	}
}

func TestEncodeMultipleMessagesStream(t *testing.T) {
	var buf bytes.Buffer
	enc := wire.NewEncoder(&buf)
	msgs := []wire.Message{
		{Type: wire.TypeHeartbeat},
		{Type: wire.TypeRequest, ReqID: 1, Payload: []byte("first")},
		{Type: wire.TypeRequest, ReqID: 2, Payload: makePayload(wire.MaxPayloadBytes + 100)},
		{Type: wire.TypeResponse, ReqID: 1, Payload: []byte("ok")},
	}
	for i := range msgs {
		if err := enc.Encode(&msgs[i]); err != nil {
			t.Fatalf("Encode[%d]: %v", i, err)
		}
	}

	dec := wire.NewDecoder(&buf)
	for i, want := range msgs {
		got, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode[%d]: %v", i, err)
		}
		assertMessageEqual(t, got, &want)
	}
}

// --- helpers ---------------------------------------------------------------

func makePayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i % 251)
	}
	return p
}

func assertMessageEqual(t *testing.T, got, want *wire.Message) {
	t.Helper()
	if got.Type != want.Type {
		t.Fatalf("Type: got %d want %d", got.Type, want.Type)
	}
	if got.ReqID != want.ReqID {
		t.Fatalf("ReqID: got %d want %d", got.ReqID, want.ReqID)
	}
	if got.RegionID != want.RegionID {
		t.Fatalf("RegionID: got %d want %d", got.RegionID, want.RegionID)
	}
	if got.HookID != want.HookID {
		t.Fatalf("HookID: got %d want %d", got.HookID, want.HookID)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("Payload: got %d bytes, want %d bytes", len(got.Payload), len(want.Payload))
	}
}

// rawChunk lets tests hand-craft individual chunks (out-of-order, duplicate,
// invalid type byte). Either Type or TypeByte is honored; TypeByte wins so
// tests can inject illegal type values.
type rawChunk struct {
	Type       wire.Type
	TypeByte   uint8
	ReqID      uint64
	RegionID   uint32
	HookID     uint8
	Flags      uint8
	ChunkIdx   uint32
	ChunkTotal uint32
	Payload    []byte
}

func writeRawChunk(t *testing.T, w *bytes.Buffer, c rawChunk) {
	t.Helper()
	const headerSize = 23
	var hdr [4 + headerSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(headerSize+len(c.Payload)))
	if c.TypeByte != 0 {
		hdr[4] = c.TypeByte
	} else {
		hdr[4] = byte(c.Type)
	}
	binary.BigEndian.PutUint64(hdr[5:13], c.ReqID)
	binary.BigEndian.PutUint32(hdr[13:17], c.RegionID)
	hdr[17] = c.HookID
	hdr[18] = c.Flags
	binary.BigEndian.PutUint32(hdr[19:23], c.ChunkIdx)
	binary.BigEndian.PutUint32(hdr[23:27], c.ChunkTotal)
	w.Write(hdr[:])
	w.Write(c.Payload)
}
