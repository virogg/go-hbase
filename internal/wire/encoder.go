// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Encoder struct {
	w          io.Writer
	maxPayload int
}

type EncoderOption func(*Encoder)

func WithChunkSize(n int) EncoderOption {
	return func(e *Encoder) { e.maxPayload = n }
}

func NewEncoder(w io.Writer, opts ...EncoderOption) *Encoder {
	e := &Encoder{w: w, maxPayload: MaxPayloadBytes}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func (e *Encoder) Encode(m *Message) error {
	if !m.Type.Valid() {
		return fmt.Errorf("%w: %d", ErrUnknownType, m.Type)
	}

	p := m.Payload
	total := 1
	if len(p) > e.maxPayload {
		if m.Type.isControl() {
			return fmt.Errorf("%w: payload %d > %d", ErrControlMultiChunk, len(p), e.maxPayload)
		}
		total = (len(p) + e.maxPayload - 1) / e.maxPayload
		if total > MaxChunks {
			return fmt.Errorf("%w: payload %d would need %d chunks > MaxChunks %d",
				ErrMessageTooLarge, len(p), total, MaxChunks)
		}
	}

	for i := 0; i < total; i++ {
		start := i * e.maxPayload
		end := min(start+e.maxPayload, len(p))
		if err := e.writeChunk(m, uint32(i), uint32(total), p[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) writeChunk(m *Message, idx, total uint32, payload []byte) error {
	var hdr [4 + headerSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(headerSize+len(payload)))
	hdr[4] = byte(m.Type)
	binary.BigEndian.PutUint64(hdr[5:13], m.ReqID)
	binary.BigEndian.PutUint32(hdr[13:17], m.RegionID)
	hdr[17] = m.HookID
	hdr[18] = 0 // chunk_flags reserved for future use (compression, etc.)
	binary.BigEndian.PutUint32(hdr[19:23], idx)
	binary.BigEndian.PutUint32(hdr[23:27], total)

	if _, err := e.w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := e.w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}
