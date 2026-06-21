// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Decoder struct {
	r            io.Reader
	pending      map[uint64]*reassembly
	maxFrame     int
	pendingBytes int
}

type reassembly struct {
	typ      Type
	regionID uint32
	hookID   uint8
	total    uint32
	chunks   [][]byte // index = chunk_idx; nil until that chunk arrives
	received uint32
	size     int
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:        r,
		pending:  make(map[uint64]*reassembly),
		maxFrame: MaxFrameSize,
	}
}

func (d *Decoder) Decode() (*Message, error) {
	for {
		msg, err := d.readChunk()
		if err != nil {
			return nil, err
		}
		if msg != nil {
			return msg, nil
		}
	}
}

func (d *Decoder) readChunk() (*Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(d.r, lenBuf[:]); err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenBuf[:])

	if l < headerSize || uint64(l) > uint64(d.maxFrame)-4 {
		return nil, fmt.Errorf("%w: %d", ErrFrameTooLarge, l)
	}

	buf := make([]byte, l)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return nil, err
	}

	typ := Type(buf[0])
	if !typ.Valid() {
		return nil, fmt.Errorf("%w: %d", ErrUnknownType, typ)
	}
	reqID := binary.BigEndian.Uint64(buf[1:9])
	regionID := binary.BigEndian.Uint32(buf[9:13])
	hookID := buf[13]
	// buf[14] is chunk_flags, reserved.
	chunkIdx := binary.BigEndian.Uint32(buf[15:19])
	chunkTotal := binary.BigEndian.Uint32(buf[19:23])
	payload := buf[23:]

	if chunkTotal == 0 || chunkIdx >= chunkTotal {
		return nil, fmt.Errorf("%w: idx=%d total=%d", ErrInvalidChunk, chunkIdx, chunkTotal)
	}
	if chunkTotal > MaxChunks {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyChunks, chunkTotal, MaxChunks)
	}

	if chunkTotal == 1 {
		out := append([]byte(nil), payload...)
		return &Message{
			Type: typ, ReqID: reqID, RegionID: regionID, HookID: hookID,
			Payload: out,
		}, nil
	}

	if typ.isControl() {
		return nil, fmt.Errorf("%w: type=%d", ErrControlMultiChunk, typ)
	}
	if reqID == 0 {
		return nil, fmt.Errorf("%w: multi-chunk requires non-zero req_id", ErrInvalidChunk)
	}

	re, ok := d.pending[reqID]
	if !ok {
		if len(d.pending) >= MaxPendingReassemblies {
			return nil, fmt.Errorf("%w: %d", ErrTooManyPending, len(d.pending))
		}
		re = &reassembly{
			typ: typ, regionID: regionID, hookID: hookID, total: chunkTotal,
			chunks: make([][]byte, chunkTotal),
		}
		d.pending[reqID] = re
	} else if re.typ != typ || re.regionID != regionID || re.hookID != hookID || re.total != chunkTotal {
		return nil, fmt.Errorf("%w: header drift on req_id %d", ErrInvalidChunk, reqID)
	}
	if re.chunks[chunkIdx] != nil {
		return nil, fmt.Errorf("%w: duplicate chunk_idx %d for req_id %d", ErrInvalidChunk, chunkIdx, reqID)
	}

	if d.pendingBytes+len(payload) > MaxPendingBytes {
		return nil, fmt.Errorf("%w: %d + %d > %d", ErrTooManyPendingBytes, d.pendingBytes, len(payload), MaxPendingBytes)
	}
	chunkCopy := append([]byte(nil), payload...)
	re.chunks[chunkIdx] = chunkCopy
	re.received++
	re.size += len(chunkCopy)
	d.pendingBytes += len(chunkCopy)

	if re.received < re.total {
		return nil, nil
	}

	out := make([]byte, 0, re.size)
	for _, c := range re.chunks {
		out = append(out, c...)
	}
	delete(d.pending, reqID)
	d.pendingBytes -= re.size
	return &Message{
		Type: re.typ, ReqID: reqID, RegionID: re.regionID, HookID: re.hookID,
		Payload: out,
	}, nil
}
