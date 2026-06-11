// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Decoder reads chunk frames from an io.Reader and reassembles
// multi-chunk Messages keyed by req_id.
//
// Decoder is not safe for concurrent use. Memory for partial
// reassemblies persists until either the matching final chunk arrives
// or the underlying reader is closed; supervisor-level inflight
// cleanup is T35.
type Decoder struct {
	r        io.Reader
	pending  map[uint64]*reassembly
	maxFrame int
	// pendingBytes is the payload total retained across all entries in
	// pending, bounded by MaxPendingBytes (the entry-count cap alone
	// would still permit hundreds of GiB of near-complete reassemblies).
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

// NewDecoder returns a Decoder reading from r with the default
// MaxFrameSize cap.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:        r,
		pending:  make(map[uint64]*reassembly),
		maxFrame: MaxFrameSize,
	}
}

// Decode returns the next fully reassembled Message. Single-chunk
// frames return immediately; multi-chunk frames are buffered until
// every chunk_idx in [0, chunk_total) has arrived (in any order).
//
// io.EOF means the stream ended cleanly between frames;
// io.ErrUnexpectedEOF means the stream ended mid-frame.
func (d *Decoder) Decode() (*Message, error) {
	for {
		msg, err := d.readChunk()
		if err != nil {
			return nil, err
		}
		if msg != nil {
			return msg, nil
		}
		// chunk accepted but reassembly not yet complete; loop.
	}
}

func (d *Decoder) readChunk() (*Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(d.r, lenBuf[:]); err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenBuf[:])

	if l < headerSize || int(l) > d.maxFrame-4 {
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
	// Bound chunk_total BEFORE any allocation keyed off it: chunkTotal is
	// peer-controlled (raw u32), so an unbounded value would make the
	// make([][]byte, chunkTotal) below request gigabytes and OOM us.
	if chunkTotal > MaxChunks {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyChunks, chunkTotal, MaxChunks)
	}

	if chunkTotal == 1 {
		// Single-chunk fast path; copy out of buf so callers may
		// retain Payload independently of the read buffer.
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
		// Cap concurrent in-progress reassemblies so abandoned req_ids
		// (final chunk never arrives) cannot grow the map without bound.
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
