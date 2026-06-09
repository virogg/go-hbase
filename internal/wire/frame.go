// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import "errors"

// On-wire layout (big-endian throughout):
//
//   [u32 len][u8 type][u64 req_id][u32 region][u8 hook_id]
//   [u8 chunk_flags][u32 chunk_idx][u32 chunk_total][bytes pb]
//
// `len` is the number of bytes that follow the len field itself
// (i.e. headerSize + len(payload)); a single frame therefore occupies
// 4 + len bytes on the wire. headerSize is constant at 23.
//
// `hook_id` is intentionally 8-bit on the wire so that the hot path
// can route without parsing the larger uint32 hook_id from the
// embedded protobuf header. The full uint32 is still carried inside
// the PB payload's FrameHeader for completeness.

const (
	// headerSize is the byte count of the fixed framing header that
	// follows the u32 length prefix.
	headerSize = 23

	// MaxFrameSize caps the total on-wire size of one chunk frame
	// (length prefix + header + payload). Default is 64 KiB.
	MaxFrameSize = 64 * 1024

	// MaxPayloadBytes is the maximum payload bytes carryable in a
	// single chunk. Payloads larger than this must be split by the
	// Encoder into multiple chunks sharing the same req_id.
	MaxPayloadBytes = MaxFrameSize - 4 - headerSize

	// MaxChunks bounds chunk_total: the largest number of chunks a
	// single message may legitimately be split into. chunk_total is
	// read straight off the wire as a u32, so without this cap a
	// hostile or corrupt frame (chunk_total up to ~4.29e9) would make
	// the decoder pre-allocate a multi-GiB chunk slice and OOM the
	// process. 1024 chunks × MaxPayloadBytes ≈ 64 MiB max message,
	// far above any real hook payload. Defended in both the Go and
	// Java decoders (kept in lockstep; see WireFormat.MAX_CHUNKS).
	MaxChunks = 1024

	// MaxPendingReassemblies bounds the number of concurrent
	// in-progress multi-chunk reassemblies a Decoder will track,
	// capping memory from abandoned req_ids whose final chunk never
	// arrives (e.g. across a Go-side crash).
	MaxPendingReassemblies = 4096
)

// Type is the on-wire payload discriminator (the `type` byte). Field
// numbering matches the human reading order in wire.proto's Frame
// oneof; the values themselves are independent of the PB tag numbers
// so that future PB additions need not perturb the wire encoding.
type Type uint8

// Payload-type discriminators carried in the on-wire `type` byte.
const (
	TypeUnknown   Type = 0 // sentinel; never written by the Encoder
	TypeRequest   Type = 1
	TypeResponse  Type = 2
	TypeHeartbeat Type = 3
	TypeError     Type = 4
	TypeShutdown  Type = 5
	TypeLog       Type = 6
)

// Valid reports whether t is a known payload type.
func (t Type) Valid() bool { return t >= TypeRequest && t <= TypeLog }

// isControl reports whether a frame type is a stateless control frame
// that must be single-chunk (Heartbeat/Shutdown/Log).
func (t Type) isControl() bool {
	return t == TypeHeartbeat || t == TypeShutdown || t == TypeLog
}

// Message is a reassembled wire-level frame: routing metadata plus the
// raw PB-encoded payload corresponding to Type.
type Message struct {
	Type     Type
	ReqID    uint64
	RegionID uint32
	HookID   uint8
	Payload  []byte
}

// Sentinel errors. Use errors.Is to discriminate.
var (
	// ErrFrameTooLarge — declared length is outside the [headerSize,
	// MaxFrameSize-4] window. Returned by Decode for both oversized
	// frames and frames shorter than the framing header.
	ErrFrameTooLarge = errors.New("wire: frame length out of range")

	// ErrUnknownType — the type byte is 0 or above TypeLog.
	ErrUnknownType = errors.New("wire: unknown frame type")

	// ErrInvalidChunk — chunk_total/chunk_idx are inconsistent, the
	// same chunk_idx was seen twice, or the per-(req_id) header
	// drifted between chunks.
	ErrInvalidChunk = errors.New("wire: invalid chunk")

	// ErrControlMultiChunk — Heartbeat/Shutdown/Log arrived with
	// chunk_total > 1. Control frames are required to be single-chunk.
	ErrControlMultiChunk = errors.New("wire: control frame must be single-chunk")

	// ErrTooManyChunks — chunk_total exceeds MaxChunks. Guards against
	// unbounded reassembly-slice allocation from a peer-controlled
	// chunk_total.
	ErrTooManyChunks = errors.New("wire: chunk_total exceeds MaxChunks")

	// ErrTooManyPending — the Decoder is already tracking
	// MaxPendingReassemblies distinct in-progress multi-chunk
	// reassemblies; a new req_id would exceed the cap.
	ErrTooManyPending = errors.New("wire: too many pending reassemblies")
)
