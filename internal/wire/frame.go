// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import "errors"

// On-wire layout (big-endian throughout):
//
//   [u32 len][u8 type][u64 req_id][u32 region][u8 hook_id]
//   [u8 chunk_flags][u32 chunk_idx][u32 chunk_total][bytes pb]
//
// `len` counts the bytes after the len field itself (headerSize +
// len(payload)), so a frame occupies 4 + len bytes on the wire.
// headerSize is constant at 23.
//
// `hook_id` is 8-bit on the wire so the hot path routes without parsing the
// uint32 hook_id from the embedded protobuf header. The full uint32 still
// rides inside the PB payload's FrameHeader.

const (
	// headerSize is the fixed framing header following the u32 length prefix.
	headerSize = 23

	// MaxFrameSize caps total on-wire size of one chunk frame (length
	// prefix + header + payload). Default 64 KiB.
	MaxFrameSize = 64 * 1024

	// MaxPayloadBytes is the max payload in a single chunk. Larger
	// payloads must be split by the Encoder into multiple chunks sharing
	// the same req_id.
	MaxPayloadBytes = MaxFrameSize - 4 - headerSize

	// MaxChunks bounds chunk_total (read off the wire as a u32). Without it a
	// hostile or corrupt frame (chunk_total up to ~4.29e9) makes the decoder
	// pre-allocate a multi-GiB chunk slice and OOM. The Encoder also caps here
	// (ErrMessageTooLarge) so it never emits a frame stream the decoder would
	// reject. Defended in both Go and Java decoders, kept in lockstep; see
	// WireFormat.MAX_CHUNKS.
	//
	// 1024 is not the practical limit: the live transport is
	// one-message-per-slot (each ring Recv yields one slot carrying a whole
	// message's chunks), so a message must fit a single ring slot
	// (HBASECOP_RING_MAX_OBJECT_SIZE, e.g. 1 MiB). Channel.Send fails closed
	// above the slot size; cross-slot reassembly is unsupported.
	MaxChunks = 1024

	// MaxPendingReassemblies bounds concurrent in-progress multi-chunk
	// reassemblies a Decoder tracks, capping memory from abandoned
	// req_ids whose final chunk never arrives (e.g. across a Go-side
	// crash).
	MaxPendingReassemblies = 4096

	// MaxPendingBytes bounds TOTAL payload bytes retained across all
	// in-progress reassemblies. The entry-count cap alone is insufficient:
	// each abandoned near-complete reassembly may hold up to
	// (MaxChunks-1)*MaxPayloadBytes ~= 67 MB, so 4096 entries would permit
	// ~256 GiB of retained heap, OOMing long before the count cap fires. Sized
	// to one max message (64 MiB) plus headroom; in the current transport a
	// ring slot carries a complete encoded message, so cross-call pending
	// bytes are already anomalous.
	MaxPendingBytes = 96 << 20
)

// Type is the on-wire payload discriminator (the `type` byte). Numbering
// matches the reading order in wire.proto's Frame oneof; values are
// independent of the PB tag numbers, so future PB additions don't perturb
// the wire encoding.
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
	// ErrFrameTooLarge: declared length is outside the [headerSize,
	// MaxFrameSize-4] window. Returned by Decode for both oversized
	// frames and frames shorter than the framing header.
	ErrFrameTooLarge = errors.New("wire: frame length out of range")

	// ErrUnknownType: the type byte is 0 or above TypeLog.
	ErrUnknownType = errors.New("wire: unknown frame type")

	// ErrInvalidChunk: chunk_total/chunk_idx are inconsistent, the same
	// chunk_idx was seen twice, or the per-(req_id) header drifted
	// between chunks.
	ErrInvalidChunk = errors.New("wire: invalid chunk")

	// ErrControlMultiChunk: Heartbeat/Shutdown/Log arrived with
	// chunk_total > 1. Control frames must be single-chunk.
	ErrControlMultiChunk = errors.New("wire: control frame must be single-chunk")

	// ErrTooManyChunks: chunk_total exceeds MaxChunks. Guards against
	// unbounded reassembly-slice allocation from a peer-controlled chunk_total.
	ErrTooManyChunks = errors.New("wire: chunk_total exceeds MaxChunks")

	// ErrTooManyPending: the Decoder is already tracking
	// MaxPendingReassemblies distinct in-progress multi-chunk
	// reassemblies; a new req_id would exceed the cap.
	ErrTooManyPending = errors.New("wire: too many pending reassemblies")

	// ErrTooManyPendingBytes: storing this chunk would push total
	// payload bytes retained across all in-progress reassemblies over
	// MaxPendingBytes. Caps heap from abandoned near-complete
	// reassemblies, which the entry-count cap alone does not bound.
	ErrTooManyPendingBytes = errors.New("wire: too many pending reassembly bytes")

	// ErrMessageTooLarge: the payload would split into more than MaxChunks
	// chunks. Returned by Encode at the producer rather than emitting a frame
	// stream with chunk_total > MaxChunks that the matching Decoder would
	// reject with ErrTooManyChunks; turns self-undecodable output into a
	// clear, early producer-side error.
	ErrMessageTooLarge = errors.New("wire: message exceeds MaxChunks")
)
