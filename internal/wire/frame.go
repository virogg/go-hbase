// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import "errors"

const (
	headerSize = 23

	MaxFrameSize = 64 * 1024

	MaxPayloadBytes = MaxFrameSize - 4 - headerSize

	MaxChunks = 1024

	MaxPendingReassemblies = 4096

	MaxPendingBytes = 96 << 20
)

type Type uint8

const (
	TypeUnknown   Type = 0
	TypeRequest   Type = 1
	TypeResponse  Type = 2
	TypeHeartbeat Type = 3
	TypeError     Type = 4
	TypeShutdown  Type = 5
	TypeLog       Type = 6

	TypeEndpointInvoke Type = 7
	TypeEndpointResult Type = 8
	TypeRPCRequest     Type = 9
	TypeRPCResponse    Type = 10
)

func (t Type) Valid() bool { return t >= TypeRequest && t <= TypeRPCResponse }

func (t Type) isControl() bool {
	return t == TypeHeartbeat || t == TypeShutdown || t == TypeLog
}

type Message struct {
	Type     Type
	ReqID    uint64
	RegionID uint32
	HookID   uint8
	Payload  []byte
}

var (
	ErrFrameTooLarge       = errors.New("wire: frame length out of range")
	ErrUnknownType         = errors.New("wire: unknown frame type")
	ErrInvalidChunk        = errors.New("wire: invalid chunk")
	ErrControlMultiChunk   = errors.New("wire: control frame must be single-chunk")
	ErrTooManyChunks       = errors.New("wire: chunk_total exceeds MaxChunks")
	ErrTooManyPending      = errors.New("wire: too many pending reassemblies")
	ErrTooManyPendingBytes = errors.New("wire: too many pending reassembly bytes")
	ErrMessageTooLarge     = errors.New("wire: message exceeds MaxChunks")
)
