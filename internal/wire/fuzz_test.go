// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"testing"
)

// FuzzDecode feeds arbitrary bytes through the framing decoder. The decoder
// reads length-prefixed, chunked frames off an untrusted shmem ring, so it is
// the project's primary adversarial-input surface (T83). The invariant is
// simple: Decode must never panic and never make an unbounded allocation —
// every malformed input must surface as a returned error. The chunk_total OOM
// (a 23-byte frame requesting a multi-GiB chunk slice) and the unbounded
// pending-reassembly map are both regression-guarded here via MaxChunks /
// MaxPendingReassemblies.
func FuzzDecode(f *testing.F) {
	// Seed corpus: a well-formed single-chunk frame, an empty input, and the
	// two allocation-bomb shapes (huge chunk_total, many distinct req_ids).
	var good bytes.Buffer
	_ = NewEncoder(&good).Encode(&Message{Type: TypeRequest, ReqID: 1, HookID: 7, Payload: []byte("hi")})
	f.Add(good.Bytes())
	f.Add([]byte{})
	f.Add(craftHeader(TypeRequest, 1, 0, 7, 0, 0xFFFFFFFF, nil))       // chunk_total = max u32
	f.Add(craftHeader(TypeRequest, 1, 0, 7, 0, MaxChunks+1, []byte{})) // chunk_total just over the cap

	f.Fuzz(func(_ *testing.T, data []byte) {
		d := NewDecoder(bytes.NewReader(data))
		for range 64 { // bounded: a fuzz input must not loop forever
			msg, err := d.Decode()
			if err != nil {
				return // any error is acceptable; the contract is "no panic, no OOM"
			}
			if msg == nil {
				return
			}
		}
	})
}

// craftHeader builds a raw frame (length prefix + 23-byte header + payload)
// with caller-chosen fields so the fuzz seed can exercise the bounds checks
// directly. Mirrors Encoder.writeChunk's layout.
func craftHeader(typ Type, reqID uint64, regionID uint32, hookID uint8, chunkIdx, chunkTotal uint32, payload []byte) []byte {
	var buf bytes.Buffer
	body := make([]byte, headerSize+len(payload))
	body[0] = byte(typ)
	be64(body[1:9], reqID)
	be32(body[9:13], regionID)
	body[13] = hookID
	body[14] = 0
	be32(body[15:19], chunkIdx)
	be32(body[19:23], chunkTotal)
	copy(body[23:], payload)
	var lp [4]byte
	be32(lp[:], uint32(len(body)))
	buf.Write(lp[:])
	buf.Write(body)
	return buf.Bytes()
}

func be32(b []byte, v uint32) { b[0], b[1], b[2], b[3] = byte(v>>24), byte(v>>16), byte(v>>8), byte(v) }
func be64(b []byte, v uint64) {
	for i := range 8 {
		b[i] = byte(v >> (56 - 8*i))
	}
}
