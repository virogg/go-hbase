// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

// preWALWriteFrame builds a representative inbound preWALWrite Request
// frame: a WALEdit carrying cellCount cells, the shape the WAL append
// hot path serialises on every edit.
func preWALWriteFrame(b *testing.B, cellCount int) *wire.Message {
	b.Helper()
	edit := &hookpb.WalEditProto{}
	for i := 0; i < cellCount; i++ {
		edit.Cell = append(edit.Cell, &hbasepb.Cell{
			Row:       []byte("row-0000000000"),
			Family:    []byte("cf"),
			Qualifier: []byte("q"),
			Value:     []byte("a representative cell value payload"),
			Timestamp: proto.Uint64(1_700_000_000_000),
		})
	}
	inner := &hookpb.PreWALWriteRequest{
		Ctx:     &hookpb.HookContext{RequestId: 1},
		LogKey:  &hookpb.WalKeyProto{LogSeqNum: 4242, WriteTime: 1_700_000_000_000},
		LogEdit: edit,
	}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		b.Fatalf("marshal PreWALWriteRequest: %v", err)
	}
	outerBytes, err := proto.Marshal(&wirepb.Request{HookCtx: innerBytes})
	if err != nil {
		b.Fatalf("marshal wirepb.Request: %v", err)
	}
	return &wire.Message{
		Type:    wire.TypeRequest,
		ReqID:   1,
		HookID:  uint8(HookIDPreWALWrite),
		Payload: outerBytes,
	}
}

// BenchmarkWALDispatchPreWALWrite measures the per-call cost the
// WALObserver bridge adds on the WAL append hot path: decode the inbound
// frame, invoke a no-op observer, encode the response frame. preWALWrite
// is latency-critical (T53), so this guards the in-process dispatch
// overhead independently of a live HBase WAL-throughput run (T82).
func BenchmarkWALDispatchPreWALWrite(b *testing.B) {
	d := newWALDispatcher(UnimplementedWALObserver{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()
	for _, cells := range []int{1, 16, 128} {
		frame := preWALWriteFrame(b, cells)
		b.Run(label(cells), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if resp := d.dispatch(ctx, frame); resp == nil || resp.Type != wire.TypeResponse {
					b.Fatal("dispatch did not return a Response frame")
				}
			}
		})
	}
}

func label(cells int) string {
	switch cells {
	case 1:
		return "cells=1"
	case 16:
		return "cells=16"
	default:
		return "cells=128"
	}
}
