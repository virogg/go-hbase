// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wirepb_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		frame *wirepb.Frame
	}{
		{
			name: "request_single_chunk",
			frame: &wirepb.Frame{
				Header: &wirepb.FrameHeader{
					ReqId:      0x0123456789ABCDEF,
					RegionId:   0,
					HookId:     1,
					ChunkIdx:   0,
					ChunkTotal: 1,
				},
				Payload: &wirepb.Frame_Request{
					Request: &wirepb.Request{HookCtx: []byte("opaque-hook-context")},
				},
			},
		},
		{
			name: "response_empty_payload",
			frame: &wirepb.Frame{
				Header: &wirepb.FrameHeader{
					ReqId: 42, HookId: 1, ChunkTotal: 1,
				},
				Payload: &wirepb.Frame_Response{
					Response: &wirepb.Response{},
				},
			},
		},
		{
			name: "heartbeat_control_frame",
			frame: &wirepb.Frame{
				Header: &wirepb.FrameHeader{ChunkTotal: 1},
				Payload: &wirepb.Frame_Heartbeat{
					Heartbeat: &wirepb.Heartbeat{MonotonicNanos: 1_700_000_000_000_000_000},
				},
			},
		},
		{
			name: "error_strict_path",
			frame: &wirepb.Frame{
				Header: &wirepb.FrameHeader{ReqId: 7, HookId: 2, ChunkTotal: 1},
				Payload: &wirepb.Frame_Error{
					Error: &wirepb.Error{Code: 42, Message: "user observer panicked"},
				},
			},
		},
		{
			name: "shutdown",
			frame: &wirepb.Frame{
				Header: &wirepb.FrameHeader{ChunkTotal: 1},
				Payload: &wirepb.Frame_Shutdown{
					Shutdown: &wirepb.Shutdown{Reason: "rs-stop"},
				},
			},
		},
		{
			name: "log_info",
			frame: &wirepb.Frame{
				Header: &wirepb.FrameHeader{ChunkTotal: 1},
				Payload: &wirepb.Frame_Log{
					Log: &wirepb.Log{Level: wirepb.Log_INFO, Message: "go runtime started"},
				},
			},
		},
		{
			name: "multi_chunk_request",
			frame: &wirepb.Frame{
				Header: &wirepb.FrameHeader{
					ReqId: 99, RegionId: 7, HookId: 3,
					ChunkIdx: 2, ChunkTotal: 5,
				},
				Payload: &wirepb.Frame_Request{
					Request: &wirepb.Request{HookCtx: []byte("chunk-2-of-5")},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := proto.Marshal(tc.frame)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			got := &wirepb.Frame{}
			if err := proto.Unmarshal(wire, got); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !proto.Equal(tc.frame, got) {
				t.Fatalf("round-trip mismatch:\n  want: %v\n  got:  %v", tc.frame, got)
			}
		})
	}
}

func TestEmptyFrameRoundTrip(t *testing.T) {
	empty := &wirepb.Frame{Header: &wirepb.FrameHeader{ChunkTotal: 1}}
	wire, err := proto.Marshal(empty)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := &wirepb.Frame{}
	if err := proto.Unmarshal(wire, got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.GetPayload() != nil {
		t.Fatalf("payload should be nil for empty oneof, got %T", got.GetPayload())
	}
	if !proto.Equal(empty, got) {
		t.Fatalf("empty-frame round-trip mismatch: %v vs %v", empty, got)
	}
}
