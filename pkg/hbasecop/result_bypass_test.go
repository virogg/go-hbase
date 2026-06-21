// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

type appendBypassObserver struct {
	UnimplementedRegionObserver
}

func (appendBypassObserver) PreAppend(_ context.Context, _ ObserverEnv, _ *hookpb.PreAppendRequest) (HookResult, error) {
	return HookResult{
		Bypass: true,
		ResultCells: []*hbasepb.Cell{
			{
				Row:       []byte("r1"),
				Family:    []byte("cf"),
				Qualifier: []byte("q"),
				Timestamp: proto.Uint64(42),
				Value:     []byte("substituted"),
			},
		},
	}, nil
}

func TestDispatchPreAppendResultBypass(t *testing.T) {
	d := newDispatcher(appendBypassObserver{}, nil)

	innerBytes, err := proto.Marshal(&hookpb.PreAppendRequest{Append: &hbasepb.MutationProto{}})
	if err != nil {
		t.Fatalf("marshal PreAppendRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPreAppend, 7, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil || resp.Type != wire.TypeResponse {
		t.Fatalf("want Response frame, got %v", resp)
	}
	hookResp := decodeHookResponse(t, resp)
	if !hookResp.GetBypass() {
		t.Fatal("HookResponse.Bypass = false, want true")
	}
	cells := hookResp.GetResult()
	if len(cells) != 1 {
		t.Fatalf("HookResponse.Result has %d cells, want 1", len(cells))
	}
	got := cells[0]
	if string(got.GetRow()) != "r1" || string(got.GetValue()) != "substituted" ||
		string(got.GetQualifier()) != "q" || got.GetTimestamp() != 42 {
		t.Fatalf("substitute cell round-trip mismatch: %+v", got)
	}
}

func TestDispatchPreAppendNoBypassNoResult(t *testing.T) {
	d := newDispatcher(UnimplementedRegionObserver{}, nil) // default PreAppend: no bypass

	innerBytes, err := proto.Marshal(&hookpb.PreAppendRequest{Append: &hbasepb.MutationProto{}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := buildRequestFrame(t, HookIDPreAppend, 8, innerBytes)

	hookResp := decodeHookResponse(t, d.dispatch(context.Background(), req))
	if hookResp.GetBypass() {
		t.Fatal("default observer should not bypass")
	}
	if len(hookResp.GetResult()) != 0 {
		t.Fatalf("expected no substitute cells, got %d", len(hookResp.GetResult()))
	}
}
