// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

type panickingObserver struct {
	UnimplementedRegionObserver
}

func (panickingObserver) PrePut(context.Context, ObserverEnv, *hbasepb.MutationProto) (HookResult, error) {
	panic("boom: nil map write in user code")
}

func TestDispatchRecoversObserverPanic(t *testing.T) {
	d := newDispatcher(panickingObserver{}, nil)

	innerBytes, err := proto.Marshal(&hookpb.PrePutRequest{Mutation: &hbasepb.MutationProto{}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := buildRequestFrame(t, HookIDPrePut, 99, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil (process would have to rely on timeout)")
	}
	if resp.Type != wire.TypeResponse {
		t.Fatalf("resp.Type = %v, want Response (panic carried in HookResponse.Error)", resp.Type)
	}
	if resp.ReqID != 99 {
		t.Fatalf("resp.ReqID = %d, want 99", resp.ReqID)
	}
	hookResp := decodeHookResponse(t, resp)
	if hookResp.GetError() == nil {
		t.Fatal("HookResponse.Error = nil, want the recovered panic")
	}
	if !strings.Contains(hookResp.GetError().GetMessage(), "panic") {
		t.Fatalf("HookResponse.Error.Message = %q, want it to mention the panic",
			hookResp.GetError().GetMessage())
	}
}
