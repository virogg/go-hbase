// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// capturingBulkLoad records bulk-load hook invocations for round-trip
// dispatch tests (T54 Wave A).
type capturingBulkLoad struct {
	UnimplementedBulkLoadObserver

	mu            sync.Mutex
	prepareCalls  int
	cleanupCalls  int
	lastTableName string
	prepareResult HookResult
	prepareErr    error
}

func (c *capturingBulkLoad) PrePrepareBulkLoad(_ context.Context, env ObserverEnv, _ *hookpb.PrePrepareBulkLoadRequest) (HookResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prepareCalls++
	c.lastTableName = env.TableName
	return c.prepareResult, c.prepareErr
}

func (c *capturingBulkLoad) PreCleanupBulkLoad(_ context.Context, _ ObserverEnv, _ *hookpb.PreCleanupBulkLoadRequest) (HookResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupCalls++
	return HookResult{}, nil
}

func TestDispatchBulkLoadPrePrepare_BypassPropagates(t *testing.T) {
	bl := &capturingBulkLoad{prepareResult: HookResult{Bypass: true}}
	d := newBulkLoadDispatcher(bl, nil)

	inner := &hookpb.PrePrepareBulkLoadRequest{
		Ctx: &hookpb.HookContext{
			TableName: &hbasepb.TableName{
				Namespace: []byte("default"),
				Qualifier: []byte("ingest"),
			},
			RequestId: 9,
		},
	}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PrePrepareBulkLoadRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPrePrepareBulkLoad, 3, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if resp.Type != wire.TypeResponse {
		t.Fatalf("resp.Type = %v, want Response", resp.Type)
	}
	if got, want := bl.prepareCalls, 1; got != want {
		t.Fatalf("prepareCalls = %d, want %d", got, want)
	}
	if got, want := bl.lastTableName, "default:ingest"; got != want {
		t.Fatalf("lastTableName = %q, want %q", got, want)
	}
	hookResp := decodeHookResponse(t, resp)
	if !hookResp.GetBypass() {
		t.Fatal("HookResponse.Bypass = false, want true")
	}
}

func TestDispatchBulkLoadPreCleanup_RoundTrip(t *testing.T) {
	bl := &capturingBulkLoad{}
	d := newBulkLoadDispatcher(bl, nil)

	inner := &hookpb.PreCleanupBulkLoadRequest{Ctx: &hookpb.HookContext{}}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PreCleanupBulkLoadRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPreCleanupBulkLoad, 4, innerBytes)
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if got, want := bl.cleanupCalls, 1; got != want {
		t.Fatalf("cleanupCalls = %d, want %d", got, want)
	}
}

func TestDispatchBulkLoadPrePrepare_ErrorPropagates(t *testing.T) {
	bl := &capturingBulkLoad{prepareErr: errors.New("bulk load vetoed")}
	d := newBulkLoadDispatcher(bl, nil)

	inner := &hookpb.PrePrepareBulkLoadRequest{Ctx: &hookpb.HookContext{}}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := buildRequestFrame(t, HookIDPrePrepareBulkLoad, 5, innerBytes)
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	hookResp := decodeHookResponse(t, resp)
	if hookResp.GetError() == nil {
		t.Fatal("HookResponse.Error = nil, want populated error")
	}
}

func TestDispatchBulkLoadUnknownHookReturnsError(t *testing.T) {
	bl := &capturingBulkLoad{}
	d := newBulkLoadDispatcher(bl, nil)

	// PrePut is a region hook id; a bulk-load-only dispatcher must reject it.
	inner := &hookpb.PrePrepareBulkLoadRequest{Ctx: &hookpb.HookContext{}}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := buildRequestFrame(t, HookIDPrePut, 1, innerBytes)
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if resp.Type != wire.TypeError {
		t.Fatalf("resp.Type = %v, want Error (unknown hook for bulk-load dispatcher)", resp.Type)
	}
}
