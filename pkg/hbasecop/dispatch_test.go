// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
	"google.golang.org/protobuf/proto"
)

// capturingObserver records the most recent invocation so tests can
// inspect the mutation/env the dispatcher decoded. The mutex makes
// concurrent calls safe — the race detector cannot see the
// happens-before edge across the shmem ring used by the loop test, so
// observer state must be guarded explicitly.
type capturingObserver struct {
	mu           sync.Mutex
	prePutCalls  int
	postPutCalls int
	lastEnv      ObserverEnv
	lastMut      *hbasepb.MutationProto
	prePutResult HookResult
	prePutErr    error
	postPutErr   error
}

func (c *capturingObserver) PrePut(_ context.Context, env ObserverEnv, mut *hbasepb.MutationProto) (HookResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prePutCalls++
	c.lastEnv = env
	c.lastMut = mut
	return c.prePutResult, c.prePutErr
}

func (c *capturingObserver) PostPut(_ context.Context, env ObserverEnv, mut *hbasepb.MutationProto) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postPutCalls++
	c.lastEnv = env
	c.lastMut = mut
	return c.postPutErr
}

func (c *capturingObserver) snapshot() (int, int, ObserverEnv, *hbasepb.MutationProto) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prePutCalls, c.postPutCalls, c.lastEnv, c.lastMut
}

func buildRequestFrame(t *testing.T, hookID HookID, reqID uint64, hookCtxBytes []byte) *wire.Message {
	t.Helper()
	outer := &wirepb.Request{HookCtx: hookCtxBytes}
	outerBytes, err := proto.Marshal(outer)
	if err != nil {
		t.Fatalf("marshal wirepb.Request: %v", err)
	}
	return &wire.Message{
		Type:    wire.TypeRequest,
		ReqID:   reqID,
		HookID:  uint8(hookID),
		Payload: outerBytes,
	}
}

func decodeHookResponse(t *testing.T, frame *wire.Message) *hookpb.HookResponse {
	t.Helper()
	var wireResp wirepb.Response
	if err := proto.Unmarshal(frame.Payload, &wireResp); err != nil {
		t.Fatalf("unmarshal wirepb.Response: %v", err)
	}
	var hookResp hookpb.HookResponse
	if err := proto.Unmarshal(wireResp.GetHookResp(), &hookResp); err != nil {
		t.Fatalf("unmarshal hookpb.HookResponse: %v", err)
	}
	return &hookResp
}

func TestDispatchPrePut(t *testing.T) {
	obs := &capturingObserver{}
	d := newDispatcher(obs, nil)

	mut := &hbasepb.MutationProto{Row: []byte("row-1")}
	hctx := &hookpb.HookContext{
		TableName: &hbasepb.TableName{
			Namespace: []byte("default"),
			Qualifier: []byte("users"),
		},
		RegionName: []byte("users,,1234567890.abc."),
		RequestId:  42,
	}
	inner := &hookpb.PrePutRequest{Ctx: hctx, Mutation: mut}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PrePutRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPrePut, 7, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if resp.Type != wire.TypeResponse {
		t.Fatalf("resp.Type = %v, want Response", resp.Type)
	}
	if resp.ReqID != req.ReqID {
		t.Fatalf("resp.ReqID = %d, want %d", resp.ReqID, req.ReqID)
	}
	if resp.HookID != req.HookID {
		t.Fatalf("resp.HookID = %d, want %d", resp.HookID, req.HookID)
	}

	if obs.prePutCalls != 1 {
		t.Fatalf("PrePut calls = %d, want 1", obs.prePutCalls)
	}
	if !bytes.Equal(obs.lastMut.GetRow(), []byte("row-1")) {
		t.Fatalf("observer received mut.Row = %q, want %q", obs.lastMut.GetRow(), []byte("row-1"))
	}
	if obs.lastEnv.TableName != "default:users" {
		t.Fatalf("env.TableName = %q, want %q", obs.lastEnv.TableName, "default:users")
	}
	if obs.lastEnv.RegionName != string(hctx.RegionName) {
		t.Fatalf("env.RegionName = %q, want %q", obs.lastEnv.RegionName, hctx.RegionName)
	}

	hookResp := decodeHookResponse(t, resp)
	if hookResp.GetBypass() {
		t.Fatalf("HookResponse.Bypass = true, want false")
	}
	if hookResp.GetError() != nil {
		t.Fatalf("HookResponse.Error = %v, want nil", hookResp.GetError())
	}
}

func TestDispatchPrePutBypass(t *testing.T) {
	obs := &capturingObserver{prePutResult: HookResult{Bypass: true}}
	d := newDispatcher(obs, nil)

	innerBytes, err := proto.Marshal(&hookpb.PrePutRequest{Mutation: &hbasepb.MutationProto{}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := buildRequestFrame(t, HookIDPrePut, 1, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil")
	}
	hookResp := decodeHookResponse(t, resp)
	if !hookResp.GetBypass() {
		t.Fatalf("HookResponse.Bypass = false, want true")
	}
}

func TestDispatchPrePutObserverError(t *testing.T) {
	obs := &capturingObserver{prePutErr: errors.New("denied by policy")}
	d := newDispatcher(obs, nil)

	innerBytes, err := proto.Marshal(&hookpb.PrePutRequest{Mutation: &hbasepb.MutationProto{}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := buildRequestFrame(t, HookIDPrePut, 1, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil")
	}
	if resp.Type != wire.TypeResponse {
		t.Fatalf("resp.Type = %v, want Response (error carried in HookResponse.Error)", resp.Type)
	}
	hookResp := decodeHookResponse(t, resp)
	if hookResp.GetError() == nil {
		t.Fatal("HookResponse.Error = nil, want non-nil")
	}
	if hookResp.GetError().GetMessage() != "denied by policy" {
		t.Fatalf("HookResponse.Error.Message = %q, want %q",
			hookResp.GetError().GetMessage(), "denied by policy")
	}
}

func TestDispatchPostPut(t *testing.T) {
	obs := &capturingObserver{}
	d := newDispatcher(obs, nil)

	mut := &hbasepb.MutationProto{Row: []byte("row-x")}
	inner := &hookpb.PostPutRequest{
		Ctx:      &hookpb.HookContext{TableName: &hbasepb.TableName{Namespace: []byte{}, Qualifier: []byte("t1")}},
		Mutation: mut,
	}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PostPutRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPostPut, 9, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil")
	}
	if resp.Type != wire.TypeResponse {
		t.Fatalf("resp.Type = %v, want Response", resp.Type)
	}
	if obs.postPutCalls != 1 {
		t.Fatalf("PostPut calls = %d, want 1", obs.postPutCalls)
	}
	if !bytes.Equal(obs.lastMut.GetRow(), []byte("row-x")) {
		t.Fatalf("PostPut row = %q, want %q", obs.lastMut.GetRow(), "row-x")
	}
	// Namespace empty → qualifier-only.
	if obs.lastEnv.TableName != "t1" {
		t.Fatalf("env.TableName = %q, want %q", obs.lastEnv.TableName, "t1")
	}
}

func TestDispatchUnknownHookReturnsError(t *testing.T) {
	obs := &capturingObserver{}
	d := newDispatcher(obs, nil)

	req := buildRequestFrame(t, HookID(0x77), 1, nil)
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil for unknown hook")
	}
	if resp.Type != wire.TypeError {
		t.Fatalf("resp.Type = %v, want Error", resp.Type)
	}
	if obs.prePutCalls != 0 || obs.postPutCalls != 0 {
		t.Fatalf("observer methods should not have been called: pre=%d post=%d",
			obs.prePutCalls, obs.postPutCalls)
	}
}

func TestDispatchMalformedRequestPayloadReturnsError(t *testing.T) {
	obs := &capturingObserver{}
	d := newDispatcher(obs, nil)

	req := &wire.Message{
		Type:    wire.TypeRequest,
		ReqID:   1,
		HookID:  uint8(HookIDPrePut),
		Payload: []byte{0xff, 0xff, 0xff, 0xff},
	}
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil")
	}
	if resp.Type != wire.TypeError {
		t.Fatalf("resp.Type = %v, want Error", resp.Type)
	}
}
