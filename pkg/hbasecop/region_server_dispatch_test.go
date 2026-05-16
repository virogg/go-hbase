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
	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// capturingRegionServer records region-server hook invocations for
// round-trip dispatch tests (T52 Wave A).
type capturingRegionServer struct {
	UnimplementedRegionServerObserver

	mu                sync.Mutex
	preStopCalls      int
	postExecProcCalls int
	lastStopHost      string
	preStopResult     HookResult
	preStopErr        error
}

func (c *capturingRegionServer) PreStopRegionServer(_ context.Context, _ ObserverEnv, req *hookpb.PreStopRegionServerRequest) (HookResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preStopCalls++
	if sn := req.GetServer(); sn != nil {
		c.lastStopHost = sn.GetHost()
	}
	return c.preStopResult, c.preStopErr
}

func (c *capturingRegionServer) PostExecuteProcedures(_ context.Context, _ ObserverEnv, _ *hookpb.PostExecuteProceduresRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postExecProcCalls++
	return nil
}

func TestDispatchRegionServerPreStop_BypassPropagates(t *testing.T) {
	rs := &capturingRegionServer{preStopResult: HookResult{Bypass: true}}
	d := newRegionServerDispatcher(rs, nil)

	inner := &hookpb.PreStopRegionServerRequest{
		Ctx:    &hookpb.HookContext{RequestId: 7},
		Server: &hookpb.ServerNameProto{Host: "rs-1.example.com", Port: 16020},
	}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PreStopRegionServerRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPreStopRegionServer, 3, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if resp.Type != wire.TypeResponse {
		t.Fatalf("resp.Type = %v, want Response", resp.Type)
	}
	if got, want := rs.preStopCalls, 1; got != want {
		t.Fatalf("preStopCalls = %d, want %d", got, want)
	}
	if got, want := rs.lastStopHost, "rs-1.example.com"; got != want {
		t.Fatalf("lastStopHost = %q, want %q", got, want)
	}
	hookResp := decodeHookResponse(t, resp)
	if !hookResp.GetBypass() {
		t.Fatal("HookResponse.Bypass = false, want true")
	}
}

func TestDispatchRegionServerPostExecuteProcedures_RoundTrip(t *testing.T) {
	rs := &capturingRegionServer{}
	d := newRegionServerDispatcher(rs, nil)

	inner := &hookpb.PostExecuteProceduresRequest{Ctx: &hookpb.HookContext{}}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PostExecuteProceduresRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPostExecuteProcedures, 11, innerBytes)
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if got, want := rs.postExecProcCalls, 1; got != want {
		t.Fatalf("postExecProcCalls = %d, want %d", got, want)
	}
}

func TestDispatchRegionServerPreStop_ErrorPropagates(t *testing.T) {
	rs := &capturingRegionServer{preStopErr: errors.New("shutdown vetoed")}
	d := newRegionServerDispatcher(rs, nil)

	inner := &hookpb.PreStopRegionServerRequest{Ctx: &hookpb.HookContext{}}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := buildRequestFrame(t, HookIDPreStopRegionServer, 5, innerBytes)
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	hookResp := decodeHookResponse(t, resp)
	if hookResp.GetError() == nil {
		t.Fatal("HookResponse.Error = nil, want populated error")
	}
}

func TestDispatchRegionServerUnknownHookReturnsError(t *testing.T) {
	rs := &capturingRegionServer{}
	d := newRegionServerDispatcher(rs, nil)

	// PrePut is a region hook id; a region-server-only dispatcher must reject it.
	inner := &hookpb.PreStopRegionServerRequest{Ctx: &hookpb.HookContext{}}
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
		t.Fatalf("resp.Type = %v, want Error (unknown hook for region-server dispatcher)", resp.Type)
	}
}
