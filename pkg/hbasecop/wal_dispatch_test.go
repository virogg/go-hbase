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

type capturingWAL struct {
	UnimplementedWALObserver

	mu             sync.Mutex
	preWriteCalls  int
	postRollCalls  int
	lastSeqNum     uint64
	lastNewPath    string
	preWriteResult HookResult
	preWriteErr    error
}

func (c *capturingWAL) PreWALWrite(_ context.Context, _ ObserverEnv, req *hookpb.PreWALWriteRequest) (HookResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preWriteCalls++
	if k := req.GetLogKey(); k != nil {
		c.lastSeqNum = k.GetLogSeqNum()
	}
	return c.preWriteResult, c.preWriteErr
}

func (c *capturingWAL) PostWALRoll(_ context.Context, _ ObserverEnv, req *hookpb.PostWALRollRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postRollCalls++
	c.lastNewPath = req.GetNewPath()
	return nil
}

func TestDispatchWALPreWALWrite_BypassPropagates(t *testing.T) {
	wal := &capturingWAL{preWriteResult: HookResult{Bypass: true}}
	d := newWALDispatcher(wal, nil)

	inner := &hookpb.PreWALWriteRequest{
		Ctx:    &hookpb.HookContext{RequestId: 4},
		LogKey: &hookpb.WalKeyProto{LogSeqNum: 4242},
	}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PreWALWriteRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPreWALWrite, 8, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if resp.Type != wire.TypeResponse {
		t.Fatalf("resp.Type = %v, want Response", resp.Type)
	}
	if got, want := wal.preWriteCalls, 1; got != want {
		t.Fatalf("preWriteCalls = %d, want %d", got, want)
	}
	if got, want := wal.lastSeqNum, uint64(4242); got != want {
		t.Fatalf("lastSeqNum = %d, want %d", got, want)
	}
	hookResp := decodeHookResponse(t, resp)
	if !hookResp.GetBypass() {
		t.Fatal("HookResponse.Bypass = false, want true")
	}
}

func TestDispatchWALPostWALRoll_RoundTrip(t *testing.T) {
	wal := &capturingWAL{}
	d := newWALDispatcher(wal, nil)

	inner := &hookpb.PostWALRollRequest{
		Ctx:     &hookpb.HookContext{},
		OldPath: "/wal/old.1",
		NewPath: "/wal/new.2",
	}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PostWALRollRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPostWALRoll, 12, innerBytes)
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if got, want := wal.postRollCalls, 1; got != want {
		t.Fatalf("postRollCalls = %d, want %d", got, want)
	}
	if got, want := wal.lastNewPath, "/wal/new.2"; got != want {
		t.Fatalf("lastNewPath = %q, want %q", got, want)
	}
}

func TestDispatchWALPreWALWrite_ErrorPropagates(t *testing.T) {
	wal := &capturingWAL{preWriteErr: errors.New("wal write vetoed")}
	d := newWALDispatcher(wal, nil)

	inner := &hookpb.PreWALWriteRequest{Ctx: &hookpb.HookContext{}}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := buildRequestFrame(t, HookIDPreWALWrite, 6, innerBytes)
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	hookResp := decodeHookResponse(t, resp)
	if hookResp.GetError() == nil {
		t.Fatal("HookResponse.Error = nil, want populated error")
	}
}

func TestDispatchWALUnknownHookReturnsError(t *testing.T) {
	wal := &capturingWAL{}
	d := newWALDispatcher(wal, nil)

	inner := &hookpb.PreWALWriteRequest{Ctx: &hookpb.HookContext{}}
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
		t.Fatalf("resp.Type = %v, want Error (unknown hook for WAL dispatcher)", resp.Type)
	}
}
