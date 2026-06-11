// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// capturingMaster records master-hook invocations for round-trip
// dispatch tests (T51 Wave A).
type capturingMaster struct {
	UnimplementedMasterObserver

	mu                  sync.Mutex
	preCreateTableCalls int
	postBalanceCalls    int
	lastTableName       string
	preCreateResult     HookResult
	preCreateErr        error
}

func (c *capturingMaster) PreCreateTable(_ context.Context, _ ObserverEnv, req *hookpb.PreCreateTableRequest) (HookResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preCreateTableCalls++
	tn := req.GetTableName()
	if tn != nil {
		c.lastTableName = string(tn.GetNamespace()) + ":" + string(tn.GetQualifier())
	}
	return c.preCreateResult, c.preCreateErr
}

func (c *capturingMaster) PostBalance(_ context.Context, _ ObserverEnv, _ *hookpb.PostBalanceRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postBalanceCalls++
	return nil
}

func TestDispatchMasterPreCreateTable_BypassPropagates(t *testing.T) {
	master := &capturingMaster{preCreateResult: HookResult{Bypass: true}}
	d := newMasterDispatcher(master, nil)

	inner := &hookpb.PreCreateTableRequest{
		Ctx: &hookpb.HookContext{RequestId: 1},
		TableName: &hbasepb.TableName{
			Namespace: []byte("default"),
			Qualifier: []byte("blocked-tbl"),
		},
	}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PreCreateTableRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPreCreateTable, 9, innerBytes)

	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if resp.Type != wire.TypeResponse {
		t.Fatalf("resp.Type = %v, want Response", resp.Type)
	}
	if got, want := master.preCreateTableCalls, 1; got != want {
		t.Fatalf("preCreateTableCalls = %d, want %d", got, want)
	}
	if got, want := master.lastTableName, "default:blocked-tbl"; got != want {
		t.Fatalf("lastTableName = %q, want %q", got, want)
	}
	hookResp := decodeHookResponse(t, resp)
	if !hookResp.GetBypass() {
		t.Fatal("HookResponse.Bypass = false, want true")
	}
}

func TestDispatchMasterPostBalance_RoundTrip(t *testing.T) {
	master := &capturingMaster{}
	d := newMasterDispatcher(master, nil)

	inner := &hookpb.PostBalanceRequest{Ctx: &hookpb.HookContext{}, BalanceMode: 1, Ran: true}
	innerBytes, err := proto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal PostBalanceRequest: %v", err)
	}
	req := buildRequestFrame(t, HookIDPostBalance, 13, innerBytes)
	resp := d.dispatch(context.Background(), req)
	if resp == nil {
		t.Fatal("dispatch returned nil response frame")
	}
	if got, want := master.postBalanceCalls, 1; got != want {
		t.Fatalf("postBalanceCalls = %d, want %d", got, want)
	}
}

func TestDispatchMasterUnknownHookReturnsError(t *testing.T) {
	master := &capturingMaster{}
	d := newMasterDispatcher(master, nil)

	// PrePut is a region hook id; a master-only dispatcher must reject it.
	inner := &hookpb.PreCreateTableRequest{Ctx: &hookpb.HookContext{}}
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
		t.Fatalf("resp.Type = %v, want Error (unknown hook for master dispatcher)", resp.Type)
	}
}
