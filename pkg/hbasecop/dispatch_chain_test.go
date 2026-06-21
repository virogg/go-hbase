// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// chainRegionObs records calls and returns a configurable result/error.
type chainRegionObs struct {
	UnimplementedRegionObserver
	calls   *int
	result  HookResult
	err     error
	tag     int
	tracked *[]int
}

func (o chainRegionObs) PrePut(context.Context, ObserverEnv, *MutationProto) (HookResult, error) {
	*o.calls++
	if o.tracked != nil {
		*o.tracked = append(*o.tracked, o.tag)
	}
	return o.result, o.err
}

func dispatchPrePut(t *testing.T, d *dispatcher) *hookpb.HookResponse {
	t.Helper()
	inner, err := proto.Marshal(&hookpb.PrePutRequest{
		Ctx:      &hookpb.HookContext{RegionName: []byte("t,,1.a.")},
		Mutation: &hbasepb.MutationProto{Row: []byte("r")},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := d.dispatch(context.Background(), buildRequestFrame(t, HookIDPrePut, 1, inner))
	return decodeHookResponse(t, resp)
}

func TestDispatchRegionChain(t *testing.T) {
	var a, b int
	var order []int
	d := &dispatcher{
		logger: slog.Default(),
		observers: []RegionObserver{
			chainRegionObs{calls: &a, tag: 1, tracked: &order, result: HookResult{BlockedIndices: []uint32{0}}},
			chainRegionObs{calls: &b, tag: 2, tracked: &order, result: HookResult{Bypass: true, BlockedIndices: []uint32{2}}},
		},
	}
	resp := dispatchPrePut(t, d)

	if a != 1 || b != 1 {
		t.Fatalf("both observers should fire once: a=%d b=%d", a, b)
	}
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("observers should run in order, got %v", order)
	}
	if !resp.GetBypass() {
		t.Error("bypass should fold via OR (second observer bypassed)")
	}
	if got := resp.GetBlockedIndices(); len(got) != 2 {
		t.Errorf("BlockedIndices should union, got %v", got)
	}
}

func TestDispatchChainErrorShortCircuits(t *testing.T) {
	var a, b int
	d := &dispatcher{
		logger: slog.Default(),
		observers: []RegionObserver{
			chainRegionObs{calls: &a, err: errors.New("denied")},
			chainRegionObs{calls: &b},
		},
	}
	resp := dispatchPrePut(t, d)

	if a != 1 {
		t.Fatalf("first observer should fire: a=%d", a)
	}
	if b != 0 {
		t.Fatalf("error must short-circuit the chain: b=%d, want 0", b)
	}
	if resp.GetError() == nil {
		t.Error("chain error should propagate to HookResponse.error")
	}
}

// chainMasterObs counts PreCreateTable for the multi-surface test.
type chainMasterObs struct {
	UnimplementedMasterObserver
	calls *int
}

func (o chainMasterObs) PreCreateTable(context.Context, ObserverEnv, *hookpb.PreCreateTableRequest) (HookResult, error) {
	*o.calls++
	return HookResult{}, nil
}

func TestDispatchMultiSurface(t *testing.T) {
	var region, master int
	d := &dispatcher{
		logger:    slog.Default(),
		observers: []RegionObserver{chainRegionObs{calls: &region}},
		masters:   []MasterObserver{chainMasterObs{calls: &master}},
	}

	dispatchPrePut(t, d) // region hook

	mInner, err := proto.Marshal(&hookpb.PreCreateTableRequest{Ctx: &hookpb.HookContext{}})
	if err != nil {
		t.Fatal(err)
	}
	d.dispatch(context.Background(), buildRequestFrame(t, HookIDPreCreateTable, 2, mInner)) // master hook

	if region != 1 || master != 1 {
		t.Fatalf("one process should serve both surfaces: region=%d master=%d", region, master)
	}
}

func TestNewMixedDispatcher(t *testing.T) {
	region := UnimplementedRegionObserver{}
	master := UnimplementedMasterObserver{}

	d, err := newMixedDispatcher(slog.Default(), region, master)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.observers) != 1 || len(d.masters) != 1 {
		t.Fatalf("want 1 region + 1 master, got %d/%d", len(d.observers), len(d.masters))
	}

	if _, err := newMixedDispatcher(slog.Default(), 42); err == nil {
		t.Error("a non-observer argument should error")
	}
	if _, err := newMixedDispatcher(slog.Default()); err == nil {
		t.Error("zero observers should error")
	}
}

type dualSurfaceObs struct {
	UnimplementedRegionObserver
	UnimplementedMasterObserver
	region, master *int
}

func (o dualSurfaceObs) PrePut(context.Context, ObserverEnv, *MutationProto) (HookResult, error) {
	*o.region++
	return HookResult{}, nil
}

func (o dualSurfaceObs) PreCreateTable(context.Context, ObserverEnv, *hookpb.PreCreateTableRequest) (HookResult, error) {
	*o.master++
	return HookResult{}, nil
}

func TestNewMixedDispatcherFansOutOneValueToEverySurface(t *testing.T) {
	var region, master int
	obs := dualSurfaceObs{region: &region, master: &master}

	d, err := newMixedDispatcher(slog.Default(), obs)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.observers) != 1 || len(d.masters) != 1 {
		t.Fatalf("one value should register on both surfaces: region=%d master=%d", len(d.observers), len(d.masters))
	}

	dispatchPrePut(t, d) // region hook
	mInner, err := proto.Marshal(&hookpb.PreCreateTableRequest{Ctx: &hookpb.HookContext{}})
	if err != nil {
		t.Fatal(err)
	}
	d.dispatch(context.Background(), buildRequestFrame(t, HookIDPreCreateTable, 2, mInner)) // master hook

	if region != 1 || master != 1 {
		t.Fatalf("both surfaces of the one observer should fire: region=%d master=%d", region, master)
	}
}

type allSurfaceObs struct {
	UnimplementedRegionObserver
	UnimplementedMasterObserver
	UnimplementedRegionServerObserver
	UnimplementedWALObserver
	UnimplementedBulkLoadObserver
}

func TestNewMixedDispatcherRegistersOnAllSurfaces(t *testing.T) {
	d, err := newMixedDispatcher(slog.Default(), allSurfaceObs{})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(d.observers); n != 1 {
		t.Errorf("region: got %d, want 1", n)
	}
	if n := len(d.masters); n != 1 {
		t.Errorf("master: got %d, want 1", n)
	}
	if n := len(d.regionServers); n != 1 {
		t.Errorf("region-server: got %d, want 1", n)
	}
	if n := len(d.wals); n != 1 {
		t.Errorf("wal: got %d, want 1", n)
	}
	if n := len(d.bulkLoads); n != 1 {
		t.Errorf("bulk-load: got %d, want 1", n)
	}
}

type regionEndpointObs struct {
	UnimplementedRegionObserver
}

func (regionEndpointObs) Call(context.Context, *EndpointEnv, string, []byte) ([]byte, error) {
	return nil, nil
}

func TestNewMixedDispatcherRegistersEndpointAlongsideObserver(t *testing.T) {
	d, err := newMixedDispatcher(slog.Default(), regionEndpointObs{})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.observers) != 1 {
		t.Errorf("region observer surface: got %d, want 1", len(d.observers))
	}
	if d.endpoint == nil {
		t.Error("endpoint surface should be registered from the same value")
	}
}

func TestNewMixedDispatcherRejectsSecondEndpoint(t *testing.T) {
	ep1 := funcEndpoint(func(context.Context, *EndpointEnv, string, []byte) ([]byte, error) { return nil, nil })
	ep2 := funcEndpoint(func(context.Context, *EndpointEnv, string, []byte) ([]byte, error) { return nil, nil })
	_, err := newMixedDispatcher(slog.Default(), ep1, ep2)
	if err == nil || !strings.Contains(err.Error(), "more than one Endpoint") {
		t.Fatalf("two endpoints should be rejected, got err=%v", err)
	}
}
