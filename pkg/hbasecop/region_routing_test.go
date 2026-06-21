// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/shmem"
	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

type barrierObserver struct {
	UnimplementedRegionObserver

	want    int
	arrived sync.WaitGroup

	mu      sync.Mutex
	regions []uint32
}

func newBarrierObserver(want int) *barrierObserver {
	o := &barrierObserver{want: want}
	o.arrived.Add(want)
	return o
}

func (o *barrierObserver) PrePut(_ context.Context, env ObserverEnv, _ *hbasepb.MutationProto) (HookResult, error) {
	o.mu.Lock()
	o.regions = append(o.regions, env.RegionID)
	o.mu.Unlock()

	o.arrived.Done() // announce this invocation reached the barrier
	o.arrived.Wait() // block until every concurrent invocation has too
	return HookResult{}, nil
}

func (o *barrierObserver) seenRegions() []uint32 {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := append([]uint32(nil), o.regions...)
	slices.Sort(out)
	return out
}

func TestMultiRegionParallelRouting(t *testing.T) {
	const nRegions = 4

	h := openLoopHarness(t)
	obs := newBarrierObserver(nRegions)
	d := newDispatcher(obs, nil)

	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            h.loopIn,
		OutCh:           h.loopOut,
		HeartbeatPeriod: -1,
		Handler:         d.dispatch,
	})
	if err != nil {
		t.Fatalf("cpruntime.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var loopWG sync.WaitGroup
	loopWG.Add(1)
	go func() {
		defer loopWG.Done()
		_ = loop.Run(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)

	for r := 1; r <= nRegions; r++ {
		regionID := uint32(r)
		frame := encodeMultiRegionPrePut(t, regionID, uint64(r))
		for time.Now().Before(deadline) {
			err := h.mockOut.Send(frame)
			if err == nil {
				break
			}
			if !errors.Is(err, shmem.ErrRingFull) {
				t.Fatalf("region %d: send: %v", regionID, err)
			}
			runtime.Gosched()
		}
	}

	gotRegions := make([]uint32, 0, nRegions)
	gotReqIDs := make(map[uint64]uint32)
	for len(gotRegions) < nRegions {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d responses within 2s - serial dispatch would "+
				"deadlock the barrier; got regions %v", len(gotRegions), nRegions, gotRegions)
		}
		data, err := h.mockIn.Recv()
		if errors.Is(err, shmem.ErrNoData) {
			runtime.Gosched()
			continue
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		resp, err := wire.NewDecoder(bytes.NewReader(data)).Decode()
		if err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp.Type != wire.TypeResponse {
			t.Fatalf("resp.Type = %v, want Response", resp.Type)
		}
		gotRegions = append(gotRegions, resp.RegionID)
		gotReqIDs[resp.ReqID] = resp.RegionID
	}

	slices.Sort(gotRegions)
	if !slices.Equal(gotRegions, []uint32{1, 2, 3, 4}) {
		t.Fatalf("response region_ids = %v, want [1 2 3 4]", gotRegions)
	}

	for r := 1; r <= nRegions; r++ {
		if got := gotReqIDs[uint64(r)]; got != uint32(r) {
			t.Fatalf("req_id %d returned region_id %d, want %d", r, got, r)
		}
	}

	if seen := obs.seenRegions(); !slices.Equal(seen, []uint32{1, 2, 3, 4}) {
		t.Fatalf("observer saw regions %v, want [1 2 3 4]", seen)
	}

	cancel()
	loopWG.Wait()
}

func encodeMultiRegionPrePut(t *testing.T, regionID uint32, reqID uint64) []byte {
	t.Helper()
	hctx := &hookpb.HookContext{
		TableName: &hbasepb.TableName{
			Namespace: []byte("default"),
			Qualifier: []byte("users"),
		},
		RegionName: []byte("users,,region-" + string(rune('0'+regionID)) + "."),
		RequestId:  reqID,
	}
	innerBytes, err := proto.Marshal(&hookpb.PrePutRequest{
		Ctx:      hctx,
		Mutation: &hbasepb.MutationProto{Row: []byte("row")},
	})
	if err != nil {
		t.Fatalf("marshal PrePutRequest: %v", err)
	}
	outerBytes, err := proto.Marshal(&wirepb.Request{HookCtx: innerBytes})
	if err != nil {
		t.Fatalf("marshal wirepb.Request: %v", err)
	}
	var encoded bytes.Buffer
	if err := wire.NewEncoder(&encoded).Encode(&wire.Message{
		Type:     wire.TypeRequest,
		ReqID:    reqID,
		RegionID: regionID,
		HookID:   uint8(HookIDPrePut),
		Payload:  outerBytes,
	}); err != nil {
		t.Fatalf("wire encode: %v", err)
	}
	return encoded.Bytes()
}
