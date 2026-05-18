// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
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

// openLoopHarnessWith opens a loopHarness whose backing rings have the
// caller-chosen capacity. The default openLoopHarness pins capacity at
// 16 frames which is intentionally tight for unit tests; the T62
// concurrency suite drives thousands of frames through the runtime and
// would spend most of its time spinning on ErrRingFull at that size.
func openLoopHarnessWith(t *testing.T, capacity int) *loopHarness {
	t.Helper()
	dir := t.TempDir()
	inFile := filepath.Join(dir, "in.mmap")
	outFile := filepath.Join(dir, "out.mmap")
	mkChan := func(file string, role shmem.Role) *shmem.Channel {
		ch, err := shmem.Open(shmem.Config{
			Filename:      file,
			Capacity:      capacity,
			MaxObjectSize: testRingMaxObjectSize,
			Role:          role,
		})
		if err != nil {
			t.Fatalf("shmem.Open: %v", err)
		}
		t.Cleanup(func() { _ = ch.Close() })
		return ch
	}
	return &loopHarness{
		mockOut: mkChan(inFile, shmem.RoleProducer),
		loopIn:  mkChan(inFile, shmem.RoleConsumer),
		loopOut: mkChan(outFile, shmem.RoleProducer),
		mockIn:  mkChan(outFile, shmem.RoleConsumer),
	}
}

// tallyObserver counts PrePut invocations per region_id with an atomic
// counter array. It is the minimal observer that proves a request was
// delivered to the user code: the SDK incremented the right slot.
type tallyObserver struct {
	UnimplementedRegionObserver

	perRegion []atomic.Int64
	total     atomic.Int64
}

func newTallyObserver(nRegions int) *tallyObserver {
	return &tallyObserver{perRegion: make([]atomic.Int64, nRegions+1)}
}

func (o *tallyObserver) PrePut(_ context.Context, env ObserverEnv, _ *hbasepb.MutationProto) (HookResult, error) {
	if int(env.RegionID) < len(o.perRegion) {
		o.perRegion[env.RegionID].Add(1)
	}
	o.total.Add(1)
	return HookResult{}, nil
}

// TestRegionConcurrencyStress is the T62 Wave-A acceptance: 100 regions
// each with 100 parallel PrePut requests (10_000 hooks total) resolve
// without race, without deadlock, and every region_id sees exactly its
// expected per-region count. The test runs under `-race` in CI so any
// data race in the dispatch path surfaces here.
//
// Why this matters: the cpruntime loop spawns one goroutine per inbound
// frame, so the contention surface scales with parallel inflight. A
// regression that serializes dispatch — e.g. a shared mutex in the hook
// table or a single-writer outbound queue without sufficient buffering
// — would either deadlock or starve at this size.
func TestRegionConcurrencyStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test: skipped under -short")
	}
	const (
		nRegions      = 100
		putsPerRegion = 100
		ringCapacity  = 256
	)
	total := nRegions * putsPerRegion

	h := openLoopHarnessWith(t, ringCapacity)
	obs := newTallyObserver(nRegions)
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

	deadline := time.Now().Add(20 * time.Second)

	// Pre-encode all frames so encoding overhead does not throttle the
	// sender and skew the test. ReqID is dense across all requests
	// (1..total) so we can correlate without collisions. Region order
	// is interleaved so the bulk of region_ids is in flight at any
	// moment — a regression that serializes by region would fail.
	type frameSpec struct {
		regionID uint32
		reqID    uint64
		bytes    []byte
	}
	frames := make([]frameSpec, 0, total)
	for p := range putsPerRegion {
		for r := 1; r <= nRegions; r++ {
			reqID := uint64(p*nRegions + r)
			frames = append(frames, frameSpec{
				regionID: uint32(r),
				reqID:    reqID,
				bytes:    encodeStressPrePut(t, uint32(r), reqID),
			})
		}
	}

	// Single sender — the shmem ring is SPSC, so production has exactly
	// one writer per channel; the test mirrors that. Receiver runs on
	// the main goroutine in parallel with the sender, draining the
	// outbound ring as the runtime fills it.
	sendDone := make(chan error, 1)
	go func() {
		for _, fr := range frames {
			for {
				if time.Now().After(deadline) {
					sendDone <- fmt.Errorf("send deadline exceeded at req_id=%d", fr.reqID)
					return
				}
				err := h.mockOut.Send(fr.bytes)
				if err == nil {
					break
				}
				if !errors.Is(err, shmem.ErrRingFull) {
					sendDone <- fmt.Errorf("send req_id=%d: %w", fr.reqID, err)
					return
				}
				runtime.Gosched()
			}
		}
		sendDone <- nil
	}()

	// Receiver tallies (region_id, req_id) pairs and stops once we
	// have all `total` responses.
	gotPerRegion := make([]int, nRegions+1)
	seenReqID := make([]bool, total+1)
	gotCount := 0
	for gotCount < total {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d responses received within 20s", gotCount, total)
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
			t.Fatalf("resp.Type = %v, want Response (req_id=%d)", resp.Type, resp.ReqID)
		}
		if resp.RegionID == 0 || int(resp.RegionID) > nRegions {
			t.Fatalf("resp.RegionID = %d out of range [1,%d]", resp.RegionID, nRegions)
		}
		if resp.ReqID == 0 || int(resp.ReqID) > total {
			t.Fatalf("resp.ReqID = %d out of range [1,%d]", resp.ReqID, total)
		}
		if seenReqID[resp.ReqID] {
			t.Fatalf("duplicate response for req_id=%d", resp.ReqID)
		}
		seenReqID[resp.ReqID] = true
		gotPerRegion[resp.RegionID]++
		gotCount++
	}

	if err := <-sendDone; err != nil {
		t.Errorf("sender: %v", err)
	}

	for r := 1; r <= nRegions; r++ {
		if gotPerRegion[r] != putsPerRegion {
			t.Errorf("region %d: got %d responses, want %d", r, gotPerRegion[r], putsPerRegion)
		}
		if got := int(obs.perRegion[r].Load()); got != putsPerRegion {
			t.Errorf("region %d observer count = %d, want %d", r, got, putsPerRegion)
		}
	}
	if got := int(obs.total.Load()); got != total {
		t.Errorf("observer total = %d, want %d", got, total)
	}

	cancel()
	loopWG.Wait()
}

func encodeStressPrePut(t *testing.T, regionID uint32, reqID uint64) []byte {
	t.Helper()
	hctx := &hookpb.HookContext{
		TableName: &hbasepb.TableName{
			Namespace: []byte("default"),
			Qualifier: []byte("stress"),
		},
		RegionName: fmt.Appendf(nil, "stress,,r%d.", regionID),
		RequestId:  reqID,
	}
	innerBytes, err := proto.Marshal(&hookpb.PrePutRequest{
		Ctx:      hctx,
		Mutation: &hbasepb.MutationProto{Row: fmt.Appendf(nil, "row-%d", reqID)},
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
