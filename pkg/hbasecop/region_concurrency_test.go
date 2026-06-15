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

// openLoopHarnessWith opens a loopHarness with caller-chosen ring capacity.
// Default openLoopHarness pins 16 frames (tight for unit tests); the T62
// suite drives thousands and would mostly spin on ErrRingFull at that size.
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

// tallyObserver counts PrePut invocations per region_id via an atomic
// counter array. A non-zero slot proves the request reached user code.
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
// x 100 parallel PrePut requests (10_000 hooks total) resolve without
// race, without deadlock, and every region_id sees exactly its per-region
// count. Runs under -race in CI to surface dispatch-path races.
//
// The cpruntime loop spawns one goroutine per inbound frame, so contention
// scales with parallel inflight. A regression that serializes dispatch
// (e.g. a shared mutex in the hook table, or an under-buffered single-writer
// outbound queue) would deadlock or starve at this size.
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
	// sender and skew the test. ReqID is dense (1..total) for collision-free
	// correlation. Region order is interleaved so most region_ids are in
	// flight at any moment; a regression that serializes by region would fail.
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

	// Single sender: the shmem ring is SPSC, so production has exactly one
	// writer per channel; the test mirrors that. Receiver runs on the main
	// goroutine in parallel, draining outbound as the runtime fills it.
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

// holdRegionObserver blocks PrePut for one designated "slow" region until
// release is closed; every other region returns immediately. Surfaces
// head-of-line regressions: if one slow observer call stalls dispatch for
// the whole runtime, the fast regions' responses never arrive.
type holdRegionObserver struct {
	UnimplementedRegionObserver

	slowRegion uint32
	entered    chan struct{}
	release    chan struct{}
}

func newHoldRegionObserver(slowRegion uint32) *holdRegionObserver {
	return &holdRegionObserver{
		slowRegion: slowRegion,
		entered:    make(chan struct{}, 1),
		release:    make(chan struct{}),
	}
}

func (o *holdRegionObserver) PrePut(ctx context.Context, env ObserverEnv, _ *hbasepb.MutationProto) (HookResult, error) {
	if env.RegionID != o.slowRegion {
		return HookResult{}, nil
	}
	select {
	case o.entered <- struct{}{}:
	default:
	}
	select {
	case <-o.release:
	case <-ctx.Done():
	}
	return HookResult{}, nil
}

// TestNoHeadOfLineBlockingAcrossRegions is the T62 Wave-B acceptance: a
// slow observer on region 1 must not block PrePut delivery for the other
// nFast regions. Sends the slow request first, waits for the observer to
// confirm it is inside the handler, then sends nFast fast requests on
// distinct region_ids. Serial dispatch (e.g. a global mutex around handler
// invocations, or a single-threaded dispatcher) would never deliver the
// fast responses while the slow handler is parked. Only genuine per-request
// parallelism drains the fast tail.
func TestNoHeadOfLineBlockingAcrossRegions(t *testing.T) {
	const (
		slowRegion   = uint32(1)
		nFast        = 8
		ringCapacity = 32
	)

	h := openLoopHarnessWith(t, ringCapacity)
	obs := newHoldRegionObserver(slowRegion)
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

	deadline := time.Now().Add(3 * time.Second)
	send := func(regionID uint32, reqID uint64) {
		frame := encodeStressPrePut(t, regionID, reqID)
		for {
			if time.Now().After(deadline) {
				t.Fatalf("send deadline exceeded at region=%d req_id=%d", regionID, reqID)
			}
			err := h.mockOut.Send(frame)
			if err == nil {
				return
			}
			if !errors.Is(err, shmem.ErrRingFull) {
				t.Fatalf("send region=%d req_id=%d: %v", regionID, reqID, err)
			}
			runtime.Gosched()
		}
	}

	// 1) Fire the slow request and wait until the observer confirms it has
	// reached the block point. Without this sync the fast requests could
	// race ahead of slow dispatch and the test would prove nothing.
	slowReqID := uint64(1)
	send(slowRegion, slowReqID)
	select {
	case <-obs.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow observer did not enter PrePut within 2s")
	}

	// 2) Fire nFast fast requests on distinct region_ids while the slow
	// handler is parked. ReqID = region_id (distinct from slowReqID).
	for r := uint32(2); r <= uint32(1+nFast); r++ {
		send(r, uint64(r))
	}

	// 3) Drain exactly nFast responses. A missing fast response while the
	// slow handler is still parked means head-of-line blocking: fail.
	gotFast := 0
	fastDeadline := time.Now().Add(2 * time.Second)
	for gotFast < nFast {
		if time.Now().After(fastDeadline) {
			t.Fatalf("only %d/%d fast responses arrived while slow region was parked - head-of-line blocking",
				gotFast, nFast)
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
			t.Fatalf("decode: %v", err)
		}
		if resp.Type != wire.TypeResponse {
			t.Fatalf("resp.Type = %v, want Response", resp.Type)
		}
		if resp.RegionID == slowRegion {
			t.Fatalf("slow region %d responded while still parked (req_id=%d)",
				slowRegion, resp.ReqID)
		}
		gotFast++
	}

	// 4) Release the slow handler and confirm its response now lands.
	close(obs.release)
	select {
	case data := <-recvOne(t, h, 2*time.Second):
		resp, err := wire.NewDecoder(bytes.NewReader(data)).Decode()
		if err != nil {
			t.Fatalf("decode slow response: %v", err)
		}
		if resp.Type != wire.TypeResponse {
			t.Fatalf("slow resp.Type = %v, want Response", resp.Type)
		}
		if resp.RegionID != slowRegion || resp.ReqID != slowReqID {
			t.Fatalf("slow response = (region=%d, req_id=%d), want (%d, %d)",
				resp.RegionID, resp.ReqID, slowRegion, slowReqID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow region did not respond after release")
	}

	cancel()
	loopWG.Wait()
}

// recvOne returns a channel yielding the next frame from h.mockIn, leaving
// it empty if none arrives within timeout. Thin shim around the busy-poll
// pattern used elsewhere here so Wave-B can select on a clean channel.
func recvOne(t *testing.T, h *loopHarness, timeout time.Duration) <-chan []byte {
	t.Helper()
	out := make(chan []byte, 1)
	go func() {
		deadline := time.Now().Add(timeout)
		for {
			if time.Now().After(deadline) {
				return
			}
			data, err := h.mockIn.Recv()
			if err == nil {
				out <- data
				return
			}
			if !errors.Is(err, shmem.ErrNoData) {
				return
			}
			runtime.Gosched()
		}
	}()
	return out
}

func encodeStressPrePut(t *testing.T, regionID uint32, reqID uint64) []byte {
	t.Helper()
	frame, err := buildStressPrePutFrame(regionID, reqID)
	if err != nil {
		t.Fatalf("encode PrePut region=%d req_id=%d: %v", regionID, reqID, err)
	}
	return frame
}

// buildStressPrePutFrame is shared between the Test and Benchmark drivers:
// both mint a valid PrePut wire frame for a given (region_id, req_id), and
// the fatal-helper variants for *testing.T / *testing.B wrap this.
func buildStressPrePutFrame(regionID uint32, reqID uint64) ([]byte, error) {
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
		return nil, fmt.Errorf("marshal PrePutRequest: %w", err)
	}
	outerBytes, err := proto.Marshal(&wirepb.Request{HookCtx: innerBytes})
	if err != nil {
		return nil, fmt.Errorf("marshal wirepb.Request: %w", err)
	}
	var encoded bytes.Buffer
	if err := wire.NewEncoder(&encoded).Encode(&wire.Message{
		Type:     wire.TypeRequest,
		ReqID:    reqID,
		RegionID: regionID,
		HookID:   uint8(HookIDPrePut),
		Payload:  outerBytes,
	}); err != nil {
		return nil, fmt.Errorf("wire encode: %w", err)
	}
	return encoded.Bytes(), nil
}
