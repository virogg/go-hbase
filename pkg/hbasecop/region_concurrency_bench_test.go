// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/shmem"
	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
)

type busyObserver struct {
	UnimplementedRegionObserver

	spinIters int
}

func (o busyObserver) PrePut(_ context.Context, _ ObserverEnv, _ *hbasepb.MutationProto) (HookResult, error) {
	x := uint64(0)
	for i := 0; i < o.spinIters; i++ {
		x = x*1103515245 + 12345 + uint64(i)
	}
	if x == 1 {
		runtime.Gosched()
	}
	return HookResult{}, nil
}

func BenchmarkRegionConcurrencyThroughput(b *testing.B) {
	const (
		ringCapacity = 1024
		spinIters    = 200_000
	)
	for _, regions := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("regions=%d", regions), func(b *testing.B) {
			runBenchRegionThroughput(b, regions, ringCapacity, spinIters)
		})
	}
}

func runBenchRegionThroughput(b *testing.B, regions, ringCapacity, spinIters int) {
	b.Helper()

	h := openBenchHarness(b, ringCapacity)
	obs := busyObserver{spinIters: spinIters}
	d := newDispatcher(obs, nil)
	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            h.loopIn,
		OutCh:           h.loopOut,
		HeartbeatPeriod: -1,
		Handler:         d.dispatch,
	})
	if err != nil {
		b.Fatalf("cpruntime.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var loopWG sync.WaitGroup
	loopWG.Add(1)
	go func() {
		defer loopWG.Done()
		_ = loop.Run(ctx)
	}()

	frames := make([][]byte, regions)
	for r := range regions {
		frames[r] = encodeBenchPrePut(b, uint32(r+1), uint64(r+1))
	}

	b.ResetTimer()
	b.ReportAllocs()

	senderDone := make(chan error, 1)
	go func() {
		for i := 0; i < b.N; i++ {
			frame := frames[i%regions]
			for {
				err := h.mockOut.Send(frame)
				if err == nil {
					break
				}
				if !errors.Is(err, shmem.ErrRingFull) {
					senderDone <- fmt.Errorf("send i=%d: %w", i, err)
					return
				}
				runtime.Gosched()
			}
		}
		senderDone <- nil
	}()

	for got := 0; got < b.N; {
		data, err := h.mockIn.Recv()
		if errors.Is(err, shmem.ErrNoData) {
			runtime.Gosched()
			continue
		}
		if err != nil {
			b.Fatalf("recv: %v", err)
		}
		resp, err := wire.NewDecoder(bytes.NewReader(data)).Decode()
		if err != nil {
			b.Fatalf("decode: %v", err)
		}
		if resp.Type != wire.TypeResponse {
			b.Fatalf("resp.Type = %v, want Response", resp.Type)
		}
		got++
	}

	b.StopTimer()
	if err := <-senderDone; err != nil {
		b.Fatalf("sender: %v", err)
	}

	cancel()
	loopWG.Wait()
}

func openBenchHarness(b *testing.B, capacity int) *loopHarness {
	b.Helper()
	dir := b.TempDir()
	mkChan := func(file string, role shmem.Role) *shmem.Channel {
		ch, err := shmem.Open(shmem.Config{
			Filename:      file,
			Capacity:      capacity,
			MaxObjectSize: testRingMaxObjectSize,
			Role:          role,
		})
		if err != nil {
			b.Fatalf("shmem.Open: %v", err)
		}
		b.Cleanup(func() { _ = ch.Close() })
		return ch
	}
	return &loopHarness{
		mockOut: mkChan(dir+"/in.mmap", shmem.RoleProducer),
		loopIn:  mkChan(dir+"/in.mmap", shmem.RoleConsumer),
		loopOut: mkChan(dir+"/out.mmap", shmem.RoleProducer),
		mockIn:  mkChan(dir+"/out.mmap", shmem.RoleConsumer),
	}
}

func encodeBenchPrePut(b *testing.B, regionID uint32, reqID uint64) []byte {
	b.Helper()
	frame, err := buildStressPrePutFrame(regionID, reqID)
	if err != nil {
		b.Fatalf("encode PrePut region=%d req_id=%d: %v", regionID, reqID, err)
	}
	return frame
}
