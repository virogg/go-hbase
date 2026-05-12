// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/shmem"
	"github.com/virogg/go-hbase/internal/wire"
)

const (
	testRingCapacity      = 16
	testRingMaxObjectSize = 4096
)

// loopChannels are the four endpoints around a Loop running in-process:
// the Loop's own InCh/OutCh, plus the mock-Java side that drives them.
type loopChannels struct {
	loopIn  *shmem.Channel // RoleConsumer  (Java → Go inbound)
	loopOut *shmem.Channel // RoleProducer  (Go → Java outbound)
	mockOut *shmem.Channel // RoleProducer  (mock-Java sends here)
	mockIn  *shmem.Channel // RoleConsumer  (mock-Java reads here)
}

func openLoopChannels(t *testing.T) *loopChannels {
	t.Helper()

	dir := t.TempDir()
	inFile := filepath.Join(dir, "in.mmap")
	outFile := filepath.Join(dir, "out.mmap")

	mkChan := func(file string, role shmem.Role) *shmem.Channel {
		ch, err := shmem.Open(shmem.Config{
			Filename:      file,
			Capacity:      testRingCapacity,
			MaxObjectSize: testRingMaxObjectSize,
			Role:          role,
		})
		if err != nil {
			t.Fatalf("shmem.Open(%s, %v): %v", file, role, err)
		}
		t.Cleanup(func() { _ = ch.Close() })
		return ch
	}

	return &loopChannels{
		mockOut: mkChan(inFile, shmem.RoleProducer),
		loopIn:  mkChan(inFile, shmem.RoleConsumer),
		loopOut: mkChan(outFile, shmem.RoleProducer),
		mockIn:  mkChan(outFile, shmem.RoleConsumer),
	}
}

func encodeWireFrame(t *testing.T, m *wire.Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := wire.NewEncoder(&buf).Encode(m); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return buf.Bytes()
}

func decodeWireFrame(t *testing.T, data []byte) *wire.Message {
	t.Helper()
	msg, err := wire.NewDecoder(bytes.NewReader(data)).Decode()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return msg
}

// TestLoopPingPong10000 is the T17 acceptance test: 10k ping/pong
// round-trips through a real shmem pair, asserting p99 latency < 1ms.
// Heartbeats are disabled so they do not contend with the latency
// measurement.
func TestLoopPingPong10000(t *testing.T) {
	ch := openLoopChannels(t)

	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            ch.loopIn,
		OutCh:           ch.loopOut,
		HeartbeatPeriod: -1, // disabled
	})
	if err != nil {
		t.Fatalf("cpruntime.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = loop.Run(ctx)
	}()

	const N = 10_000
	latencies := make([]time.Duration, 0, N)

	for i := range N {
		payload := []byte{byte(i), byte(i >> 8)}
		req := &wire.Message{
			Type:    wire.TypeRequest,
			ReqID:   uint64(i + 1),
			HookID:  cpruntime.HookPing,
			Payload: payload,
		}
		frame := encodeWireFrame(t, req)

		start := time.Now()
		for {
			err := ch.mockOut.Send(frame)
			if err == nil {
				break
			}
			if !errors.Is(err, shmem.ErrRingFull) {
				t.Fatalf("iter %d send: %v", i, err)
			}
			runtime.Gosched()
		}

		var resp []byte
		for {
			data, err := ch.mockIn.Recv()
			if err == nil {
				resp = data
				break
			}
			if !errors.Is(err, shmem.ErrNoData) {
				t.Fatalf("iter %d recv: %v", i, err)
			}
			runtime.Gosched()
		}
		latencies = append(latencies, time.Since(start))

		respMsg := decodeWireFrame(t, resp)
		if respMsg.Type != wire.TypeResponse {
			t.Fatalf("iter %d: want Response, got Type=%d", i, respMsg.Type)
		}
		if respMsg.ReqID != req.ReqID {
			t.Fatalf("iter %d: req_id mismatch: %d != %d", i, respMsg.ReqID, req.ReqID)
		}
		if !bytes.Equal(respMsg.Payload, payload) {
			t.Fatalf("iter %d: payload mismatch: %x vs %x", i, respMsg.Payload, payload)
		}
	}

	cancel()
	wg.Wait()

	slices.Sort(latencies)
	p50 := latencies[N/2]
	p99 := latencies[int(float64(N)*0.99)]
	t.Logf("ping/pong latency: p50=%v p99=%v max=%v", p50, p99, latencies[N-1])
	if p99 > time.Millisecond {
		t.Fatalf("p99 latency %v > 1ms", p99)
	}
}

func TestLoopHeartbeats(t *testing.T) {
	ch := openLoopChannels(t)

	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            ch.loopIn,
		OutCh:           ch.loopOut,
		HeartbeatPeriod: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("cpruntime.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = loop.Run(ctx)
	}()

	deadline := time.Now().Add(200 * time.Millisecond)
	beats := 0
	for time.Now().Before(deadline) {
		data, err := ch.mockIn.Recv()
		if err != nil {
			runtime.Gosched()
			continue
		}
		msg := decodeWireFrame(t, data)
		if msg.Type == wire.TypeHeartbeat {
			beats++
		}
	}

	cancel()
	wg.Wait()

	// 200ms / 10ms = 20 expected; allow scheduler slop, demand ≥ 10.
	if beats < 10 {
		t.Fatalf("expected ≥10 heartbeats in 200ms with 10ms period, got %d", beats)
	}
}

func TestLoopShutdownOnContextCancel(t *testing.T) {
	ch := openLoopChannels(t)

	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            ch.loopIn,
		OutCh:           ch.loopOut,
		HeartbeatPeriod: -1,
	})
	if err != nil {
		t.Fatalf("cpruntime.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not exit within 1s of context cancel")
	}
}

func TestNewValidation(t *testing.T) {
	t.Parallel()

	if _, err := cpruntime.New(cpruntime.Config{}); err == nil {
		t.Fatal("New with nil channels: expected error")
	}

	ch := openLoopChannels(t)
	if _, err := cpruntime.New(cpruntime.Config{InCh: ch.loopIn}); err == nil {
		t.Fatal("New with only InCh: expected error")
	}
	if _, err := cpruntime.New(cpruntime.Config{OutCh: ch.loopOut}); err == nil {
		t.Fatal("New with only OutCh: expected error")
	}
}
