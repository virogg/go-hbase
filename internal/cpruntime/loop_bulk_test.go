// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/shmem"
	"github.com/virogg/go-hbase/internal/wire"
)

func openBulkPair(t *testing.T) (mockOut, loopIn *shmem.Channel) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "bulk.mmap")
	mk := func(role shmem.Role) *shmem.Channel {
		ch, err := shmem.Open(shmem.Config{
			Filename:      file,
			Capacity:      testRingCapacity,
			MaxObjectSize: testRingMaxObjectSize,
			Role:          role,
		})
		if err != nil {
			t.Fatalf("shmem.Open(bulk, %v): %v", role, err)
		}
		t.Cleanup(func() { _ = ch.Close() })
		return ch
	}
	mockOut = mk(shmem.RoleProducer) // producer creates the segment first
	loopIn = mk(shmem.RoleConsumer)
	return mockOut, loopIn
}

func TestLoopBulkReaderDeliversRpcResponse(t *testing.T) {
	ch := openLoopChannels(t)
	bulkMockOut, bulkIn := openBulkPair(t)

	got := make(chan *wire.Message, 1)
	loop, err := cpruntime.New(cpruntime.Config{
		InCh:                   ch.loopIn,
		OutCh:                  ch.loopOut,
		InBulkCh:               bulkIn,
		HeartbeatPeriod:        -1,
		ReverseResponseHandler: func(m *wire.Message) { got <- m },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = loop.Run(ctx); close(done) }()

	want := &wire.Message{Type: wire.TypeRPCResponse, ReqID: 1234, Payload: []byte{0xAA, 0xBB}}
	if err := bulkMockOut.Send(encodeWireFrame(t, want)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-got:
		if m.Type != wire.TypeRPCResponse || m.ReqID != 1234 {
			t.Fatalf("handler got %+v, want RpcResponse req_id=1234", m)
		}
		if string(m.Payload) != string(want.Payload) {
			t.Fatalf("payload = %x, want %x", m.Payload, want.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bulk reader did not route an RpcResponse to the ReverseResponseHandler")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel (bulk reader leaked)")
	}
}
