// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime_test

import (
	"context"
	"testing"
	"time"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/wire"
)

// TE12: an inbound RpcResponse frame (the reply to a Go-initiated reverse RPC)
// is routed to the ReverseResponseHandler stub, keyed by the wire-header
// req_id, without the reader unmarshalling the protobuf payload.
func TestLoopRoutesRpcResponseToReverseHandler(t *testing.T) {
	ch := openLoopChannels(t)

	got := make(chan *wire.Message, 1)
	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            ch.loopIn,
		OutCh:           ch.loopOut,
		HeartbeatPeriod: -1, // disabled: keep the outbound ring quiet
		ReverseResponseHandler: func(m *wire.Message) {
			got <- m
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = loop.Run(ctx); close(done) }()

	want := &wire.Message{Type: wire.TypeRpcResponse, ReqID: 4242, Payload: []byte{0x01, 0x02}}
	if err := ch.mockOut.Send(encodeWireFrame(t, want)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-got:
		if m.Type != wire.TypeRpcResponse || m.ReqID != 4242 {
			t.Fatalf("handler got %+v, want RpcResponse req_id=4242", m)
		}
		if string(m.Payload) != string(want.Payload) {
			t.Fatalf("payload = %x, want %x", m.Payload, want.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReverseResponseHandler was not invoked for an RpcResponse frame")
	}

	cancel()
	<-done
}
