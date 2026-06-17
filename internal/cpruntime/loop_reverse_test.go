// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime_test

import (
	"context"
	"runtime"
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

// TE22: an inbound EndpointInvoke frame is routed to the EndpointHandler (not
// the observer Handler), and the result the handler returns is sent back out
// correlated by the same req_id.
func TestLoopRoutesEndpointInvokeToEndpointHandler(t *testing.T) {
	ch := openLoopChannels(t)

	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            ch.loopIn,
		OutCh:           ch.loopOut,
		HeartbeatPeriod: -1,
		Handler: func(_ context.Context, _ *wire.Message) *wire.Message {
			t.Error("observer Handler must not see an EndpointInvoke frame")
			return nil
		},
		EndpointHandler: func(_ context.Context, req *wire.Message) *wire.Message {
			return &wire.Message{
				Type:    wire.TypeEndpointResult,
				ReqID:   req.ReqID,
				Payload: append([]byte("R:"), req.Payload...),
			}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = loop.Run(ctx); close(done) }()

	if err := ch.mockOut.Send(encodeWireFrame(t, &wire.Message{
		Type: wire.TypeEndpointInvoke, ReqID: 77, Payload: []byte("X"),
	})); err != nil {
		t.Fatalf("Send: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("no EndpointResult returned for an EndpointInvoke frame")
		default:
		}
		raw, rerr := ch.mockIn.Recv()
		if rerr != nil {
			runtime.Gosched()
			continue
		}
		got := decodeWireFrame(t, raw)
		if got.Type != wire.TypeEndpointResult || got.ReqID != 77 {
			t.Fatalf("got %+v, want EndpointResult req_id=77", got)
		}
		if string(got.Payload) != "R:X" {
			t.Fatalf("payload=%q, want %q", got.Payload, "R:X")
		}
		break
	}

	cancel()
	<-done
}
