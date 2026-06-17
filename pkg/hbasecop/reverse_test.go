// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

// TE31: ReverseGet outside an endpoint call (no handle on the ctx) fails cleanly
// instead of panicking or blocking.
func TestReverseGetUnavailableWithoutHandle(t *testing.T) {
	if _, err := ReverseGet(context.Background(), nil); err == nil {
		t.Fatal("want error when no reverse handle is on the context")
	}
}

// TE31: inside an endpoint call, ReverseGet routes through the bound
// ReverseClient to the invoking region and returns the Result payload.
func TestReverseGetRoutesToReverseClient(t *testing.T) {
	out := make(chan *wire.Message, 4)
	rc := cpruntime.NewReverseClient(nil)
	rc.Bind(out)
	d := &dispatcher{reverse: rc, logger: newLogger()}

	reqCh := make(chan *wire.Message, 1)
	go func() {
		req := <-out
		reqCh <- req
		respPayload, _ := proto.Marshal(&wirepb.RpcResponse{
			Status:  wirepb.RpcResponse_OK,
			Payload: []byte("VALUE"),
		})
		rc.Deliver(&wire.Message{Type: wire.TypeRpcResponse, ReqID: req.ReqID, Payload: respPayload})
	}()

	ctx := d.withReverse(context.Background(), 9)
	got, err := ReverseGet(ctx, []byte("get-bytes"))
	if err != nil {
		t.Fatalf("ReverseGet: %v", err)
	}
	if string(got) != "VALUE" {
		t.Fatalf("got %q, want %q", got, "VALUE")
	}

	req := <-reqCh
	if req.RegionID != 9 {
		t.Fatalf("reverse request region_id = %d, want 9 (invoking region)", req.RegionID)
	}
}

// When the reverse path is disabled (no bound client), withReverse leaves the
// ctx untouched and ReverseGet reports it as unavailable.
func TestWithReverseNoopWhenDisabled(t *testing.T) {
	d := &dispatcher{logger: newLogger()}
	ctx := d.withReverse(context.Background(), 1)
	if _, err := ReverseGet(ctx, nil); err == nil {
		t.Fatal("want error when reverse path is disabled")
	}
}
