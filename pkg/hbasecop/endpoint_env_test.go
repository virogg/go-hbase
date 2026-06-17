// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

func revCell(family, qualifier, value string) *hbasepb.Cell {
	return &hbasepb.Cell{
		Family:    []byte(family),
		Qualifier: []byte(qualifier),
		Value:     []byte(value),
	}
}

// serveReverse stands up a fake bridge servicer: it drains reverse GET requests
// off the client's writer, looks the row up in rows, and delivers the matching
// Result. An absent row yields an empty Result. Returns a stop func.
func serveReverse(
	t *testing.T, rc *cpruntime.ReverseClient, out <-chan *wire.Message, rows map[string][]*hbasepb.Cell,
) func() {
	t.Helper()
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case req := <-out:
				var rr wirepb.RpcRequest
				if err := proto.Unmarshal(req.Payload, &rr); err != nil {
					t.Errorf("unmarshal RpcRequest: %v", err)
					continue
				}
				var g hbasepb.Get
				if err := proto.Unmarshal(rr.GetOpPayload(), &g); err != nil {
					t.Errorf("unmarshal Get: %v", err)
					continue
				}
				resultBytes, _ := proto.Marshal(&hbasepb.Result{Cell: rows[string(g.GetRow())]})
				payload, _ := proto.Marshal(&wirepb.RpcResponse{
					Status:  wirepb.RpcResponse_OK,
					Payload: resultBytes,
				})
				rc.Deliver(&wire.Message{
					Type:    wire.TypeRpcResponse,
					ReqID:   req.ReqID,
					Payload: payload,
				})
			}
		}
	}()
	return func() { close(done) }
}

func newTestEnv(t *testing.T, rows map[string][]*hbasepb.Cell) (*EndpointEnv, func()) {
	t.Helper()
	out := make(chan *wire.Message, 8)
	rc := cpruntime.NewReverseClient(nil)
	rc.Bind(out)
	stop := serveReverse(t, rc, out, rows)
	return &EndpointEnv{rc: rc, regionID: 1}, stop
}

// TE32: env.Get reads a row's cells from the invoking region.
func TestEndpointEnvGet(t *testing.T) {
	env, stop := newTestEnv(t, map[string][]*hbasepb.Cell{
		"row-1": {revCell("cf", "q", "hello")},
	})
	defer stop()

	r, err := env.Get(context.Background(), []byte("row-1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	v, ok := CellValue(r, []byte("cf"), []byte("q"))
	if !ok || string(v) != "hello" {
		t.Fatalf("CellValue = %q,%v; want hello,true", v, ok)
	}
}

// TE32: the canonical data-dependent pattern — read A, then read B by a key from
// A — works because each Get is an independent correlated round-trip.
func TestEndpointEnvGetDataDependent(t *testing.T) {
	env, stop := newTestEnv(t, map[string][]*hbasepb.Cell{
		"a": {revCell("cf", "next", "b")},
		"b": {revCell("cf", "val", "deep-value")},
	})
	defer stop()

	a, err := env.Get(context.Background(), []byte("a"))
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	next, ok := CellValue(a, []byte("cf"), []byte("next"))
	if !ok {
		t.Fatal("row a missing cf:next")
	}
	b, err := env.Get(context.Background(), next)
	if err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	val, ok := CellValue(b, []byte("cf"), []byte("val"))
	if !ok || string(val) != "deep-value" {
		t.Fatalf("followed value = %q,%v; want deep-value,true", val, ok)
	}
}

// A missing row is an empty (non-nil) Result, not an error.
func TestEndpointEnvGetMissingRow(t *testing.T) {
	env, stop := newTestEnv(t, map[string][]*hbasepb.Cell{})
	defer stop()

	r, err := env.Get(context.Background(), []byte("nope"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(r.GetCell()) != 0 {
		t.Fatalf("want empty result for absent row, got %d cells", len(r.GetCell()))
	}
}

// With the reverse path disabled (nil client), Get fails cleanly.
func TestEndpointEnvGetUnavailable(t *testing.T) {
	var env EndpointEnv // rc == nil
	if _, err := env.Get(context.Background(), []byte("x")); err == nil {
		t.Fatal("want error when reverse path is disabled")
	}
}
