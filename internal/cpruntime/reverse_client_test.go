// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

// TE31: ReverseClient.Get marshals an RpcRequest(GET), sends it on the bound
// writer, and blocks until the matching RpcResponse is delivered by req_id.
func TestReverseClientGetRoutesResponseByReqID(t *testing.T) {
	out := make(chan *wire.Message, 4)
	rc := cpruntime.NewReverseClient(nil)
	rc.Bind(out)

	getProto, err := proto.Marshal(&hbasepb.Get{Row: []byte("row-1")})
	if err != nil {
		t.Fatalf("marshal Get: %v", err)
	}

	gotReq := make(chan *wire.Message, 1)
	go func() {
		req := <-out
		gotReq <- req
		respPayload, _ := proto.Marshal(&wirepb.RpcResponse{
			Status:  wirepb.RpcResponse_OK,
			Payload: []byte("RESULT-BYTES"),
		})
		rc.Deliver(&wire.Message{Type: wire.TypeRPCResponse, ReqID: req.ReqID, Payload: respPayload})
	}()

	resp, err := rc.Get(context.Background(), 7, getProto)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetStatus() != wirepb.RpcResponse_OK {
		t.Fatalf("status = %v, want OK", resp.GetStatus())
	}
	if string(resp.GetPayload()) != "RESULT-BYTES" {
		t.Fatalf("payload = %q, want %q", resp.GetPayload(), "RESULT-BYTES")
	}

	// The frame that went out must be a GET RpcRequest carrying the Get proto
	// and the target region id in the wire header.
	req := <-gotReq
	if req.Type != wire.TypeRPCRequest {
		t.Fatalf("outbound type = %v, want RpcRequest", req.Type)
	}
	if req.RegionID != 7 {
		t.Fatalf("outbound region_id = %d, want 7", req.RegionID)
	}
	var rr wirepb.RpcRequest
	if err := proto.Unmarshal(req.Payload, &rr); err != nil {
		t.Fatalf("unmarshal RpcRequest: %v", err)
	}
	if rr.GetOp() != wirepb.RpcRequest_GET {
		t.Fatalf("op = %v, want GET", rr.GetOp())
	}
	if string(rr.GetOpPayload()) != string(getProto) {
		t.Fatalf("op_payload mismatch")
	}
}

// TE41: ReverseClient.Mutate marshals an RpcRequest(MUTATE) carrying the
// vendored MutationProto, sends it on the bound writer, and returns OK.
func TestReverseClientMutate(t *testing.T) {
	out := make(chan *wire.Message, 4)
	rc := cpruntime.NewReverseClient(nil)
	rc.Bind(out)

	mutProto, err := proto.Marshal(&hbasepb.MutationProto{
		Row:        []byte("row-1"),
		MutateType: hbasepb.MutationProto_PUT.Enum(),
	})
	if err != nil {
		t.Fatalf("marshal MutationProto: %v", err)
	}

	gotReq := make(chan *wire.Message, 1)
	go func() {
		req := <-out
		gotReq <- req
		respPayload, _ := proto.Marshal(&wirepb.RpcResponse{Status: wirepb.RpcResponse_OK})
		rc.Deliver(&wire.Message{Type: wire.TypeRPCResponse, ReqID: req.ReqID, Payload: respPayload})
	}()

	if err := rc.Mutate(context.Background(), 9, mutProto); err != nil {
		t.Fatalf("Mutate: %v", err)
	}

	req := <-gotReq
	if req.Type != wire.TypeRPCRequest {
		t.Fatalf("outbound type = %v, want RpcRequest", req.Type)
	}
	if req.RegionID != 9 {
		t.Fatalf("outbound region_id = %d, want 9", req.RegionID)
	}
	var rr wirepb.RpcRequest
	if err := proto.Unmarshal(req.Payload, &rr); err != nil {
		t.Fatalf("unmarshal RpcRequest: %v", err)
	}
	if rr.GetOp() != wirepb.RpcRequest_MUTATE {
		t.Fatalf("op = %v, want MUTATE", rr.GetOp())
	}
	if string(rr.GetOpPayload()) != string(mutProto) {
		t.Fatalf("op_payload mismatch")
	}
}

// An ERROR status surfaces as an error carrying the servicer's detail.
func TestReverseClientGetErrorStatus(t *testing.T) {
	out := make(chan *wire.Message, 4)
	rc := cpruntime.NewReverseClient(nil)
	rc.Bind(out)

	go func() {
		req := <-out
		respPayload, _ := proto.Marshal(&wirepb.RpcResponse{
			Status:  wirepb.RpcResponse_ERROR,
			Payload: []byte("no region for id"),
		})
		rc.Deliver(&wire.Message{Type: wire.TypeRPCResponse, ReqID: req.ReqID, Payload: respPayload})
	}()

	_, err := rc.Get(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("want error for ERROR status, got nil")
	}
}

// With no matching reply the call blocks until ctx is done and returns its error
// (a response delivered for a different req_id must not wake it).
func TestReverseClientGetBlocksUntilCtxDone(t *testing.T) {
	out := make(chan *wire.Message, 4)
	rc := cpruntime.NewReverseClient(nil)
	rc.Bind(out)

	go func() {
		<-out // drain the request, then deliver a mismatched req_id
		rc.Deliver(&wire.Message{Type: wire.TypeRPCResponse, ReqID: 999999})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := rc.Get(ctx, 1, nil)
	if err == nil {
		t.Fatal("want ctx error, got nil")
	}
}

// req_ids are monotonic and start at 1 (0 is the reserved no-correlation id).
func TestReverseClientReqIDStartsAtOneMonotonic(t *testing.T) {
	out := make(chan *wire.Message, 8)
	rc := cpruntime.NewReverseClient(nil)
	rc.Bind(out)

	okPayload, _ := proto.Marshal(&wirepb.RpcResponse{Status: wirepb.RpcResponse_OK})
	for want := uint64(1); want <= 3; want++ {
		done := make(chan struct{})
		go func() { _, _ = rc.Get(context.Background(), 1, nil); close(done) }()

		req := <-out
		if req.ReqID != want {
			t.Fatalf("req_id = %d, want %d", req.ReqID, want)
		}
		rc.Deliver(&wire.Message{Type: wire.TypeRPCResponse, ReqID: req.ReqID, Payload: okPayload})
		<-done
	}
}

// TE34 hardening: a reverse call is bounded by a per-call deadline, so a
// dropped/lost reply (the bridge swallowed a bulk-send failure) returns a clean
// error instead of blocking the endpoint goroutine for the process lifetime.
func TestReverseClientCallDeadlineBoundsLostReply(t *testing.T) {
	out := make(chan *wire.Message, 4)
	rc := cpruntime.NewReverseClient(nil)
	rc.SetTimeout(150 * time.Millisecond)
	rc.Bind(out)

	go func() { <-out }() // consume the request but never deliver a reply

	start := time.Now()
	_, err := rc.Get(context.Background(), 1, nil)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want a deadline error when no reply arrives")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Get blocked %v; the per-call deadline must bound it", elapsed)
	}
}

// Get on an unbound client fails cleanly rather than panicking on a nil writer.
func TestReverseClientGetUnboundErrors(t *testing.T) {
	rc := cpruntime.NewReverseClient(nil)
	if _, err := rc.Get(context.Background(), 1, nil); err == nil {
		t.Fatal("want error for unbound client, got nil")
	}
}

// TE33: the reverse scanner lifecycle — OpenScanner returns the bridge-assigned
// scanner id; ScanNext pulls a batch reporting has_more; ScanClose deregisters.
// Each op's RpcRequest carries the right op/call/scanner/region.
func TestReverseClientScannerLifecycle(t *testing.T) {
	out := make(chan *wire.Message, 8)
	rc := cpruntime.NewReverseClient(nil)
	rc.Bind(out)

	type seen struct {
		op                wirepb.RpcRequest_Op
		callID, scannerID uint64
		regionID          uint32
	}
	got := make(chan seen, 3)
	go func() {
		for range 3 {
			req := <-out
			var rr wirepb.RpcRequest
			_ = proto.Unmarshal(req.Payload, &rr)
			got <- seen{rr.GetOp(), rr.GetCallId(), rr.GetScannerId(), req.RegionID}
			resp := &wirepb.RpcResponse{Status: wirepb.RpcResponse_OK}
			switch rr.GetOp() {
			case wirepb.RpcRequest_SCAN_OPEN:
				resp.ScannerId = 42
			case wirepb.RpcRequest_SCAN_NEXT:
				resp.Payload, _ = proto.Marshal(&hbasepb.Result{})
				resp.HasMore = false
			}
			rp, _ := proto.Marshal(resp)
			rc.Deliver(&wire.Message{Type: wire.TypeRPCResponse, ReqID: req.ReqID, Payload: rp})
		}
	}()

	scanProto, _ := proto.Marshal(&hbasepb.Scan{})
	sid, err := rc.OpenScanner(context.Background(), 5, 100, scanProto)
	if err != nil {
		t.Fatalf("OpenScanner: %v", err)
	}
	if sid != 42 {
		t.Fatalf("scanner id = %d, want 42", sid)
	}

	resp, err := rc.ScanNext(context.Background(), 5, 100, sid)
	if err != nil {
		t.Fatalf("ScanNext: %v", err)
	}
	if resp.GetHasMore() {
		t.Fatal("has_more = true, want false")
	}

	if err := rc.ScanClose(context.Background(), 5, 100, sid); err != nil {
		t.Fatalf("ScanClose: %v", err)
	}

	o := <-got
	if o.op != wirepb.RpcRequest_SCAN_OPEN || o.regionID != 5 || o.callID != 100 {
		t.Fatalf("OPEN req = %+v", o)
	}
	n := <-got
	if n.op != wirepb.RpcRequest_SCAN_NEXT || n.scannerID != 42 || n.callID != 100 {
		t.Fatalf("NEXT req = %+v", n)
	}
	cl := <-got
	if cl.op != wirepb.RpcRequest_SCAN_CLOSE || cl.scannerID != 42 {
		t.Fatalf("CLOSE req = %+v", cl)
	}
}

// OpenScanner surfaces a servicer ERROR (e.g. per-call scanner cap) as an error.
func TestReverseClientOpenScannerError(t *testing.T) {
	out := make(chan *wire.Message, 4)
	rc := cpruntime.NewReverseClient(nil)
	rc.Bind(out)

	go func() {
		req := <-out
		rp, _ := proto.Marshal(&wirepb.RpcResponse{
			Status:  wirepb.RpcResponse_ERROR,
			Payload: []byte("max scanners per call exceeded"),
		})
		rc.Deliver(&wire.Message{Type: wire.TypeRPCResponse, ReqID: req.ReqID, Payload: rp})
	}()

	if _, err := rc.OpenScanner(context.Background(), 1, 1, nil); err == nil {
		t.Fatal("want error for ERROR status on OpenScanner, got nil")
	}
}
