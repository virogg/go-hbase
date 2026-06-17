// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"fmt"
	"runtime/debug"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

// Endpoint is a Go-implemented HBase coprocessor endpoint (Tier 2): a
// client-initiated, server-side RPC that runs inside the database process.
// Unlike an observer (fired automatically on a read/write path), an endpoint
// is invoked explicitly by a client through HBase's Table.coprocessorService.
//
// Register an Endpoint alongside observers via [RunAll]; the bridge's generic
// GoEndpointService forwards each client Call to this method, routed by method
// name. payload and the returned bytes are opaque, user-defined bodies.
//
// Reverse data access (server-side scan/get of region-local data) arrives in a
// later phase; this minimal surface round-trips a request/response only.
type Endpoint interface {
	Call(ctx context.Context, method string, payload []byte) ([]byte, error)
}

// dispatchEndpoint decodes one EndpointInvoke frame, invokes the registered
// Endpoint, and returns an EndpointResult frame (or an Error frame). It is the
// cpruntime EndpointHandler that inbound ENDPOINT_INVOKE frames route to.
func (d *dispatcher) dispatchEndpoint(ctx context.Context, req *wire.Message) *wire.Message {
	var invoke wirepb.EndpointInvoke
	if err := proto.Unmarshal(req.Payload, &invoke); err != nil {
		d.logger.Error("hbasecop: invalid wirepb.EndpointInvoke", "err", err, "req_id", req.ReqID)
		return d.errorFrame(req, errCodeInvalidWireRequest, "invalid EndpointInvoke: "+err.Error())
	}
	if d.endpoint == nil {
		d.logger.Warn("hbasecop: EndpointInvoke but no endpoint registered",
			"req_id", req.ReqID, "method", invoke.GetMethod())
		return d.errorFrame(req, errCodeUnknownHook, "no endpoint registered")
	}
	// TE31: carry the reverse-RPC handle (bound to the invoking region) on the
	// ctx so the handler can read region-local data via ReverseGet.
	ctx = d.withReverse(ctx, req.RegionID)
	out, callErr := d.callEndpoint(ctx, invoke.GetMethod(), invoke.GetPayload())
	if callErr != nil {
		return d.errorFrame(req, errCodeEndpointFailed, callErr.Error())
	}
	payload, err := proto.Marshal(&wirepb.EndpointResult{Payload: out})
	if err != nil {
		d.logger.Error("hbasecop: marshal EndpointResult failed", "err", err, "req_id", req.ReqID)
		return d.errorFrame(req, errCodeMarshalResponse, "marshal EndpointResult: "+err.Error())
	}
	return &wire.Message{Type: wire.TypeEndpointResult, ReqID: req.ReqID, Payload: payload}
}

// callEndpoint invokes the endpoint, recovering a panic into an error so user
// endpoint code can never crash the shared per-RegionServer Go process (mirrors
// recoverInvoke for observer hooks).
func (d *dispatcher) callEndpoint(
	ctx context.Context, method string, payload []byte,
) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Error("hbasecop: endpoint panic recovered",
				"method", method, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("endpoint panic in %q: %v", method, r)
		}
	}()
	return d.endpoint.Call(ctx, method, payload)
}
