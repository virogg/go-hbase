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
// env (Tier 2, TE32) gives the handler region-local read access via
// [EndpointEnv.Get], so it can read data inside the call — including
// data-dependent reads (read A, then read B by a key from A). When the reverse
// path is disabled, env.Get returns an error; an endpoint that does no reverse
// reads can ignore env entirely.
//
// ctx is the Go process lifetime context, not the client's call deadline: the
// hbasecop.endpoint.timeout budget is enforced on the Java side (the bridge
// abandons the call and returns an error when it expires), so ctx.Done fires
// only on process shutdown. A long handler should therefore bound its own work;
// reverse reads via env are separately deadline-bounded (see
// HBASECOP_REVERSE_CALL_TIMEOUT_MS, derived from the same endpoint timeout).
type Endpoint interface {
	Call(ctx context.Context, env *EndpointEnv, method string, payload []byte) ([]byte, error)
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
	// TE32/TE33: hand the handler an EndpointEnv bound to the invoking region so it
	// can read region-local data (env.Get / env.OpenScanner). callID = the invoke's
	// req_id groups this call's scanners for lifecycle/reaping.
	env := &EndpointEnv{rc: d.reverse, regionID: req.RegionID, callID: req.ReqID}
	out, callErr := d.callEndpoint(ctx, env, invoke.GetMethod(), invoke.GetPayload())
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
	ctx context.Context, env *EndpointEnv, method string, payload []byte,
) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Error("hbasecop: endpoint panic recovered",
				"method", method, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("endpoint panic in %q: %v", method, r)
		}
	}()
	return d.endpoint.Call(ctx, env, method, payload)
}
