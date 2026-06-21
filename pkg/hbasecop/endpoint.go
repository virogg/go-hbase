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

type Endpoint interface {
	Call(ctx context.Context, env *EndpointEnv, method string, payload []byte) ([]byte, error)
}

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
