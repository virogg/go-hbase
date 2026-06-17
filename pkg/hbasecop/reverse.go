// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"errors"

	"github.com/virogg/go-hbase/internal/cpruntime"
)

// reverseCtxKey keys the per-call reverse-RPC handle carried on the context an
// endpoint Call receives.
type reverseCtxKey struct{}

// reverseHandle binds the process-wide reverse-RPC client to the region the
// in-flight endpoint was invoked on, so ReverseGet targets the right region.
type reverseHandle struct {
	rc       *cpruntime.ReverseClient
	regionID uint32
}

// ReverseGet (Tier 2, TE31) issues a region-local GET from inside an
// [Endpoint.Call], against the region the endpoint was invoked on. getProto is a
// marshalled vendored HBase Get; the returned bytes are a marshalled vendored
// HBase Result.
//
// This is minimal plumbing: the ergonomic EndpointEnv.Get/OpenScanner API lands
// in TE34. ReverseGet is only valid inside an endpoint call on a build where the
// supervisor provisioned the reverse (bulk) ring; otherwise it returns an error
// rather than blocking. The passed ctx bounds the call.
func ReverseGet(ctx context.Context, getProto []byte) ([]byte, error) {
	h, ok := ctx.Value(reverseCtxKey{}).(*reverseHandle)
	if !ok || h == nil || h.rc == nil {
		return nil, errors.New(
			"hbasecop: reverse GET unavailable (not in an endpoint call, or reverse path disabled)")
	}
	resp, err := h.rc.Get(ctx, h.regionID, getProto)
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

// withReverse returns ctx carrying the reverse-RPC handle for an endpoint invoke
// on region regionID, so ReverseGet inside the handler reaches the bound client.
// When the reverse path is disabled (no bulk ring), ctx is returned unchanged
// and ReverseGet reports it as unavailable.
func (d *dispatcher) withReverse(ctx context.Context, regionID uint32) context.Context {
	if d.reverse == nil {
		return ctx
	}
	return context.WithValue(ctx, reverseCtxKey{}, &reverseHandle{rc: d.reverse, regionID: regionID})
}
