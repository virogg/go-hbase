// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
)

// MutationProto is the on-wire HBase mutation (Put/Delete/Append/…) as
// vendored under proto/hbase/Client.proto. The SDK re-exports the type
// so coprocessor authors do not have to depend on internal packages.
//
// Phase 2 only delivers Put-shaped hooks (PrePut/PostPut); broader hook
// coverage and richer accessor types are T41+.
type MutationProto = hbasepb.MutationProto

// RegionObserver is the public SDK contract for region-scoped HBase
// coprocessors. The runtime invokes one method per HBase hook; method
// calls may be concurrent across different regions, so implementations
// must be safe for concurrent use.
//
// Returning a non-nil error from PrePut surfaces as IOException on the
// Java side under strict policy (default in Phase 2); best-effort
// policy (T31/T32) downgrades to a WARN log.
type RegionObserver interface {
	PrePut(ctx context.Context, env ObserverEnv, mutation *MutationProto) (HookResult, error)
	PostPut(ctx context.Context, env ObserverEnv, mutation *MutationProto) error
}

// HookResult is the per-call decision the observer relays back to the
// Java adapter. Bypass=true causes the adapter to call
// ObserverContext.bypass() so HBase skips its own implementation of
// the hook.
type HookResult struct {
	Bypass bool
}
