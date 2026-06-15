// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// BulkLoadObserver is the Go-side mirror of HBase 2.5 BulkLoadObserver.
// Both hooks are region-scoped - the table/region the bulk load targets
// travels in ObserverEnv. Implement the methods whose hooks your
// observer needs; embed UnimplementedBulkLoadObserver to inherit no-op
// defaults for the rest.
//
// Returning HookResult{Bypass:true} from a Pre-* method causes the Java
// BulkLoadObserverAdapter to invoke ObserverContext.bypass(), which
// short-circuits the in-progress bulk-load step. Returning a non-nil
// error fails the call back to the HBase admin client per the
// configured failure policy (T31/T32, same wiring as every other
// surface).
type BulkLoadObserver interface {
	PrePrepareBulkLoad(ctx context.Context, env ObserverEnv, req *hookpb.PrePrepareBulkLoadRequest) (HookResult, error)
	PreCleanupBulkLoad(ctx context.Context, env ObserverEnv, req *hookpb.PreCleanupBulkLoadRequest) (HookResult, error)
}

// UnimplementedBulkLoadObserver provides no-op implementations of every
// BulkLoadObserver method. Embed it in your own struct so adding a new
// hook to BulkLoadObserver later doesn't break your code.
type UnimplementedBulkLoadObserver struct{}

var _ BulkLoadObserver = UnimplementedBulkLoadObserver{}

// The methods below are intentionally undocumented one-liners - they all
// do the same thing (return the zero value, no error). The type
// doc-comment above is the single source of truth for the contract.
//revive:disable:exported

func (UnimplementedBulkLoadObserver) PrePrepareBulkLoad(context.Context, ObserverEnv, *hookpb.PrePrepareBulkLoadRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedBulkLoadObserver) PreCleanupBulkLoad(context.Context, ObserverEnv, *hookpb.PreCleanupBulkLoadRequest) (HookResult, error) {
	return HookResult{}, nil
}
