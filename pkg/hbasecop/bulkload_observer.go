// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

type BulkLoadObserver interface {
	PrePrepareBulkLoad(ctx context.Context, env ObserverEnv, req *hookpb.PrePrepareBulkLoadRequest) (HookResult, error)
	PreCleanupBulkLoad(ctx context.Context, env ObserverEnv, req *hookpb.PreCleanupBulkLoadRequest) (HookResult, error)
}
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
