// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

type WALObserver interface {
	PreWALWrite(ctx context.Context, env ObserverEnv, req *hookpb.PreWALWriteRequest) (HookResult, error)
	PostWALWrite(ctx context.Context, env ObserverEnv, req *hookpb.PostWALWriteRequest) error

	PreWALRoll(ctx context.Context, env ObserverEnv, req *hookpb.PreWALRollRequest) (HookResult, error)
	PostWALRoll(ctx context.Context, env ObserverEnv, req *hookpb.PostWALRollRequest) error
}

type UnimplementedWALObserver struct{}

var _ WALObserver = UnimplementedWALObserver{}

// The methods below are intentionally undocumented one-liners - they all
// do the same thing (return the zero value, no error). The type
// doc-comment above is the single source of truth for the contract.
//revive:disable:exported

func (UnimplementedWALObserver) PreWALWrite(context.Context, ObserverEnv, *hookpb.PreWALWriteRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedWALObserver) PostWALWrite(context.Context, ObserverEnv, *hookpb.PostWALWriteRequest) error {
	return nil
}

func (UnimplementedWALObserver) PreWALRoll(context.Context, ObserverEnv, *hookpb.PreWALRollRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedWALObserver) PostWALRoll(context.Context, ObserverEnv, *hookpb.PostWALRollRequest) error {
	return nil
}
