// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// WALObserver is the Go-side mirror of HBase 2.5 WALObserver. Implement
// the methods whose hooks your WAL coprocessor needs; embed
// UnimplementedWALObserver to inherit no-op defaults for the rest.
//
// preWALWrite / postWALWrite sit on the latency-critical WAL append hot
// path — keep their implementations cheap. Returning HookResult{Bypass:true}
// from a Pre-* method causes the Java WALObserverAdapter to invoke
// ObserverContext.bypass(); returning a non-nil error fails the call
// back to HBase per the configured failure policy (T31/T32, same wiring
// as the region, master and region-server surfaces).
type WALObserver interface {
	// WAL write — latency-critical hot path.
	PreWALWrite(ctx context.Context, env ObserverEnv, req *hookpb.PreWALWriteRequest) (HookResult, error)
	PostWALWrite(ctx context.Context, env ObserverEnv, req *hookpb.PostWALWriteRequest) error

	// WAL roll.
	PreWALRoll(ctx context.Context, env ObserverEnv, req *hookpb.PreWALRollRequest) (HookResult, error)
	PostWALRoll(ctx context.Context, env ObserverEnv, req *hookpb.PostWALRollRequest) error
}

// UnimplementedWALObserver provides no-op implementations of every
// WALObserver method. Embed it in your own struct so adding a new hook
// to WALObserver later doesn't break your code.
type UnimplementedWALObserver struct{}

var _ WALObserver = UnimplementedWALObserver{}

// The methods below are intentionally undocumented one-liners — they all
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
