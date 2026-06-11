// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// RegionServerObserver is the Go-side mirror of HBase 2.5
// RegionServerObserver. Implement the methods whose hooks your
// region-server coprocessor needs; embed UnimplementedRegionServerObserver
// to inherit no-op defaults for the rest. Returning HookResult{Bypass:true}
// from a Pre-* method causes the Java RegionServerObserverAdapter to
// invoke ObserverContext.bypass(), short-circuiting the in-progress
// region-server operation. Returning a non-nil error fails the call back
// to HBase per the configured failure policy (T31/T32, same wiring as
// RegionObserver and MasterObserver).
type RegionServerObserver interface {
	// Server lifecycle.
	PreStopRegionServer(ctx context.Context, env ObserverEnv, req *hookpb.PreStopRegionServerRequest) (HookResult, error)

	// WAL writer roll.
	PreRollWALWriterRequest(ctx context.Context, env ObserverEnv, req *hookpb.PreRollWalWriterRequestRequest) (HookResult, error)
	PostRollWALWriterRequest(ctx context.Context, env ObserverEnv, req *hookpb.PostRollWalWriterRequestRequest) error

	// Replication log entries.
	PreReplicateLogEntries(ctx context.Context, env ObserverEnv, req *hookpb.PreReplicateLogEntriesRequest) (HookResult, error)
	PostReplicateLogEntries(ctx context.Context, env ObserverEnv, req *hookpb.PostReplicateLogEntriesRequest) error

	// Compaction queue clearing.
	PreClearCompactionQueues(ctx context.Context, env ObserverEnv, req *hookpb.PreClearCompactionQueuesRequest) (HookResult, error)
	PostClearCompactionQueues(ctx context.Context, env ObserverEnv, req *hookpb.PostClearCompactionQueuesRequest) error

	// Procedure execution.
	PreExecuteProcedures(ctx context.Context, env ObserverEnv, req *hookpb.PreExecuteProceduresRequest) (HookResult, error)
	PostExecuteProcedures(ctx context.Context, env ObserverEnv, req *hookpb.PostExecuteProceduresRequest) error
}

// UnimplementedRegionServerObserver provides no-op implementations of
// every RegionServerObserver method. Embed it in your own struct so
// adding a new hook to RegionServerObserver later doesn't break your
// code.
type UnimplementedRegionServerObserver struct{}

var _ RegionServerObserver = UnimplementedRegionServerObserver{}

// The methods below are intentionally undocumented one-liners — they all
// do the same thing (return the zero value, no error). The type
// doc-comment above is the single source of truth for the contract.
//revive:disable:exported

func (UnimplementedRegionServerObserver) PreStopRegionServer(context.Context, ObserverEnv, *hookpb.PreStopRegionServerRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionServerObserver) PreRollWALWriterRequest(context.Context, ObserverEnv, *hookpb.PreRollWalWriterRequestRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionServerObserver) PostRollWALWriterRequest(context.Context, ObserverEnv, *hookpb.PostRollWalWriterRequestRequest) error {
	return nil
}

func (UnimplementedRegionServerObserver) PreReplicateLogEntries(context.Context, ObserverEnv, *hookpb.PreReplicateLogEntriesRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionServerObserver) PostReplicateLogEntries(context.Context, ObserverEnv, *hookpb.PostReplicateLogEntriesRequest) error {
	return nil
}

func (UnimplementedRegionServerObserver) PreClearCompactionQueues(context.Context, ObserverEnv, *hookpb.PreClearCompactionQueuesRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionServerObserver) PostClearCompactionQueues(context.Context, ObserverEnv, *hookpb.PostClearCompactionQueuesRequest) error {
	return nil
}

func (UnimplementedRegionServerObserver) PreExecuteProcedures(context.Context, ObserverEnv, *hookpb.PreExecuteProceduresRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionServerObserver) PostExecuteProcedures(context.Context, ObserverEnv, *hookpb.PostExecuteProceduresRequest) error {
	return nil
}
