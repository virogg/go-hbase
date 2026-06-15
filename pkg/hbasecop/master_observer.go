// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// MasterObserver is the Go-side surface for HBase 2.5 master hooks. It
// covers a curated subset: the 20 most common master hooks (table
// lifecycle, enable/disable, region placement and balance), NOT the
// full ~165-method HBase MasterObserver interface; ACL, quota,
// namespace, snapshot and merge hooks are out of MVP scope (see
// docs/coverage-region-observer.md). Implement the methods whose hooks
// your master coprocessor needs; embed UnimplementedMasterObserver to
// inherit no-op defaults for the rest. Returning HookResult{Bypass:true} from a Pre-* method causes
// the Java MasterObserverAdapter to invoke ObserverContext.bypass(),
// which short-circuits the in-progress master operation. Returning a
// non-nil error fails the call back to the HBase admin client per the
// configured failure policy (T31/T32, same wiring as RegionObserver).
type MasterObserver interface {
	// Table lifecycle.
	PreCreateTable(ctx context.Context, env ObserverEnv, req *hookpb.PreCreateTableRequest) (HookResult, error)
	PostCreateTable(ctx context.Context, env ObserverEnv, req *hookpb.PostCreateTableRequest) error
	PreDeleteTable(ctx context.Context, env ObserverEnv, req *hookpb.PreDeleteTableRequest) (HookResult, error)
	PostDeleteTable(ctx context.Context, env ObserverEnv, req *hookpb.PostDeleteTableRequest) error
	PreModifyTable(ctx context.Context, env ObserverEnv, req *hookpb.PreModifyTableRequest) (HookResult, error)
	PostModifyTable(ctx context.Context, env ObserverEnv, req *hookpb.PostModifyTableRequest) error
	PreTruncateTable(ctx context.Context, env ObserverEnv, req *hookpb.PreTruncateTableRequest) (HookResult, error)
	PostTruncateTable(ctx context.Context, env ObserverEnv, req *hookpb.PostTruncateTableRequest) error

	// Enable / disable.
	PreEnableTable(ctx context.Context, env ObserverEnv, req *hookpb.PreEnableTableRequest) (HookResult, error)
	PostEnableTable(ctx context.Context, env ObserverEnv, req *hookpb.PostEnableTableRequest) error
	PreDisableTable(ctx context.Context, env ObserverEnv, req *hookpb.PreDisableTableRequest) (HookResult, error)
	PostDisableTable(ctx context.Context, env ObserverEnv, req *hookpb.PostDisableTableRequest) error

	// Region placement.
	PreMove(ctx context.Context, env ObserverEnv, req *hookpb.PreMoveRequest) (HookResult, error)
	PostMove(ctx context.Context, env ObserverEnv, req *hookpb.PostMoveRequest) error
	PreAssign(ctx context.Context, env ObserverEnv, req *hookpb.PreAssignRequest) (HookResult, error)
	PostAssign(ctx context.Context, env ObserverEnv, req *hookpb.PostAssignRequest) error
	PreUnassign(ctx context.Context, env ObserverEnv, req *hookpb.PreUnassignRequest) (HookResult, error)
	PostUnassign(ctx context.Context, env ObserverEnv, req *hookpb.PostUnassignRequest) error

	// Cluster balance.
	PreBalance(ctx context.Context, env ObserverEnv, req *hookpb.PreBalanceRequest) (HookResult, error)
	PostBalance(ctx context.Context, env ObserverEnv, req *hookpb.PostBalanceRequest) error
}

// UnimplementedMasterObserver provides no-op implementations of every
// MasterObserver method. Embed it in your own struct so adding a new
// hook to MasterObserver later doesn't break your code.
type UnimplementedMasterObserver struct{}

var _ MasterObserver = UnimplementedMasterObserver{}

// The methods below are intentionally undocumented one-liners: they
// all do the same thing (return the zero value, no error). The type
// doc-comment above is the single source of truth for the contract.
//revive:disable:exported

func (UnimplementedMasterObserver) PreCreateTable(context.Context, ObserverEnv, *hookpb.PreCreateTableRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostCreateTable(context.Context, ObserverEnv, *hookpb.PostCreateTableRequest) error {
	return nil
}

func (UnimplementedMasterObserver) PreDeleteTable(context.Context, ObserverEnv, *hookpb.PreDeleteTableRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostDeleteTable(context.Context, ObserverEnv, *hookpb.PostDeleteTableRequest) error {
	return nil
}

func (UnimplementedMasterObserver) PreModifyTable(context.Context, ObserverEnv, *hookpb.PreModifyTableRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostModifyTable(context.Context, ObserverEnv, *hookpb.PostModifyTableRequest) error {
	return nil
}

func (UnimplementedMasterObserver) PreTruncateTable(context.Context, ObserverEnv, *hookpb.PreTruncateTableRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostTruncateTable(context.Context, ObserverEnv, *hookpb.PostTruncateTableRequest) error {
	return nil
}

func (UnimplementedMasterObserver) PreEnableTable(context.Context, ObserverEnv, *hookpb.PreEnableTableRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostEnableTable(context.Context, ObserverEnv, *hookpb.PostEnableTableRequest) error {
	return nil
}

func (UnimplementedMasterObserver) PreDisableTable(context.Context, ObserverEnv, *hookpb.PreDisableTableRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostDisableTable(context.Context, ObserverEnv, *hookpb.PostDisableTableRequest) error {
	return nil
}

func (UnimplementedMasterObserver) PreMove(context.Context, ObserverEnv, *hookpb.PreMoveRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostMove(context.Context, ObserverEnv, *hookpb.PostMoveRequest) error {
	return nil
}

func (UnimplementedMasterObserver) PreAssign(context.Context, ObserverEnv, *hookpb.PreAssignRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostAssign(context.Context, ObserverEnv, *hookpb.PostAssignRequest) error {
	return nil
}

func (UnimplementedMasterObserver) PreUnassign(context.Context, ObserverEnv, *hookpb.PreUnassignRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostUnassign(context.Context, ObserverEnv, *hookpb.PostUnassignRequest) error {
	return nil
}

func (UnimplementedMasterObserver) PreBalance(context.Context, ObserverEnv, *hookpb.PreBalanceRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedMasterObserver) PostBalance(context.Context, ObserverEnv, *hookpb.PostBalanceRequest) error {
	return nil
}
