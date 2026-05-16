// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// masterHookEntry is the master-side analogue of hookEntry: maps a
// HookID to its decoder + a closure that invokes the matching
// MasterObserver method.
type masterHookEntry struct {
	id     HookID
	name   string
	decode func() proto.Message
	invoke masterHookInvoker
}

type masterHookInvoker func(observer MasterObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error)

func preMasterHook[Req proto.Message](method func(MasterObserver, context.Context, ObserverEnv, Req) (HookResult, error)) masterHookInvoker {
	return func(o MasterObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
		return method(o, ctx, env, req.(Req))
	}
}

func postMasterHook[Req proto.Message](method func(MasterObserver, context.Context, ObserverEnv, Req) error) masterHookInvoker {
	return func(o MasterObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
		return HookResult{}, method(o, ctx, env, req.(Req))
	}
}

// masterHookTable is the T51 master dispatch table. Order mirrors
// proto/hooks.proto's HookId master section (IDs 100-119).
var masterHookTable = []masterHookEntry{
	{HookIDPreCreateTable, "PreCreateTable", newReq[hookpb.PreCreateTableRequest], preMasterHook(MasterObserver.PreCreateTable)},
	{HookIDPostCreateTable, "PostCreateTable", newReq[hookpb.PostCreateTableRequest], postMasterHook(MasterObserver.PostCreateTable)},
	{HookIDPreDeleteTable, "PreDeleteTable", newReq[hookpb.PreDeleteTableRequest], preMasterHook(MasterObserver.PreDeleteTable)},
	{HookIDPostDeleteTable, "PostDeleteTable", newReq[hookpb.PostDeleteTableRequest], postMasterHook(MasterObserver.PostDeleteTable)},
	{HookIDPreModifyTable, "PreModifyTable", newReq[hookpb.PreModifyTableRequest], preMasterHook(MasterObserver.PreModifyTable)},
	{HookIDPostModifyTable, "PostModifyTable", newReq[hookpb.PostModifyTableRequest], postMasterHook(MasterObserver.PostModifyTable)},
	{HookIDPreTruncateTable, "PreTruncateTable", newReq[hookpb.PreTruncateTableRequest], preMasterHook(MasterObserver.PreTruncateTable)},
	{HookIDPostTruncateTable, "PostTruncateTable", newReq[hookpb.PostTruncateTableRequest], postMasterHook(MasterObserver.PostTruncateTable)},

	{HookIDPreEnableTable, "PreEnableTable", newReq[hookpb.PreEnableTableRequest], preMasterHook(MasterObserver.PreEnableTable)},
	{HookIDPostEnableTable, "PostEnableTable", newReq[hookpb.PostEnableTableRequest], postMasterHook(MasterObserver.PostEnableTable)},
	{HookIDPreDisableTable, "PreDisableTable", newReq[hookpb.PreDisableTableRequest], preMasterHook(MasterObserver.PreDisableTable)},
	{HookIDPostDisableTable, "PostDisableTable", newReq[hookpb.PostDisableTableRequest], postMasterHook(MasterObserver.PostDisableTable)},

	{HookIDPreMove, "PreMove", newReq[hookpb.PreMoveRequest], preMasterHook(MasterObserver.PreMove)},
	{HookIDPostMove, "PostMove", newReq[hookpb.PostMoveRequest], postMasterHook(MasterObserver.PostMove)},
	{HookIDPreAssign, "PreAssign", newReq[hookpb.PreAssignRequest], preMasterHook(MasterObserver.PreAssign)},
	{HookIDPostAssign, "PostAssign", newReq[hookpb.PostAssignRequest], postMasterHook(MasterObserver.PostAssign)},
	{HookIDPreUnassign, "PreUnassign", newReq[hookpb.PreUnassignRequest], preMasterHook(MasterObserver.PreUnassign)},
	{HookIDPostUnassign, "PostUnassign", newReq[hookpb.PostUnassignRequest], postMasterHook(MasterObserver.PostUnassign)},

	{HookIDPreBalance, "PreBalance", newReq[hookpb.PreBalanceRequest], preMasterHook(MasterObserver.PreBalance)},
	{HookIDPostBalance, "PostBalance", newReq[hookpb.PostBalanceRequest], postMasterHook(MasterObserver.PostBalance)},
}

// masterHooksByID indexes the master dispatch table for O(1) lookup.
var masterHooksByID = func() map[HookID]masterHookEntry {
	m := make(map[HookID]masterHookEntry, len(masterHookTable))
	for _, h := range masterHookTable {
		m[h.id] = h
	}
	return m
}()
