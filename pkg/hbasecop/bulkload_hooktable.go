// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// bulkLoadHookEntry is the bulk-load analogue of hookEntry: maps a
// HookID to its decoder + a closure that invokes the matching
// BulkLoadObserver method.
type bulkLoadHookEntry struct {
	id     HookID
	name   string
	decode func() proto.Message
	invoke bulkLoadHookInvoker
}

type bulkLoadHookInvoker func(observer BulkLoadObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error)

func preBulkLoadHook[Req proto.Message](method func(BulkLoadObserver, context.Context, ObserverEnv, Req) (HookResult, error)) bulkLoadHookInvoker {
	return func(o BulkLoadObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
		return method(o, ctx, env, req.(Req))
	}
}

// bulkLoadHookTable is the T54 bulk-load dispatch table. Order mirrors
// proto/hooks.proto's HookId bulk-load section (IDs 224-225).
var bulkLoadHookTable = []bulkLoadHookEntry{
	{HookIDPrePrepareBulkLoad, "PrePrepareBulkLoad", newReq[hookpb.PrePrepareBulkLoadRequest], preBulkLoadHook(BulkLoadObserver.PrePrepareBulkLoad)},
	{HookIDPreCleanupBulkLoad, "PreCleanupBulkLoad", newReq[hookpb.PreCleanupBulkLoadRequest], preBulkLoadHook(BulkLoadObserver.PreCleanupBulkLoad)},
}

// bulkLoadHooksByID indexes the bulk-load dispatch table for O(1) lookup.
var bulkLoadHooksByID = func() map[HookID]bulkLoadHookEntry {
	m := make(map[HookID]bulkLoadHookEntry, len(bulkLoadHookTable))
	for _, h := range bulkLoadHookTable {
		m[h.id] = h
	}
	return m
}()
