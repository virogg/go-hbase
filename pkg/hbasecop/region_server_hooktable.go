// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// regionServerHookEntry is the region-server analogue of hookEntry:
// maps a HookID to its decoder + a closure that invokes the matching
// RegionServerObserver method.
type regionServerHookEntry struct {
	id     HookID
	name   string
	decode func() proto.Message
	invoke regionServerHookInvoker
}

type regionServerHookInvoker func(observer RegionServerObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error)

func preRegionServerHook[Req proto.Message](method func(RegionServerObserver, context.Context, ObserverEnv, Req) (HookResult, error)) regionServerHookInvoker {
	return func(o RegionServerObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
		return method(o, ctx, env, req.(Req))
	}
}

func postRegionServerHook[Req proto.Message](method func(RegionServerObserver, context.Context, ObserverEnv, Req) error) regionServerHookInvoker {
	return func(o RegionServerObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
		return HookResult{}, method(o, ctx, env, req.(Req))
	}
}

// regionServerHookTable is the T52 region-server dispatch table. Order
// mirrors proto/hooks.proto's HookId region-server section (IDs 200-210).
var regionServerHookTable = []regionServerHookEntry{
	{HookIDPreStopRegionServer, "PreStopRegionServer", newReq[hookpb.PreStopRegionServerRequest], preRegionServerHook(RegionServerObserver.PreStopRegionServer)},

	{HookIDPreRollWALWriterRequest, "PreRollWALWriterRequest", newReq[hookpb.PreRollWalWriterRequestRequest], preRegionServerHook(RegionServerObserver.PreRollWALWriterRequest)},
	{HookIDPostRollWALWriterRequest, "PostRollWALWriterRequest", newReq[hookpb.PostRollWalWriterRequestRequest], postRegionServerHook(RegionServerObserver.PostRollWALWriterRequest)},

	{HookIDPreReplicateLogEntries, "PreReplicateLogEntries", newReq[hookpb.PreReplicateLogEntriesRequest], preRegionServerHook(RegionServerObserver.PreReplicateLogEntries)},
	{HookIDPostReplicateLogEntries, "PostReplicateLogEntries", newReq[hookpb.PostReplicateLogEntriesRequest], postRegionServerHook(RegionServerObserver.PostReplicateLogEntries)},

	{HookIDPreClearCompactionQueues, "PreClearCompactionQueues", newReq[hookpb.PreClearCompactionQueuesRequest], preRegionServerHook(RegionServerObserver.PreClearCompactionQueues)},
	{HookIDPostClearCompactionQueues, "PostClearCompactionQueues", newReq[hookpb.PostClearCompactionQueuesRequest], postRegionServerHook(RegionServerObserver.PostClearCompactionQueues)},

	{HookIDPreExecuteProcedures, "PreExecuteProcedures", newReq[hookpb.PreExecuteProceduresRequest], preRegionServerHook(RegionServerObserver.PreExecuteProcedures)},
	{HookIDPostExecuteProcedures, "PostExecuteProcedures", newReq[hookpb.PostExecuteProceduresRequest], postRegionServerHook(RegionServerObserver.PostExecuteProcedures)},

	{HookIDPreUpdateRegionServerConfiguration, "PreUpdateRegionServerConfiguration", newReq[hookpb.PreUpdateRegionServerConfigurationRequest], preRegionServerHook(RegionServerObserver.PreUpdateRegionServerConfiguration)},
	{HookIDPostUpdateRegionServerConfiguration, "PostUpdateRegionServerConfiguration", newReq[hookpb.PostUpdateRegionServerConfigurationRequest], postRegionServerHook(RegionServerObserver.PostUpdateRegionServerConfiguration)},
}

// regionServerHooksByID indexes the region-server dispatch table for
// O(1) lookup.
var regionServerHooksByID = func() map[HookID]regionServerHookEntry {
	m := make(map[HookID]regionServerHookEntry, len(regionServerHookTable))
	for _, h := range regionServerHookTable {
		m[h.id] = h
	}
	return m
}()
