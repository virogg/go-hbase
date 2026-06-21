// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

type walHookEntry struct {
	id     HookID
	name   string
	decode func() proto.Message
	invoke walHookInvoker
}

type walHookInvoker func(observer WALObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error)

func preWALHook[Req proto.Message](method func(WALObserver, context.Context, ObserverEnv, Req) (HookResult, error)) walHookInvoker {
	return func(o WALObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
		return method(o, ctx, env, req.(Req))
	}
}

func postWALHook[Req proto.Message](method func(WALObserver, context.Context, ObserverEnv, Req) error) walHookInvoker {
	return func(o WALObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
		return HookResult{}, method(o, ctx, env, req.(Req))
	}
}

var walHookTable = []walHookEntry{
	{HookIDPreWALWrite, "PreWALWrite", newReq[hookpb.PreWALWriteRequest], preWALHook(WALObserver.PreWALWrite)},
	{HookIDPostWALWrite, "PostWALWrite", newReq[hookpb.PostWALWriteRequest], postWALHook(WALObserver.PostWALWrite)},

	{HookIDPreWALRoll, "PreWALRoll", newReq[hookpb.PreWALRollRequest], preWALHook(WALObserver.PreWALRoll)},
	{HookIDPostWALRoll, "PostWALRoll", newReq[hookpb.PostWALRollRequest], postWALHook(WALObserver.PostWALRoll)},
}

var walHooksByID = func() map[HookID]walHookEntry {
	m := make(map[HookID]walHookEntry, len(walHookTable))
	for _, h := range walHookTable {
		m[h.id] = h
	}
	return m
}()
