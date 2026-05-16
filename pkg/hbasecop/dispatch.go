// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

// HookError codes carried in wirepb.Error / hookpb.HookError payloads.
// The values are stable so the Java adapter can branch on them; the
// production hook-error taxonomy lands in T31.
const (
	errCodeInvalidWireRequest uint32 = 1
	errCodeUnknownHook        uint32 = 2
	errCodeMarshalResponse    uint32 = 3
)

// dispatcher routes inbound wire-level Request frames to the
// appropriate RegionObserver method via the canonical hookTable (T41).
// It is the only place where the SDK crosses between the wire encoding
// and the user-facing API.
type dispatcher struct {
	observer     RegionObserver
	master       MasterObserver
	regionServer RegionServerObserver
	wal          WALObserver
	logger       *slog.Logger
}

func newDispatcher(observer RegionObserver, logger *slog.Logger) *dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &dispatcher{observer: observer, logger: logger}
}

func newMasterDispatcher(master MasterObserver, logger *slog.Logger) *dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &dispatcher{master: master, logger: logger}
}

func newRegionServerDispatcher(rs RegionServerObserver, logger *slog.Logger) *dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &dispatcher{regionServer: rs, logger: logger}
}

func newWALDispatcher(wal WALObserver, logger *slog.Logger) *dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &dispatcher{wal: wal, logger: logger}
}

// dispatch decodes one Request frame, looks up the hook in the
// canonical dispatch table, invokes the matching observer method and
// returns the frame to send back. Returning a TypeError frame signals
// a protocol-level failure (malformed Request, unknown hook id,
// marshal error); observer-level failures travel inside a TypeResponse
// with HookResponse.error populated, so the Java adapter can apply the
// configured failure policy uniformly (T31/T32).
func (d *dispatcher) dispatch(ctx context.Context, req *wire.Message) *wire.Message {
	var wireReq wirepb.Request
	if err := proto.Unmarshal(req.Payload, &wireReq); err != nil {
		d.logger.Error("hbasecop: invalid wirepb.Request",
			"err", err, "req_id", req.ReqID, "hook_id", req.HookID)
		return d.errorFrame(req, errCodeInvalidWireRequest, "invalid wire Request: "+err.Error())
	}

	hookID := HookID(req.HookID)
	if d.observer != nil {
		if entry, ok := hooksByID[hookID]; ok {
			return d.dispatchRegion(ctx, req, &wireReq, entry)
		}
	}
	if d.master != nil {
		if entry, ok := masterHooksByID[hookID]; ok {
			return d.dispatchMaster(ctx, req, &wireReq, entry)
		}
	}
	if d.regionServer != nil {
		if entry, ok := regionServerHooksByID[hookID]; ok {
			return d.dispatchRegionServer(ctx, req, &wireReq, entry)
		}
	}
	if d.wal != nil {
		if entry, ok := walHooksByID[hookID]; ok {
			return d.dispatchWAL(ctx, req, &wireReq, entry)
		}
	}
	d.logger.Warn("hbasecop: unknown hook_id",
		"hook_id", req.HookID, "req_id", req.ReqID)
	return d.errorFrame(req, errCodeUnknownHook, "unknown hook")
}

func (d *dispatcher) dispatchRegion(ctx context.Context, req *wire.Message, wireReq *wirepb.Request, entry hookEntry) *wire.Message {
	inner := entry.decode()
	if err := proto.Unmarshal(wireReq.GetHookCtx(), inner); err != nil {
		d.logger.Error("hbasecop: invalid "+entry.name+"Request",
			"err", err, "req_id", req.ReqID, "hook", entry.name)
		return d.errorFrame(req, errCodeInvalidWireRequest,
			"invalid "+entry.name+"Request: "+err.Error())
	}
	env := envFromHookContext(extractHookCtx(inner))
	result, callErr := entry.invoke(d.observer, ctx, env, inner)
	return d.responseFrame(req, result, callErr)
}

func (d *dispatcher) dispatchMaster(ctx context.Context, req *wire.Message, wireReq *wirepb.Request, entry masterHookEntry) *wire.Message {
	inner := entry.decode()
	if err := proto.Unmarshal(wireReq.GetHookCtx(), inner); err != nil {
		d.logger.Error("hbasecop: invalid "+entry.name+"Request",
			"err", err, "req_id", req.ReqID, "hook", entry.name)
		return d.errorFrame(req, errCodeInvalidWireRequest,
			"invalid "+entry.name+"Request: "+err.Error())
	}
	env := envFromHookContext(extractHookCtx(inner))
	result, callErr := entry.invoke(d.master, ctx, env, inner)
	return d.responseFrame(req, result, callErr)
}

func (d *dispatcher) dispatchRegionServer(ctx context.Context, req *wire.Message, wireReq *wirepb.Request, entry regionServerHookEntry) *wire.Message {
	inner := entry.decode()
	if err := proto.Unmarshal(wireReq.GetHookCtx(), inner); err != nil {
		d.logger.Error("hbasecop: invalid "+entry.name+"Request",
			"err", err, "req_id", req.ReqID, "hook", entry.name)
		return d.errorFrame(req, errCodeInvalidWireRequest,
			"invalid "+entry.name+"Request: "+err.Error())
	}
	env := envFromHookContext(extractHookCtx(inner))
	result, callErr := entry.invoke(d.regionServer, ctx, env, inner)
	return d.responseFrame(req, result, callErr)
}

func (d *dispatcher) dispatchWAL(ctx context.Context, req *wire.Message, wireReq *wirepb.Request, entry walHookEntry) *wire.Message {
	inner := entry.decode()
	if err := proto.Unmarshal(wireReq.GetHookCtx(), inner); err != nil {
		d.logger.Error("hbasecop: invalid "+entry.name+"Request",
			"err", err, "req_id", req.ReqID, "hook", entry.name)
		return d.errorFrame(req, errCodeInvalidWireRequest,
			"invalid "+entry.name+"Request: "+err.Error())
	}
	env := envFromHookContext(extractHookCtx(inner))
	result, callErr := entry.invoke(d.wal, ctx, env, inner)
	return d.responseFrame(req, result, callErr)
}

// extractHookCtx pulls the shared HookContext out of any per-hook
// Request type. Every T41 stub message embeds HookContext at field 1
// (see proto/hooks.proto); the generated Go code therefore exposes
// GetCtx() on each. We rely on the structural interface so the
// dispatch table can stay request-type-agnostic.
type hookContextGetter interface {
	GetCtx() *hookpb.HookContext
}

func extractHookCtx(msg proto.Message) *hookpb.HookContext {
	if g, ok := msg.(hookContextGetter); ok {
		return g.GetCtx()
	}
	return nil
}

func envFromHookContext(hc *hookpb.HookContext) ObserverEnv {
	if hc == nil {
		return ObserverEnv{}
	}
	var tn string
	if t := hc.GetTableName(); t != nil {
		ns := t.GetNamespace()
		q := t.GetQualifier()
		if len(ns) == 0 {
			tn = string(q)
		} else {
			tn = string(ns) + ":" + string(q)
		}
	}
	return ObserverEnv{
		TableName:  tn,
		RegionName: string(hc.GetRegionName()),
	}
}

func (d *dispatcher) responseFrame(req *wire.Message, result HookResult, callErr error) *wire.Message {
	hookResp := &hookpb.HookResponse{Bypass: result.Bypass}
	if len(result.BlockedIndices) > 0 {
		hookResp.BlockedIndices = append([]uint32(nil), result.BlockedIndices...)
	}
	if callErr != nil {
		hookResp.Error = &hookpb.HookError{Code: 1, Message: callErr.Error()}
	}
	hookRespBytes, err := proto.Marshal(hookResp)
	if err != nil {
		d.logger.Error("hbasecop: marshal HookResponse failed", "err", err, "req_id", req.ReqID)
		return d.errorFrame(req, errCodeMarshalResponse, "marshal HookResponse: "+err.Error())
	}
	payload, err := proto.Marshal(&wirepb.Response{HookResp: hookRespBytes})
	if err != nil {
		d.logger.Error("hbasecop: marshal wirepb.Response failed", "err", err, "req_id", req.ReqID)
		return d.errorFrame(req, errCodeMarshalResponse, "marshal wire Response: "+err.Error())
	}
	return &wire.Message{
		Type:     wire.TypeResponse,
		ReqID:    req.ReqID,
		RegionID: req.RegionID,
		HookID:   req.HookID,
		Payload:  payload,
	}
}

func (d *dispatcher) errorFrame(req *wire.Message, code uint32, msg string) *wire.Message {
	payload, err := proto.Marshal(&wirepb.Error{Code: code, Message: msg})
	if err != nil {
		d.logger.Error("hbasecop: marshal wirepb.Error failed", "err", err, "req_id", req.ReqID)
		return nil
	}
	return &wire.Message{
		Type:     wire.TypeError,
		ReqID:    req.ReqID,
		RegionID: req.RegionID,
		HookID:   req.HookID,
		Payload:  payload,
	}
}
