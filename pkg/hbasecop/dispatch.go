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
// appropriate RegionObserver method and serialises the result back
// into a Response (or Error) frame. It is the only place where the SDK
// crosses between the wire encoding and the user-facing API.
type dispatcher struct {
	observer RegionObserver
	logger   *slog.Logger
}

func newDispatcher(observer RegionObserver, logger *slog.Logger) *dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &dispatcher{observer: observer, logger: logger}
}

// dispatch decodes one Request frame, invokes the matching observer
// method and returns the frame to send back. Returning a TypeError
// frame signals a protocol-level failure (malformed Request, unknown
// hook id, marshal error); observer-level failures travel inside a
// TypeResponse with HookResponse.error populated, so the Java adapter
// can apply the configured failure policy uniformly (T31/T32).
func (d *dispatcher) dispatch(ctx context.Context, req *wire.Message) *wire.Message {
	var wireReq wirepb.Request
	if err := proto.Unmarshal(req.Payload, &wireReq); err != nil {
		d.logger.Error("hbasecop: invalid wirepb.Request",
			"err", err, "req_id", req.ReqID, "hook_id", req.HookID)
		return d.errorFrame(req, errCodeInvalidWireRequest, "invalid wire Request: "+err.Error())
	}

	switch HookID(req.HookID) {
	case HookIDPrePut:
		return d.dispatchPrePut(ctx, req, wireReq.GetHookCtx())
	case HookIDPostPut:
		return d.dispatchPostPut(ctx, req, wireReq.GetHookCtx())
	default:
		d.logger.Warn("hbasecop: unknown hook_id",
			"hook_id", req.HookID, "req_id", req.ReqID)
		return d.errorFrame(req, errCodeUnknownHook, "unknown hook")
	}
}

func (d *dispatcher) dispatchPrePut(ctx context.Context, req *wire.Message, hookCtxBytes []byte) *wire.Message {
	var prePut hookpb.PrePutRequest
	if err := proto.Unmarshal(hookCtxBytes, &prePut); err != nil {
		d.logger.Error("hbasecop: invalid PrePutRequest",
			"err", err, "req_id", req.ReqID)
		return d.errorFrame(req, errCodeInvalidWireRequest, "invalid PrePutRequest: "+err.Error())
	}
	env := envFromHookContext(prePut.GetCtx())
	result, callErr := d.observer.PrePut(ctx, env, prePut.GetMutation())
	return d.responseFrame(req, result.Bypass, callErr)
}

func (d *dispatcher) dispatchPostPut(ctx context.Context, req *wire.Message, hookCtxBytes []byte) *wire.Message {
	var postPut hookpb.PostPutRequest
	if err := proto.Unmarshal(hookCtxBytes, &postPut); err != nil {
		d.logger.Error("hbasecop: invalid PostPutRequest",
			"err", err, "req_id", req.ReqID)
		return d.errorFrame(req, errCodeInvalidWireRequest, "invalid PostPutRequest: "+err.Error())
	}
	env := envFromHookContext(postPut.GetCtx())
	callErr := d.observer.PostPut(ctx, env, postPut.GetMutation())
	return d.responseFrame(req, false, callErr)
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

func (d *dispatcher) responseFrame(req *wire.Message, bypass bool, callErr error) *wire.Message {
	hookResp := &hookpb.HookResponse{Bypass: bypass}
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
