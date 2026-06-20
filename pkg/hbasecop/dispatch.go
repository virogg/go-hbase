// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"runtime/debug"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
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
	errCodeEndpointFailed     uint32 = 4
)

// dispatcher routes inbound wire-level Request frames to the
// appropriate RegionObserver method via the canonical hookTable (T41).
// It is the only place where the SDK crosses between the wire encoding
// and the user-facing API.
// dispatcher holds one slice of observers per surface; multiple observers on a
// surface are chained (see foldObservers), and several surfaces may be set so
// one process serves Region+Master+... over a single shmem pair.
type dispatcher struct {
	observers     []RegionObserver
	masters       []MasterObserver
	regionServers []RegionServerObserver
	wals          []WALObserver
	bulkLoads     []BulkLoadObserver
	endpoint      Endpoint
	logger        *slog.Logger

	// reverse is the Go-initiated reverse-RPC client (Tier 2, TE31), set when
	// the supervisor provisioned the bulk ring. It lets an endpoint Call read
	// region-local data; nil when the reverse path is disabled.
	reverse *cpruntime.ReverseClient
}

func newDispatcher(observer RegionObserver, logger *slog.Logger) *dispatcher {
	return &dispatcher{observers: []RegionObserver{observer}, logger: orDefaultLogger(logger)}
}

func newMasterDispatcher(master MasterObserver, logger *slog.Logger) *dispatcher {
	return &dispatcher{masters: []MasterObserver{master}, logger: orDefaultLogger(logger)}
}

func newRegionServerDispatcher(rs RegionServerObserver, logger *slog.Logger) *dispatcher {
	return &dispatcher{regionServers: []RegionServerObserver{rs}, logger: orDefaultLogger(logger)}
}

func newWALDispatcher(wal WALObserver, logger *slog.Logger) *dispatcher {
	return &dispatcher{wals: []WALObserver{wal}, logger: orDefaultLogger(logger)}
}

func newBulkLoadDispatcher(bl BulkLoadObserver, logger *slog.Logger) *dispatcher {
	return &dispatcher{bulkLoads: []BulkLoadObserver{bl}, logger: orDefaultLogger(logger)}
}

func orDefaultLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}

// foldObservers runs invoke for each observer in registration order, folding
// their HookResults: Bypass is OR-ed, BlockedIndices concatenated (duplicates
// are tolerated Java-side), and ResultCells follow last-non-empty-wins, so a
// later observer's substitute Result overrides an earlier one's. The first
// non-nil error (or recovered panic) short-circuits the chain; the erroring
// observer's own result is still folded in before returning, so a single
// observer (the common case) yields exactly the (result, err) pair the old
// single-observer dispatch did.
func foldObservers[O any](d *dispatcher, hookName string, reqID uint64, observers []O, invoke func(O) (HookResult, error)) (HookResult, error) {
	var fold HookResult
	for _, obs := range observers {
		res, err := recoverInvoke(d.logger, hookName, reqID, func() (HookResult, error) {
			return invoke(obs)
		})
		if res.Bypass {
			fold.Bypass = true
		}
		if len(res.BlockedIndices) > 0 {
			fold.BlockedIndices = append(fold.BlockedIndices, res.BlockedIndices...)
		}
		if len(res.ResultCells) > 0 {
			fold.ResultCells = res.ResultCells
		}
		if err != nil {
			return fold, err
		}
	}
	return fold, nil
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
	if len(d.observers) > 0 {
		if entry, ok := hooksByID[hookID]; ok {
			return d.dispatchRegion(ctx, req, &wireReq, entry)
		}
	}
	if len(d.masters) > 0 {
		if entry, ok := masterHooksByID[hookID]; ok {
			return d.dispatchMaster(ctx, req, &wireReq, entry)
		}
	}
	if len(d.regionServers) > 0 {
		if entry, ok := regionServerHooksByID[hookID]; ok {
			return d.dispatchRegionServer(ctx, req, &wireReq, entry)
		}
	}
	if len(d.wals) > 0 {
		if entry, ok := walHooksByID[hookID]; ok {
			return d.dispatchWAL(ctx, req, &wireReq, entry)
		}
	}
	if len(d.bulkLoads) > 0 {
		if entry, ok := bulkLoadHooksByID[hookID]; ok {
			return d.dispatchBulkLoad(ctx, req, &wireReq, entry)
		}
	}
	d.logger.Warn("hbasecop: unknown hook_id",
		"hook_id", req.HookID, "req_id", req.ReqID)
	return d.errorFrame(req, errCodeUnknownHook, "unknown hook")
}

// recoverInvoke runs one observer-method invocation and converts a panic
// from user callback code into an error. SPEC §6 requires that a panic in
// a user observer never escape to crash the long-lived per-RegionServer Go
// process - it must become an Error travelling back to the Java side, which
// then applies the configured failure policy (strict → IOException to the
// client; best-effort → WARN + no-op). The recovered error flows through
// responseFrame as HookResponse.error.
func recoverInvoke(logger *slog.Logger, hookName string, reqID uint64, fn func() (HookResult, error)) (result HookResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("hbasecop: observer panic recovered",
				"hook", hookName, "req_id", reqID, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("observer panic in %s: %v", hookName, r)
		}
	}()
	return fn()
}

func (d *dispatcher) dispatchRegion(ctx context.Context, req *wire.Message, wireReq *wirepb.Request, entry hookEntry) *wire.Message {
	inner := entry.decode()
	if err := proto.Unmarshal(wireReq.GetHookCtx(), inner); err != nil {
		d.logger.Error("hbasecop: invalid "+entry.name+"Request",
			"err", err, "req_id", req.ReqID, "hook", entry.name)
		return d.errorFrame(req, errCodeInvalidWireRequest,
			"invalid "+entry.name+"Request: "+err.Error())
	}
	env := envFromHookContext(d.logger, entry.name, req.ReqID, req.RegionID, extractHookCtx(inner))
	result, callErr := foldObservers(d, entry.name, req.ReqID, d.observers, func(o RegionObserver) (HookResult, error) {
		return entry.invoke(o, ctx, env, inner)
	})
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
	env := envFromHookContext(d.logger, entry.name, req.ReqID, req.RegionID, extractHookCtx(inner))
	result, callErr := foldObservers(d, entry.name, req.ReqID, d.masters, func(o MasterObserver) (HookResult, error) {
		return entry.invoke(o, ctx, env, inner)
	})
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
	env := envFromHookContext(d.logger, entry.name, req.ReqID, req.RegionID, extractHookCtx(inner))
	result, callErr := foldObservers(d, entry.name, req.ReqID, d.regionServers, func(o RegionServerObserver) (HookResult, error) {
		return entry.invoke(o, ctx, env, inner)
	})
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
	env := envFromHookContext(d.logger, entry.name, req.ReqID, req.RegionID, extractHookCtx(inner))
	result, callErr := foldObservers(d, entry.name, req.ReqID, d.wals, func(o WALObserver) (HookResult, error) {
		return entry.invoke(o, ctx, env, inner)
	})
	return d.responseFrame(req, result, callErr)
}

func (d *dispatcher) dispatchBulkLoad(ctx context.Context, req *wire.Message, wireReq *wirepb.Request, entry bulkLoadHookEntry) *wire.Message {
	inner := entry.decode()
	if err := proto.Unmarshal(wireReq.GetHookCtx(), inner); err != nil {
		d.logger.Error("hbasecop: invalid "+entry.name+"Request",
			"err", err, "req_id", req.ReqID, "hook", entry.name)
		return d.errorFrame(req, errCodeInvalidWireRequest,
			"invalid "+entry.name+"Request: "+err.Error())
	}
	env := envFromHookContext(d.logger, entry.name, req.ReqID, req.RegionID, extractHookCtx(inner))
	result, callErr := foldObservers(d, entry.name, req.ReqID, d.bulkLoads, func(o BulkLoadObserver) (HookResult, error) {
		return entry.invoke(o, ctx, env, inner)
	})
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

func envFromHookContext(logger *slog.Logger, hookName string, reqID uint64, regionID uint32, hc *hookpb.HookContext) ObserverEnv {
	env := ObserverEnv{RegionID: regionID, logger: logger, hook: hookName, reqID: reqID}
	if hc != nil {
		if t := hc.GetTableName(); t != nil {
			ns, q := t.GetNamespace(), t.GetQualifier()
			if len(ns) == 0 {
				env.TableName = string(q)
			} else {
				env.TableName = string(ns) + ":" + string(q)
			}
		}
		env.RegionName = string(hc.GetRegionName())
	}
	return env
}

func (d *dispatcher) responseFrame(req *wire.Message, result HookResult, callErr error) *wire.Message {
	hookResp := &hookpb.HookResponse{Bypass: result.Bypass}
	if len(result.BlockedIndices) > 0 {
		hookResp.BlockedIndices = append([]uint32(nil), result.BlockedIndices...)
	}
	if len(result.ResultCells) > 0 {
		hookResp.Result = append([]*hbasepb.Cell(nil), result.ResultCells...)
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
		// Marshalling a fixed wirepb.Error effectively never fails, but if it
		// did, returning nil sends no reply and the client blocks until the
		// Java-side timeout. Hand-encode a minimal Error so the client always
		// gets a prompt failure frame instead of a silent hang.
		d.logger.Error("hbasecop: marshal wirepb.Error failed", "err", err, "req_id", req.ReqID)
		payload = encodeMinimalError(code, msg)
	}
	return &wire.Message{
		Type:     wire.TypeError,
		ReqID:    req.ReqID,
		RegionID: req.RegionID,
		HookID:   req.HookID,
		Payload:  payload,
	}
}

// encodeMinimalError hand-encodes a wirepb.Error (code=field 1 varint,
// message=field 2 length-delimited) without proto.Marshal, as the fallback
// when Marshal fails so an Error frame is always deliverable.
func encodeMinimalError(code uint32, msg string) []byte {
	b := make([]byte, 0, len(msg)+12)
	b = append(b, 0x08) // field 1 (code), wire type 0 (varint)
	b = binary.AppendUvarint(b, uint64(code))
	b = append(b, 0x12) // field 2 (message), wire type 2 (length-delimited)
	b = binary.AppendUvarint(b, uint64(len(msg)))
	b = append(b, msg...)
	return b
}
