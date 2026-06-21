// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"bytes"
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type Observer struct {
	hbasecop.UnimplementedRegionObserver

	blocked []byte
	logger  atomic.Pointer[slog.Logger]

	preGetOps            atomic.Uint64
	preScannerOpens      atomic.Uint64
	preScannerNexts      atomic.Uint64
	preBatchMutates      atomic.Uint64
	blockedGets          atomic.Uint64
	blockedScans         atomic.Uint64
	blockedBatchOps      atomic.Uint64
	preFlushes           atomic.Uint64
	postFlushes          atomic.Uint64
	preCompactSelections atomic.Uint64
	preCompacts          atomic.Uint64
	postCompacts         atomic.Uint64
}

func New(blockedPrefix []byte) *Observer {
	cp := append([]byte(nil), blockedPrefix...)
	return &Observer{blocked: cp}
}

func (o *Observer) SetLogger(l *slog.Logger) {
	o.logger.Store(l)
}

func (o *Observer) log() *slog.Logger {
	if l := o.logger.Load(); l != nil {
		return l
	}
	return slog.Default()
}

func (o *Observer) PreGetOp(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	req *hookpb.PreGetOpRequest,
) (hbasecop.HookResult, error) {
	o.preGetOps.Add(1)
	if o.matchesBlocked(req.GetGet().GetRow()) {
		o.blockedGets.Add(1)
		return hbasecop.HookResult{Bypass: true}, nil
	}
	return hbasecop.HookResult{}, nil
}

func (o *Observer) PreScannerOpen(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	req *hookpb.PreScannerOpenRequest,
) (hbasecop.HookResult, error) {
	o.preScannerOpens.Add(1)
	if o.matchesBlocked(req.GetScan().GetStartRow()) {
		o.blockedScans.Add(1)
		return hbasecop.HookResult{Bypass: true}, nil
	}
	return hbasecop.HookResult{}, nil
}

func (o *Observer) PreScannerNext(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hookpb.PreScannerNextRequest,
) (hbasecop.HookResult, error) {
	o.preScannerNexts.Add(1)
	return hbasecop.HookResult{}, nil
}

func (o *Observer) PreBatchMutate(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	req *hookpb.PreBatchMutateRequest,
) (hbasecop.HookResult, error) {
	o.preBatchMutates.Add(1)
	ops := req.GetOperation()
	var blocked []uint32
	for i, op := range ops {
		if o.matchesBlocked(op.GetMutation().GetRow()) {
			blocked = append(blocked, uint32(i))
		}
	}
	o.blockedBatchOps.Add(uint64(len(blocked)))
	return hbasecop.HookResult{BlockedIndices: blocked}, nil
}

func (o *Observer) PreFlush(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hookpb.PreFlushRequest,
) (hbasecop.HookResult, error) {
	o.preFlushes.Add(1)
	o.log().Info("filter-observer: preFlush")
	return hbasecop.HookResult{}, nil
}

func (o *Observer) PostFlush(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hookpb.PostFlushRequest,
) error {
	o.postFlushes.Add(1)
	o.log().Info("filter-observer: postFlush")
	return nil
}

func (o *Observer) PreCompactSelection(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	req *hookpb.PreCompactSelectionRequest,
) (hbasecop.HookResult, error) {
	o.preCompactSelections.Add(1)
	o.log().Info(
		"filter-observer: preCompactSelection",
		"family", string(req.GetColumnFamily()),
		"candidates", len(req.GetCandidate()),
	)
	return hbasecop.HookResult{}, nil
}

func (o *Observer) PreCompact(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	req *hookpb.PreCompactRequest,
) (hbasecop.HookResult, error) {
	o.preCompacts.Add(1)
	o.log().Info(
		"filter-observer: preCompact",
		"family", string(req.GetColumnFamily()),
		"is_major", req.GetRequest().GetIsMajor(),
	)
	return hbasecop.HookResult{}, nil
}

func (o *Observer) PostCompact(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	req *hookpb.PostCompactRequest,
) error {
	o.postCompacts.Add(1)
	o.log().Info(
		"filter-observer: postCompact",
		"family", string(req.GetColumnFamily()),
		"is_major", req.GetRequest().GetIsMajor(),
	)
	return nil
}

func (o *Observer) PreFlushCount() uint64 { return o.preFlushes.Load() }

func (o *Observer) PostFlushCount() uint64 { return o.postFlushes.Load() }

func (o *Observer) PreCompactSelectionCount() uint64 { return o.preCompactSelections.Load() }

func (o *Observer) PreCompactCount() uint64 { return o.preCompacts.Load() }

func (o *Observer) PostCompactCount() uint64 { return o.postCompacts.Load() }

func (o *Observer) PreGetCount() uint64 { return o.preGetOps.Load() }

func (o *Observer) PreScannerOpenCount() uint64 { return o.preScannerOpens.Load() }

func (o *Observer) PreScannerNextCount() uint64 { return o.preScannerNexts.Load() }

func (o *Observer) BlockedGetCount() uint64 { return o.blockedGets.Load() }

func (o *Observer) BlockedScanCount() uint64 { return o.blockedScans.Load() }

func (o *Observer) PreBatchMutateCount() uint64 { return o.preBatchMutates.Load() }

func (o *Observer) BlockedBatchOps() uint64 { return o.blockedBatchOps.Load() }

func (o *Observer) matchesBlocked(row []byte) bool {
	return len(o.blocked) > 0 && bytes.HasPrefix(row, o.blocked)
}
