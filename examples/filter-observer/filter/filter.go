// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package filter implements the read-path RegionObserver used by the T43
// integration test. The Observer inspects every Get's row key and every
// Scan's start row: those carrying the configured blocked prefix are
// bypassed (HookResult.Bypass=true), causing the Java adapter to invoke
// ObserverContext.bypass() so HBase skips its own read implementation.
// PreScannerNext is a counter-only hook so the IT can verify the scanner
// hook fires end-to-end without mutating scan progress.
package filter

import (
	"bytes"
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

// Observer is a read-path RegionObserver that bypasses Get / Scan
// operations whose target row carries the configured blocked prefix.
// All other RegionObserver methods inherit the no-op behaviour of
// UnimplementedRegionObserver.
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

// New constructs an Observer that bypasses reads whose target row begins
// with blockedPrefix. A nil or empty prefix disables bypass entirely
// (every read is allowed), which is useful for negative-case integration
// runs.
func New(blockedPrefix []byte) *Observer {
	cp := append([]byte(nil), blockedPrefix...)
	return &Observer{blocked: cp}
}

// SetLogger overrides the slog.Logger used by storage-hook handlers. Nil
// resets to slog.Default(). Safe for concurrent use.
func (o *Observer) SetLogger(l *slog.Logger) {
	o.logger.Store(l)
}

func (o *Observer) log() *slog.Logger {
	if l := o.logger.Load(); l != nil {
		return l
	}
	return slog.Default()
}

// PreGetOp bypasses the Get when its row matches the blocked prefix.
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

// PreScannerOpen bypasses scanner creation when its start row matches the
// blocked prefix.
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

// PreScannerNext counts invocations only — never bypasses — so the IT
// can prove the hook fires on every batch fetch without changing the
// scanner's progression.
func (o *Observer) PreScannerNext(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hookpb.PreScannerNextRequest,
) (hbasecop.HookResult, error) {
	o.preScannerNexts.Add(1)
	return hbasecop.HookResult{}, nil
}

// PreBatchMutate returns per-mutation BlockedIndices for every operation
// in the inbound MiniBatch whose target row matches the blocked prefix.
// The Java adapter applies SANITY_CHECK_FAILURE per index, so blocked
// mutations land as individual failures while the rest of the batch
// proceeds — this is the partial-block path required by T44's AC.
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

// PreFlush is a passive recorder: counts the invocation and emits a
// uniquely-tagged slog line the live IT can grep for.
func (o *Observer) PreFlush(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hookpb.PreFlushRequest,
) (hbasecop.HookResult, error) {
	o.preFlushes.Add(1)
	o.log().Info("filter-observer: preFlush")
	return hbasecop.HookResult{}, nil
}

// PostFlush is a passive recorder: counts the invocation and logs.
func (o *Observer) PostFlush(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hookpb.PostFlushRequest,
) error {
	o.postFlushes.Add(1)
	o.log().Info("filter-observer: postFlush")
	return nil
}

// PreCompactSelection records the candidate-store-file list size so the IT
// can prove the compaction lifecycle traversed the bridge end-to-end.
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

// PreCompact is a passive recorder for the compaction-scan entry hook.
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

// PostCompact is a passive recorder for the compaction-completed hook.
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

// PreFlushCount returns the cumulative count of PreFlush invocations.
func (o *Observer) PreFlushCount() uint64 { return o.preFlushes.Load() }

// PostFlushCount returns the cumulative count of PostFlush invocations.
func (o *Observer) PostFlushCount() uint64 { return o.postFlushes.Load() }

// PreCompactSelectionCount returns the cumulative count of PreCompactSelection invocations.
func (o *Observer) PreCompactSelectionCount() uint64 { return o.preCompactSelections.Load() }

// PreCompactCount returns the cumulative count of PreCompact invocations.
func (o *Observer) PreCompactCount() uint64 { return o.preCompacts.Load() }

// PostCompactCount returns the cumulative count of PostCompact invocations.
func (o *Observer) PostCompactCount() uint64 { return o.postCompacts.Load() }

// PreGetCount returns the cumulative count of PreGetOp invocations.
func (o *Observer) PreGetCount() uint64 { return o.preGetOps.Load() }

// PreScannerOpenCount returns the cumulative count of PreScannerOpen invocations.
func (o *Observer) PreScannerOpenCount() uint64 { return o.preScannerOpens.Load() }

// PreScannerNextCount returns the cumulative count of PreScannerNext invocations.
func (o *Observer) PreScannerNextCount() uint64 { return o.preScannerNexts.Load() }

// BlockedGetCount returns how many Get calls were bypassed.
func (o *Observer) BlockedGetCount() uint64 { return o.blockedGets.Load() }

// BlockedScanCount returns how many scanner-open calls were bypassed.
func (o *Observer) BlockedScanCount() uint64 { return o.blockedScans.Load() }

// PreBatchMutateCount returns the cumulative count of PreBatchMutate invocations.
func (o *Observer) PreBatchMutateCount() uint64 { return o.preBatchMutates.Load() }

// BlockedBatchOps returns the cumulative count of individual batch mutations marked blocked.
func (o *Observer) BlockedBatchOps() uint64 { return o.blockedBatchOps.Load() }

func (o *Observer) matchesBlocked(row []byte) bool {
	return len(o.blocked) > 0 && bytes.HasPrefix(row, o.blocked)
}
