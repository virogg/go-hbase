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

	preGetOps       atomic.Uint64
	preScannerOpens atomic.Uint64
	preScannerNexts atomic.Uint64
	preBatchMutates atomic.Uint64
	blockedGets     atomic.Uint64
	blockedScans    atomic.Uint64
	blockedBatchOps atomic.Uint64
}

// New constructs an Observer that bypasses reads whose target row begins
// with blockedPrefix. A nil or empty prefix disables bypass entirely
// (every read is allowed), which is useful for negative-case integration
// runs.
func New(blockedPrefix []byte) *Observer {
	cp := append([]byte(nil), blockedPrefix...)
	return &Observer{blocked: cp}
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
