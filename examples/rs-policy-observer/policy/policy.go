// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package policy implements the RegionServerObserver used by the T52
// integration test. The Observer audits region-server lifecycle hooks
// and, when WAL-roll vetoing is enabled, rejects preRollWALWriterRequest
// by returning an error so the (strict-by-default) Java
// RegionServerObserverAdapter surfaces it as an IOException back to the
// HBase admin client — the failure path required by T52's AC.
package policy

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

// Observer is a RegionServerObserver that audits server-lifecycle hooks
// and optionally vetoes WAL-writer rolls. Every hook other than the two
// it overrides inherits the no-op behaviour of
// UnimplementedRegionServerObserver.
type Observer struct {
	hbasecop.UnimplementedRegionServerObserver

	vetoWalRoll bool

	preStopCalls    atomic.Uint64
	preRollWALCalls atomic.Uint64
	rejectedRolls   atomic.Uint64
}

// New constructs an Observer. When vetoWalRoll is true every
// preRollWALWriterRequest hook is rejected with an error.
func New(vetoWalRoll bool) *Observer {
	return &Observer{vetoWalRoll: vetoWalRoll}
}

// PreStopRegionServer records that the RegionServer is shutting down.
// The hook is observe-only — it never vetoes a stop.
func (o *Observer) PreStopRegionServer(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hookpb.PreStopRegionServerRequest,
) (hbasecop.HookResult, error) {
	o.preStopCalls.Add(1)
	return hbasecop.HookResult{}, nil
}

// PreRollWALWriterRequest rejects the WAL roll when vetoing is enabled.
// Returning an error drives the strict-mode RegionServerObserverAdapter
// to throw IOException at the admin client.
func (o *Observer) PreRollWALWriterRequest(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hookpb.PreRollWalWriterRequestRequest,
) (hbasecop.HookResult, error) {
	o.preRollWALCalls.Add(1)
	if o.vetoWalRoll {
		o.rejectedRolls.Add(1)
		return hbasecop.HookResult{}, errors.New(
			"rs-policy-observer: WAL writer roll rejected by region-server policy")
	}
	return hbasecop.HookResult{}, nil
}

// PreStopCount returns the cumulative count of PreStopRegionServer invocations.
func (o *Observer) PreStopCount() uint64 { return o.preStopCalls.Load() }

// PreRollWALCount returns the cumulative count of PreRollWALWriterRequest invocations.
func (o *Observer) PreRollWALCount() uint64 { return o.preRollWALCalls.Load() }

// RejectedRollCount returns how many WAL-roll requests were rejected.
func (o *Observer) RejectedRollCount() uint64 { return o.rejectedRolls.Load() }
