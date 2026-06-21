// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type Observer struct {
	hbasecop.UnimplementedRegionServerObserver

	vetoWalRoll bool

	preStopCalls    atomic.Uint64
	preRollWALCalls atomic.Uint64
	rejectedRolls   atomic.Uint64
}

func New(vetoWalRoll bool) *Observer {
	return &Observer{vetoWalRoll: vetoWalRoll}
}

func (o *Observer) PreStopRegionServer(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hookpb.PreStopRegionServerRequest,
) (hbasecop.HookResult, error) {
	o.preStopCalls.Add(1)
	return hbasecop.HookResult{}, nil
}

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

func (o *Observer) PreStopCount() uint64 { return o.preStopCalls.Load() }

func (o *Observer) PreRollWALCount() uint64 { return o.preRollWALCalls.Load() }

func (o *Observer) RejectedRollCount() uint64 { return o.rejectedRolls.Load() }
