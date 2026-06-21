// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package fault

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type Mode int

const (
	ModeNone Mode = iota
	ModeKill9
	ModeHang
	ModeExit1
	ModeProtocolError
	ModeOOM
)

var ErrProtocolFault = errors.New("fault-observer: protocol-error injected")

var modeStrings = map[string]Mode{
	"none":           ModeNone,
	"kill-9":         ModeKill9,
	"hang":           ModeHang,
	"exit-1":         ModeExit1,
	"protocol-error": ModeProtocolError,
	"oom":            ModeOOM,
}

func ParseMode(s string) (Mode, error) {
	if s == "" {
		return ModeNone, nil
	}
	m, ok := modeStrings[s]
	if !ok {
		return 0, fmt.Errorf("fault: unknown mode %q (valid: none, kill-9, hang, exit-1, protocol-error, oom)", s)
	}
	return m, nil
}

func (m Mode) String() string {
	switch m {
	case ModeNone:
		return "none"
	case ModeKill9:
		return "kill-9"
	case ModeHang:
		return "hang"
	case ModeExit1:
		return "exit-1"
	case ModeProtocolError:
		return "protocol-error"
	case ModeOOM:
		return "oom"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

type Actions interface {
	Kill9()
	Exit1()
	Hang(ctx context.Context)
	AllocateOOM()
}

type Observer struct {
	hbasecop.UnimplementedRegionObserver

	mode        Mode
	actions     Actions
	invocations atomic.Uint64
}

func New(m Mode, actions Actions) *Observer {
	if actions == nil {
		panic("fault.New: actions must not be nil")
	}
	return &Observer{mode: m, actions: actions}
}

func (o *Observer) Mode() Mode { return o.mode }

func (o *Observer) Invocations() uint64 { return o.invocations.Load() }

func (o *Observer) PrePut(
	ctx context.Context,
	_ hbasecop.ObserverEnv,
	_ *hbasecop.MutationProto,
) (hbasecop.HookResult, error) {
	o.invocations.Add(1)
	switch o.mode {
	case ModeNone:
		return hbasecop.HookResult{}, nil
	case ModeKill9:
		o.actions.Kill9()
		return hbasecop.HookResult{}, nil
	case ModeHang:
		o.actions.Hang(ctx)
		return hbasecop.HookResult{}, nil
	case ModeExit1:
		o.actions.Exit1()
		return hbasecop.HookResult{}, nil
	case ModeProtocolError:
		return hbasecop.HookResult{}, ErrProtocolFault
	case ModeOOM:
		o.actions.AllocateOOM()
		return hbasecop.HookResult{}, nil
	default:
		return hbasecop.HookResult{}, fmt.Errorf("fault: unknown mode %v", o.mode)
	}
}

func (o *Observer) PostPut(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hbasecop.MutationProto,
) error {
	return nil
}
