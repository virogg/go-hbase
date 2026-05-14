// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package fault implements the configurable fault-injection RegionObserver
// used by the T36 fault-injection matrix. The Observer dispatches each
// PrePut invocation to a [Mode]-specific action exposed via the [Actions]
// interface; production wires the syscall-backed [DefaultActions] and tests
// inject a stub so dispatch logic can be exercised without terminating the
// test process.
package fault

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

// Mode selects which fault the [Observer] injects on every PrePut.
type Mode int

const (
	// ModeNone is the pass-through default; PrePut returns success.
	ModeNone Mode = iota
	// ModeKill9 invokes [Actions.Kill9] before returning — production SIGKILLs self.
	ModeKill9
	// ModeHang invokes [Actions.Hang] — production blocks indefinitely.
	ModeHang
	// ModeExit1 invokes [Actions.Exit1] — production calls os.Exit(1).
	ModeExit1
	// ModeProtocolError returns [ErrProtocolFault] from PrePut, surfacing
	// as a wirepb Error frame to Java.
	ModeProtocolError
	// ModeOOM invokes [Actions.AllocateOOM] — production allocates until the
	// process is reaped by the kernel OOM killer.
	ModeOOM
)

// ErrProtocolFault is returned by PrePut under [ModeProtocolError]. Wrapped
// errors retain this as the sentinel so callers can match via [errors.Is].
var ErrProtocolFault = errors.New("fault-observer: protocol-error injected")

var modeStrings = map[string]Mode{
	"none":           ModeNone,
	"kill-9":         ModeKill9,
	"hang":           ModeHang,
	"exit-1":         ModeExit1,
	"protocol-error": ModeProtocolError,
	"oom":            ModeOOM,
}

// ParseMode resolves a mode token (matching the [Mode.String] form) to its
// enum value. An empty string maps to [ModeNone] so an unset env var Just
// Works. Unknown tokens are an error.
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

// String renders the on-the-wire token consumed by [ParseMode].
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

// Actions abstracts the destructive side-effects so tests can stub them out.
// Real implementations live in [DefaultActions].
type Actions interface {
	// Kill9 should SIGKILL the current process. Real implementations do not
	// return; the interface keeps a return-less signature for stub-ability.
	Kill9()
	// Exit1 should terminate the current process with status 1.
	Exit1()
	// Hang should block indefinitely (or until ctx is done) so the watchdog
	// observes missed heartbeats. Real implementations may select on ctx so
	// the framework's shutdown signal still wins.
	Hang(ctx context.Context)
	// AllocateOOM should allocate until the process is reaped by the OOM
	// killer. The call returns only if allocation surprisingly succeeds.
	AllocateOOM()
}

// Observer is a [hbasecop.RegionObserver] that injects the configured fault
// on every PrePut. PostPut is always a no-op so post-state HBase scans are
// not perturbed by the test apparatus.
//
// Embeds [hbasecop.UnimplementedRegionObserver] to inherit no-op defaults
// for the rest of the RegionObserver surface (T41); only PrePut/PostPut are
// overridden.
type Observer struct {
	hbasecop.UnimplementedRegionObserver

	mode        Mode
	actions     Actions
	invocations atomic.Uint64
}

// New constructs an Observer that applies mode m via actions. Panics when
// actions is nil — every mode (including ModeNone) consults the counter, and
// non-trivial modes consult actions.
func New(m Mode, actions Actions) *Observer {
	if actions == nil {
		panic("fault.New: actions must not be nil")
	}
	return &Observer{mode: m, actions: actions}
}

// Mode returns the configured fault mode.
func (o *Observer) Mode() Mode { return o.mode }

// Invocations returns the cumulative count of PrePut calls observed.
func (o *Observer) Invocations() uint64 { return o.invocations.Load() }

// PrePut increments the invocation counter and dispatches to the configured
// fault behaviour. See [Mode] for the per-mode semantics.
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

// PostPut is always a no-op so post-state scans observe a clean table.
func (o *Observer) PostPut(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	_ *hbasecop.MutationProto,
) error {
	return nil
}
