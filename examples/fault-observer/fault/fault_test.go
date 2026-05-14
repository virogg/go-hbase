// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package fault

import (
	"context"
	"errors"
	"testing"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

func TestParseMode_Valid(t *testing.T) {
	cases := map[string]Mode{
		"":               ModeNone,
		"none":           ModeNone,
		"kill-9":         ModeKill9,
		"hang":           ModeHang,
		"exit-1":         ModeExit1,
		"protocol-error": ModeProtocolError,
		"oom":            ModeOOM,
	}
	for in, want := range cases {
		got, err := ParseMode(in)
		if err != nil {
			t.Fatalf("ParseMode(%q) returned err: %v", in, err)
		}
		if got != want {
			t.Errorf("ParseMode(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestParseMode_Unknown(t *testing.T) {
	if _, err := ParseMode("not-a-mode"); err == nil {
		t.Fatal("expected error for unknown mode, got nil")
	}
}

func TestMode_String_RoundTrip(t *testing.T) {
	for _, m := range []Mode{ModeNone, ModeKill9, ModeHang, ModeExit1, ModeProtocolError, ModeOOM} {
		got, err := ParseMode(m.String())
		if err != nil {
			t.Fatalf("ParseMode(%q) err: %v", m.String(), err)
		}
		if got != m {
			t.Errorf("round-trip: %v → %q → %v", m, m.String(), got)
		}
	}
}

// stubActions records which side-effecting calls were invoked. The Sleep / Exit
// / Kill stubs do not actually block or terminate, so observer logic can run
// to completion under test.
type stubActions struct {
	kill9    int
	exit1    int
	hang     int
	oom      int
	hangChan chan struct{} // optional; when non-nil, Hang sends here so tests can synchronise
}

func (s *stubActions) Kill9()       { s.kill9++ }
func (s *stubActions) Exit1()       { s.exit1++ }
func (s *stubActions) AllocateOOM() { s.oom++ }
func (s *stubActions) Hang(ctx context.Context) {
	s.hang++
	if s.hangChan != nil {
		s.hangChan <- struct{}{}
	}
}

func TestObserver_None_PassThrough(t *testing.T) {
	stub := &stubActions{}
	obs := New(ModeNone, stub)
	res, err := obs.PrePut(context.Background(), hbasecop.ObserverEnv{}, &hbasecop.MutationProto{})
	if err != nil {
		t.Fatalf("PrePut returned err: %v", err)
	}
	if res.Bypass {
		t.Errorf("expected Bypass=false, got true")
	}
	if stub.kill9+stub.exit1+stub.hang+stub.oom != 0 {
		t.Errorf("none-mode must not invoke any action; stub=%+v", stub)
	}
	if got := obs.Invocations(); got != 1 {
		t.Errorf("Invocations()=%d, want 1", got)
	}
}

func TestObserver_Kill9_InvokesAction(t *testing.T) {
	stub := &stubActions{}
	obs := New(ModeKill9, stub)
	_, _ = obs.PrePut(context.Background(), hbasecop.ObserverEnv{}, &hbasecop.MutationProto{})
	if stub.kill9 != 1 {
		t.Errorf("kill-9 mode must invoke Kill9 exactly once, got %d", stub.kill9)
	}
}

func TestObserver_Exit1_InvokesAction(t *testing.T) {
	stub := &stubActions{}
	obs := New(ModeExit1, stub)
	_, _ = obs.PrePut(context.Background(), hbasecop.ObserverEnv{}, &hbasecop.MutationProto{})
	if stub.exit1 != 1 {
		t.Errorf("exit-1 mode must invoke Exit1 exactly once, got %d", stub.exit1)
	}
}

func TestObserver_Hang_InvokesAction(t *testing.T) {
	stub := &stubActions{}
	obs := New(ModeHang, stub)
	_, _ = obs.PrePut(context.Background(), hbasecop.ObserverEnv{}, &hbasecop.MutationProto{})
	if stub.hang != 1 {
		t.Errorf("hang mode must invoke Hang exactly once, got %d", stub.hang)
	}
}

func TestObserver_OOM_InvokesAction(t *testing.T) {
	stub := &stubActions{}
	obs := New(ModeOOM, stub)
	_, _ = obs.PrePut(context.Background(), hbasecop.ObserverEnv{}, &hbasecop.MutationProto{})
	if stub.oom != 1 {
		t.Errorf("oom mode must invoke AllocateOOM exactly once, got %d", stub.oom)
	}
}

func TestObserver_ProtocolError_ReturnsError(t *testing.T) {
	stub := &stubActions{}
	obs := New(ModeProtocolError, stub)
	_, err := obs.PrePut(context.Background(), hbasecop.ObserverEnv{}, &hbasecop.MutationProto{})
	if err == nil {
		t.Fatal("protocol-error mode must return non-nil error from PrePut")
	}
	if !errors.Is(err, ErrProtocolFault) {
		t.Errorf("expected ErrProtocolFault, got %v", err)
	}
	if stub.kill9+stub.exit1+stub.hang+stub.oom != 0 {
		t.Errorf("protocol-error mode must not invoke side-effect actions; stub=%+v", stub)
	}
}

func TestObserver_PostPut_AlwaysNoop(t *testing.T) {
	// PostPut isn't part of the fault matrix in T36 (matrix uses prePut only); the observer
	// must keep PostPut neutral so it doesn't interfere with post-state HBase scans.
	for _, m := range []Mode{ModeNone, ModeKill9, ModeHang, ModeExit1, ModeProtocolError, ModeOOM} {
		stub := &stubActions{}
		obs := New(m, stub)
		if err := obs.PostPut(context.Background(), hbasecop.ObserverEnv{}, &hbasecop.MutationProto{}); err != nil {
			t.Errorf("mode=%v: PostPut err=%v, want nil", m, err)
		}
		if stub.kill9+stub.exit1+stub.hang+stub.oom != 0 {
			t.Errorf("mode=%v: PostPut must not invoke actions; stub=%+v", m, stub)
		}
	}
}

func TestObserver_Invocations_Counts(t *testing.T) {
	stub := &stubActions{}
	obs := New(ModeNone, stub)
	for range 7 {
		_, _ = obs.PrePut(context.Background(), hbasecop.ObserverEnv{}, &hbasecop.MutationProto{})
	}
	if got := obs.Invocations(); got != 7 {
		t.Errorf("Invocations()=%d, want 7", got)
	}
}

func TestNew_NilActions_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic from New(_, nil)")
		}
	}()
	_ = New(ModeNone, nil)
}
