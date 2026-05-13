// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package multiplex_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/virogg/go-hbase/internal/multiplex"
	"github.com/virogg/go-hbase/internal/wire"
)

// startEchoResponder spins up a goroutine that consumes outbound
// frames from out, builds a matching TypeResponse and delivers it
// through m. It stops when stop closes.
func startEchoResponder(t *testing.T, m *multiplex.Multiplexer, out <-chan *wire.Message, stop <-chan struct{}) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case msg, ok := <-out:
				if !ok {
					return
				}
				resp := &wire.Message{
					Type:     wire.TypeResponse,
					ReqID:    msg.ReqID,
					RegionID: msg.RegionID,
					HookID:   msg.HookID,
					Payload:  append([]byte(nil), msg.Payload...),
				}
				m.Deliver(resp)
			}
		}
	}()
	return done
}

// TestMultiplexer_ConcurrentCallsAllMatched is the T24 acceptance
// test: 1000 parallel Call invocations against a single Multiplexer
// must all observe their own matching Response. No goroutine may
// observe another caller's payload.
func TestMultiplexer_ConcurrentCallsAllMatched(t *testing.T) {
	out := make(chan *wire.Message, 1024)
	send := func(msg *wire.Message) error {
		out <- msg
		return nil
	}
	m := multiplex.New(send)

	stop := make(chan struct{})
	responderDone := startEchoResponder(t, m, out, stop)

	const N = 1000
	var wg sync.WaitGroup
	errs := make(chan error, N)

	for i := range N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := fmt.Appendf(nil, "payload-%d", i)
			req := &wire.Message{
				RegionID: uint32(i),
				HookID:   byte(i % 256),
				Payload:  payload,
			}
			resp, err := m.Call(context.Background(), req)
			if err != nil {
				errs <- fmt.Errorf("call %d: %w", i, err)
				return
			}
			if resp.Type != wire.TypeResponse {
				errs <- fmt.Errorf("call %d: want TypeResponse, got %d", i, resp.Type)
				return
			}
			if string(resp.Payload) != string(payload) {
				errs <- fmt.Errorf("call %d: payload mismatch: got %q want %q", i, resp.Payload, payload)
			}
		}(i)
	}

	wg.Wait()
	close(stop)
	<-responderDone

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestMultiplexer_AllocatesMonotonicReqIDs documents the invariant
// that req_id is monotonically increasing across concurrent Call
// invocations.
func TestMultiplexer_AllocatesMonotonicReqIDs(t *testing.T) {
	var seen sync.Map // uint64 → struct{}
	var maxSeen atomic.Uint64

	send := func(msg *wire.Message) error {
		if msg.ReqID == 0 {
			t.Errorf("req_id must not be zero")
		}
		if _, dup := seen.LoadOrStore(msg.ReqID, struct{}{}); dup {
			t.Errorf("duplicate req_id %d", msg.ReqID)
		}
		for {
			cur := maxSeen.Load()
			if msg.ReqID <= cur || maxSeen.CompareAndSwap(cur, msg.ReqID) {
				break
			}
		}
		return nil
	}
	m := multiplex.New(send)

	const N = 256
	var wg sync.WaitGroup
	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			// We expect ctx timeout — the responder isn't running.
			_, _ = m.Call(ctx, &wire.Message{})
		}()
	}
	wg.Wait()

	if got := maxSeen.Load(); got != N {
		t.Errorf("expected max req_id %d, got %d", N, got)
	}
}

// TestMultiplexer_CloseFailsPending — outstanding Calls return
// ErrChannelClosed when Close is invoked.
func TestMultiplexer_CloseFailsPending(t *testing.T) {
	send := func(*wire.Message) error { return nil }
	m := multiplex.New(send)

	errs := make(chan error, 4)
	for range 4 {
		go func() {
			_, err := m.Call(context.Background(), &wire.Message{})
			errs <- err
		}()
	}

	// Give the goroutines a chance to register before Close.
	time.Sleep(20 * time.Millisecond)
	m.Close()

	for range 4 {
		select {
		case err := <-errs:
			if !errors.Is(err, multiplex.ErrChannelClosed) {
				t.Errorf("want ErrChannelClosed, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Call did not return within 1s of Close")
		}
	}
}

// TestMultiplexer_CallAfterCloseRejected — Close is sticky; later
// Call attempts also fail with ErrChannelClosed without invoking the
// sender.
func TestMultiplexer_CallAfterCloseRejected(t *testing.T) {
	var sends atomic.Int32
	send := func(*wire.Message) error {
		sends.Add(1)
		return nil
	}
	m := multiplex.New(send)
	m.Close()

	_, err := m.Call(context.Background(), &wire.Message{})
	if !errors.Is(err, multiplex.ErrChannelClosed) {
		t.Errorf("want ErrChannelClosed, got %v", err)
	}
	if got := sends.Load(); got != 0 {
		t.Errorf("send called %d times after Close; want 0", got)
	}
}

// TestMultiplexer_ContextCancelReleasesSlot — a canceled Call must
// not leak the pending entry; a subsequent Deliver for that req_id
// returns false rather than blocking on a dead receiver.
func TestMultiplexer_ContextCancelReleasesSlot(t *testing.T) {
	captured := make(chan *wire.Message, 1)
	send := func(msg *wire.Message) error {
		captured <- msg
		return nil
	}
	m := multiplex.New(send)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.Call(ctx, &wire.Message{})
		done <- err
	}()

	sent := <-captured
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("want context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Call did not unblock after ctx cancel")
	}

	if m.Deliver(&wire.Message{Type: wire.TypeResponse, ReqID: sent.ReqID}) {
		t.Error("Deliver after cancel returned true; pending slot leaked")
	}
}

// TestMultiplexer_DeliverUnknownReqID — Deliver for a req_id that
// was never issued (or already consumed) returns false without panic.
func TestMultiplexer_DeliverUnknownReqID(t *testing.T) {
	m := multiplex.New(func(*wire.Message) error { return nil })
	if m.Deliver(&wire.Message{Type: wire.TypeResponse, ReqID: 42}) {
		t.Error("Deliver on empty mux returned true")
	}
}

// TestMultiplexer_SendErrorReleasesSlot — when the sender fails the
// Call must return the sender's error and free the pending slot
// immediately.
func TestMultiplexer_SendErrorReleasesSlot(t *testing.T) {
	wantErr := errors.New("send boom")
	m := multiplex.New(func(*wire.Message) error { return wantErr })

	_, err := m.Call(context.Background(), &wire.Message{})
	if !errors.Is(err, wantErr) {
		t.Errorf("want %v, got %v", wantErr, err)
	}

	// Close must be idempotent and not block even though Call failed.
	m.Close()
}
