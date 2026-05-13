// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package multiplex

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/virogg/go-hbase/internal/wire"
)

// ErrChannelClosed is returned by Call when the Multiplexer has been
// (or is) shut down and pending requests can no longer be matched
// against incoming responses.
var ErrChannelClosed = errors.New("multiplex: channel closed")

// Sender writes one outbound frame. Implementations must be safe for
// concurrent invocation from multiple Call goroutines.
type Sender func(*wire.Message) error

// Multiplexer correlates outbound Requests with inbound Responses
// over a single shared channel. Each Call allocates a strictly
// monotonic uint64 req_id, registers a waiter, hands the message to
// the Sender and blocks until the reader Deliver()s a Response with
// the same req_id, the caller's context is canceled, or Close is
// invoked.
//
// One Multiplexer instance corresponds to one (in,out) channel pair;
// region/hook fan-out lives in higher layers.
type Multiplexer struct {
	send   Sender
	nextID atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]chan<- *wire.Message
	closed  bool
}

// New returns a ready-to-use Multiplexer that writes outbound frames
// via send. send must be safe to call concurrently.
func New(send Sender) *Multiplexer {
	return &Multiplexer{
		send:    send,
		pending: make(map[uint64]chan<- *wire.Message),
	}
}

// Call assigns msg a fresh monotonic req_id, marks it as TypeRequest
// and dispatches it through the Sender, then blocks until a matching
// Response is delivered or the call is unblocked by ctx cancellation
// or Close.
//
// The supplied msg is shallow-copied before its Type/ReqID are
// rewritten, so the caller's value is left untouched.
func (m *Multiplexer) Call(ctx context.Context, msg *wire.Message) (*wire.Message, error) {
	id := m.nextID.Add(1)
	ch := make(chan *wire.Message, 1)

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrChannelClosed
	}
	m.pending[id] = ch
	m.mu.Unlock()

	out := *msg
	out.Type = wire.TypeRequest
	out.ReqID = id

	if err := m.send(&out); err != nil {
		m.release(id)
		return nil, err
	}

	select {
	case resp, ok := <-ch:
		if !ok || resp == nil {
			return nil, ErrChannelClosed
		}
		return resp, nil
	case <-ctx.Done():
		m.release(id)
		return nil, ctx.Err()
	}
}

// Deliver hands resp to the goroutine that issued the matching Call.
// Returns true when a waiter was found and notified, false when no
// pending entry exists for resp.ReqID — the caller can log the
// orphan or treat it as a benign late arrival.
func (m *Multiplexer) Deliver(resp *wire.Message) bool {
	m.mu.Lock()
	ch, ok := m.pending[resp.ReqID]
	if ok {
		delete(m.pending, resp.ReqID)
	}
	m.mu.Unlock()
	if !ok {
		return false
	}
	// ch is buffered (cap=1) and we own the only producer slot, so
	// the send never blocks.
	ch <- resp
	return true
}

// Close transitions the Multiplexer to a terminal state: every
// pending Call returns ErrChannelClosed and subsequent Calls are
// rejected the same way. Idempotent.
func (m *Multiplexer) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	pending := m.pending
	m.pending = nil
	m.mu.Unlock()
	for _, ch := range pending {
		close(ch)
	}
}

func (m *Multiplexer) release(id uint64) {
	m.mu.Lock()
	delete(m.pending, id)
	m.mu.Unlock()
}
