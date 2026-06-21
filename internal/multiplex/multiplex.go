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

var ErrChannelClosed = errors.New("multiplex: channel closed")

type Sender func(*wire.Message) error

type Multiplexer struct {
	send   Sender
	nextID atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]chan<- *wire.Message
	closed  bool
}

func New(send Sender) *Multiplexer {
	return &Multiplexer{
		send:    send,
		pending: make(map[uint64]chan<- *wire.Message),
	}
}

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
	ch <- resp
	return true
}

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
