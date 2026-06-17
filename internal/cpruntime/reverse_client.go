// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

// ReverseClient (Tier 2, TE31) is the Go-initiated reverse-RPC channel: a
// running endpoint handler reads region-local data by sending an RpcRequest to
// the Java servicing pool and awaiting the matching RpcResponse.
//
// Requests are written onto the Loop's single outbound writer (the existing
// Go→Java ring); replies arrive on the dedicated bulk ring and are routed back
// here by [ReverseClient.Deliver], wired as the Loop's ReverseResponseHandler.
// Correlation is by a Go-allocated, monotonic wire-header req_id, an id space
// independent of the Java-assigned EndpointInvoke req_ids (only the Go side
// matches reverse RpcResponse frames, so the two never collide).
//
// All methods are safe for concurrent use: one endpoint goroutine per in-flight
// call, all funnelling onto the shared writer.
type ReverseClient struct {
	logger *slog.Logger

	out    chan<- *wire.Message
	nextID atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]chan *wire.Message
}

// NewReverseClient returns a client with no bound writer; call [Bind] with the
// Loop's outbound channel before issuing requests. nil logger uses the default.
func NewReverseClient(logger *slog.Logger) *ReverseClient {
	if logger == nil {
		logger = slog.Default()
	}
	return &ReverseClient{logger: logger, pending: make(map[uint64]chan *wire.Message)}
}

// Bind attaches the outbound writer the client enqueues RpcRequest frames onto
// (the Loop's single Go→Java writer). Call once at startup, before any request.
func (c *ReverseClient) Bind(out chan<- *wire.Message) { c.out = out }

// Deliver routes an inbound RpcResponse to the caller waiting on its req_id.
// Wire it as [Config.ReverseResponseHandler]. It runs on the bulk-reader
// goroutine, so it never blocks: the waiter's channel is buffered for exactly
// one reply, and a frame for an unknown or already-served req_id (a late
// arrival after the caller's context was cancelled) is dropped.
func (c *ReverseClient) Deliver(m *wire.Message) {
	c.mu.Lock()
	ch := c.pending[m.ReqID]
	delete(c.pending, m.ReqID)
	c.mu.Unlock()
	if ch == nil {
		c.logger.Warn("cpruntime: reverse response for unknown req_id (dropped)", "req_id", m.ReqID)
		return
	}
	ch <- m // cap-1 buffered, single delivery: never blocks
}

// Get issues a reverse GET against region regionID and blocks until the Java
// servicer replies or ctx is done. getProto is a marshalled vendored HBase Get;
// on success the returned RpcResponse payload carries a marshalled vendored
// HBase Result. A servicing error (RpcResponse_ERROR) is returned as an error
// with the response carrying the failure detail in its payload.
func (c *ReverseClient) Get(ctx context.Context, regionID uint32, getProto []byte) (*wirepb.RpcResponse, error) {
	if c.out == nil {
		return nil, fmt.Errorf("cpruntime: ReverseClient not bound to an outbound writer")
	}
	payload, err := proto.Marshal(&wirepb.RpcRequest{Op: wirepb.RpcRequest_GET, OpPayload: getProto})
	if err != nil {
		return nil, fmt.Errorf("cpruntime: marshal RpcRequest: %w", err)
	}

	// Allocate and register the waiter before sending, so a fast reply can never
	// race ahead of registration. First id is 1; req_id 0 is reserved.
	id := c.nextID.Add(1)
	ch := make(chan *wire.Message, 1)
	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	msg := &wire.Message{Type: wire.TypeRpcRequest, ReqID: id, RegionID: regionID, Payload: payload}
	select {
	case c.out <- msg:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case m := <-ch:
		var resp wirepb.RpcResponse
		if err := proto.Unmarshal(m.Payload, &resp); err != nil {
			return nil, fmt.Errorf("cpruntime: unmarshal RpcResponse: %w", err)
		}
		if resp.GetStatus() == wirepb.RpcResponse_ERROR {
			return &resp, fmt.Errorf("cpruntime: reverse GET failed: %s", string(resp.GetPayload()))
		}
		return &resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
