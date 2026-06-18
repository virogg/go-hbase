// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

// defaultReverseTimeout bounds a single reverse RPC. Without it, a dropped or
// lost reply (e.g. the bridge's bulk-ring send failed and was swallowed) would
// block the calling endpoint goroutine — and leak its pending entry and any open
// scanner — for the whole process lifetime, since the endpoint ctx is
// process-scoped. The bound converts that into a clean error. (Per-call tuning
// is TE42 admission control.)
const defaultReverseTimeout = 30 * time.Second

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
	logger  *slog.Logger
	timeout time.Duration

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
	return &ReverseClient{
		logger:  logger,
		timeout: defaultReverseTimeout,
		pending: make(map[uint64]chan *wire.Message),
	}
}

// SetTimeout overrides the per-reverse-call deadline. A non-positive value keeps
// the caller's context as the only bound (intended for tests).
func (c *ReverseClient) SetTimeout(d time.Duration) { c.timeout = d }

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

// call sends one reverse RPC and blocks until the Java servicer replies or ctx
// is done. It allocates and registers the waiter before sending, so a fast reply
// can never race ahead of registration. A servicing error (RpcResponse_ERROR) is
// returned as an error with the response carrying the failure detail in its
// payload; the response is still returned for callers that want it.
func (c *ReverseClient) call(ctx context.Context, regionID uint32, req *wirepb.RpcRequest) (*wirepb.RpcResponse, error) {
	if c.out == nil {
		return nil, fmt.Errorf("cpruntime: ReverseClient not bound to an outbound writer")
	}
	// Bound every reverse call so a dropped/lost reply can never block the calling
	// endpoint goroutine (and leak its pending entry + open scanner) for the whole
	// process lifetime; the endpoint ctx is process-scoped.
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("cpruntime: marshal RpcRequest: %w", err)
	}

	id := c.nextID.Add(1) // first id == 1; req_id 0 is reserved
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
			return &resp, fmt.Errorf("cpruntime: reverse %s failed: %s", req.GetOp(), string(resp.GetPayload()))
		}
		return &resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Get issues a reverse GET against region regionID. getProto is a marshalled
// vendored HBase Get; on success the returned RpcResponse payload carries a
// marshalled vendored HBase Result.
func (c *ReverseClient) Get(ctx context.Context, regionID uint32, getProto []byte) (*wirepb.RpcResponse, error) {
	return c.call(ctx, regionID, &wirepb.RpcRequest{Op: wirepb.RpcRequest_GET, OpPayload: getProto})
}

// OpenScanner opens a server-side scanner (Tier 2, TE33). scanProto is a
// marshalled vendored HBase Scan; callID groups the scanner with its endpoint
// call for lifecycle/reaping. Returns the bridge-assigned scanner id.
func (c *ReverseClient) OpenScanner(ctx context.Context, regionID uint32, callID uint64, scanProto []byte) (uint64, error) {
	resp, err := c.call(ctx, regionID, &wirepb.RpcRequest{
		Op:        wirepb.RpcRequest_SCAN_OPEN,
		CallId:    callID,
		OpPayload: scanProto,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetScannerId(), nil
}

// ScanNext pulls the next batch from scanner scannerID. The returned RpcResponse
// payload carries a marshalled vendored HBase Result (flat cells across rows);
// RpcResponse.has_more reports whether more rows remain (resumable).
func (c *ReverseClient) ScanNext(ctx context.Context, regionID uint32, callID, scannerID uint64) (*wirepb.RpcResponse, error) {
	return c.call(ctx, regionID, &wirepb.RpcRequest{
		Op:        wirepb.RpcRequest_SCAN_NEXT,
		CallId:    callID,
		ScannerId: scannerID,
	})
}

// ScanClose closes and deregisters scanner scannerID.
func (c *ReverseClient) ScanClose(ctx context.Context, regionID uint32, callID, scannerID uint64) error {
	_, err := c.call(ctx, regionID, &wirepb.RpcRequest{
		Op:        wirepb.RpcRequest_SCAN_CLOSE,
		CallId:    callID,
		ScannerId: scannerID,
	})
	return err
}

// Mutate issues a reverse MUTATE against region regionID (Tier 2, TE41).
// mutateProto is a marshalled vendored HBase MutationProto (PUT or DELETE); on
// success the bridge applies it to the region and replies OK with an empty
// payload, so (like ScanClose) only the error is returned. Gated server-side by
// hbasecop.endpoint.allow-mutate (off by default): when disabled the reply is an
// ERROR, surfaced here as an error.
func (c *ReverseClient) Mutate(ctx context.Context, regionID uint32, mutateProto []byte) error {
	_, err := c.call(ctx, regionID, &wirepb.RpcRequest{Op: wirepb.RpcRequest_MUTATE, OpPayload: mutateProto})
	return err
}
