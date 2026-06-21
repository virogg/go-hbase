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

const defaultReverseTimeout = 30 * time.Second

type ReverseClient struct {
	logger  *slog.Logger
	timeout time.Duration

	out    chan<- *wire.Message
	nextID atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]chan *wire.Message
}

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

func (c *ReverseClient) SetTimeout(d time.Duration) { c.timeout = d }

func (c *ReverseClient) Bind(out chan<- *wire.Message) { c.out = out }

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

func (c *ReverseClient) call(ctx context.Context, regionID uint32, req *wirepb.RpcRequest) (*wirepb.RpcResponse, error) {
	if c.out == nil {
		return nil, fmt.Errorf("cpruntime: ReverseClient not bound to an outbound writer")
	}
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

	msg := &wire.Message{Type: wire.TypeRPCRequest, ReqID: id, RegionID: regionID, Payload: payload}
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

func (c *ReverseClient) Get(ctx context.Context, regionID uint32, getProto []byte) (*wirepb.RpcResponse, error) {
	return c.call(ctx, regionID, &wirepb.RpcRequest{Op: wirepb.RpcRequest_GET, OpPayload: getProto})
}

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

func (c *ReverseClient) ScanNext(ctx context.Context, regionID uint32, callID, scannerID uint64) (*wirepb.RpcResponse, error) {
	return c.call(ctx, regionID, &wirepb.RpcRequest{
		Op:        wirepb.RpcRequest_SCAN_NEXT,
		CallId:    callID,
		ScannerId: scannerID,
	})
}

func (c *ReverseClient) ScanClose(ctx context.Context, regionID uint32, callID, scannerID uint64) error {
	_, err := c.call(ctx, regionID, &wirepb.RpcRequest{
		Op:        wirepb.RpcRequest_SCAN_CLOSE,
		CallId:    callID,
		ScannerId: scannerID,
	})
	return err
}

func (c *ReverseClient) Mutate(ctx context.Context, regionID uint32, mutateProto []byte) error {
	_, err := c.call(ctx, regionID, &wirepb.RpcRequest{Op: wirepb.RpcRequest_MUTATE, OpPayload: mutateProto})
	return err
}
