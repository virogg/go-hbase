// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/virogg/go-hbase/internal/shmem"
	"github.com/virogg/go-hbase/internal/wire"
)

const HookPing uint8 = 0xFF

const (
	defaultHeartbeatPeriod = 500 * time.Millisecond

	outboundQueueSize = 256
)

type Handler func(ctx context.Context, req *wire.Message) *wire.Message

type Config struct {
	InCh     *shmem.Channel
	OutCh    *shmem.Channel
	InBulkCh *shmem.Channel

	HeartbeatPeriod time.Duration

	Logger *slog.Logger

	Handler Handler

	ReverseResponseHandler func(*wire.Message)

	EndpointHandler Handler
}

type Loop struct {
	cfg Config
	out chan *wire.Message
}

func New(cfg Config) (*Loop, error) {
	if cfg.InCh == nil {
		return nil, errors.New("cpruntime: Config.InCh is required")
	}
	if cfg.OutCh == nil {
		return nil, errors.New("cpruntime: Config.OutCh is required")
	}
	if cfg.HeartbeatPeriod == 0 {
		cfg.HeartbeatPeriod = defaultHeartbeatPeriod
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Handler == nil {
		cfg.Handler = defaultHandler(cfg.Logger)
	}
	return &Loop{cfg: cfg, out: make(chan *wire.Message, outboundQueueSize)}, nil
}

func defaultHandler(logger *slog.Logger) Handler {
	return func(_ context.Context, req *wire.Message) *wire.Message {
		if req.HookID == HookPing {
			return &wire.Message{
				Type:     wire.TypeResponse,
				ReqID:    req.ReqID,
				RegionID: req.RegionID,
				HookID:   req.HookID,
				Payload:  append([]byte(nil), req.Payload...),
			}
		}
		logger.Warn(
			"cpruntime: no handler for hook",
			"hook_id", req.HookID,
			"req_id", req.ReqID,
		)
		return nil
	}
}

func (l *Loop) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); l.runReader(ctx, cancel) }()
	go func() { defer wg.Done(); l.runWriter(ctx) }()

	if l.cfg.HeartbeatPeriod > 0 {
		wg.Add(1)
		go func() { defer wg.Done(); l.runHeartbeat(ctx) }()
	}

	if l.cfg.InBulkCh != nil {
		wg.Add(1)
		go func() { defer wg.Done(); l.runBulkReader(ctx, cancel) }()
	}

	wg.Wait()
	return parent.Err()
}

func (l *Loop) OutboundChan() chan<- *wire.Message { return l.out }

func (l *Loop) runReader(ctx context.Context, cancel context.CancelFunc) {
	for {
		if ctx.Err() != nil {
			return
		}
		frame, err := l.cfg.InCh.Recv()
		if errors.Is(err, shmem.ErrNoData) {
			runtime.Gosched()
			continue
		}
		if err != nil {
			l.cfg.Logger.Error("cpruntime: inbound recv failed", "err", err)
			cancel()
			return
		}

		msg, err := decodeFrame(frame)
		if err != nil {
			l.cfg.Logger.Error("cpruntime: decode failed", "err", err)
			continue
		}

		switch msg.Type {
		case wire.TypeRequest:
			go l.handle(ctx, msg)
		case wire.TypeEndpointInvoke:
			go l.handleEndpoint(ctx, msg)
		case wire.TypeRPCResponse:
			if h := l.cfg.ReverseResponseHandler; h != nil {
				h(msg)
			}
		case wire.TypeShutdown:
			l.cfg.Logger.Info("cpruntime: inbound SHUTDOWN received")
			cancel()
			return
		default:
			l.cfg.Logger.Debug("cpruntime: unexpected frame on control ring (ignored)",
				"type", uint8(msg.Type), "req_id", msg.ReqID)
		}
	}
}

func (l *Loop) runBulkReader(ctx context.Context, cancel context.CancelFunc) {
	for {
		if ctx.Err() != nil {
			return
		}
		frame, err := l.cfg.InBulkCh.Recv()
		if errors.Is(err, shmem.ErrNoData) {
			runtime.Gosched()
			continue
		}
		if err != nil {
			l.cfg.Logger.Error("cpruntime: bulk recv failed", "err", err)
			cancel()
			return
		}

		msg, err := decodeFrame(frame)
		if err != nil {
			l.cfg.Logger.Error("cpruntime: bulk decode failed", "err", err)
			continue
		}

		if msg.Type == wire.TypeRPCResponse {
			if h := l.cfg.ReverseResponseHandler; h != nil {
				h(msg)
			}
			continue
		}
		l.cfg.Logger.Warn("cpruntime: unexpected frame on bulk ring (ignored)",
			"type", uint8(msg.Type), "req_id", msg.ReqID)
	}
}

func (l *Loop) handle(ctx context.Context, req *wire.Message) {
	defer func() {
		if r := recover(); r != nil {
			l.cfg.Logger.Error("cpruntime: handler panic recovered",
				"req_id", req.ReqID, "hook_id", req.HookID, "panic", r)
		}
	}()
	resp := l.cfg.Handler(ctx, req)
	if resp == nil {
		return
	}
	select {
	case l.out <- resp:
	case <-ctx.Done():
	}
}

func (l *Loop) handleEndpoint(ctx context.Context, req *wire.Message) {
	defer func() {
		if r := recover(); r != nil {
			l.cfg.Logger.Error("cpruntime: endpoint handler panic recovered",
				"req_id", req.ReqID, "panic", r)
		}
	}()
	h := l.cfg.EndpointHandler
	if h == nil {
		l.cfg.Logger.Warn("cpruntime: EndpointInvoke with no EndpointHandler", "req_id", req.ReqID)
		return
	}
	resp := h(ctx, req)
	if resp == nil {
		return
	}
	select {
	case l.out <- resp:
	case <-ctx.Done():
	}
}

func (l *Loop) runWriter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-l.out:
			frame, err := encodeFrame(msg)
			if err != nil {
				l.cfg.Logger.Error("cpruntime: encode failed", "err", err)
				continue
			}
			if !l.sendWithBackoff(ctx, frame) {
				return
			}
		}
	}
}

func (l *Loop) sendWithBackoff(ctx context.Context, frame []byte) bool {
	for {
		err := l.cfg.OutCh.Send(frame)
		if err == nil {
			return true
		}
		if !errors.Is(err, shmem.ErrRingFull) {
			l.cfg.Logger.Error("cpruntime: outbound send failed", "err", err)
			return false
		}
		if ctx.Err() != nil {
			return false
		}
		runtime.Gosched()
	}
}

func (l *Loop) runHeartbeat(ctx context.Context) {
	t := time.NewTicker(l.cfg.HeartbeatPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			select {
			case l.out <- &wire.Message{Type: wire.TypeHeartbeat}:
			default:
				// Outbound queue saturated; drop this beat rather than
				// block the ticker. A missed heartbeat surfaces via the
				// Java supervisor's watchdog (T33).
				l.cfg.Logger.Warn("cpruntime: heartbeat dropped (outbound queue full)")
			}
		}
	}
}

func encodeFrame(m *wire.Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := wire.NewEncoder(&buf).Encode(m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeFrame(data []byte) (*wire.Message, error) {
	r := bytes.NewReader(data)
	msg, err := wire.NewDecoder(r).Decode()
	if err != nil {
		return nil, err
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("wire: %d trailing bytes after message (slot must carry exactly one)", r.Len())
	}
	return msg, nil
}
