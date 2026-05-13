// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package cpruntime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/virogg/go-hbase/internal/shmem"
	"github.com/virogg/go-hbase/internal/wire"
)

// HookPing is the sentinel HookID for the in-process ping/pong probe
// used by T17 and T19. The HBase observer table (T41) numbers real
// hooks starting at 1, so 0xFF is reserved here and will never collide.
const HookPing uint8 = 0xFF

const (
	defaultHeartbeatPeriod = 500 * time.Millisecond

	// outboundQueueSize bounds the in-flight backlog between handler
	// goroutines and the single writer. A handler that finds the queue
	// full blocks until either the writer drains or the run context is
	// canceled — backpressure rather than silent drop.
	outboundQueueSize = 256
)

// Handler processes one inbound Request frame and returns the frame
// to send back to the Java side. Returning nil drops the request
// silently — appropriate for fire-and-forget hooks that have no reply.
//
// Handler is invoked on a fresh goroutine per request, so blocking
// inside a Handler does not stall the reader; the Loop's outbound
// queue applies backpressure when handlers outrun the writer.
type Handler func(ctx context.Context, req *wire.Message) *wire.Message

// Config configures a Loop.
type Config struct {
	// InCh is the consumer endpoint of the Java→Go ring.
	InCh *shmem.Channel
	// OutCh is the producer endpoint of the Go→Java ring.
	OutCh *shmem.Channel

	// HeartbeatPeriod is the interval between outbound Heartbeat
	// frames. Zero defaults to 500ms; a negative value disables
	// heartbeats (tests use this to keep the latency channel clean).
	HeartbeatPeriod time.Duration

	// Logger receives structured event records. Nil falls back to
	// slog.Default().
	Logger *slog.Logger

	// Handler dispatches inbound Request frames. When nil the Loop
	// falls back to a default that echoes HookPing (test probe) and
	// warns for every other hook id — sufficient for T17/T19 but not
	// for real coprocessors. The SDK (pkg/hbasecop) supplies a
	// dispatcher that routes to user-implemented Observer methods.
	Handler Handler
}

// Loop owns one in-process Go-runtime event loop: one reader goroutine
// over InCh, one writer goroutine over OutCh, and optionally one
// heartbeat ticker. Handler goroutines are spawned per inbound
// Request.
type Loop struct {
	cfg Config
	out chan *wire.Message
}

// New validates cfg and returns a non-running Loop. Call Run to start.
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

// defaultHandler is installed when Config.Handler is nil. It exists so
// the pre-SDK tests (T17/T19 ping/pong) keep working without forcing
// callers to register their own handler. Real coprocessors install a
// Handler via pkg/hbasecop.Run.
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

// Run starts the reader, writer and heartbeat goroutines and blocks
// until either the parent context is canceled or an inbound SHUTDOWN
// frame arrives. Returns parent.Err() — nil if the loop stopped via
// SHUTDOWN.
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

	wg.Wait()
	return parent.Err()
}

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
		case wire.TypeShutdown:
			l.cfg.Logger.Info("cpruntime: inbound SHUTDOWN received")
			cancel()
			return
		}
		// Other inbound types (Heartbeat, Log, Response, Error) are
		// ignored at this layer; the supervisor (T18+) drives them
		// from the Java side.
	}
}

func (l *Loop) handle(ctx context.Context, req *wire.Message) {
	resp := l.cfg.Handler(ctx, req)
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
				// Outbound queue saturated — drop this beat rather
				// than block the ticker. A missed heartbeat surfaces
				// via the Java supervisor's watchdog (T33).
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
	return wire.NewDecoder(bytes.NewReader(data)).Decode()
}
