// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package shmem

import (
	"errors"
	"fmt"

	"github.com/viroge/go-shmem/pkg/config"
	"github.com/viroge/go-shmem/pkg/memory"
	"github.com/viroge/go-shmem/pkg/message"
	"github.com/viroge/go-shmem/pkg/ring"
	"github.com/viroge/go-shmem/pkg/ring/consumer"
	"github.com/viroge/go-shmem/pkg/ring/producer"
)

// Role discriminates the direction of a Channel endpoint.
type Role uint8

// Channel role values. The zero Role is invalid by design so a
// caller-forgotten Role field is rejected at Open.
const (
	RoleProducer Role = iota + 1
	RoleConsumer
)

// Frame is one opaque payload exchanged through a Channel. The wrapper
// carries bytes verbatim; framing and serialization are the caller's
// concern (see internal/wire).
type Frame = []byte

// Config selects a single shmem ring and the role this Channel plays.
// Backend defaults to "mmap" if empty. For "mmap" set Filename; for
// "posix_shm" set ShmName (must start with '/').
type Config struct {
	Backend       string
	Filename      string
	ShmName       string
	Capacity      int
	MaxObjectSize int
	Role          Role
}

// Channel is a one-way endpoint over a java-go-shmem ring.
type Channel struct {
	role           Role
	maxPayloadSize int
	msg            *message.PayloadMessage

	prod *producer.Producer
	cons *consumer.Consumer
}

// Sentinel errors. ErrRingFull / ErrNoData mirror the upstream ring
// constants verbatim so errors.Is keeps working across the boundary.
var (
	ErrRingFull      = ring.ErrRingFull
	ErrNoData        = ring.ErrNoData
	ErrFrameTooLarge = errors.New("shmem: frame exceeds max payload size")
	ErrWrongRole     = errors.New("shmem: operation not permitted for this role")
)

// Open creates one endpoint of a shmem ring as specified by cfg. The
// underlying region is auto-created on first Open and shared with the
// peer endpoint opened against the same Filename/ShmName.
func Open(cfg Config) (*Channel, error) {
	r, err := toRingConfig(cfg)
	if err != nil {
		return nil, err
	}

	maxPayload := cfg.MaxObjectSize - 4 // 4-byte length prefix per PayloadMessage layout
	if maxPayload <= 0 {
		return nil, fmt.Errorf("shmem: MaxObjectSize=%d leaves no room for payload", cfg.MaxObjectSize)
	}

	ch := &Channel{
		role:           cfg.Role,
		maxPayloadSize: maxPayload,
		msg:            message.NewPayloadMessage(maxPayload),
	}

	switch cfg.Role {
	case RoleProducer:
		p, err := producer.Open(r)
		if err != nil {
			return nil, fmt.Errorf("shmem: open producer: %w", err)
		}
		ch.prod = p
	case RoleConsumer:
		c, err := consumer.Open(r)
		if err != nil {
			return nil, fmt.Errorf("shmem: open consumer: %w", err)
		}
		ch.cons = c
	default:
		return nil, fmt.Errorf("shmem: invalid role: %d", cfg.Role)
	}

	return ch, nil
}

// Send publishes one frame to the ring. Returns ErrWrongRole on a
// consumer channel, ErrFrameTooLarge if the frame does not fit one
// slot, or ErrRingFull if the consumer has not yet caught up.
func (c *Channel) Send(f Frame) error {
	if c.role != RoleProducer {
		return ErrWrongRole
	}
	if len(f) > c.maxPayloadSize {
		return fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, len(f), c.maxPayloadSize)
	}

	addr, err := c.prod.NextToDispatch()
	if err != nil {
		return err
	}

	if err := c.msg.SetPayload(f); err != nil {
		return err
	}
	c.msg.WriteTo(addr)
	c.prod.Flush()
	return nil
}

// Recv fetches the next available frame. Returns ErrWrongRole on a
// producer channel or ErrNoData if the ring is empty. The returned
// slice is a fresh copy and safe to retain past the next Recv.
func (c *Channel) Recv() (Frame, error) {
	if c.role != RoleConsumer {
		return nil, ErrWrongRole
	}

	addr, err := c.cons.FetchNext()
	if err != nil {
		return nil, err
	}

	if err := c.msg.ReadFrom(addr); err != nil {
		c.cons.RollBack()
		return nil, err
	}

	payload := c.msg.GetPayload()
	out := make([]byte, len(payload))
	copy(out, payload)

	c.cons.DoneFetching()
	return out, nil
}

// Close releases the mmap/shm region for this endpoint. The underlying
// file or shm object is left in place; the supervisor decides when to
// unlink it.
func (c *Channel) Close() error {
	switch {
	case c.prod != nil:
		err := c.prod.Close(false)
		c.prod = nil
		return err
	case c.cons != nil:
		err := c.cons.Close(false)
		c.cons = nil
		return err
	}
	return nil
}

func toRingConfig(cfg Config) (config.Ring, error) {
	if cfg.Capacity <= 0 {
		return config.Ring{}, fmt.Errorf("shmem: Capacity must be > 0, got %d", cfg.Capacity)
	}
	if cfg.MaxObjectSize <= 0 {
		return config.Ring{}, fmt.Errorf("shmem: MaxObjectSize must be > 0, got %d", cfg.MaxObjectSize)
	}

	backend := cfg.Backend
	if backend == "" {
		backend = memory.BackendMmap
	}

	switch backend {
	case memory.BackendMmap:
		if cfg.Filename == "" {
			return config.Ring{}, errors.New("shmem: Filename required for mmap backend")
		}
	case memory.BackendPosixShm:
		if cfg.ShmName == "" {
			return config.Ring{}, errors.New("shmem: ShmName required for posix_shm backend")
		}
	default:
		return config.Ring{}, fmt.Errorf("shmem: unknown backend %q", backend)
	}

	return config.Ring{
		Backend:       backend,
		Filename:      cfg.Filename,
		ShmName:       cfg.ShmName,
		Capacity:      cfg.Capacity,
		MaxObjectSize: cfg.MaxObjectSize,
	}, nil
}
