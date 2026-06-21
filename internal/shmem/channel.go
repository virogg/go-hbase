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

type Role uint8

const (
	RoleProducer Role = iota + 1
	RoleConsumer
)

type Frame = []byte

type Config struct {
	Backend       string
	Filename      string
	ShmName       string
	Capacity      int
	MaxObjectSize int
	Role          Role
}

type Channel struct {
	role           Role
	maxPayloadSize int
	msg            *message.PayloadMessage

	prod *producer.Producer
	cons *consumer.Consumer
}

var (
	ErrRingFull      = ring.ErrRingFull
	ErrNoData        = ring.ErrNoData
	ErrFrameTooLarge = errors.New("shmem: frame exceeds max payload size")
	ErrWrongRole     = errors.New("shmem: operation not permitted for this role")
)

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
