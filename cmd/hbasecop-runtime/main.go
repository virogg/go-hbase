// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command hbasecop-runtime is the long-running Go process spawned by the
// Java RegionServer-side bridge (T18). It reads its shmem configuration
// from environment variables, opens the inbound (Java→Go) and outbound
// (Go→Java) rings and runs internal/cpruntime.Loop until either a
// SIGTERM/SIGINT is received or an inbound SHUTDOWN frame arrives.
//
// Environment variables:
//
//	HBASECOP_SHMEM_IN_PATH         mmap file consumed by Go (Java writes)
//	HBASECOP_SHMEM_OUT_PATH        mmap file produced by Go (Java reads)
//	HBASECOP_RING_CAPACITY         ring slot count
//	HBASECOP_RING_MAX_OBJECT_SIZE  ring slot byte size
//	HBASECOP_HEARTBEAT_MS          heartbeat period in ms; 0 = default
//	                               (500ms), <0 disables heartbeats.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/shmem"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("hbasecop-runtime: fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}

	inCh, err := shmem.Open(shmem.Config{
		Filename:      cfg.inPath,
		Capacity:      cfg.capacity,
		MaxObjectSize: cfg.maxObjectSize,
		Role:          shmem.RoleConsumer,
	})
	if err != nil {
		return fmt.Errorf("open inbound ring: %w", err)
	}
	defer func() { _ = inCh.Close() }()

	outCh, err := shmem.Open(shmem.Config{
		Filename:      cfg.outPath,
		Capacity:      cfg.capacity,
		MaxObjectSize: cfg.maxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err != nil {
		return fmt.Errorf("open outbound ring: %w", err)
	}
	defer func() { _ = outCh.Close() }()

	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            inCh,
		OutCh:           outCh,
		HeartbeatPeriod: cfg.heartbeat,
		Logger:          log,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log.Info("hbasecop-runtime: started",
		"pid", os.Getpid(),
		"in_path", cfg.inPath,
		"out_path", cfg.outPath,
		"capacity", cfg.capacity,
		"max_object_size", cfg.maxObjectSize,
	)
	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("hbasecop-runtime: clean exit")
	return nil
}

type config struct {
	inPath, outPath         string
	capacity, maxObjectSize int
	heartbeat               time.Duration
}

func configFromEnv() (config, error) {
	var c config

	c.inPath = os.Getenv("HBASECOP_SHMEM_IN_PATH")
	if c.inPath == "" {
		return c, errors.New("HBASECOP_SHMEM_IN_PATH required")
	}
	c.outPath = os.Getenv("HBASECOP_SHMEM_OUT_PATH")
	if c.outPath == "" {
		return c, errors.New("HBASECOP_SHMEM_OUT_PATH required")
	}

	var err error
	if c.capacity, err = mustEnvInt("HBASECOP_RING_CAPACITY"); err != nil {
		return c, err
	}
	if c.maxObjectSize, err = mustEnvInt("HBASECOP_RING_MAX_OBJECT_SIZE"); err != nil {
		return c, err
	}

	hbMs, err := optionalEnvInt("HBASECOP_HEARTBEAT_MS", 0)
	if err != nil {
		return c, err
	}
	switch {
	case hbMs < 0:
		c.heartbeat = -1 // disabled
	case hbMs == 0:
		c.heartbeat = 0 // cpruntime fills in its default
	default:
		c.heartbeat = time.Duration(hbMs) * time.Millisecond
	}

	return c, nil
}

func mustEnvInt(name string) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, fmt.Errorf("%s required", name)
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: %w", name, v, err)
	}
	return n, nil
}

func optionalEnvInt(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q: %w", name, v, err)
	}
	return n, nil
}
