// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

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

// Run is the SDK entrypoint for an HBase coprocessor implemented in
// Go. The user's main function calls Run with one or more observer
// implementations; Run blocks for the lifetime of the coprocessor,
// draining inbound hook invocations off the Java↔Go shmem rings and
// dispatching them onto observer methods.
//
// Configuration is taken from the environment, set by the Java
// supervisor (T18):
//
//	HBASECOP_SHMEM_IN_PATH         mmap file consumed by Go (Java writes)
//	HBASECOP_SHMEM_OUT_PATH        mmap file produced by Go (Java reads)
//	HBASECOP_RING_CAPACITY         ring slot count
//	HBASECOP_RING_MAX_OBJECT_SIZE  ring slot byte size
//	HBASECOP_HEARTBEAT_MS          heartbeat period in ms; 0 = default,
//	                               <0 disables (tests only)
//
// Run returns nil on clean shutdown (SIGINT/SIGTERM or inbound
// SHUTDOWN frame), or an error on a setup/transport failure. Phase 2
// supports a single RegionObserver; richer fan-out across Observer
// surfaces is T41+.
func Run(observers ...RegionObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.Run: at least one observer required")
	}
	if len(observers) > 1 {
		return errors.New("hbasecop.Run: multiple observers not supported in Phase 2")
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := loadShmemConfigFromEnv()
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
		return fmt.Errorf("hbasecop.Run: open inbound ring: %w", err)
	}
	defer func() { _ = inCh.Close() }()

	outCh, err := shmem.Open(shmem.Config{
		Filename:      cfg.outPath,
		Capacity:      cfg.capacity,
		MaxObjectSize: cfg.maxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err != nil {
		return fmt.Errorf("hbasecop.Run: open outbound ring: %w", err)
	}
	defer func() { _ = outCh.Close() }()

	d := newDispatcher(observers[0], logger)
	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            inCh,
		OutCh:           outCh,
		HeartbeatPeriod: cfg.heartbeat,
		Logger:          logger,
		Handler:         d.dispatch,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("hbasecop: started",
		"pid", os.Getpid(),
		"in_path", cfg.inPath,
		"out_path", cfg.outPath,
	)
	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("hbasecop: clean exit")
	return nil
}

// RunMaster is the T51 master-side counterpart of Run. It boots the
// shared runtime (shmem + heartbeat + dispatcher loop) against a
// MasterObserver instead of a RegionObserver. A single process serves
// a single observer surface — region or master, not both — which keeps
// the coproc-jar packaging symmetric between RegionCoprocessor and
// MasterCoprocessor on the Java side.
func RunMaster(masters ...MasterObserver) error {
	if len(masters) == 0 {
		return errors.New("hbasecop.RunMaster: at least one master observer required")
	}
	if len(masters) > 1 {
		return errors.New("hbasecop.RunMaster: multiple observers not supported")
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := loadShmemConfigFromEnv()
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
		return fmt.Errorf("hbasecop.RunMaster: open inbound ring: %w", err)
	}
	defer func() { _ = inCh.Close() }()

	outCh, err := shmem.Open(shmem.Config{
		Filename:      cfg.outPath,
		Capacity:      cfg.capacity,
		MaxObjectSize: cfg.maxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err != nil {
		return fmt.Errorf("hbasecop.RunMaster: open outbound ring: %w", err)
	}
	defer func() { _ = outCh.Close() }()

	d := newMasterDispatcher(masters[0], logger)
	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            inCh,
		OutCh:           outCh,
		HeartbeatPeriod: cfg.heartbeat,
		Logger:          logger,
		Handler:         d.dispatch,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("hbasecop: started (master)",
		"pid", os.Getpid(),
		"in_path", cfg.inPath,
		"out_path", cfg.outPath,
	)
	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("hbasecop: clean exit (master)")
	return nil
}

// RunRegionServer is the T52 region-server counterpart of Run and
// RunMaster. It boots the shared runtime (shmem + heartbeat + dispatcher
// loop) against a RegionServerObserver. A single process serves a single
// observer surface — region, master or region-server — which keeps the
// coproc-jar packaging symmetric across the three Coprocessor kinds on
// the Java side.
func RunRegionServer(observers ...RegionServerObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunRegionServer: at least one region-server observer required")
	}
	if len(observers) > 1 {
		return errors.New("hbasecop.RunRegionServer: multiple observers not supported")
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := loadShmemConfigFromEnv()
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
		return fmt.Errorf("hbasecop.RunRegionServer: open inbound ring: %w", err)
	}
	defer func() { _ = inCh.Close() }()

	outCh, err := shmem.Open(shmem.Config{
		Filename:      cfg.outPath,
		Capacity:      cfg.capacity,
		MaxObjectSize: cfg.maxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err != nil {
		return fmt.Errorf("hbasecop.RunRegionServer: open outbound ring: %w", err)
	}
	defer func() { _ = outCh.Close() }()

	d := newRegionServerDispatcher(observers[0], logger)
	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            inCh,
		OutCh:           outCh,
		HeartbeatPeriod: cfg.heartbeat,
		Logger:          logger,
		Handler:         d.dispatch,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("hbasecop: started (region-server)",
		"pid", os.Getpid(),
		"in_path", cfg.inPath,
		"out_path", cfg.outPath,
	)
	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("hbasecop: clean exit (region-server)")
	return nil
}

// RunWAL is the T53 WAL-side counterpart of Run, RunMaster and
// RunRegionServer. It boots the shared runtime (shmem + heartbeat +
// dispatcher loop) against a WALObserver. A single process serves a
// single observer surface — region, master, region-server or WAL — which
// keeps the coproc-jar packaging symmetric across the Coprocessor kinds
// on the Java side.
func RunWAL(observers ...WALObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunWAL: at least one WAL observer required")
	}
	if len(observers) > 1 {
		return errors.New("hbasecop.RunWAL: multiple observers not supported")
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := loadShmemConfigFromEnv()
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
		return fmt.Errorf("hbasecop.RunWAL: open inbound ring: %w", err)
	}
	defer func() { _ = inCh.Close() }()

	outCh, err := shmem.Open(shmem.Config{
		Filename:      cfg.outPath,
		Capacity:      cfg.capacity,
		MaxObjectSize: cfg.maxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err != nil {
		return fmt.Errorf("hbasecop.RunWAL: open outbound ring: %w", err)
	}
	defer func() { _ = outCh.Close() }()

	d := newWALDispatcher(observers[0], logger)
	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            inCh,
		OutCh:           outCh,
		HeartbeatPeriod: cfg.heartbeat,
		Logger:          logger,
		Handler:         d.dispatch,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("hbasecop: started (wal)",
		"pid", os.Getpid(),
		"in_path", cfg.inPath,
		"out_path", cfg.outPath,
	)
	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("hbasecop: clean exit (wal)")
	return nil
}

type shmemEnvConfig struct {
	inPath, outPath         string
	capacity, maxObjectSize int
	heartbeat               time.Duration
}

func loadShmemConfigFromEnv() (shmemEnvConfig, error) {
	var c shmemEnvConfig
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
		c.heartbeat = -1
	case hbMs == 0:
		c.heartbeat = 0
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
