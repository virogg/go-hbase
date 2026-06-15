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
	"strings"
	"syscall"
	"time"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/shmem"
)

// Run is the SDK entrypoint for a Go-implemented HBase coprocessor.
// User main calls Run with one or more observers; Run blocks for the
// coprocessor lifetime, draining inbound hook invocations off the
// Java↔Go shmem rings and dispatching them onto observer methods.
//
// Config from environment, set by the Java supervisor (T18):
//
//	HBASECOP_SHMEM_IN_PATH         mmap file consumed by Go (Java writes)
//	HBASECOP_SHMEM_OUT_PATH        mmap file produced by Go (Java reads)
//	HBASECOP_RING_CAPACITY         ring slot count
//	HBASECOP_RING_MAX_OBJECT_SIZE  ring slot byte size
//	HBASECOP_HEARTBEAT_MS          heartbeat period in ms; 0 = default,
//	                               <0 disables (tests only)
//
// Returns nil on clean shutdown (SIGINT/SIGTERM or inbound SHUTDOWN
// frame), error on setup/transport failure. Phase 2 supports a single
// RegionObserver; fan-out across Observer surfaces is T41+.
func Run(observers ...RegionObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.Run: at least one observer required")
	}
	if len(observers) > 1 {
		return errors.New("hbasecop.Run: multiple observers not supported in Phase 2")
	}
	logger := newLogger()

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

// RunMaster (T51) is the master-side counterpart of Run: same shared
// runtime (shmem + heartbeat + dispatcher loop) against a MasterObserver
// instead of a RegionObserver. One process serves one observer surface,
// region or master, not both; keeps coproc-jar packaging symmetric
// between RegionCoprocessor and MasterCoprocessor on the Java side.
func RunMaster(masters ...MasterObserver) error {
	if len(masters) == 0 {
		return errors.New("hbasecop.RunMaster: at least one master observer required")
	}
	if len(masters) > 1 {
		return errors.New("hbasecop.RunMaster: multiple observers not supported")
	}
	logger := newLogger()

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

// RunRegionServer (T52) is the region-server counterpart: shared runtime
// against a RegionServerObserver. One process serves one observer surface
// (region, master or region-server), keeping coproc-jar packaging
// symmetric across the three Coprocessor kinds on the Java side.
func RunRegionServer(observers ...RegionServerObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunRegionServer: at least one region-server observer required")
	}
	if len(observers) > 1 {
		return errors.New("hbasecop.RunRegionServer: multiple observers not supported")
	}
	logger := newLogger()

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

// RunWAL (T53) is the WAL-side counterpart: shared runtime against a
// WALObserver. One process serves one observer surface (region, master,
// region-server or WAL), keeping coproc-jar packaging symmetric across
// the Coprocessor kinds on the Java side.
func RunWAL(observers ...WALObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunWAL: at least one WAL observer required")
	}
	if len(observers) > 1 {
		return errors.New("hbasecop.RunWAL: multiple observers not supported")
	}
	logger := newLogger()

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

// RunBulkLoad (T54) is the bulk-load counterpart: shared runtime against
// a BulkLoadObserver. One process serves one observer surface (region,
// master, region-server, WAL or bulk-load), keeping coproc-jar packaging
// symmetric across the Coprocessor kinds on the Java side.
func RunBulkLoad(observers ...BulkLoadObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunBulkLoad: at least one bulk-load observer required")
	}
	if len(observers) > 1 {
		return errors.New("hbasecop.RunBulkLoad: multiple observers not supported")
	}
	logger := newLogger()

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
		return fmt.Errorf("hbasecop.RunBulkLoad: open inbound ring: %w", err)
	}
	defer func() { _ = inCh.Close() }()

	outCh, err := shmem.Open(shmem.Config{
		Filename:      cfg.outPath,
		Capacity:      cfg.capacity,
		MaxObjectSize: cfg.maxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err != nil {
		return fmt.Errorf("hbasecop.RunBulkLoad: open outbound ring: %w", err)
	}
	defer func() { _ = outCh.Close() }()

	d := newBulkLoadDispatcher(observers[0], logger)
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

	logger.Info("hbasecop: started (bulk-load)",
		"pid", os.Getpid(),
		"in_path", cfg.inPath,
		"out_path", cfg.outPath,
	)
	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("hbasecop: clean exit (bulk-load)")
	return nil
}

// newLogger builds the slog JSON logger shared by every Run* entrypoint.
// Level comes from HBASECOP_LOG_LEVEL (SPEC §6); unset or unrecognized
// falls back to info. JSON to stderr is the only MVP observability surface.
func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: logLevelFromEnv()}))
}

// logLevelFromEnv maps HBASECOP_LOG_LEVEL (case-insensitive,
// whitespace-trimmed) onto a slog.Level. Accepts debug|info|warn|error
// (and "warning" as an alias); anything else, including empty, is info.
func logLevelFromEnv() slog.Level {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("HBASECOP_LOG_LEVEL"))) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
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
