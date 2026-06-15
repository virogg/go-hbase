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
	return runDispatcher(&dispatcher{observers: observers, logger: newLogger()}, "")
}

// runDispatcher is the shared event-loop core behind every Run* entrypoint:
// load the shmem config from env, open the in/out rings, then drain inbound
// hook frames onto the dispatcher until SIGINT/SIGTERM or a SHUTDOWN frame.
// label is appended to the start/exit log lines to name the surface(s).
func runDispatcher(d *dispatcher, label string) error {
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
		return fmt.Errorf("hbasecop: open inbound ring: %w", err)
	}
	defer func() { _ = inCh.Close() }()

	outCh, err := shmem.Open(shmem.Config{
		Filename:      cfg.outPath,
		Capacity:      cfg.capacity,
		MaxObjectSize: cfg.maxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err != nil {
		return fmt.Errorf("hbasecop: open outbound ring: %w", err)
	}
	defer func() { _ = outCh.Close() }()

	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            inCh,
		OutCh:           outCh,
		HeartbeatPeriod: cfg.heartbeat,
		Logger:          d.logger,
		Handler:         d.dispatch,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	d.logger.Info("hbasecop: started"+label,
		"pid", os.Getpid(),
		"in_path", cfg.inPath,
		"out_path", cfg.outPath,
	)
	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	d.logger.Info("hbasecop: clean exit" + label)
	return nil
}

// RunAll serves observers of mixed surfaces in one process over a single shmem
// pair. Each argument is routed to every Observer surface it satisfies
// (Region/Master/RegionServer/WAL/BulkLoad); an argument implementing none is
// an error. Same-surface observers are chained (see foldObservers).
func RunAll(observers ...any) error {
	d, err := newMixedDispatcher(newLogger(), observers...)
	if err != nil {
		return err
	}
	return runDispatcher(d, " (multi)")
}

func newMixedDispatcher(logger *slog.Logger, observers ...any) (*dispatcher, error) {
	if len(observers) == 0 {
		return nil, errors.New("hbasecop.RunAll: at least one observer required")
	}
	d := &dispatcher{logger: logger}
	for _, o := range observers {
		matched := false
		if obs, ok := o.(RegionObserver); ok {
			d.observers = append(d.observers, obs)
			matched = true
		}
		if obs, ok := o.(MasterObserver); ok {
			d.masters = append(d.masters, obs)
			matched = true
		}
		if obs, ok := o.(RegionServerObserver); ok {
			d.regionServers = append(d.regionServers, obs)
			matched = true
		}
		if obs, ok := o.(WALObserver); ok {
			d.wals = append(d.wals, obs)
			matched = true
		}
		if obs, ok := o.(BulkLoadObserver); ok {
			d.bulkLoads = append(d.bulkLoads, obs)
			matched = true
		}
		if !matched {
			return nil, fmt.Errorf("hbasecop.RunAll: %T implements no Observer surface", o)
		}
	}
	return d, nil
}

// RunMaster (T51) is the master-side counterpart of Run against a
// MasterObserver. Pass several to chain them; to serve more than one surface
// (e.g. master + region) in one process use RunAll.
func RunMaster(masters ...MasterObserver) error {
	if len(masters) == 0 {
		return errors.New("hbasecop.RunMaster: at least one master observer required")
	}
	return runDispatcher(&dispatcher{masters: masters, logger: newLogger()}, " (master)")
}

// RunRegionServer (T52) is the region-server counterpart against a
// RegionServerObserver. Pass several to chain them; use RunAll to serve
// multiple surfaces in one process.
func RunRegionServer(observers ...RegionServerObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunRegionServer: at least one region-server observer required")
	}
	return runDispatcher(&dispatcher{regionServers: observers, logger: newLogger()}, " (region-server)")
}

// RunWAL (T53) is the WAL-side counterpart against a WALObserver. Pass several
// to chain them; use RunAll to serve multiple surfaces in one process.
func RunWAL(observers ...WALObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunWAL: at least one WAL observer required")
	}
	return runDispatcher(&dispatcher{wals: observers, logger: newLogger()}, " (wal)")
}

// RunBulkLoad (T54) is the bulk-load counterpart against a BulkLoadObserver.
// Pass several to chain them; use RunAll to serve multiple surfaces in one
// process.
func RunBulkLoad(observers ...BulkLoadObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunBulkLoad: at least one bulk-load observer required")
	}
	return runDispatcher(&dispatcher{bulkLoads: observers, logger: newLogger()}, " (bulk-load)")
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
