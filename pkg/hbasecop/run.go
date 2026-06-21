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

func Run(observers ...RegionObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.Run: at least one observer required")
	}
	return runDispatcher(&dispatcher{observers: observers, logger: newLogger()}, "")
}

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

	loopCfg := cpruntime.Config{
		InCh:            inCh,
		OutCh:           outCh,
		HeartbeatPeriod: cfg.heartbeat,
		Logger:          d.logger,
		Handler:         d.dispatch,
		EndpointHandler: d.dispatchEndpoint,
	}

	var reverse *cpruntime.ReverseClient
	if cfg.bulkPath != "" {
		inBulkCh, err := shmem.Open(shmem.Config{
			Filename:      cfg.bulkPath,
			Capacity:      cfg.bulkCapacity,
			MaxObjectSize: cfg.bulkMaxObjectSize,
			Role:          shmem.RoleConsumer,
		})
		if err != nil {
			return fmt.Errorf("hbasecop: open bulk ring: %w", err)
		}
		defer func() { _ = inBulkCh.Close() }()

		reverse = cpruntime.NewReverseClient(d.logger)

		if cfg.reverseCallTimeout > 0 {
			reverse.SetTimeout(cfg.reverseCallTimeout)
		}
		loopCfg.InBulkCh = inBulkCh
		loopCfg.ReverseResponseHandler = reverse.Deliver
	}

	loop, err := cpruntime.New(loopCfg)
	if err != nil {
		return err
	}
	if reverse != nil {
		reverse.Bind(loop.OutboundChan())
		d.reverse = reverse
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
		if ep, ok := o.(Endpoint); ok {
			if d.endpoint != nil {
				return nil, errors.New("hbasecop.RunAll: more than one Endpoint registered")
			}
			d.endpoint = ep
			matched = true
		}
		if !matched {
			return nil, fmt.Errorf("hbasecop.RunAll: %T implements no Observer or Endpoint surface", o)
		}
	}
	return d, nil
}

func RunMaster(masters ...MasterObserver) error {
	if len(masters) == 0 {
		return errors.New("hbasecop.RunMaster: at least one master observer required")
	}
	return runDispatcher(&dispatcher{masters: masters, logger: newLogger()}, " (master)")
}

func RunRegionServer(observers ...RegionServerObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunRegionServer: at least one region-server observer required")
	}
	return runDispatcher(&dispatcher{regionServers: observers, logger: newLogger()}, " (region-server)")
}

func RunWAL(observers ...WALObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunWAL: at least one WAL observer required")
	}
	return runDispatcher(&dispatcher{wals: observers, logger: newLogger()}, " (wal)")
}

func RunBulkLoad(observers ...BulkLoadObserver) error {
	if len(observers) == 0 {
		return errors.New("hbasecop.RunBulkLoad: at least one bulk-load observer required")
	}
	return runDispatcher(&dispatcher{bulkLoads: observers, logger: newLogger()}, " (bulk-load)")
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: logLevelFromEnv()}))
}

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

	bulkPath                        string
	bulkCapacity, bulkMaxObjectSize int

	reverseCallTimeout time.Duration
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

	c.bulkPath = os.Getenv("HBASECOP_SHMEM_BULK_PATH")
	if c.bulkPath != "" {
		if c.bulkCapacity, err = optionalEnvInt("HBASECOP_BULK_RING_CAPACITY", c.capacity); err != nil {
			return c, err
		}
		if c.bulkMaxObjectSize, err = optionalEnvInt("HBASECOP_BULK_RING_MAX_OBJECT_SIZE", c.maxObjectSize); err != nil {
			return c, err
		}
	}

	rtMs, err := optionalEnvInt("HBASECOP_REVERSE_CALL_TIMEOUT_MS", 0)
	if err != nil {
		return c, err
	}
	if rtMs < 0 {
		return c, fmt.Errorf("HBASECOP_REVERSE_CALL_TIMEOUT_MS=%d: must be >= 0", rtMs)
	}
	c.reverseCallTimeout = time.Duration(rtMs) * time.Millisecond
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
