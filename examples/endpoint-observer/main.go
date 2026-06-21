// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

var cf = []byte("cf")

type upperEndpoint struct{}

func (upperEndpoint) Call(ctx context.Context, env *hbasecop.EndpointEnv, method string, payload []byte) ([]byte, error) {
	slog.Info("endpoint-observer: call", "method", method, "bytes", len(payload))
	switch method {
	case "panic":
		panic("endpoint-observer: injected panic")
	case "exit":
		slog.Warn("endpoint-observer: injected os.Exit(1)")
		os.Exit(1)
		return nil, nil // unreachable
	case "get":
		return getValue(ctx, env, payload)
	case "follow":
		return follow(ctx, env, payload)
	case "scan":
		return scanCount(ctx, env)
	case "sum":
		return sumColumn(ctx, env, payload)
	case "put":
		return putRow(ctx, env, payload)
	case "scan-leak":
		return scanLeak(ctx, env)
	case "slow":
		return slowSleep(ctx, payload)
	case "manyscan":
		return manyScan(ctx, env, payload)
	case "scanstats":
		return scanStats(ctx, env)
	default:
		return bytes.ToUpper(payload), nil
	}
}

func getValue(ctx context.Context, env *hbasecop.EndpointEnv, row []byte) ([]byte, error) {
	r, err := env.Get(ctx, row)
	if err != nil {
		return nil, err
	}
	cells := r.GetCell()
	if len(cells) == 0 {
		return nil, nil
	}
	return cells[0].GetValue(), nil
}

func follow(ctx context.Context, env *hbasecop.EndpointEnv, rowA []byte) ([]byte, error) {
	a, err := env.Get(ctx, rowA)
	if err != nil {
		return nil, err
	}
	next, ok := hbasecop.CellValue(a, cf, []byte("next"))
	if !ok {
		return nil, fmt.Errorf("endpoint-observer: row %q has no cf:next pointer", rowA)
	}
	b, err := env.Get(ctx, next)
	if err != nil {
		return nil, err
	}
	val, ok := hbasecop.CellValue(b, cf, []byte("val"))
	if !ok {
		return nil, fmt.Errorf("endpoint-observer: row %q has no cf:val", next)
	}
	slog.Info("endpoint-observer: follow ok", "from", string(rowA), "to", string(next))
	return val, nil
}

func scanCount(ctx context.Context, env *hbasecop.EndpointEnv) ([]byte, error) {
	sc, err := env.OpenScanner(ctx, &hbasecop.Scan{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = sc.Close(ctx) }()

	count := 0
	for {
		cells, hasMore, err := sc.Next(ctx)
		if err != nil {
			return nil, err
		}
		count += len(cells)
		if !hasMore {
			break
		}
	}
	slog.Info("endpoint-observer: scan ok", "cells", count)
	return []byte(strconv.Itoa(count)), nil
}

func sumColumn(ctx context.Context, env *hbasecop.EndpointEnv, qualifier []byte) ([]byte, error) {
	sc, err := env.OpenScanner(ctx, &hbasecop.Scan{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = sc.Close(ctx) }()

	var total int64
	for {
		cells, hasMore, err := sc.Next(ctx)
		if err != nil {
			return nil, err
		}
		for _, c := range cells {
			if bytes.Equal(c.GetFamily(), cf) && bytes.Equal(c.GetQualifier(), qualifier) {
				n, err := strconv.ParseInt(string(c.GetValue()), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("endpoint-observer: non-numeric cf:%s value %q: %w",
						qualifier, c.GetValue(), err)
				}
				total += n
			}
		}
		if !hasMore {
			break
		}
	}
	slog.Info("endpoint-observer: sum ok", "qualifier", string(qualifier), "total", total)
	return []byte(strconv.FormatInt(total, 10)), nil
}

func putRow(ctx context.Context, env *hbasecop.EndpointEnv, payload []byte) ([]byte, error) {
	row, value, found := bytes.Cut(payload, []byte("="))
	if !found {
		value = []byte("mutated")
	}
	if _, err := env.Get(ctx, row); err != nil { // read-then-write on the invoking region
		return nil, err
	}
	m := &hbasecop.MutationProto{
		Row: row,
		ColumnValue: []*hbasecop.MutationProto_ColumnValue{
			{
				Family: cf,
				QualifierValue: []*hbasecop.MutationProto_ColumnValue_QualifierValue{
					{Qualifier: []byte("val"), Value: value},
				},
			},
		},
	}
	if err := env.Put(ctx, m); err != nil {
		return nil, err
	}
	slog.Info("endpoint-observer: put ok", "row", string(row), "value", string(value))
	return []byte("ok"), nil
}

func scanLeak(ctx context.Context, env *hbasecop.EndpointEnv) ([]byte, error) {
	sc, err := env.OpenScanner(ctx, &hbasecop.Scan{})
	if err != nil {
		return nil, err
	}
	if _, _, err := sc.Next(ctx); err != nil {
		return nil, err
	}
	slog.Warn("endpoint-observer: leaking scanner then os.Exit(1)")
	os.Exit(1) // crash without sc.Close → bridge reaps the scanner
	return nil, nil
}

func slowSleep(ctx context.Context, payload []byte) ([]byte, error) {
	d, err := time.ParseDuration(string(payload))
	if err != nil {
		return nil, fmt.Errorf("endpoint-observer: slow wants a duration payload, got %q: %w", payload, err)
	}
	slog.Info("endpoint-observer: slow start", "dur", d.String())
	select {
	case <-time.After(d):
		slog.Info("endpoint-observer: slow ok", "slept", d.String())
		return []byte("ok"), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func manyScan(ctx context.Context, env *hbasecop.EndpointEnv, payload []byte) ([]byte, error) {
	n, err := strconv.Atoi(string(payload))
	if err != nil {
		return nil, fmt.Errorf("endpoint-observer: manyscan wants an int payload, got %q: %w", payload, err)
	}
	opened := 0
	for range n {
		if _, err := env.OpenScanner(ctx, &hbasecop.Scan{}); err != nil {
			return nil, fmt.Errorf("endpoint-observer: manyscan opened %d then: %w", opened, err)
		}
		opened++
	}
	slog.Info("endpoint-observer: manyscan opened", "count", opened)
	return []byte(strconv.Itoa(opened)), nil
}

func scanStats(ctx context.Context, env *hbasecop.EndpointEnv) ([]byte, error) {
	sc, err := env.OpenScanner(ctx, &hbasecop.Scan{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = sc.Close(ctx) }()

	cells, batches := 0, 0
	for {
		cs, hasMore, err := sc.Next(ctx)
		if err != nil {
			return nil, err
		}
		batches++
		cells += len(cs)
		if !hasMore {
			break
		}
	}
	slog.Info("endpoint-observer: scanstats ok", "cells", cells, "batches", batches)
	return fmt.Appendf(nil, "%d,%d", cells, batches), nil
}

func main() {
	err := hbasecop.RunAll(
		hbasecop.UnimplementedRegionObserver{},
		hbasecop.UnimplementedMasterObserver{},
		upperEndpoint{},
	)
	if err != nil {
		slog.Error("endpoint-observer: fatal", "err", err)
		os.Exit(1)
	}
}
