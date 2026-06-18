// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command endpoint-observer is the Go-side runtime of the endpoint integration
// tests. It registers a no-op RegionObserver plus a Go Endpoint, so the stock
// GenericRegionObserver entrypoint can host the generic GoEndpointService: a
// client calls it via Table.coprocessorService and the call round-trips to Go.
//
// Methods: "upper" (upper-cases its payload); "get" (TE31 reverse GET: reads the
// row named by its payload and returns the first cell's value); "follow" (TE32
// data-dependent reverse read: reads row A=payload, takes cf:next as a pointer to
// row B, reads B, returns cf:val); "put" (TE41 reverse MUTATE: read-then-write of
// cf:val on the invoking region, gated by hbasecop.endpoint.allow-mutate). The
// no-op observer is required because
// GenericRegionObserver is a RegionCoprocessor: without a registered region
// observer the strict preOpen hook would dispatch to Go, find no observer, and
// abort region open. With UnimplementedRegionObserver every region hook is a no-op.
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

// cf is the column family the reverse-read methods read from.
var cf = []byte("cf")

type upperEndpoint struct{}

// Call dispatches by method name. "get"/"follow" exercise the reverse-RPC read
// channel. Two names inject faults for the TE24 fault matrix: "panic" panics
// inside the handler (the SDK must recover it into a client error without killing
// the shared process), and "exit" crashes the whole Go process mid-call (the
// Java supervisor must fail the in-flight call promptly and restart, not hang).
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
	default:
		return bytes.ToUpper(payload), nil
	}
}

// getValue reads the row named by payload from the invoking region and returns
// the first cell's value (empty if the row is absent).
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

// follow performs a data-dependent reverse read: read row A (=payload), take its
// cf:next cell as a pointer to row B, read B, and return B's cf:val. Proves the
// reverse channel handles "read A -> read B by a key from A" within one call.
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

// scanCount opens a server-side scanner over the whole region, counts the cells
// across all batches, closes it, and returns the count. Exercises the TE33
// pull-scan SCAN_OPEN/NEXT/CLOSE round-trip.
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

// sumColumn is the canonical E3 aggregating endpoint (CP-E3): it scans the whole
// region server-side and sums the cf:<qualifier> cells as int64, returning the
// total — the aggregation runs in the database process over region-local data,
// shipping one number to the client instead of every row.
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

// putRow performs a reverse write against the invoking region (TE41). payload is
// "row=value" (no '=' → value defaults to "mutated"). It first reads the row —
// exercising read+write on the same region within one endpoint call, the
// re-entry path — then writes cf:val=value. Reverse MUTATE is gated server-side;
// when hbasecop.endpoint.allow-mutate is off, env.Put returns an error that
// surfaces to the client.
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

// scanLeak opens a scanner, pulls one batch, and crashes WITHOUT closing it —
// the TE33 fault probe. The bridge must reap the orphaned RegionScanner on the
// crash path so no read point leaks; a later scan must still work.
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

// slowSleep sleeps for the duration named by payload (e.g. "5s") then returns ok.
// TE54 watchdog probe: a long endpoint must NOT starve the dedicated heartbeat
// goroutine, so no false heartbeat-miss restart fires mid-call. Respects ctx so the
// endpoint timeout still bounds it.
func slowSleep(ctx context.Context, payload []byte) ([]byte, error) {
	d, err := time.ParseDuration(string(payload))
	if err != nil {
		return nil, fmt.Errorf("endpoint-observer: slow wants a duration payload, got %q: %w", payload, err)
	}
	select {
	case <-time.After(d):
		slog.Info("endpoint-observer: slow ok", "slept", d.String())
		return []byte("ok"), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// manyScan opens n scanners (n from payload) on the invoking region without closing
// them, to trip the per-call scanner cap (max-scanners-per-call). It returns the
// open error verbatim (the bridge's "max scanners per call exceeded") once the cap
// is hit; the call's scanners are reaped when it ends.
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

func main() {
	// Register no-op region AND master observers so the same binary deploys as either a region or a
	// master coprocessor (TE43): without a matching observer the strict hooks would dispatch to Go,
	// find none, and abort region-open / master-init. The endpoint is the same in both roles; a
	// master-scoped invoke carries region_id 0, so region-local reverse reads/writes are unavailable
	// there (master endpoints read master/meta state only).
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
