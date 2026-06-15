// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package audit implements the post-hook audit RegionObserver (T72). On
// every PostPut / PostDelete it emits one structured JSON audit record via
// slog; the bridge tails the Go process's stderr into the RegionServer log,
// so each record becomes one greppable audit line.
//
// Privacy: per SPEC §8, row keys and cell values are potentially sensitive
// and must not reach logs at the default level. Audit records therefore
// carry a short SHA-256 digest of the row key - stable enough to correlate
// repeated operations on the same row, useless for recovering the key.
package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync/atomic"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

// Marker is the log-message prefix every audit record carries. The T72
// integration test greps the RegionServer log for it; operators can do the
// same with their log aggregator.
const Marker = "audit-observer: audit"

// Record is one audit entry derived from a mutation.
type Record struct {
	// Op is the audited operation: "put" or "delete".
	Op string
	// Table and Region scope the operation.
	Table  string
	Region string
	// RowDigest is the first 8 bytes of SHA-256(row key), hex-encoded.
	// Empty when the mutation carries no row (defensive; not expected).
	RowDigest string
	// Cells is the number of cells the mutation carried.
	Cells int
	// Seq is the observer-local 1-based sequence number of this record.
	Seq uint64
}

// Observer audits write-path operations. All other RegionObserver methods
// inherit the no-op behaviour of UnimplementedRegionObserver. Post-hooks run
// under the best-effort policy by default, so auditing never blocks or
// fails the client's operation.
type Observer struct {
	hbasecop.UnimplementedRegionObserver

	logger atomic.Pointer[slog.Logger]
	seq    atomic.Uint64
}

// New constructs an audit Observer logging via slog.Default().
func New() *Observer { return &Observer{} }

// SetLogger overrides the slog.Logger audit records are written to. Nil
// resets to slog.Default(). Safe for concurrent use.
func (o *Observer) SetLogger(l *slog.Logger) { o.logger.Store(l) }

func (o *Observer) log() *slog.Logger {
	if l := o.logger.Load(); l != nil {
		return l
	}
	return slog.Default()
}

// PostPut audits a completed Put.
func (o *Observer) PostPut(_ context.Context, env hbasecop.ObserverEnv, mut *hbasecop.MutationProto) error {
	o.emit(NewRecord("put", env, mut, o.seq.Add(1)))
	return nil
}

// PostDelete audits a completed Delete.
func (o *Observer) PostDelete(_ context.Context, env hbasecop.ObserverEnv, req *hookpb.PostDeleteRequest) error {
	o.emit(NewRecord("delete", env, req.GetMutation(), o.seq.Add(1)))
	return nil
}

func (o *Observer) emit(r Record) {
	o.log().Info(Marker,
		"op", r.Op,
		"table", r.Table,
		"region", r.Region,
		"row_digest", r.RowDigest,
		"cells", r.Cells,
		"seq", r.Seq,
	)
}

// NewRecord derives the audit Record for one mutation. Exported so the unit
// tests (and any future sink besides slog) can exercise the derivation
// without a running observer.
func NewRecord(op string, env hbasecop.ObserverEnv, mut *hbasecop.MutationProto, seq uint64) Record {
	r := Record{Op: op, Table: env.TableName, Region: env.RegionName, Seq: seq}
	if mut == nil {
		return r
	}
	if row := mut.GetRow(); len(row) > 0 {
		r.RowDigest = RowDigest(row)
	}
	for _, cv := range mut.GetColumnValue() {
		r.Cells += len(cv.GetQualifierValue())
	}
	return r
}

// RowDigest returns the first 8 bytes of SHA-256(row), hex-encoded -
// 16 hex chars. Stable per row key, non-reversible.
func RowDigest(row []byte) string {
	sum := sha256.Sum256(row)
	return hex.EncodeToString(sum[:8])
}
