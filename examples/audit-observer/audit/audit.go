// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

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

const Marker = "audit-observer: audit"

type Record struct {
	Op        string
	Table     string
	Region    string
	RowDigest string
	Cells     int
	Seq       uint64
}

type Observer struct {
	hbasecop.UnimplementedRegionObserver

	logger atomic.Pointer[slog.Logger]
	seq    atomic.Uint64
}

func New() *Observer { return &Observer{} }

func (o *Observer) SetLogger(l *slog.Logger) { o.logger.Store(l) }

func (o *Observer) log() *slog.Logger {
	if l := o.logger.Load(); l != nil {
		return l
	}
	return slog.Default()
}

func (o *Observer) PostPut(_ context.Context, env hbasecop.ObserverEnv, mut *hbasecop.MutationProto) error {
	o.emit(NewRecord("put", env, mut, o.seq.Add(1)))
	return nil
}

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

func RowDigest(row []byte) string {
	sum := sha256.Sum256(row)
	return hex.EncodeToString(sum[:8])
}
