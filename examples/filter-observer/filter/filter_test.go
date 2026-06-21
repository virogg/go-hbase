// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

func TestPreGetOp_BypassesBlockedPrefix(t *testing.T) {
	o := New([]byte("block-"))
	req := &hookpb.PreGetOpRequest{Get: &hbasepb.Get{Row: []byte("block-1")}}

	res, err := o.PreGetOp(context.Background(), hbasecop.ObserverEnv{}, req)
	if err != nil {
		t.Fatalf("PreGetOp: unexpected err: %v", err)
	}
	if !res.Bypass {
		t.Fatalf("PreGetOp: want Bypass=true for blocked row, got false")
	}
	if got, want := o.PreGetCount(), uint64(1); got != want {
		t.Fatalf("PreGetCount: got %d, want %d", got, want)
	}
	if got, want := o.BlockedGetCount(), uint64(1); got != want {
		t.Fatalf("BlockedGetCount: got %d, want %d", got, want)
	}
}

func TestPreGetOp_AllowsNonBlockedPrefix(t *testing.T) {
	o := New([]byte("block-"))
	req := &hookpb.PreGetOpRequest{Get: &hbasepb.Get{Row: []byte("ok-1")}}

	res, err := o.PreGetOp(context.Background(), hbasecop.ObserverEnv{}, req)
	if err != nil {
		t.Fatalf("PreGetOp: unexpected err: %v", err)
	}
	if res.Bypass {
		t.Fatalf("PreGetOp: want Bypass=false for allowed row, got true")
	}
	if got, want := o.BlockedGetCount(), uint64(0); got != want {
		t.Fatalf("BlockedGetCount: got %d, want %d", got, want)
	}
}

func TestPreScannerOpen_BypassesBlockedPrefix(t *testing.T) {
	o := New([]byte("block-"))
	req := &hookpb.PreScannerOpenRequest{Scan: &hbasepb.Scan{StartRow: []byte("block-")}}

	res, err := o.PreScannerOpen(context.Background(), hbasecop.ObserverEnv{}, req)
	if err != nil {
		t.Fatalf("PreScannerOpen: unexpected err: %v", err)
	}
	if !res.Bypass {
		t.Fatalf("PreScannerOpen: want Bypass=true for blocked startRow, got false")
	}
	if got, want := o.PreScannerOpenCount(), uint64(1); got != want {
		t.Fatalf("PreScannerOpenCount: got %d, want %d", got, want)
	}
	if got, want := o.BlockedScanCount(), uint64(1); got != want {
		t.Fatalf("BlockedScanCount: got %d, want %d", got, want)
	}
}

func TestPreScannerOpen_AllowsNonBlockedPrefix(t *testing.T) {
	o := New([]byte("block-"))
	req := &hookpb.PreScannerOpenRequest{Scan: &hbasepb.Scan{StartRow: []byte("ok-")}}

	res, err := o.PreScannerOpen(context.Background(), hbasecop.ObserverEnv{}, req)
	if err != nil {
		t.Fatalf("PreScannerOpen: unexpected err: %v", err)
	}
	if res.Bypass {
		t.Fatalf("PreScannerOpen: want Bypass=false for allowed startRow, got true")
	}
}

func TestPreScannerNext_CountsButNeverBypasses(t *testing.T) {
	o := New([]byte("block-"))
	req := &hookpb.PreScannerNextRequest{Limit: 16, HasMore: true}

	res, err := o.PreScannerNext(context.Background(), hbasecop.ObserverEnv{}, req)
	if err != nil {
		t.Fatalf("PreScannerNext: unexpected err: %v", err)
	}
	if res.Bypass {
		t.Fatalf("PreScannerNext: want Bypass=false (counter-only), got true")
	}
	if got, want := o.PreScannerNextCount(), uint64(1); got != want {
		t.Fatalf("PreScannerNextCount: got %d, want %d", got, want)
	}
}

func TestPreBatchMutate_PartiallyBlocksByPrefix(t *testing.T) {
	o := New([]byte("block-"))
	req := &hookpb.PreBatchMutateRequest{
		Operation: []*hookpb.MutationOperation{
			{Mutation: &hbasepb.MutationProto{Row: []byte("ok-1")}},
			{Mutation: &hbasepb.MutationProto{Row: []byte("block-1")}},
			{Mutation: &hbasepb.MutationProto{Row: []byte("ok-2")}},
			{Mutation: &hbasepb.MutationProto{Row: []byte("block-2")}},
			{Mutation: &hbasepb.MutationProto{Row: []byte("ok-3")}},
		},
	}

	res, err := o.PreBatchMutate(context.Background(), hbasecop.ObserverEnv{}, req)
	if err != nil {
		t.Fatalf("PreBatchMutate: unexpected err: %v", err)
	}
	if res.Bypass {
		t.Fatalf("PreBatchMutate: want Bypass=false (partial block, not whole-batch), got true")
	}
	want := []uint32{1, 3}
	if len(res.BlockedIndices) != len(want) {
		t.Fatalf("BlockedIndices = %v, want %v", res.BlockedIndices, want)
	}
	for i := range want {
		if res.BlockedIndices[i] != want[i] {
			t.Fatalf("BlockedIndices[%d] = %d, want %d", i, res.BlockedIndices[i], want[i])
		}
	}
	if got, want := o.PreBatchMutateCount(), uint64(1); got != want {
		t.Fatalf("PreBatchMutateCount: got %d, want %d", got, want)
	}
	if got, want := o.BlockedBatchOps(), uint64(2); got != want {
		t.Fatalf("BlockedBatchOps: got %d, want %d", got, want)
	}
}

func TestPreBatchMutate_AllOkReturnsNoIndices(t *testing.T) {
	o := New([]byte("block-"))
	req := &hookpb.PreBatchMutateRequest{
		Operation: []*hookpb.MutationOperation{
			{Mutation: &hbasepb.MutationProto{Row: []byte("ok-1")}},
			{Mutation: &hbasepb.MutationProto{Row: []byte("ok-2")}},
		},
	}

	res, err := o.PreBatchMutate(context.Background(), hbasecop.ObserverEnv{}, req)
	if err != nil {
		t.Fatalf("PreBatchMutate: unexpected err: %v", err)
	}
	if len(res.BlockedIndices) != 0 {
		t.Fatalf("BlockedIndices = %v, want []", res.BlockedIndices)
	}
}

func TestPreBatchMutate_EmptyPrefixBlocksNothing(t *testing.T) {
	o := New(nil)
	req := &hookpb.PreBatchMutateRequest{
		Operation: []*hookpb.MutationOperation{
			{Mutation: &hbasepb.MutationProto{Row: []byte("block-1")}},
		},
	}

	res, err := o.PreBatchMutate(context.Background(), hbasecop.ObserverEnv{}, req)
	if err != nil {
		t.Fatalf("PreBatchMutate: unexpected err: %v", err)
	}
	if len(res.BlockedIndices) != 0 {
		t.Fatalf("empty prefix must never block, got BlockedIndices=%v", res.BlockedIndices)
	}
}

func TestEmptyPrefix_DisablesBypass(t *testing.T) {
	o := New(nil)
	getReq := &hookpb.PreGetOpRequest{Get: &hbasepb.Get{Row: []byte("block-1")}}
	res, err := o.PreGetOp(context.Background(), hbasecop.ObserverEnv{}, getReq)
	if err != nil {
		t.Fatalf("PreGetOp: unexpected err: %v", err)
	}
	if res.Bypass {
		t.Fatalf("empty prefix must never bypass, got Bypass=true")
	}

	scanReq := &hookpb.PreScannerOpenRequest{Scan: &hbasepb.Scan{StartRow: []byte("block-")}}
	res, err = o.PreScannerOpen(context.Background(), hbasecop.ObserverEnv{}, scanReq)
	if err != nil {
		t.Fatalf("PreScannerOpen: unexpected err: %v", err)
	}
	if res.Bypass {
		t.Fatalf("empty prefix must never bypass scanner, got Bypass=true")
	}
}

func TestPreFlush_CountsAndPassesThrough(t *testing.T) {
	o := New([]byte("block-"))
	res, err := o.PreFlush(context.Background(), hbasecop.ObserverEnv{}, &hookpb.PreFlushRequest{})
	if err != nil {
		t.Fatalf("PreFlush: unexpected err: %v", err)
	}
	if res.Bypass {
		t.Fatalf("PreFlush must not bypass the flush, got Bypass=true")
	}
	if got, want := o.PreFlushCount(), uint64(1); got != want {
		t.Fatalf("PreFlushCount: got %d, want %d", got, want)
	}
}

func TestPostFlush_Counts(t *testing.T) {
	o := New([]byte("block-"))
	if err := o.PostFlush(context.Background(), hbasecop.ObserverEnv{}, &hookpb.PostFlushRequest{}); err != nil {
		t.Fatalf("PostFlush: unexpected err: %v", err)
	}
	if got, want := o.PostFlushCount(), uint64(1); got != want {
		t.Fatalf("PostFlushCount: got %d, want %d", got, want)
	}
}

func TestPreCompactSelection_CountsAndPassesThrough(t *testing.T) {
	o := New([]byte("block-"))
	res, err := o.PreCompactSelection(context.Background(), hbasecop.ObserverEnv{},
		&hookpb.PreCompactSelectionRequest{ColumnFamily: []byte("cf")})
	if err != nil {
		t.Fatalf("PreCompactSelection: unexpected err: %v", err)
	}
	if res.Bypass {
		t.Fatalf("PreCompactSelection must not bypass, got Bypass=true")
	}
	if got, want := o.PreCompactSelectionCount(), uint64(1); got != want {
		t.Fatalf("PreCompactSelectionCount: got %d, want %d", got, want)
	}
}

func TestPreCompact_CountsAndPassesThrough(t *testing.T) {
	o := New([]byte("block-"))
	res, err := o.PreCompact(context.Background(), hbasecop.ObserverEnv{},
		&hookpb.PreCompactRequest{ColumnFamily: []byte("cf")})
	if err != nil {
		t.Fatalf("PreCompact: unexpected err: %v", err)
	}
	if res.Bypass {
		t.Fatalf("PreCompact must not bypass, got Bypass=true")
	}
	if got, want := o.PreCompactCount(), uint64(1); got != want {
		t.Fatalf("PreCompactCount: got %d, want %d", got, want)
	}
}

func TestPostCompact_Counts(t *testing.T) {
	o := New([]byte("block-"))
	if err := o.PostCompact(context.Background(), hbasecop.ObserverEnv{},
		&hookpb.PostCompactRequest{ColumnFamily: []byte("cf")}); err != nil {
		t.Fatalf("PostCompact: unexpected err: %v", err)
	}
	if got, want := o.PostCompactCount(), uint64(1); got != want {
		t.Fatalf("PostCompactCount: got %d, want %d", got, want)
	}
}

func TestStorageHooks_EmitTaggedLogLines(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	o := New([]byte("block-"))
	o.SetLogger(logger)

	if _, err := o.PreFlush(context.Background(), hbasecop.ObserverEnv{}, &hookpb.PreFlushRequest{}); err != nil {
		t.Fatalf("PreFlush: %v", err)
	}
	if err := o.PostFlush(context.Background(), hbasecop.ObserverEnv{}, &hookpb.PostFlushRequest{}); err != nil {
		t.Fatalf("PostFlush: %v", err)
	}
	if _, err := o.PreCompactSelection(context.Background(), hbasecop.ObserverEnv{},
		&hookpb.PreCompactSelectionRequest{ColumnFamily: []byte("cf")}); err != nil {
		t.Fatalf("PreCompactSelection: %v", err)
	}
	if _, err := o.PreCompact(context.Background(), hbasecop.ObserverEnv{},
		&hookpb.PreCompactRequest{ColumnFamily: []byte("cf")}); err != nil {
		t.Fatalf("PreCompact: %v", err)
	}
	if err := o.PostCompact(context.Background(), hbasecop.ObserverEnv{},
		&hookpb.PostCompactRequest{ColumnFamily: []byte("cf")}); err != nil {
		t.Fatalf("PostCompact: %v", err)
	}

	out := buf.String()
	for _, needle := range []string{
		"filter-observer: preFlush",
		"filter-observer: postFlush",
		"filter-observer: preCompactSelection",
		"filter-observer: preCompact",
		"filter-observer: postCompact",
	} {
		if !strings.Contains(out, needle) {
			t.Fatalf("expected log to contain %q; full output:\n%s", needle, out)
		}
	}
}
