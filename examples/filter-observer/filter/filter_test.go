// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
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
