// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"context"
	"strings"
	"testing"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

func createReq(qualifier string) *hookpb.PreCreateTableRequest {
	return &hookpb.PreCreateTableRequest{
		TableName: &hbasepb.TableName{
			Namespace: []byte("default"),
			Qualifier: []byte(qualifier),
		},
	}
}

func deleteReq(qualifier string) *hookpb.PreDeleteTableRequest {
	return &hookpb.PreDeleteTableRequest{
		TableName: &hbasepb.TableName{
			Namespace: []byte("default"),
			Qualifier: []byte(qualifier),
		},
	}
}

func TestPreCreateTable_RejectsBlockedPrefix(t *testing.T) {
	o := New([]byte("forbidden-"))

	_, err := o.PreCreateTable(context.Background(), hbasecop.ObserverEnv{}, createReq("forbidden-users"))
	if err == nil {
		t.Fatal("PreCreateTable: want error for blocked qualifier, got nil")
	}
	if !strings.Contains(err.Error(), "forbidden-users") {
		t.Fatalf("PreCreateTable: error %q must name the rejected table", err)
	}
	if got, want := o.PreCreateTableCount(), uint64(1); got != want {
		t.Fatalf("PreCreateTableCount: got %d, want %d", got, want)
	}
	if got, want := o.RejectedCreateCount(), uint64(1); got != want {
		t.Fatalf("RejectedCreateCount: got %d, want %d", got, want)
	}
}

func TestPreCreateTable_AllowsNonBlockedPrefix(t *testing.T) {
	o := New([]byte("forbidden-"))

	res, err := o.PreCreateTable(context.Background(), hbasecop.ObserverEnv{}, createReq("ok-users"))
	if err != nil {
		t.Fatalf("PreCreateTable: unexpected err for allowed table: %v", err)
	}
	if res.Bypass {
		t.Fatal("PreCreateTable: allowed create must not bypass")
	}
	if got, want := o.RejectedCreateCount(), uint64(0); got != want {
		t.Fatalf("RejectedCreateCount: got %d, want %d", got, want)
	}
}

func TestPreDeleteTable_RejectsBlockedPrefix(t *testing.T) {
	o := New([]byte("forbidden-"))

	_, err := o.PreDeleteTable(context.Background(), hbasecop.ObserverEnv{}, deleteReq("forbidden-x"))
	if err == nil {
		t.Fatal("PreDeleteTable: want error for blocked qualifier, got nil")
	}
	if got, want := o.RejectedDeleteCount(), uint64(1); got != want {
		t.Fatalf("RejectedDeleteCount: got %d, want %d", got, want)
	}
}

func TestEmptyPrefix_AllowsEverything(t *testing.T) {
	o := New(nil)

	if _, err := o.PreCreateTable(context.Background(), hbasecop.ObserverEnv{}, createReq("forbidden-x")); err != nil {
		t.Fatalf("empty prefix must allow create, got err: %v", err)
	}
	if _, err := o.PreDeleteTable(context.Background(), hbasecop.ObserverEnv{}, deleteReq("forbidden-x")); err != nil {
		t.Fatalf("empty prefix must allow delete, got err: %v", err)
	}
	if o.RejectedCreateCount() != 0 || o.RejectedDeleteCount() != 0 {
		t.Fatal("empty prefix must never reject")
	}
}
