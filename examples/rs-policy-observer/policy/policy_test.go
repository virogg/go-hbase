// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"context"
	"strings"
	"testing"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

func TestPreRollWALWriterRequest_RejectsWhenVetoEnabled(t *testing.T) {
	o := New(true)

	_, err := o.PreRollWALWriterRequest(
		context.Background(), hbasecop.ObserverEnv{}, &hookpb.PreRollWalWriterRequestRequest{})
	if err == nil {
		t.Fatal("PreRollWALWriterRequest: want error when vetoing enabled, got nil")
	}
	if !strings.Contains(err.Error(), "region-server policy") {
		t.Fatalf("PreRollWALWriterRequest: error %q must name the policy", err)
	}
	if got, want := o.PreRollWALCount(), uint64(1); got != want {
		t.Fatalf("PreRollWALCount: got %d, want %d", got, want)
	}
	if got, want := o.RejectedRollCount(), uint64(1); got != want {
		t.Fatalf("RejectedRollCount: got %d, want %d", got, want)
	}
}

func TestPreRollWALWriterRequest_AllowsWhenVetoDisabled(t *testing.T) {
	o := New(false)

	res, err := o.PreRollWALWriterRequest(
		context.Background(), hbasecop.ObserverEnv{}, &hookpb.PreRollWalWriterRequestRequest{})
	if err != nil {
		t.Fatalf("PreRollWALWriterRequest: unexpected err when veto disabled: %v", err)
	}
	if res.Bypass {
		t.Fatal("PreRollWALWriterRequest: allowed roll must not bypass")
	}
	if got, want := o.RejectedRollCount(), uint64(0); got != want {
		t.Fatalf("RejectedRollCount: got %d, want %d", got, want)
	}
}

func TestPreStopRegionServer_IsObserveOnly(t *testing.T) {
	o := New(true)

	res, err := o.PreStopRegionServer(
		context.Background(), hbasecop.ObserverEnv{}, &hookpb.PreStopRegionServerRequest{})
	if err != nil {
		t.Fatalf("PreStopRegionServer: must never veto a stop, got err: %v", err)
	}
	if res.Bypass {
		t.Fatal("PreStopRegionServer: must not bypass")
	}
	if got, want := o.PreStopCount(), uint64(1); got != want {
		t.Fatalf("PreStopCount: got %d, want %d", got, want)
	}
}
