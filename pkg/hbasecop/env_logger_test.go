// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// The runtime hands each hook a logger pre-tagged with hook + table, so
// observers log through env.Logger() instead of reinventing one.
func TestObserverEnvLoggerIsSetAndTagged(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	obs := NewRegion().OnPrePut(func(_ context.Context, env ObserverEnv, _ *MutationProto) (HookResult, error) {
		env.Logger().Info("decision")
		return HookResult{}, nil
	})
	d := &dispatcher{observers: []RegionObserver{obs}, logger: logger}

	inner, err := proto.Marshal(&hookpb.PrePutRequest{
		Ctx: &hookpb.HookContext{
			TableName:  &hbasepb.TableName{Namespace: []byte("default"), Qualifier: []byte("users")},
			RegionName: []byte("reg-7"),
		},
		Mutation: &hbasepb.MutationProto{Row: []byte("r")},
	})
	if err != nil {
		t.Fatal(err)
	}
	d.dispatch(context.Background(), buildRequestFrame(t, HookIDPrePut, 7, inner))

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log line not JSON: %v (%q)", err, buf.String())
	}
	// All four tags the runtime attaches must be present so a dropped tag is caught.
	// req_id is a JSON number, so it round-trips as float64.
	if rec["hook"] != "PrePut" || rec["table"] != "default:users" ||
		rec["region"] != "reg-7" || rec["req_id"] != float64(7) {
		t.Fatalf("logger not tagged with hook/table/region/req_id: %v", rec)
	}
	if !strings.Contains(buf.String(), "decision") {
		t.Error("observer's own message missing")
	}
}

// A hand-constructed ObserverEnv (no runtime base logger) still yields a usable
// logger rather than panicking, so direct unit-test calls are safe.
func TestObserverEnvLoggerNilSafe(t *testing.T) {
	if got := (ObserverEnv{TableName: "users"}).Logger(); got == nil {
		t.Fatal("Logger() on a hand-built env must not be nil")
	}
}
