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

// The runtime hands each hook a non-nil logger pre-tagged with hook + table, so
// observers log through env.Logger instead of reinventing one.
func TestObserverEnvLoggerIsSetAndTagged(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	obs := NewRegion().OnPrePut(func(_ context.Context, env ObserverEnv, _ *MutationProto) (HookResult, error) {
		if env.Logger == nil {
			t.Fatal("env.Logger must be set by the runtime")
		}
		env.Logger.Info("decision")
		return HookResult{}, nil
	})
	d := &dispatcher{observers: []RegionObserver{obs}, logger: logger}

	inner, err := proto.Marshal(&hookpb.PrePutRequest{
		Ctx:      &hookpb.HookContext{TableName: &hbasepb.TableName{Namespace: []byte("default"), Qualifier: []byte("users")}},
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
	if rec["hook"] != "PrePut" || rec["table"] != "default:users" {
		t.Fatalf("logger not tagged: %v", rec)
	}
	if !strings.Contains(buf.String(), "decision") {
		t.Error("observer's own message missing")
	}
}
