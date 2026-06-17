// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecoptest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/virogg/go-hbase/pkg/hbasecop"
	"github.com/virogg/go-hbase/pkg/hbasecop/hbasecoptest"
)

func TestPrePutEnvRoundTrip(t *testing.T) {
	var got hbasecop.ObserverEnv
	obs := hbasecop.NewRegion().OnPrePut(func(_ context.Context, env hbasecop.ObserverEnv, _ *hbasecop.MutationProto) (hbasecop.HookResult, error) {
		got = env
		return hbasecop.HookResult{Bypass: true}, nil
	})

	res, err := hbasecoptest.PrePut(obs, hbasecoptest.Env{Table: "default:users", Region: "r1"}, &hbasecop.MutationProto{Row: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Bypass {
		t.Fatal("want bypass")
	}
	if got.TableName != "default:users" || got.RegionName != "r1" {
		t.Fatalf("env = %+v, want table=default:users region=r1", got)
	}
}

func TestInvokeEnvelopeHookAndPostPut(t *testing.T) {
	var posts int
	obs := hbasecop.NewRegion().
		OnPreGetOp(func(context.Context, hbasecop.ObserverEnv, *hbasecop.PreGetOpRequest) (hbasecop.HookResult, error) {
			return hbasecop.HookResult{Bypass: true}, nil
		}).
		OnPostPut(func(context.Context, hbasecop.ObserverEnv, *hbasecop.MutationProto) error {
			posts++
			return nil
		})

	env := hbasecoptest.Env{Table: "users", Region: "r1"}
	res, err := hbasecoptest.Invoke(obs, hbasecop.HookIDPreGetOp, &hbasecop.PreGetOpRequest{Ctx: hbasecoptest.Context(env)})
	if err != nil || !res.Bypass {
		t.Fatalf("PreGetOp via Invoke: res=%+v err=%v", res, err)
	}
	if _, err := hbasecoptest.PostPut(obs, env, &hbasecop.MutationProto{Row: []byte("k")}); err != nil || posts != 1 {
		t.Fatalf("PostPut: posts=%d err=%v", posts, err)
	}
}

func TestPanicBecomesError(t *testing.T) {
	obs := hbasecop.NewRegion().OnPrePut(func(context.Context, hbasecop.ObserverEnv, *hbasecop.MutationProto) (hbasecop.HookResult, error) {
		panic("boom")
	})
	if _, err := hbasecoptest.PrePut(obs, hbasecoptest.Env{Table: "t"}, nil); err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("err = %v, want panic", err)
	}
}
