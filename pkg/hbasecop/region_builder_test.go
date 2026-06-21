// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

func ExampleNewRegion() {
	obs := NewRegion().OnPrePut(func(_ context.Context, env ObserverEnv, _ *MutationProto) (HookResult, error) {
		fmt.Println("prePut on", env.TableName)
		return HookResult{}, nil
	})
	res, _ := obs.PrePut(context.Background(), ObserverEnv{TableName: "users"}, nil)
	fmt.Println("bypass:", res.Bypass)
}

func TestRegionBuilderDispatch(t *testing.T) {
	var prePuts int
	b := NewRegion().OnPrePut(func(context.Context, ObserverEnv, *MutationProto) (HookResult, error) {
		prePuts++
		return HookResult{Bypass: true}, nil
	})
	d := newDispatcher(b, nil)

	hctx := &hookpb.HookContext{RegionName: []byte("t,,1.a."), RequestId: 1}

	prePut, err := proto.Marshal(&hookpb.PrePutRequest{Ctx: hctx, Mutation: &hbasepb.MutationProto{Row: []byte("r")}})
	if err != nil {
		t.Fatal(err)
	}
	resp := decodeHookResponse(t, d.dispatch(context.Background(), buildRequestFrame(t, HookIDPrePut, 1, prePut)))
	if prePuts != 1 {
		t.Fatalf("prePut calls = %d, want 1", prePuts)
	}
	if !resp.GetBypass() {
		t.Fatal("registered PrePut should have bypassed")
	}

	getOp, err := proto.Marshal(&hookpb.PreGetOpRequest{Ctx: hctx})
	if err != nil {
		t.Fatal(err)
	}
	resp = decodeHookResponse(t, d.dispatch(context.Background(), buildRequestFrame(t, HookIDPreGetOp, 2, getOp)))
	if resp.GetBypass() || resp.GetError() != nil {
		t.Fatalf("unregistered PreGetOp should be a no-op, got bypass=%v err=%v", resp.GetBypass(), resp.GetError())
	}
}

func TestRegionBuilderUpToDate(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "region_builder.go")
	cmd := exec.Command("go", "run", "../../tools/gen-builder",
		"-src", "observer.go", "-iface", "RegionObserver", "-out", tmp)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("gen-builder: %v\n%s", err, out)
	}
	want, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("region_builder.go")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatal("region_builder.go is stale; run: go generate ./pkg/hbasecop")
	}
}
