// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// aliasSmoke overrides non-Put hooks using the public alias types. It compiles
// only if the aliases are identical to the internal *hookpb types the
// interface is signed against, which the assertion below pins.
type aliasSmoke struct{ UnimplementedRegionObserver }

func (aliasSmoke) PreGetOp(context.Context, ObserverEnv, *PreGetOpRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (aliasSmoke) PreDelete(context.Context, ObserverEnv, *PreDeleteRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (aliasSmoke) PreBatchMutate(context.Context, ObserverEnv, *PreBatchMutateRequest) (HookResult, error) {
	return HookResult{}, nil
}

var (
	_ RegionObserver = aliasSmoke{}
	_ *Cell          = (*Cell)(nil) // payload alias is namable
)

// TestWireTypesUpToDate fails if wiretypes.go drifts from the generator output,
// e.g. a new hookpb request type was added without re-running go generate.
func TestWireTypesUpToDate(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "wiretypes.go")
	cmd := exec.Command("go", "run", "../../tools/gen-wiretypes",
		"-hookpb", "../../internal/wire/hookpb",
		"-hbasepb", "../../internal/wire/hbasepb",
		"-out", tmp)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("gen-wiretypes: %v\n%s", err, out)
	}
	want, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("wiretypes.go")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatal("wiretypes.go is stale; run: go generate ./pkg/hbasecop")
	}
}
