// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"reflect"
	"testing"
)

func TestHookNames(t *testing.T) {
	names := HookNames()
	if len(names) == 0 {
		t.Fatal("HookNames returned empty")
	}
	found := false
	for _, n := range names {
		if n == "PrePut" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("HookNames missing PrePut; got %v", names)
	}
}

func TestHookTableIsCanonical(t *testing.T) {
	if len(hookTable) == 0 {
		t.Fatal("hookTable empty: T41 dispatch table not populated")
	}
	seenIDs := make(map[HookID]string, len(hookTable))
	seenNames := make(map[string]HookID, len(hookTable))
	for _, h := range hookTable {
		if h.id == HookIDUnknown {
			t.Errorf("hookTable contains HookIDUnknown entry for %q", h.name)
		}
		if h.name == "" {
			t.Errorf("hookTable entry id=%d has empty name", h.id)
		}
		if prev, dup := seenIDs[h.id]; dup {
			t.Errorf("duplicate HookID %d: %q and %q", h.id, prev, h.name)
		}
		if prev, dup := seenNames[h.name]; dup {
			t.Errorf("duplicate hook name %q: ids %d and %d", h.name, prev, h.id)
		}
		seenIDs[h.id] = h.name
		seenNames[h.name] = h.id
	}
}

func TestRegionObserverInterfaceCoversAllHooks(t *testing.T) {
	rt := reflect.TypeOf((*RegionObserver)(nil)).Elem()
	methods := make(map[string]bool, rt.NumMethod())
	for i := 0; i < rt.NumMethod(); i++ {
		methods[rt.Method(i).Name] = true
	}
	for _, h := range hookTable {
		if !methods[h.name] {
			t.Errorf("RegionObserver missing method %q (HookID=%d)", h.name, h.id)
		}
	}
	if got, want := rt.NumMethod(), len(hookTable); got != want {
		t.Errorf("RegionObserver method count = %d, hookTable len = %d; "+
			"every interface method must have a HookID entry and vice versa", got, want)
	}
}

func TestUnimplementedRegionObserverSatisfiesInterface(t *testing.T) {
	var obs RegionObserver = UnimplementedRegionObserver{}
	res, err := obs.PrePut(context.Background(), ObserverEnv{}, nil)
	if err != nil {
		t.Fatalf("noop PrePut returned err=%v, want nil", err)
	}
	if res.Bypass {
		t.Fatal("noop PrePut returned Bypass=true, want false")
	}
	if err := obs.PostPut(context.Background(), ObserverEnv{}, nil); err != nil {
		t.Fatalf("noop PostPut returned err=%v, want nil", err)
	}
}
