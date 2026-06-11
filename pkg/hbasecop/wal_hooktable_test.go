// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"reflect"
	"testing"
)

// TestWALHookTableIsCanonical mirrors TestMasterHookTableIsCanonical for
// the WAL surface introduced in T53.
func TestWALHookTableIsCanonical(t *testing.T) {
	if len(walHookTable) == 0 {
		t.Fatal("walHookTable empty: T53 dispatch table not populated")
	}
	seenIDs := make(map[HookID]string, len(walHookTable))
	seenNames := make(map[string]HookID, len(walHookTable))
	for _, h := range walHookTable {
		if h.id == HookIDUnknown {
			t.Errorf("walHookTable contains HookIDUnknown entry for %q", h.name)
		}
		if h.name == "" {
			t.Errorf("walHookTable entry id=%d has empty name", h.id)
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

// TestWALHookIDsAreInReservedRange pins the ID partitioning (region
// 1-99, master 100-199, region-server 200-219, WAL 220-255) so a future
// hook addition can't accidentally claim a value taken by another
// surface.
func TestWALHookIDsAreInReservedRange(t *testing.T) {
	taken := make(map[HookID]string)
	for _, h := range hookTable {
		taken[h.id] = h.name
	}
	for _, h := range masterHookTable {
		taken[h.id] = h.name
	}
	for _, h := range regionServerHookTable {
		taken[h.id] = h.name
	}
	for _, h := range walHookTable {
		if prev, clash := taken[h.id]; clash {
			t.Errorf("WAL hook %q (id=%d) collides with %q", h.name, h.id, prev)
		}
		if h.id < 220 {
			t.Errorf("WAL hook %q has id=%d below the reserved 220-255 range", h.name, h.id)
		}
	}
}

// TestWALObserverInterfaceCoversAllHooks mirrors the master reflection
// check: every entry in walHookTable corresponds to a WALObserver
// method and vice versa.
func TestWALObserverInterfaceCoversAllHooks(t *testing.T) {
	rt := reflect.TypeOf((*WALObserver)(nil)).Elem()
	methods := make(map[string]bool, rt.NumMethod())
	for i := range rt.NumMethod() {
		methods[rt.Method(i).Name] = true
	}
	for _, h := range walHookTable {
		if !methods[h.name] {
			t.Errorf("WALObserver missing method %q (HookID=%d)", h.name, h.id)
		}
	}
	if got, want := rt.NumMethod(), len(walHookTable); got != want {
		t.Errorf(
			"WALObserver method count = %d, walHookTable len = %d; "+
				"every interface method must have a HookID entry and vice versa",
			got, want,
		)
	}
}

// TestUnimplementedWALObserverSatisfiesInterface promotes the
// compile-time satisfaction check to runtime and pins the noop return
// contract (HookResult{}, nil) on Pre-* methods.
func TestUnimplementedWALObserverSatisfiesInterface(t *testing.T) {
	var obs WALObserver = UnimplementedWALObserver{}
	res, err := obs.PreWALWrite(context.Background(), ObserverEnv{}, nil)
	if err != nil {
		t.Fatalf("noop PreWALWrite returned err=%v, want nil", err)
	}
	if res.Bypass {
		t.Fatal("noop PreWALWrite returned Bypass=true, want false")
	}
	if err := obs.PostWALRoll(context.Background(), ObserverEnv{}, nil); err != nil {
		t.Fatalf("noop PostWALRoll returned err=%v, want nil", err)
	}
}
