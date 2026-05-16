// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"reflect"
	"testing"
)

// TestMasterHookTableIsCanonical mirrors TestHookTableIsCanonical for
// the master surface introduced in T51.
func TestMasterHookTableIsCanonical(t *testing.T) {
	if len(masterHookTable) == 0 {
		t.Fatal("masterHookTable empty: T51 dispatch table not populated")
	}
	seenIDs := make(map[HookID]string, len(masterHookTable))
	seenNames := make(map[string]HookID, len(masterHookTable))
	for _, h := range masterHookTable {
		if h.id == HookIDUnknown {
			t.Errorf("masterHookTable contains HookIDUnknown entry for %q", h.name)
		}
		if h.name == "" {
			t.Errorf("masterHookTable entry id=%d has empty name", h.id)
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

// TestMasterHookIDsDoNotCollideWithRegion pins the ID partitioning
// (region 1-99, master 100-199) so a future hook addition can't
// accidentally claim a value already taken by the other surface.
func TestMasterHookIDsDoNotCollideWithRegion(t *testing.T) {
	regionIDs := make(map[HookID]string)
	for _, h := range hookTable {
		regionIDs[h.id] = h.name
	}
	for _, h := range masterHookTable {
		if prev, clash := regionIDs[h.id]; clash {
			t.Errorf(
				"master hook %q (id=%d) collides with region hook %q",
				h.name, h.id, prev,
			)
		}
		if h.id < 100 || h.id > 199 {
			t.Errorf(
				"master hook %q has id=%d outside the reserved 100-199 range",
				h.name, h.id,
			)
		}
	}
}

// TestMasterObserverInterfaceCoversAllHooks mirrors the RegionObserver
// reflection check: every entry in masterHookTable corresponds to a
// MasterObserver method and vice versa.
func TestMasterObserverInterfaceCoversAllHooks(t *testing.T) {
	rt := reflect.TypeOf((*MasterObserver)(nil)).Elem()
	methods := make(map[string]bool, rt.NumMethod())
	for i := range rt.NumMethod() {
		methods[rt.Method(i).Name] = true
	}
	for _, h := range masterHookTable {
		if !methods[h.name] {
			t.Errorf("MasterObserver missing method %q (HookID=%d)", h.name, h.id)
		}
	}
	if got, want := rt.NumMethod(), len(masterHookTable); got != want {
		t.Errorf(
			"MasterObserver method count = %d, masterHookTable len = %d; "+
				"every interface method must have a HookID entry and vice versa",
			got, want,
		)
	}
}

// TestUnimplementedMasterObserverSatisfiesInterface promotes the
// compile-time satisfaction check to runtime and pins the noop return
// contract (HookResult{}, nil) on Pre-* methods.
func TestUnimplementedMasterObserverSatisfiesInterface(t *testing.T) {
	var obs MasterObserver = UnimplementedMasterObserver{}
	res, err := obs.PreCreateTable(context.Background(), ObserverEnv{}, nil)
	if err != nil {
		t.Fatalf("noop PreCreateTable returned err=%v, want nil", err)
	}
	if res.Bypass {
		t.Fatal("noop PreCreateTable returned Bypass=true, want false")
	}
	if err := obs.PostBalance(context.Background(), ObserverEnv{}, nil); err != nil {
		t.Fatalf("noop PostBalance returned err=%v, want nil", err)
	}
}
