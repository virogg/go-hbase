// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"reflect"
	"testing"
)

// TestBulkLoadHookTableIsCanonical mirrors TestMasterHookTableIsCanonical for
// the bulk-load surface introduced in T54.
func TestBulkLoadHookTableIsCanonical(t *testing.T) {
	if len(bulkLoadHookTable) == 0 {
		t.Fatal("bulkLoadHookTable empty: T54 dispatch table not populated")
	}
	seenIDs := make(map[HookID]string, len(bulkLoadHookTable))
	seenNames := make(map[string]HookID, len(bulkLoadHookTable))
	for _, h := range bulkLoadHookTable {
		if h.id == HookIDUnknown {
			t.Errorf("bulkLoadHookTable contains HookIDUnknown entry for %q", h.name)
		}
		if h.name == "" {
			t.Errorf("bulkLoadHookTable entry id=%d has empty name", h.id)
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

// TestBulkLoadHookIDsDoNotCollide pins the ID partitioning so a future
// hook addition can't accidentally claim a value taken by another
// surface (region 1-99, master 100-199, region-server 200-219,
// WAL 220-223, bulk-load 224-225).
func TestBulkLoadHookIDsDoNotCollide(t *testing.T) {
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
		taken[h.id] = h.name
	}
	for _, h := range bulkLoadHookTable {
		if prev, clash := taken[h.id]; clash {
			t.Errorf("bulk-load hook %q (id=%d) collides with %q", h.name, h.id, prev)
		}
	}
}

// TestBulkLoadObserverInterfaceCoversAllHooks mirrors the master
// reflection check: every entry in bulkLoadHookTable corresponds to a
// BulkLoadObserver method and vice versa.
func TestBulkLoadObserverInterfaceCoversAllHooks(t *testing.T) {
	rt := reflect.TypeOf((*BulkLoadObserver)(nil)).Elem()
	methods := make(map[string]bool, rt.NumMethod())
	for i := range rt.NumMethod() {
		methods[rt.Method(i).Name] = true
	}
	for _, h := range bulkLoadHookTable {
		if !methods[h.name] {
			t.Errorf("BulkLoadObserver missing method %q (HookID=%d)", h.name, h.id)
		}
	}
	if got, want := rt.NumMethod(), len(bulkLoadHookTable); got != want {
		t.Errorf(
			"BulkLoadObserver method count = %d, bulkLoadHookTable len = %d; "+
				"every interface method must have a HookID entry and vice versa",
			got, want,
		)
	}
}

// TestUnimplementedBulkLoadObserverSatisfiesInterface promotes the
// compile-time satisfaction check to runtime and pins the noop return
// contract (HookResult{}, nil) on every method.
func TestUnimplementedBulkLoadObserverSatisfiesInterface(t *testing.T) {
	var obs BulkLoadObserver = UnimplementedBulkLoadObserver{}
	res, err := obs.PrePrepareBulkLoad(context.Background(), ObserverEnv{}, nil)
	if err != nil {
		t.Fatalf("noop PrePrepareBulkLoad returned err=%v, want nil", err)
	}
	if res.Bypass {
		t.Fatal("noop PrePrepareBulkLoad returned Bypass=true, want false")
	}
	if _, err := obs.PreCleanupBulkLoad(context.Background(), ObserverEnv{}, nil); err != nil {
		t.Fatalf("noop PreCleanupBulkLoad returned err=%v, want nil", err)
	}
}
