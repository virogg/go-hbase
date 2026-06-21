// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"reflect"
	"testing"
)

func TestRegionServerHookTableIsCanonical(t *testing.T) {
	if len(regionServerHookTable) == 0 {
		t.Fatal("regionServerHookTable empty: T52 dispatch table not populated")
	}
	seenIDs := make(map[HookID]string, len(regionServerHookTable))
	seenNames := make(map[string]HookID, len(regionServerHookTable))
	for _, h := range regionServerHookTable {
		if h.id == HookIDUnknown {
			t.Errorf("regionServerHookTable contains HookIDUnknown entry for %q", h.name)
		}
		if h.name == "" {
			t.Errorf("regionServerHookTable entry id=%d has empty name", h.id)
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

func TestRegionServerHookIDsAreInReservedRange(t *testing.T) {
	taken := make(map[HookID]string)
	for _, h := range hookTable {
		taken[h.id] = h.name
	}
	for _, h := range masterHookTable {
		taken[h.id] = h.name
	}
	for _, h := range regionServerHookTable {
		if prev, clash := taken[h.id]; clash {
			t.Errorf("region-server hook %q (id=%d) collides with %q", h.name, h.id, prev)
		}
		if h.id < 200 {
			t.Errorf(
				"region-server hook %q has id=%d below the reserved 200-255 range",
				h.name, h.id,
			)
		}
	}
}

func TestRegionServerObserverInterfaceCoversAllHooks(t *testing.T) {
	rt := reflect.TypeOf((*RegionServerObserver)(nil)).Elem()
	methods := make(map[string]bool, rt.NumMethod())
	for i := range rt.NumMethod() {
		methods[rt.Method(i).Name] = true
	}
	for _, h := range regionServerHookTable {
		if !methods[h.name] {
			t.Errorf("RegionServerObserver missing method %q (HookID=%d)", h.name, h.id)
		}
	}
	if got, want := rt.NumMethod(), len(regionServerHookTable); got != want {
		t.Errorf(
			"RegionServerObserver method count = %d, regionServerHookTable len = %d; "+
				"every interface method must have a HookID entry and vice versa",
			got, want,
		)
	}
}

func TestUnimplementedRegionServerObserverSatisfiesInterface(t *testing.T) {
	var obs RegionServerObserver = UnimplementedRegionServerObserver{}
	res, err := obs.PreStopRegionServer(context.Background(), ObserverEnv{}, nil)
	if err != nil {
		t.Fatalf("noop PreStopRegionServer returned err=%v, want nil", err)
	}
	if res.Bypass {
		t.Fatal("noop PreStopRegionServer returned Bypass=true, want false")
	}
	if err := obs.PostExecuteProcedures(context.Background(), ObserverEnv{}, nil); err != nil {
		t.Fatalf("noop PostExecuteProcedures returned err=%v, want nil", err)
	}
}
