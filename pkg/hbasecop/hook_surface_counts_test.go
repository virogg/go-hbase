// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import "testing"

// TestHookSurfaceCounts pins the per-surface hook counts that the docs,
// CHANGELOG and SPEC scope statements quote. The Master surface is a
// deliberate curated subset of HBase 2.5's ~165 master hooks (not full
// coverage); freezing the counts here means any change to a surface's
// breadth is a conscious edit that updates this test and the docs
// together, instead of silently drifting from the published numbers.
//
// Region 68 + Master 20 + RegionServer 9 + WAL 4 + BulkLoad 2 = 103
// Observer hooks total (the "103 hooks" figure in CHANGELOG/README).
func TestHookSurfaceCounts(t *testing.T) {
	cases := []struct {
		surface string
		got     int
		want    int
	}{
		{"RegionObserver", len(hookTable), 68},
		{"MasterObserver (curated subset)", len(masterHookTable), 20},
		{"RegionServerObserver", len(regionServerHookTable), 9},
		{"WALObserver", len(walHookTable), 4},
		{"BulkLoadObserver", len(bulkLoadHookTable), 2},
	}
	total := 0
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s hook count = %d, want %d (update the docs/CHANGELOG scope statement "+
				"in the same change if this is intentional)", c.surface, c.got, c.want)
		}
		total += c.got
	}
	if total != 103 {
		t.Errorf("total Observer hooks = %d, want 103 (the figure published in CHANGELOG/README)", total)
	}
}
