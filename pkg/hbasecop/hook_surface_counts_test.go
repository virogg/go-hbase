// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import "testing"

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
