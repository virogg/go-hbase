// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"strings"
	"testing"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

func normalizeHookName(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

func TestHookIDMatchesProtoEnum(t *testing.T) {
	protoByNorm := make(map[string]int32, len(hookpb.HookId_value))
	for name, val := range hookpb.HookId_value {
		tail := strings.TrimPrefix(name, "HOOK_ID_")
		if tail == "UNSPECIFIED" {
			continue
		}
		protoByNorm[normalizeHookName(tail)] = val
	}

	type row struct {
		id   HookID
		name string
	}
	var all []row
	for _, h := range hookTable {
		all = append(all, row{h.id, h.name})
	}
	for _, h := range masterHookTable {
		all = append(all, row{h.id, h.name})
	}
	for _, h := range regionServerHookTable {
		all = append(all, row{h.id, h.name})
	}
	for _, h := range walHookTable {
		all = append(all, row{h.id, h.name})
	}
	for _, h := range bulkLoadHookTable {
		all = append(all, row{h.id, h.name})
	}
	if len(all) == 0 {
		t.Fatal("no hook tables populated")
	}

	seen := make(map[HookID]string, len(all))
	for _, r := range all {
		if prev, dup := seen[r.id]; dup {
			t.Errorf("duplicate HookID %d across tables: %q and %q", r.id, prev, r.name)
		}
		seen[r.id] = r.name

		norm := normalizeHookName(r.name)
		val, ok := protoByNorm[norm]
		if !ok {
			t.Errorf("hook %q (id=%d): no proto HookId enumerator matches (normalized %q)",
				r.name, r.id, norm)
			continue
		}
		if int32(r.id) != val {
			t.Errorf("hook %q: Go HookID=%d but proto enum=%d - on-wire dispatch key drift",
				r.name, r.id, val)
		}
	}
}
