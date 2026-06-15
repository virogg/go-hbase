// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"strings"
	"testing"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// normalizeHookName collapses a hook identifier to a casing/separator-free
// form so the Go camelCase method name ("preWALWrite") and the proto
// SCREAMING_SNAKE enum tail ("PRE_WAL_WRITE") compare equal regardless of
// how acronyms are split.
func normalizeHookName(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

// TestHookIDMatchesProtoEnum is the parity guard that hooks.go's doc
// comment has long promised but that never existed: it pins every Go
// HookID constant used by the five dispatch tables to the numeric value
// of its proto.HookId enumerator. hook_id is the on-wire dispatch key, so
// drift between the Go constants and the proto enum would silently
// mis-route hooks across the language boundary with no other test failing.
func TestHookIDMatchesProtoEnum(t *testing.T) {
	// normalized(proto enum tail) -> enum value, from the generated map.
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

	// A HookID must be unique across ALL surfaces: the wire frame carries a
	// single hook_id byte with no observer-class qualifier, so a collision
	// would route one byte to two different hooks.
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
