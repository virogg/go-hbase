// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestCoverageMatrixDocCoversAllHooks is the T46 CI gate: it parses
// docs/coverage-region-observer.md and asserts every hook in the
// canonical T41 dispatch table appears as a row with status=covered
// and a non-empty covered_by column. Adding a new hook to hookTable
// without recording its coverage anchor will fail the suite.
func TestCoverageMatrixDocCoversAllHooks(t *testing.T) {
	doc := readCoverageDoc(t)
	rows := parseCoverageMatrix(t, doc)

	for _, h := range hookTable {
		row, ok := rows[h.name]
		if !ok {
			t.Errorf("coverage matrix missing row for hook %q (id=%d)", h.name, h.id)
			continue
		}
		if row.status != "covered" {
			t.Errorf(
				"coverage matrix row for %q has status=%q, want %q",
				h.name, row.status, "covered",
			)
		}
		if row.coveredBy == "" {
			t.Errorf("coverage matrix row for %q has empty covered_by", h.name)
		}
		if row.hookID != h.id {
			t.Errorf(
				"coverage matrix row for %q lists hook_id=%d, want %d",
				h.name, row.hookID, h.id,
			)
		}
	}

	if len(rows) != len(hookTable) {
		t.Errorf(
			"coverage matrix row count = %d, hookTable len = %d; "+
				"every row must correspond to one canonical hook and vice versa",
			len(rows), len(hookTable),
		)
	}
}

type coverageRow struct {
	hookID    HookID
	status    string
	coveredBy string
}

// readCoverageDoc resolves docs/coverage-region-observer.md by walking
// up from this test file's location to the repo root, so the test runs
// the same way under `go test ./...` and from inside the package dir.
func readCoverageDoc(t *testing.T) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed: cannot resolve repo root")
	}
	dir := filepath.Dir(here)
	for range 8 {
		candidate := filepath.Join(dir, "docs", "coverage-region-observer.md")
		if _, err := os.Stat(candidate); err == nil {
			b, err := os.ReadFile(candidate)
			if err != nil {
				t.Fatalf("read coverage doc: %v", err)
			}
			return string(b)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("docs/coverage-region-observer.md not found from %s", here)
	return ""
}

// parseCoverageMatrix extracts rows from the markdown table whose header
// is `| hook_id | name | proto Request | covered_by | status |`. Any
// other markdown content is ignored.
func parseCoverageMatrix(t *testing.T, doc string) map[string]coverageRow {
	t.Helper()
	rows := make(map[string]coverageRow)
	inTable := false
	for raw := range strings.SplitSeq(doc, "\n") {
		line := strings.TrimSpace(raw)
		if !strings.HasPrefix(line, "|") {
			inTable = false
			continue
		}
		cells := splitMarkdownRow(line)
		if !inTable {
			if len(cells) == 5 &&
				cells[0] == "hook_id" &&
				cells[1] == "name" &&
				cells[2] == "proto Request" &&
				cells[3] == "covered_by" &&
				cells[4] == "status" {
				inTable = true
			}
			continue
		}
		// Separator row (`| --- | --- | … |`).
		if len(cells) > 0 && strings.HasPrefix(cells[0], "-") {
			continue
		}
		if len(cells) != 5 {
			t.Fatalf("coverage matrix: malformed row (want 5 cells, got %d): %q", len(cells), line)
		}
		id, ok := parseHookID(cells[0])
		if !ok {
			t.Fatalf("coverage matrix: row %q has unparseable hook_id %q", cells[1], cells[0])
		}
		rows[cells[1]] = coverageRow{
			hookID:    id,
			coveredBy: cells[3],
			status:    cells[4],
		}
	}
	return rows
}

func splitMarkdownRow(line string) []string {
	trimmed := strings.Trim(line, "|")
	parts := strings.Split(trimmed, "|")
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = strings.TrimSpace(p)
	}
	return out
}

func parseHookID(cell string) (HookID, bool) {
	var v int64
	for _, r := range cell {
		if r < '0' || r > '9' {
			return 0, false
		}
		v = v*10 + int64(r-'0')
	}
	if cell == "" {
		return 0, false
	}
	return HookID(v), true
}
