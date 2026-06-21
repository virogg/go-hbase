// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

func mutation(row string, cellsPerFamily ...int) *hbasepb.MutationProto {
	m := &hbasepb.MutationProto{Row: []byte(row)}
	for _, n := range cellsPerFamily {
		cv := &hbasepb.MutationProto_ColumnValue{Family: []byte("cf")}
		for range n {
			cv.QualifierValue = append(cv.QualifierValue,
				&hbasepb.MutationProto_ColumnValue_QualifierValue{
					Qualifier: []byte("q"), Value: []byte("v"),
				})
		}
		m.ColumnValue = append(m.ColumnValue, cv)
	}
	return m
}

func TestNewRecord(t *testing.T) {
	env := hbasecop.ObserverEnv{TableName: "ns:users", RegionName: "r1"}

	tests := []struct {
		name      string
		op        string
		mut       *hbasepb.MutationProto
		wantCells int
		wantRowDg bool
	}{
		{"put two families", "put", mutation("row-1", 2, 3), 5, true},
		{"delete single cell", "delete", mutation("row-2", 1), 1, true},
		{"nil mutation", "put", nil, 0, false},
		{"empty row", "put", mutation(""), 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRecord(tc.op, env, tc.mut, 7)
			if r.Op != tc.op || r.Table != "ns:users" || r.Region != "r1" || r.Seq != 7 {
				t.Fatalf("scope fields wrong: %+v", r)
			}
			if r.Cells != tc.wantCells {
				t.Fatalf("Cells = %d, want %d", r.Cells, tc.wantCells)
			}
			if tc.wantRowDg != (r.RowDigest != "") {
				t.Fatalf("RowDigest presence = %q, want present=%v", r.RowDigest, tc.wantRowDg)
			}
		})
	}
}

func TestRowDigestNeverLeaksKey(t *testing.T) {
	key := "secret-customer-id-42"
	d1, d2 := RowDigest([]byte(key)), RowDigest([]byte(key))
	if d1 != d2 {
		t.Fatalf("digest not deterministic: %s vs %s", d1, d2)
	}
	if len(d1) != 16 {
		t.Fatalf("digest length = %d, want 16 hex chars", len(d1))
	}
	if strings.Contains(d1, key) {
		t.Fatal("digest contains the raw row key")
	}
	if RowDigest([]byte("other")) == d1 {
		t.Fatal("distinct keys produced identical digests")
	}
}

func TestObserverEmitsOneAuditLinePerOp(t *testing.T) {
	var buf bytes.Buffer
	o := New()
	o.SetLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	env := hbasecop.ObserverEnv{TableName: "t", RegionName: "r"}

	if err := o.PostPut(context.Background(), env, mutation("row-A", 1)); err != nil {
		t.Fatal(err)
	}
	if err := o.PostDelete(context.Background(), env,
		&hookpb.PostDeleteRequest{Mutation: mutation("row-A", 1)}); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d audit lines, want 2:\n%s", len(lines), buf.String())
	}
	wantOps := []string{"put", "delete"}
	for i, line := range lines {
		if !strings.Contains(line, Marker) {
			t.Fatalf("line %d missing marker %q: %s", i, Marker, line)
		}
		if strings.Contains(line, "row-A") {
			t.Fatalf("line %d leaks the raw row key: %s", i, line)
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d not JSON: %v", i, err)
		}
		if rec["op"] != wantOps[i] {
			t.Fatalf("line %d op = %v, want %s", i, rec["op"], wantOps[i])
		}
		if rec["seq"] != float64(i+1) {
			t.Fatalf("line %d seq = %v, want %d", i, rec["seq"], i+1)
		}
	}
}
