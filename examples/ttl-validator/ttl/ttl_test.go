// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package ttl

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{"valid one hour", "ttl=3600;payload", 3600 * time.Second, false},
		{"valid min", "ttl=1;", time.Second, false},
		{"valid max digits", "ttl=999999999;x", 999999999 * time.Second, false},
		{"missing envelope", "payload-without-ttl", 0, true},
		{"empty", "", 0, true},
		{"zero ttl", "ttl=0;x", 0, true},
		{"no digits", "ttl=;x", 0, true},
		{"no terminator", "ttl=360", 0, true},
		{"non-digit seconds", "ttl=3a0;x", 0, true},
		{"too many digits", "ttl=1234567890;x", 0, true},
		{"prefix case-sensitive", "TTL=60;x", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Validate([]byte(tc.value))
			if tc.wantErr != (err != nil) {
				t.Fatalf("Validate(%q) error = %v, wantErr %v", tc.value, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("Validate(%q) = %v, want %v", tc.value, got, tc.want)
			}
			if err != nil && len(tc.value) > 4 && strings.Contains(err.Error(), tc.value) {
				t.Fatalf("error leaks the cell value: %v", err)
			}
		})
	}
}

func put(values ...string) *hbasepb.MutationProto {
	cv := &hbasepb.MutationProto_ColumnValue{Family: []byte("cf")}
	for _, v := range values {
		cv.QualifierValue = append(cv.QualifierValue,
			&hbasepb.MutationProto_ColumnValue_QualifierValue{
				Qualifier: []byte("q"), Value: []byte(v),
			})
	}
	return &hbasepb.MutationProto{
		Row:         []byte("row-1"),
		ColumnValue: []*hbasepb.MutationProto_ColumnValue{cv},
	}
}

func TestPrePutAcceptsAndRejects(t *testing.T) {
	var buf bytes.Buffer
	o := New()
	o.SetLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	env := hbasecop.ObserverEnv{TableName: "t", RegionName: "r"}
	ctx := context.Background()

	if _, err := o.PrePut(ctx, env, put("ttl=60;ok")); err != nil {
		t.Fatalf("valid put rejected: %v", err)
	}
	if _, err := o.PrePut(ctx, env, put("no-envelope")); err == nil {
		t.Fatal("invalid put accepted")
	} else if !strings.Contains(err.Error(), "ttl-validator") {
		t.Fatalf("rejection error lacks observer prefix: %v", err)
	}
	if _, err := o.PrePut(ctx, env, put("ttl=60;ok", "bad")); err == nil {
		t.Fatal("mixed put accepted; the whole Put must be rejected")
	}

	if o.Accepted() != 1 || o.Rejected() != 2 {
		t.Fatalf("counters accepted=%d rejected=%d, want 1/2", o.Accepted(), o.Rejected())
	}
	if strings.Contains(buf.String(), "no-envelope") {
		t.Fatal("decision log leaks a cell value")
	}
}
