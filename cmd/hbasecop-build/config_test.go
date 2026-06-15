// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name, value string
		bad         bool
	}{
		{"hbasecop.policy.prePut", "strict", false},
		{"hbasecop.policy.prePut", "strct", true},
		{"hbasecop.timeout.prePut", "500ms", false},
		{"hbasecop.timeout.prePut", "500", true},
		{"hbasecop.timeout.default", "2s", false},
		{"hbasecop.ring.capacity", "16", false},
		{"hbasecop.ring.capacity", "0", true},
		{"hbasecop.ring.capacity", "x", true},
		{"hbasecop.heartbeat.period", "500ms", false},
		{"hbasecop.bogus.key", "whatever", false}, // unknown -> not malformed
	}
	for _, tc := range tests {
		msg := validateKey(tc.name, tc.value)
		if (msg != "") != tc.bad {
			t.Errorf("validateKey(%q,%q) = %q, want bad=%v", tc.name, tc.value, msg, tc.bad)
		}
	}
}

func writeSite(t *testing.T, props map[string]string) string {
	t.Helper()
	var b string
	b = `<?xml version="1.0"?><configuration>`
	for k, v := range props {
		b += "<property><name>" + k + "</name><value>" + v + "</value></property>"
	}
	b += `</configuration>`
	p := filepath.Join(t.TempDir(), "hbase-site.xml")
	if err := os.WriteFile(p, []byte(b), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCheckSiteFile(t *testing.T) {
	t.Run("clean passes", func(t *testing.T) {
		p := writeSite(t, map[string]string{
			"hbasecop.policy.prePut":   "strict",
			"hbasecop.timeout.default": "2s",
			"unrelated.key":            "ignored",
		})
		if err := checkSiteFile(p); err != nil {
			t.Fatalf("clean site should pass, got %v", err)
		}
	})

	t.Run("malformed fails", func(t *testing.T) {
		p := writeSite(t, map[string]string{
			"hbasecop.policy.prePut": "nope",
			"hbasecop.ring.capacity": "-1",
		})
		if err := checkSiteFile(p); err == nil {
			t.Fatal("malformed site should fail")
		}
	})

	t.Run("missing file errors", func(t *testing.T) {
		if err := checkSiteFile(filepath.Join(t.TempDir(), "none.xml")); err == nil {
			t.Fatal("missing file should error")
		}
	})
}

func TestRunConfigRequiresMode(t *testing.T) {
	if err := runConfig(nil); err == nil {
		t.Error("config with no flags should error")
	}
}
