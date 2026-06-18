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
		{"hbasecop.endpoint.timeout", "30s", false},
		{"hbasecop.endpoint.timeout", "30", true}, // duration needs a unit
		{"hbasecop.endpoint.scanner-idle-lease", "2m", false},
		{"hbasecop.endpoint.allow-mutate", "true", false},
		{"hbasecop.endpoint.allow-mutate", "FALSE", false}, // case-insensitive
		{"hbasecop.endpoint.allow-mutate", "yes", true},    // not a bool
		{"hbasecop.endpoint.max-concurrent-calls", "8", false},
		{"hbasecop.endpoint.max-concurrent-calls", "-5", true},
		{"hbasecop.endpoint.bulk-ring.capacity", "16", false},
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

	t.Run("endpoint keys are validated, not ignored", func(t *testing.T) {
		bad := writeSite(t, map[string]string{
			"hbasecop.endpoint.allow-mutate":         "notabool",
			"hbasecop.endpoint.max-concurrent-calls": "-5",
		})
		if err := checkSiteFile(bad); err == nil {
			t.Fatal("malformed endpoint values should fail (not pass as ignored notices)")
		}
		good := writeSite(t, map[string]string{
			"hbasecop.endpoint.allow-mutate":       "true",
			"hbasecop.endpoint.timeout":            "30s",
			"hbasecop.endpoint.max-rows-per-next":  "1000",
			"hbasecop.endpoint.scanner-idle-lease": "2m",
		})
		if err := checkSiteFile(good); err != nil {
			t.Fatalf("valid endpoint values should pass, got %v", err)
		}
	})
}

func TestUnknownHookSuffix(t *testing.T) {
	tests := []struct {
		name string
		want string // the flagged unknown hook, or "" when the key is fine
	}{
		{"hbasecop.policy.prePut", ""},              // valid lower-camel hook
		{"hbasecop.timeout.preWALAppend", ""},       // valid multi-caps hook
		{"hbasecop.timeout.default", ""},            // the one non-hook suffix
		{"hbasecop.timeout.PrePut", "PrePut"},       // Go casing is not the key form
		{"hbasecop.policy.prePutt", "prePutt"},      // typo
		{"hbasecop.timeout.bogusHook", "bogusHook"}, // unknown hook
		{"hbasecop.ring.capacity", ""},              // not a per-hook key
		{"hbasecop.policy.", ""},                    // empty suffix: not a hook claim
	}
	for _, tc := range tests {
		if got := unknownHookSuffix(tc.name); got != tc.want {
			t.Errorf("unknownHookSuffix(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestRunConfigRequiresMode(t *testing.T) {
	if err := runConfig(nil); err == nil {
		t.Error("config with no flags should error")
	}
}
