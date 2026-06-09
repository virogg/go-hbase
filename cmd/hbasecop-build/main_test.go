// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

func TestParseFlags(t *testing.T) {
	t.Run("full valid set", func(t *testing.T) {
		opts, err := parseFlags([]string{
			"--go-bin", "/bin/obs",
			"--bridge-jar", "/m2/bridge.jar",
			"--observer-class", "com.example.Audit",
			"--coproc-id", "audit",
			"--out", "/out/audit.jar",
			"--policy-config", "/cfg/policy.properties",
			"--bin-name", "bin/linux-amd64/custom",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.GoBinPath != "/bin/obs" || opts.BridgeJarPath != "/m2/bridge.jar" ||
			opts.ObserverClass != "com.example.Audit" || opts.CoprocID != "audit" ||
			opts.OutJarPath != "/out/audit.jar" || opts.PolicyConfig != "/cfg/policy.properties" ||
			opts.BinName != "bin/linux-amd64/custom" {
			t.Fatalf("flags not parsed into options: %+v", opts)
		}
	})

	t.Run("empty args parse to zero options", func(t *testing.T) {
		// parseFlags only parses; required-flag validation lives in Build.
		opts, err := parseFlags(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.GoBinPath != "" || opts.OutJarPath != "" {
			t.Fatalf("expected zero-value options, got %+v", opts)
		}
	})

	t.Run("unknown flag errors", func(t *testing.T) {
		if _, err := parseFlags([]string{"--nope", "x"}); err == nil {
			t.Fatal("expected error on unknown flag")
		}
	})

	t.Run("unexpected positional errors", func(t *testing.T) {
		if _, err := parseFlags([]string{"--go-bin", "/b", "stray"}); err == nil {
			t.Fatal("expected error on positional arg")
		}
	})
}
