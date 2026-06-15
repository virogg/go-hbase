// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunDeployRequiresSubcommand(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := runDeploy([]string{"--bridge-jar", "/tmp/b.jar"}); err == nil {
		t.Error("deploy with no subcommand should error")
	}
}

func TestRunDeployPrintsCommandWithoutHbase(t *testing.T) {
	// Force the "hbase not on PATH" branch: empty PATH, explicit bridge jar.
	t.Setenv("PATH", "")
	jar := filepath.Join(t.TempDir(), "hbasecop-bridge-0.0.1-all.jar")
	if err := os.WriteFile(jar, []byte("jar"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Should not error: it prints the ready-to-run command and returns nil.
	if err := runDeploy([]string{"--bridge-jar", jar, "list", "--table", "t"}); err != nil {
		t.Fatalf("expected printed-command fallback, got %v", err)
	}
}
