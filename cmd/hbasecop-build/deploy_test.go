// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunAdminRequiresSubcommand(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := runAdmin([]string{"--bridge-jar", "/tmp/b.jar"}); err == nil {
		t.Error("deploy with no subcommand should error")
	}
}

func TestRunAdminPrintsCommandWithoutHbase(t *testing.T) {
	t.Setenv("PATH", "")
	jar := filepath.Join(t.TempDir(), "hbasecop-bridge-0.0.1-all.jar")
	if err := os.WriteFile(jar, []byte("jar"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runAdmin([]string{"--bridge-jar", jar, "list", "--table", "t"}); err != nil {
		t.Fatalf("expected printed-command fallback, got %v", err)
	}
}
