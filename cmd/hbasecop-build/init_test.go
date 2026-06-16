// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunInitScaffolds(t *testing.T) {
	for _, surface := range []string{"region", "master", "wal"} {
		t.Run(surface, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "obs")
			if err := runInit([]string{"--surface", surface, "--dir", dir, "myobs"}); err != nil {
				t.Fatal(err)
			}
			main := filepath.Join(dir, "main.go")
			if _, err := parser.ParseFile(token.NewFileSet(), main, nil, 0); err != nil {
				t.Fatalf("generated main.go does not parse: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, "README.md")); err != nil {
				t.Fatal("README.md not generated")
			}
		})
	}
}

func TestRunInitRejectsBadSurfaceAndClobber(t *testing.T) {
	dir := t.TempDir()
	if err := runInit([]string{"--surface", "nope", "--dir", dir, "x"}); err == nil {
		t.Error("bad surface should error")
	}
	if err := runInit([]string{"--dir", dir, "x"}); err != nil {
		t.Fatal(err)
	}
	if err := runInit([]string{"--dir", dir, "x"}); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Errorf("second init should refuse to clobber, got %v", err)
	}
}

func TestRunInitRequiresName(t *testing.T) {
	if err := runInit([]string{"--surface", "region"}); err == nil {
		t.Error("missing name should error")
	}
}
