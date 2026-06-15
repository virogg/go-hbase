// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func elfHeader(class, machineLo, machineHi byte) []byte {
	h := make([]byte, 20)
	h[0], h[1], h[2], h[3] = 0x7f, 'E', 'L', 'F'
	h[4] = class
	h[18], h[19] = machineLo, machineHi
	return h
}

func TestCheckLinuxAmd64ELF(t *testing.T) {
	tests := []struct {
		name    string
		bytes   []byte
		wantErr bool
	}{
		{"amd64", elfHeader(2, 0x3e, 0x00), false},
		{"not elf", []byte("#!/bin/sh\necho hi\n"), true},
		{"32-bit", elfHeader(1, 0x3e, 0x00), true},
		{"arm64", elfHeader(2, 0xb7, 0x00), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "bin")
			if err := os.WriteFile(p, tc.bytes, 0o644); err != nil {
				t.Fatal(err)
			}
			err := checkLinuxAmd64ELF(p)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestDefaultCoprocID(t *testing.T) {
	for in, want := range map[string]string{
		"audit.jar":           "audit",
		"out/my-observer.jar": "my-observer",
		"noext":               "noext",
	} {
		if got := defaultCoprocID(in); got != want {
			t.Errorf("defaultCoprocID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParsePackageFlags(t *testing.T) {
	if _, err := parsePackageFlags([]string{"--out", "x.jar"}); err == nil {
		t.Error("missing --src should error")
	}
	if _, err := parsePackageFlags([]string{"--src", "./x"}); err == nil {
		t.Error("missing --out should error")
	}
	o, err := parsePackageFlags([]string{"--src", "./x", "--out", "x.jar"})
	if err != nil {
		t.Fatal(err)
	}
	if o.Surface != "region" {
		t.Errorf("default surface = %q, want region", o.Surface)
	}
}

func TestResolveBridgeJar(t *testing.T) {
	t.Run("explicit flag wins", func(t *testing.T) {
		got, err := resolveBridgeJar("/path/to/b.jar")
		if err != nil || got != "/path/to/b.jar" {
			t.Fatalf("got %q err %v", got, err)
		}
	})

	t.Run("newest under .m2", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		dir := filepath.Join(home, ".m2", "repository", "com", "virogg", "hbasecop-bridge")
		old := filepath.Join(dir, "0.0.1", "hbasecop-bridge-0.0.1-all.jar")
		newer := filepath.Join(dir, "0.0.2", "hbasecop-bridge-0.0.2-all.jar")
		for _, p := range []string{old, newer} {
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte("jar"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		// Pin mtimes so the newest pick is deterministic regardless of FS order.
		if err := os.Chtimes(old, time.Unix(1000, 0), time.Unix(1000, 0)); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(newer, time.Unix(2000, 0), time.Unix(2000, 0)); err != nil {
			t.Fatal(err)
		}
		got, err := resolveBridgeJar("")
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Base(got) != "hbasecop-bridge-0.0.2-all.jar" {
			t.Fatalf("got %q, want newest 0.0.2", got)
		}
	})

	t.Run("missing errors", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		if _, err := resolveBridgeJar(""); err == nil {
			t.Error("expected error when no bridge jar present")
		}
	})
}
