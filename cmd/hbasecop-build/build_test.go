// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBuild_HappyPath checks the full coproc-jar shape produced from a synthetic
// bridge.jar plus an ELF stand-in. The asserts target what the supervisor
// (T71 Wave A/B) consumes: HbaseCop-* manifest entries with correct SHA-256 +
// the ELF at the declared resource path.
func TestBuild_HappyPath(t *testing.T) {
	dir := t.TempDir()

	bridgeJar := filepath.Join(dir, "bridge.jar")
	writeBridgeJar(t, bridgeJar, map[string]string{
		"com/virogg/hbasecop/bridge/Foo.class": "bridge-class-bytes",
		"bin/linux-amd64/hbasecop-runtime":     "stale-runtime-should-be-stripped",
		"META-INF/services/foo":                "service-line",
	})

	elf := filepath.Join(dir, "myelf")
	elfBytes := []byte("\x7fELF-not-really-but-fine-for-tests")
	if err := os.WriteFile(elf, elfBytes, 0o755); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out.jar")
	if err := Build(BuildOptions{
		GoBinPath:     elf,
		BridgeJarPath: bridgeJar,
		ObserverClass: "com.example.Audit",
		CoprocID:      "audit-observer",
		OutJarPath:    out,
	}); err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	zr, err := zip.OpenReader(out)
	if err != nil {
		t.Fatalf("open output jar: %v", err)
	}
	defer func() { _ = zr.Close() }()

	entries := map[string][]byte{}
	for _, f := range zr.File {
		entries[f.Name] = readZipEntry(t, f)
	}

	// Manifest must be present and parseable.
	mfBytes, ok := entries["META-INF/MANIFEST.MF"]
	if !ok {
		t.Fatalf("output jar missing META-INF/MANIFEST.MF; entries=%v", keys(entries))
	}
	mf := parseManifest(t, mfBytes)
	checkAttr(t, mf, "HbaseCop-Observer-Class", "com.example.Audit")
	checkAttr(t, mf, "HbaseCop-Coproc-Id", "audit-observer")
	checkAttr(t, mf, "HbaseCop-Go-Bin-Name", "bin/linux-amd64/myelf")

	wantSha := sha256Hex(elfBytes)
	checkAttr(t, mf, "HbaseCop-Go-Bin-SHA256", wantSha)

	// ELF must be at the declared resource path with original bytes.
	gotElf, ok := entries["bin/linux-amd64/myelf"]
	if !ok {
		t.Fatalf("output jar missing bin/linux-amd64/myelf; entries=%v", keys(entries))
	}
	if !bytes.Equal(gotElf, elfBytes) {
		t.Fatalf("ELF bytes mismatch: got %q want %q", gotElf, elfBytes)
	}

	// Bridge classes preserved.
	if _, ok := entries["com/virogg/hbasecop/bridge/Foo.class"]; !ok {
		t.Fatalf("bridge class dropped from output jar")
	}
	if _, ok := entries["META-INF/services/foo"]; !ok {
		t.Fatalf("META-INF/services lost")
	}
	// Stale bin/** from bridge must NOT win — the user's ELF is canonical.
	if _, ok := entries["bin/linux-amd64/hbasecop-runtime"]; ok {
		t.Fatalf("stale bridge ELF leaked into output jar (must be stripped)")
	}
}

// TestBuild_FirstEntryIsManifest pins the contract JarInputStream.getManifest()
// relies on: MANIFEST.MF must be the first regular entry (after the optional
// META-INF/ dir). Without this, ManifestBinaryDescriptor.fromJar returns null.
func TestBuild_FirstEntryIsManifest(t *testing.T) {
	dir := t.TempDir()
	bridgeJar := filepath.Join(dir, "bridge.jar")
	writeBridgeJar(t, bridgeJar, map[string]string{
		"com/virogg/hbasecop/bridge/Foo.class": "bridge-class-bytes",
	})
	elf := filepath.Join(dir, "elf")
	if err := os.WriteFile(elf, []byte("elf"), 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.jar")
	if err := Build(BuildOptions{
		GoBinPath:     elf,
		BridgeJarPath: bridgeJar,
		ObserverClass: "com.example.X",
		CoprocID:      "x",
		OutJarPath:    out,
	}); err != nil {
		t.Fatal(err)
	}

	zr, err := zip.OpenReader(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = zr.Close() }()
	if len(zr.File) == 0 {
		t.Fatalf("empty jar")
	}
	// JarInputStream tolerates a leading META-INF/ directory entry — the
	// manifest must come either first or right after it.
	first := zr.File[0].Name
	switch first {
	case "META-INF/MANIFEST.MF":
		// good
	case "META-INF/":
		if len(zr.File) < 2 || zr.File[1].Name != "META-INF/MANIFEST.MF" {
			t.Fatalf("expected MANIFEST.MF right after META-INF/, got entries=%v", filenames(zr))
		}
	default:
		t.Fatalf("first entry must be META-INF/ or MANIFEST.MF; got %q (entries=%v)", first, filenames(zr))
	}
}

func TestBuild_WithPolicyConfig(t *testing.T) {
	dir := t.TempDir()
	bridgeJar := filepath.Join(dir, "bridge.jar")
	writeBridgeJar(t, bridgeJar, map[string]string{
		"com/virogg/hbasecop/bridge/Foo.class": "bridge",
	})
	elf := filepath.Join(dir, "elf")
	if err := os.WriteFile(elf, []byte("elf"), 0o755); err != nil {
		t.Fatal(err)
	}
	policy := filepath.Join(dir, "policy.properties")
	policyContent := "hbasecop.policy.PRE_PUT=strict\nhbasecop.timeout.PRE_PUT=2s\n"
	if err := os.WriteFile(policy, []byte(policyContent), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.jar")
	if err := Build(BuildOptions{
		GoBinPath:     elf,
		BridgeJarPath: bridgeJar,
		ObserverClass: "com.example.X",
		CoprocID:      "x",
		OutJarPath:    out,
		PolicyConfig:  policy,
	}); err != nil {
		t.Fatal(err)
	}

	zr, err := zip.OpenReader(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = zr.Close() }()
	for _, f := range zr.File {
		if f.Name == "META-INF/hbasecop-policy.properties" {
			got := string(readZipEntry(t, f))
			if got != policyContent {
				t.Fatalf("policy file content mismatch: got %q want %q", got, policyContent)
			}
			return
		}
	}
	t.Fatalf("output jar missing META-INF/hbasecop-policy.properties")
}

func TestBuild_CustomBinName(t *testing.T) {
	dir := t.TempDir()
	bridgeJar := filepath.Join(dir, "bridge.jar")
	writeBridgeJar(t, bridgeJar, map[string]string{"x": "y"})
	elf := filepath.Join(dir, "weird-name.bin")
	if err := os.WriteFile(elf, []byte("e"), 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.jar")
	if err := Build(BuildOptions{
		GoBinPath:     elf,
		BridgeJarPath: bridgeJar,
		ObserverClass: "com.example.X",
		CoprocID:      "x",
		OutJarPath:    out,
		BinName:       "bin/linux-amd64/custom",
	}); err != nil {
		t.Fatal(err)
	}
	zr, err := zip.OpenReader(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = zr.Close() }()
	var found bool
	for _, f := range zr.File {
		if f.Name == "bin/linux-amd64/custom" {
			found = true
		}
	}
	if !found {
		t.Fatalf("custom BinName not honoured (entries=%v)", filenames(zr))
	}
}

func TestBuild_ValidatesInputs(t *testing.T) {
	dir := t.TempDir()
	bridgeJar := filepath.Join(dir, "bridge.jar")
	writeBridgeJar(t, bridgeJar, map[string]string{"x": "y"})
	elf := filepath.Join(dir, "elf")
	if err := os.WriteFile(elf, []byte("e"), 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.jar")
	base := BuildOptions{
		GoBinPath:     elf,
		BridgeJarPath: bridgeJar,
		ObserverClass: "com.example.X",
		CoprocID:      "x",
		OutJarPath:    out,
	}

	cases := []struct {
		name string
		mut  func(*BuildOptions)
		want string
	}{
		{"missing-go-bin", func(o *BuildOptions) { o.GoBinPath = "" }, "go-bin"},
		{"missing-bridge", func(o *BuildOptions) { o.BridgeJarPath = "" }, "bridge-jar"},
		{"missing-class", func(o *BuildOptions) { o.ObserverClass = "" }, "observer-class"},
		{"missing-id", func(o *BuildOptions) { o.CoprocID = "" }, "coproc-id"},
		{"missing-out", func(o *BuildOptions) { o.OutJarPath = "" }, "out"},
		{"class-no-dot", func(o *BuildOptions) { o.ObserverClass = "NotFq" }, "observer-class"},
		{"go-bin-missing-file", func(o *BuildOptions) { o.GoBinPath = filepath.Join(dir, "nope") }, "go-bin"},
		{"bridge-missing-file", func(o *BuildOptions) { o.BridgeJarPath = filepath.Join(dir, "nope.jar") }, "bridge-jar"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := base
			opts.OutJarPath = filepath.Join(dir, tc.name+".jar")
			tc.mut(&opts) // mutator runs last so it can null-out OutJarPath
			err := Build(opts)
			if err == nil {
				t.Fatalf("expected validation error, got nil")
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("error message %q must mention %q", err.Error(), tc.want)
			}
		})
	}
}

// --- helpers ---

func writeBridgeJar(t *testing.T, path string, entries map[string]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zw := zip.NewWriter(f)
	// Start with a minimal MANIFEST.MF to mirror real bridge jars.
	mfW, err := zw.Create("META-INF/MANIFEST.MF")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mfW.Write([]byte("Manifest-Version: 1.0\r\nCreated-By: test\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	for name, body := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
}

func readZipEntry(t *testing.T, f *zip.File) []byte {
	t.Helper()
	r, err := f.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func parseManifest(t *testing.T, raw []byte) map[string]string {
	t.Helper()
	// Unfold continuation lines (leading-space lines append to previous after
	// stripping the leading space and any preceding CR/LF). Then split into
	// key:value records on bare blank lines (we only need the main section
	// for HbaseCop-* lookups; per-entry sections come after a blank line).
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	text = strings.ReplaceAll(text, "\n ", "")
	out := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		if line == "" {
			break // main section ends at first blank line
		}
		idx := strings.Index(line, ": ")
		if idx < 0 {
			continue
		}
		out[line[:idx]] = line[idx+2:]
	}
	return out
}

func checkAttr(t *testing.T, mf map[string]string, key, want string) {
	t.Helper()
	got, ok := mf[key]
	if !ok {
		t.Fatalf("manifest missing %s; got=%v", key, mf)
	}
	if got != want {
		t.Fatalf("manifest %s: got %q want %q", key, got, want)
	}
}

func keys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func filenames(zr *zip.ReadCloser) []string {
	out := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		out = append(out, f.Name)
	}
	return out
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
