// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/zip"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// TestBuild_PreservesDirectoryEntries pins the fix for the path.Clean jar
// corruption: a bridge jar (as produced by maven-jar-plugin) carries explicit
// directory entries whose names end in "/". The coproc-jar assembler must copy
// those verbatim. path.Clean used to strip the trailing slash, demoting
// "META-INF/" → "META-INF" (a zero-length file colliding with its own
// directory path) and yielding a jar that `jar x`/unzip/the classloader cannot
// extract. This test fails (entry renamed, mode not a dir) if the bug returns.
func TestBuild_PreservesDirectoryEntries(t *testing.T) {
	dir := t.TempDir()

	bridgeJar := filepath.Join(dir, "bridge.jar")
	writeJarWithDirEntries(t, bridgeJar)

	elf := filepath.Join(dir, "myelf")
	if err := os.WriteFile(elf, []byte("\x7fELF-test"), 0o755); err != nil {
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

	byName := map[string]*zip.File{}
	for _, f := range zr.File {
		byName[f.Name] = f
	}

	// The directory entry must survive with its trailing slash AND be marked
	// as a directory - not demoted to a "com" file.
	d, ok := byName["com/"]
	if !ok {
		t.Fatalf("directory entry \"com/\" missing from output jar; entries=%v", names(zr))
	}
	if !d.FileInfo().IsDir() || (d.Mode()&fs.ModeDir) == 0 {
		t.Fatalf("\"com/\" is not a directory entry (mode=%v)", d.Mode())
	}
	if _, bad := byName["com"]; bad {
		t.Fatalf("directory was demoted to a plain file \"com\" (path.Clean regression)")
	}
	// The class under it must still be present and intact.
	cf, ok := byName["com/example/Foo.class"]
	if !ok {
		t.Fatalf("class entry lost; entries=%v", names(zr))
	}
	rc, err := cf.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	body, _ := io.ReadAll(rc)
	if string(body) != "class-bytes" {
		t.Fatalf("class body corrupted: %q", body)
	}
}

// writeJarWithDirEntries creates a jar containing an explicit directory entry
// ("com/") plus a class beneath it and a manifest, mirroring what
// maven-jar-plugin emits.
func writeJarWithDirEntries(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zw := zip.NewWriter(f)

	dirHdr := &zip.FileHeader{Name: "com/", Method: zip.Store}
	dirHdr.SetMode(fs.ModeDir | 0o755)
	if _, err := zw.CreateHeader(dirHdr); err != nil {
		t.Fatal(err)
	}

	for name, body := range map[string]string{
		"META-INF/MANIFEST.MF":  "Manifest-Version: 1.0\r\n\r\n",
		"com/example/Foo.class": "class-bytes",
	} {
		fh := &zip.FileHeader{Name: name, Method: zip.Deflate}
		fh.SetMode(0o644)
		w, err := zw.CreateHeader(fh)
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

func names(zr *zip.ReadCloser) []string {
	out := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		out = append(out, f.Name)
	}
	return out
}
