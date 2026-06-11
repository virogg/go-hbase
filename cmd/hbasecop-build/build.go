// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// BuildOptions are the inputs to assemble a deployable coproc-jar.
//
// The CLI hands these straight from flags. Every field is required except
// PolicyConfig (optional .properties file) and BinName (auto-derived from
// GoBinPath's basename if empty).
type BuildOptions struct {
	GoBinPath     string // path on disk to the compiled Go ELF
	BridgeJarPath string // path to the shaded hbasecop-bridge jar
	ObserverClass string // FQ Java observer class, e.g. com.example.Audit
	CoprocID      string // operator-chosen coprocessor id, e.g. audit-observer
	OutJarPath    string // where to write the resulting coproc-jar
	PolicyConfig  string // optional path to a .properties file
	BinName       string // optional override; default "bin/linux-amd64/<basename(GoBinPath)>"
}

// Build assembles a coproc-jar at opts.OutJarPath. It is the in-process
// counterpart of the CLI's main(); pulled out so tests can drive it without
// shelling out.
//
// Pipeline:
//  1. validate inputs
//  2. compute SHA-256 of the ELF
//  3. open BridgeJarPath, iterate entries
//  4. write META-INF/MANIFEST.MF first (JarInputStream contract)
//  5. copy every bridge entry except its original manifest and any bin/**
//     (the bridge ships a fallback ELF for ping/pong tests — the user's
//     binary must win)
//  6. write the user's ELF at BinName
//  7. optionally write META-INF/hbasecop-policy.properties
func Build(opts BuildOptions) error {
	if err := validateOptions(&opts); err != nil {
		return err
	}

	elfBytes, err := os.ReadFile(opts.GoBinPath)
	if err != nil {
		return fmt.Errorf("go-bin: read %s: %w", opts.GoBinPath, err)
	}
	sha := sha256.Sum256(elfBytes)
	shaHex := hex.EncodeToString(sha[:])

	if opts.BinName == "" {
		opts.BinName = "bin/linux-amd64/" + filepath.Base(opts.GoBinPath)
	}

	bridge, err := zip.OpenReader(opts.BridgeJarPath)
	if err != nil {
		return fmt.Errorf("bridge-jar: open %s: %w", opts.BridgeJarPath, err)
	}
	defer func() { _ = bridge.Close() }()

	outFile, err := os.Create(opts.OutJarPath)
	if err != nil {
		return fmt.Errorf("out: create %s: %w", opts.OutJarPath, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = outFile.Close()
		}
	}()

	zw := zip.NewWriter(outFile)

	if err := writeManifest(zw, opts, shaHex); err != nil {
		return err
	}
	if err := copyBridgeEntries(zw, &bridge.Reader); err != nil {
		return err
	}
	if err := writeStored(zw, opts.BinName, elfBytes); err != nil {
		return fmt.Errorf("write ELF entry: %w", err)
	}
	if opts.PolicyConfig != "" {
		policyBytes, err := os.ReadFile(opts.PolicyConfig)
		if err != nil {
			return fmt.Errorf("policy-config: read %s: %w", opts.PolicyConfig, err)
		}
		if err := writeStored(zw, "META-INF/hbasecop-policy.properties", policyBytes); err != nil {
			return fmt.Errorf("write policy entry: %w", err)
		}
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("zip close: %w", err)
	}
	closed = true
	if err := outFile.Close(); err != nil {
		return fmt.Errorf("out close: %w", err)
	}
	return nil
}

func validateOptions(o *BuildOptions) error {
	var errs []string
	if o.GoBinPath == "" {
		errs = append(errs, "--go-bin is required")
	}
	if o.BridgeJarPath == "" {
		errs = append(errs, "--bridge-jar is required")
	}
	if o.ObserverClass == "" {
		errs = append(errs, "--observer-class is required")
	} else if !strings.Contains(o.ObserverClass, ".") {
		errs = append(errs, fmt.Sprintf(
			"--observer-class must be fully-qualified (e.g. com.example.Audit), got %q",
			o.ObserverClass))
	}
	if o.CoprocID == "" {
		errs = append(errs, "--coproc-id is required")
	}
	if o.OutJarPath == "" {
		errs = append(errs, "--out is required")
	}
	if o.GoBinPath != "" {
		if _, err := os.Stat(o.GoBinPath); err != nil {
			errs = append(errs, fmt.Sprintf("--go-bin %s: %s", o.GoBinPath, err))
		}
	}
	if o.BridgeJarPath != "" {
		if _, err := os.Stat(o.BridgeJarPath); err != nil {
			errs = append(errs, fmt.Sprintf("--bridge-jar %s: %s", o.BridgeJarPath, err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func writeManifest(zw *zip.Writer, opts BuildOptions, shaHex string) error {
	var b bytes.Buffer
	writeManifestLine(&b, "Manifest-Version", "1.0")
	writeManifestLine(&b, "Created-By", "hbasecop-build")
	writeManifestLine(&b, "HbaseCop-Observer-Class", opts.ObserverClass)
	writeManifestLine(&b, "HbaseCop-Coproc-Id", opts.CoprocID)
	writeManifestLine(&b, "HbaseCop-Go-Bin-Name", opts.BinName)
	writeManifestLine(&b, "HbaseCop-Go-Bin-SHA256", shaHex)
	b.WriteString("\r\n")
	return writeStored(zw, "META-INF/MANIFEST.MF", b.Bytes())
}

// writeManifestLine emits one logical "Name: value" record honouring the
// jar-manifest 72-byte line limit: subsequent physical lines start with a
// single space (continuation marker). All terminators are CRLF.
//
// The 72-byte budget includes the line terminator, so we cap payload bytes
// per physical line at 70 (then write CRLF). Continuation lines reserve one
// byte for the leading space.
func writeManifestLine(buf *bytes.Buffer, key, value string) {
	const maxLine = 72
	const term = "\r\n"
	header := key + ": "
	combined := header + value
	if len(combined)+len(term) <= maxLine {
		buf.WriteString(combined)
		buf.WriteString(term)
		return
	}
	// First physical line: as much of combined as fits.
	firstChunk := maxLine - len(term)
	buf.WriteString(combined[:firstChunk])
	buf.WriteString(term)
	remaining := combined[firstChunk:]
	for len(remaining) > 0 {
		chunkSize := min(maxLine-len(term)-1, len(remaining)) // -1 for leading space
		buf.WriteByte(' ')
		buf.WriteString(remaining[:chunkSize])
		buf.WriteString(term)
		remaining = remaining[chunkSize:]
	}
}

func copyBridgeEntries(zw *zip.Writer, br *zip.Reader) error {
	// Deterministic ordering — useful for reproducible builds and stable
	// integration-test asserts.
	names := make([]string, 0, len(br.File))
	for _, f := range br.File {
		names = append(names, f.Name)
	}
	sort.Strings(names)
	byName := make(map[string]*zip.File, len(br.File))
	for _, f := range br.File {
		byName[f.Name] = f
	}

	for _, name := range names {
		if shouldSkipBridgeEntry(name) {
			continue
		}
		f := byName[name]
		if err := copyZipEntry(zw, f); err != nil {
			return fmt.Errorf("copy %s: %w", name, err)
		}
	}
	return nil
}

// shouldSkipBridgeEntry excludes entries the coproc-jar must own:
//   - META-INF/MANIFEST.MF — we write our own first.
//   - bin/** — the bridge bundles a generic runtime ELF (for E2E tests);
//     the user's compiled observer takes its place.
//   - jar signing artefacts — meaningless once we re-shade.
func shouldSkipBridgeEntry(name string) bool {
	switch name {
	case "META-INF/MANIFEST.MF":
		return true
	}
	if strings.HasPrefix(name, "bin/") {
		return true
	}
	upper := strings.ToUpper(name)
	if strings.HasPrefix(upper, "META-INF/") {
		if strings.HasSuffix(upper, ".SF") ||
			strings.HasSuffix(upper, ".DSA") ||
			strings.HasSuffix(upper, ".RSA") {
			return true
		}
	}
	return false
}

func copyZipEntry(zw *zip.Writer, f *zip.File) error {
	r, err := f.Open()
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	// Use the same compression method the source used — that keeps already-
	// stored entries (e.g. classpath dir markers) stored.
	return writeWith(zw, f.Name, body, f.Method)
}

func writeWith(zw *zip.Writer, name string, body []byte, method uint16) error {
	// Preserve the entry name VERBATIM. zip/jar names use forward slashes
	// and a trailing slash marks a directory entry; path.Clean would strip
	// that slash, demoting "META-INF/" → "META-INF" (a zero-length file
	// colliding with its own directory) and producing a jar that `jar x`,
	// unzip and the JVM classloader cannot extract.
	hdr := &zip.FileHeader{
		Name:   name,
		Method: method,
	}
	if strings.HasSuffix(name, "/") {
		hdr.SetMode(fs.ModeDir | 0o755)
	} else {
		hdr.SetMode(0o644)
	}
	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

func writeStored(zw *zip.Writer, name string, body []byte) error {
	return writeWith(zw, name, body, zip.Deflate)
}
