// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// surfaceDelegate maps a --surface value to the stock generic entrypoint class
// shipped in the bridge jar, so authors deploy without hand-writing Java.
var surfaceDelegate = map[string]string{
	"region":       "com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver",
	"master":       "com.virogg.hbasecop.bridge.entrypoint.GenericMasterObserver",
	"regionserver": "com.virogg.hbasecop.bridge.entrypoint.GenericRegionServerObserver",
	"wal":          "com.virogg.hbasecop.bridge.entrypoint.GenericWALObserver",
}

type packageOptions struct {
	SrcPath       string
	Surface       string
	OutJarPath    string
	BridgeJarPath string // optional; resolved from ~/.m2 when empty
	CoprocID      string // optional; defaults to the output basename
	PolicyConfig  string
}

// runPackage is the one-shot pipeline: cross-compile the Go observer for
// linux/amd64, pick the stock delegate for the surface, resolve the bridge jar,
// then shade+embed+SHA-256 via Build. No Java, no hand-pasted ~/.m2 path.
func runPackage(args []string) error {
	opts, err := parsePackageFlags(args)
	if err != nil {
		return err
	}
	delegate, ok := surfaceDelegate[opts.Surface]
	if !ok {
		return fmt.Errorf("--surface %q invalid; want one of region|master|regionserver|wal", opts.Surface)
	}

	bridgeJar, err := resolveBridgeJar(opts.BridgeJarPath)
	if err != nil {
		return err
	}

	tmpDir, err := os.MkdirTemp("", "hbasecop-package-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()
	elfPath := filepath.Join(tmpDir, "hbasecop-runtime")

	if err := crossCompile(opts.SrcPath, elfPath); err != nil {
		return fmt.Errorf("cross-compile %s: %w", opts.SrcPath, err)
	}
	if err := checkLinuxAmd64ELF(elfPath); err != nil {
		return err
	}

	coprocID := opts.CoprocID
	if coprocID == "" {
		coprocID = defaultCoprocID(opts.OutJarPath)
	}

	return Build(BuildOptions{
		GoBinPath:     elfPath,
		BridgeJarPath: bridgeJar,
		ObserverClass: delegate,
		CoprocID:      coprocID,
		OutJarPath:    opts.OutJarPath,
		PolicyConfig:  opts.PolicyConfig,
		BinName:       "bin/linux-amd64/hbasecop-runtime",
	})
}

func parsePackageFlags(args []string) (packageOptions, error) {
	fs := flag.NewFlagSet("hbasecop-build package", flag.ContinueOnError)
	var o packageOptions
	fs.StringVar(&o.SrcPath, "src", "", "Go package path of the observer main (required)")
	fs.StringVar(&o.Surface, "surface", "region", "observer surface: region|master|regionserver|wal")
	fs.StringVar(&o.OutJarPath, "out", "", "output coproc-jar path (required)")
	fs.StringVar(&o.BridgeJarPath, "bridge-jar", "", "bridge jar (default: newest in ~/.m2)")
	fs.StringVar(&o.CoprocID, "coproc-id", "", "coprocessor id (default: output basename)")
	fs.StringVar(&o.PolicyConfig, "policy-config", "", "optional .properties file")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if o.SrcPath == "" {
		return o, errors.New("--src is required")
	}
	if o.OutJarPath == "" {
		return o, errors.New("--out is required")
	}
	return o, nil
}

func crossCompile(srcPath, outELF string) error {
	cmd := exec.Command("go", "build", "-o", outELF, srcPath)
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// checkLinuxAmd64ELF fails fast if path is not a 64-bit little-endian x86-64
// ELF, catching a host-arch binary before it is embedded and rejected at
// RegionServer start.
func checkLinuxAmd64ELF(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var h [20]byte
	if _, err := f.Read(h[:]); err != nil {
		return fmt.Errorf("read ELF header %s: %w", path, err)
	}
	if h[0] != 0x7f || h[1] != 'E' || h[2] != 'L' || h[3] != 'F' {
		return fmt.Errorf("%s: not an ELF binary (build with GOOS=linux GOARCH=amd64)", path)
	}
	if h[4] != 2 { // EI_CLASS: 2 = 64-bit
		return fmt.Errorf("%s: not a 64-bit ELF", path)
	}
	if machine := uint16(h[18]) | uint16(h[19])<<8; machine != 0x3e { // EM_X86_64
		return fmt.Errorf("%s: ELF machine 0x%x, want x86-64 (0x3e)", path, machine)
	}
	return nil
}

// resolveBridgeJar returns flagVal when set, else the newest installed bridge
// jar under the local Maven repository.
func resolveBridgeJar(flagVal string) (string, error) {
	if flagVal != "" {
		return flagVal, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	// Prefer the uber jar (classifier "all"): the thin module jar lacks the
	// runtime deps (protobuf, jgshmem) and yields a coproc-jar that fails to load.
	glob := filepath.Join(home, ".m2", "repository", "com", "virogg", "hbasecop-bridge", "*", "hbasecop-bridge-*-all.jar")
	matches, err := filepath.Glob(glob)
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no uber bridge jar (hbasecop-bridge-*-all.jar) under %s; run `mvn install` or pass --bridge-jar", filepath.Dir(filepath.Dir(glob)))
	}
	sort.Slice(matches, func(i, j int) bool {
		fi, _ := os.Stat(matches[i])
		fj, _ := os.Stat(matches[j])
		return fi.ModTime().After(fj.ModTime())
	})
	return matches[0], nil
}

func defaultCoprocID(outJarPath string) string {
	return strings.TrimSuffix(filepath.Base(outJarPath), ".jar")
}
