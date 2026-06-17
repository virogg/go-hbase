// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Command hbasecop-build packages a compiled Go observer ELF together with
// the shaded hbasecop-bridge into a deployable HBase coprocessor jar.
//
// Replaces the hand-maintained Maven shade flow from T25 (see
// examples/counter-observer/pom.xml). Writes a manifest carrying:
//
//	HbaseCop-Observer-Class   fully-qualified Java observer
//	HbaseCop-Coproc-Id        operator-chosen coprocessor id
//	HbaseCop-Go-Bin-Name      jar-internal ELF path
//	HbaseCop-Go-Bin-SHA256    digest of the ELF (supervisor validates at extract)
//
// Usage:
//
//	hbasecop-build \
//	  --go-bin       path/to/myobserver \
//	  --bridge-jar   path/to/hbasecop-bridge-0.0.1-SNAPSHOT.jar \
//	  --observer-class com.example.Audit \
//	  --coproc-id    audit-observer \
//	  --out          audit-observer.jar \
//	  [--policy-config path/to/policy.properties] \
//	  [--bin-name    bin/linux-amd64/custom]
package main

import (
	"flag"
	"fmt"
	"os"
)

// subcommands maps the first CLI arg to its handler. Each handler receives the
// args following the subcommand and returns a non-nil error to exit non-zero.
var subcommands = map[string]func([]string) error{
	"package": runPackage,
	"config":  runConfig,
	"admin":   runAdmin,
	"init":    runInit,
}

func main() {
	// Subcommands: `package` is the one-shot pipeline (cross-compile + stock
	// delegate + shade), `config`/`admin`/`init` the DX helpers. Bare flags (no
	// recognized subcommand) stay the low-level packer for back-compat.
	if len(os.Args) > 1 {
		if run, ok := subcommands[os.Args[1]]; ok {
			if err := run(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "hbasecop-build %s: %v\n", os.Args[1], err)
				os.Exit(1)
			}
			return
		}
	}

	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "hbasecop-build:", err)
		os.Exit(2)
	}
	if err := Build(opts); err != nil {
		fmt.Fprintln(os.Stderr, "hbasecop-build:", err)
		os.Exit(1)
	}
	fmt.Printf("hbasecop-build: wrote %s\n", opts.OutJarPath)
}

func parseFlags(args []string) (BuildOptions, error) {
	fs := flag.NewFlagSet("hbasecop-build", flag.ContinueOnError)
	var opts BuildOptions
	fs.StringVar(&opts.GoBinPath, "go-bin", "", "path to the compiled Go observer ELF")
	fs.StringVar(&opts.BridgeJarPath, "bridge-jar", "", "path to the shaded hbasecop-bridge jar")
	fs.StringVar(&opts.ObserverClass, "observer-class", "",
		"fully-qualified Java observer class (e.g. com.example.Audit)")
	fs.StringVar(&opts.CoprocID, "coproc-id", "",
		"operator-chosen coprocessor id (e.g. audit-observer)")
	fs.StringVar(&opts.OutJarPath, "out", "", "output coproc-jar path")
	fs.StringVar(&opts.PolicyConfig, "policy-config", "",
		"optional .properties file copied to META-INF/hbasecop-policy.properties")
	fs.StringVar(&opts.BinName, "bin-name", "",
		"override for the ELF's jar resource path (default: bin/linux-amd64/<basename>)")
	if err := fs.Parse(args); err != nil {
		return BuildOptions{}, err
	}
	if fs.NArg() > 0 {
		return BuildOptions{}, fmt.Errorf("unexpected positional args: %v", fs.Args())
	}
	return opts, nil
}
