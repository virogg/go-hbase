// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const deployToolClass = "com.virogg.hbasecop.bridge.admin.DeployTool"

// runAdmin registers/removes/lists an hbasecop coprocessor by invoking the Java
// DeployTool through the `hbase` CLI (which supplies the HBase client classpath)
// with the bridge jar on HBASE_CLASSPATH. If `hbase` is not on PATH, it prints
// the ready-to-run command for a host that has HBase installed.
func runAdmin(args []string) error {
	fs := flag.NewFlagSet("hbasecop-build admin", flag.ContinueOnError)
	bridgeJar := fs.String("bridge-jar", "", "bridge jar (default: newest -all jar in ~/.m2)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args() // <deploy|remove|list> [--flags] passed through to DeployTool
	if len(rest) == 0 {
		return fmt.Errorf("usage: hbasecop-build admin [--bridge-jar J] <deploy|remove|list> [--flags]")
	}

	jar, err := resolveBridgeJar(*bridgeJar)
	if err != nil {
		return err
	}

	toolArgs := append([]string{deployToolClass}, rest...)
	if path, lookErr := exec.LookPath("hbase"); lookErr == nil {
		cmd := exec.Command(path, toolArgs...)
		cmd.Env = append(os.Environ(), "HBASE_CLASSPATH="+jar)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		return cmd.Run()
	}

	fmt.Fprintln(os.Stderr, "hbase CLI not on PATH; run this on a host with HBase installed:")
	fmt.Printf("HBASE_CLASSPATH=%s hbase %s\n", jar, strings.Join(toolArgs, " "))
	return nil
}
