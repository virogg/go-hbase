// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// surfaceScaffold maps a --surface to the observer construction snippet, the
// Run* entrypoint, and whether the body uses context (drives the import set).
var surfaceScaffold = map[string]struct {
	body, run string
	ctx       bool
}{
	"region": {
		body: `obs := hbasecop.NewRegion().
		OnPrePut(func(_ context.Context, env hbasecop.ObserverEnv, _ *hbasecop.MutationProto) (hbasecop.HookResult, error) {
			slog.Info("prePut", "table", env.TableName)
			return hbasecop.HookResult{}, nil
		})`,
		run: "hbasecop.Run(obs)",
		ctx: true,
	},
	"master":       {body: embedded("Master"), run: "hbasecop.RunMaster(obs)"},
	"regionserver": {body: embedded("RegionServer"), run: "hbasecop.RunRegionServer(obs)"},
	"wal":          {body: embedded("WAL"), run: "hbasecop.RunWAL(obs)"},
	"bulkload":     {body: embedded("BulkLoad"), run: "hbasecop.RunBulkLoad(obs)"},
}

// embedded returns a skeleton that embeds the no-op observer for surfaces
// without a builder; the author overrides the hooks they need.
func embedded(surface string) string {
	return fmt.Sprintf(`var obs struct {
		hbasecop.Unimplemented%sObserver // embed defaults; override the hooks you need
	}`, surface)
}

// runInit scaffolds a buildable Go observer (main.go + README) for the surface.
func runInit(args []string) error {
	fs := flag.NewFlagSet("hbasecop-build init", flag.ContinueOnError)
	surface := fs.String("surface", "region", "observer surface: region|master|regionserver|wal|bulkload")
	dir := fs.String("dir", "", "output dir (default: ./<name>)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: hbasecop-build init [--surface S] [--dir D] <name>")
	}
	name := fs.Arg(0)
	sc, ok := surfaceScaffold[*surface]
	if !ok {
		return fmt.Errorf("--surface %q invalid; want region|master|regionserver|wal|bulkload", *surface)
	}
	out := *dir
	if out == "" {
		out = name
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}

	ctxImport := ""
	if sc.ctx {
		ctxImport = "\t\"context\"\n"
	}
	main := fmt.Sprintf(`// Command %s is a hbasecop observer scaffold.
package main

import (
%s	"log/slog"
	"os"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

func main() {
	%s
	if err := %s; err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
`, name, ctxImport, sc.body, sc.run)

	readme := fmt.Sprintf("# %s\n\nhbasecop %s observer.\n\n"+
		"```bash\n"+
		"# build + package (cross-compiles, embeds the stock %s delegate)\n"+
		"hbasecop-build package --src . --surface %s --out %s.jar\n\n"+
		"# deploy onto an existing table\n"+
		"hbasecop-build admin deploy --table T \\\n"+
		"  --jar file:///coproc-jars/%s.jar \\\n"+
		"  --class %s\n"+
		"```\n", name, *surface, *surface, *surface, name, name, delegateFor(*surface))

	if err := writeNew(filepath.Join(out, "main.go"), main); err != nil {
		return err
	}
	if err := writeNew(filepath.Join(out, "README.md"), readme); err != nil {
		return err
	}
	fmt.Printf("hbasecop-build: scaffolded %s observer in %s/\n", *surface, out)
	return nil
}

func delegateFor(surface string) string {
	cls := map[string]string{
		"region":       "GenericRegionObserver",
		"master":       "GenericMasterObserver",
		"regionserver": "GenericRegionServerObserver",
		"wal":          "GenericWALObserver",
	}[surface]
	if cls == "" {
		cls = "GenericRegionObserver"
	}
	return "com.virogg.hbasecop.bridge.entrypoint." + cls
}

// writeNew writes content, refusing to clobber an existing file.
func writeNew(path, content string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s already exists", path)
	}
	return os.WriteFile(path, []byte(content), 0o644)
}
