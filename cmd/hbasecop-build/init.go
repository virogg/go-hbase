// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// surfaceScaffold maps a --surface to the scaffold pieces: an optional top-level
// declaration (decl), the in-main construction snippet (body), the Run*
// entrypoint (run), whether the body/decl uses context, and any extra stdlib
// imports beyond log/slog + os.
var surfaceScaffold = map[string]struct {
	decl, body, run string
	ctx             bool
	extraImports    []string
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
	// An endpoint is a client-initiated server RPC, not an event observer; it is
	// hosted by the stock GenericRegionObserver, so RunAll registers it alongside
	// a no-op region observer (a region coprocessor needs an observer to attach).
	"endpoint": {
		decl: `// endpoint is a server-side endpoint coprocessor: a client invokes it via the
// generic GoEndpointService and the bridge dispatches the call here. Replace the
// body with your logic; use env to read/scan/mutate the invoking region.
type endpoint struct{}

func (endpoint) Call(_ context.Context, env *hbasecop.EndpointEnv, method string, payload []byte) ([]byte, error) {
	slog.Info("endpoint call", "method", method, "bytes", len(payload))
	return bytes.ToUpper(payload), nil
}`,
		run:          "hbasecop.RunAll(hbasecop.UnimplementedRegionObserver{}, endpoint{})",
		ctx:          true,
		extraImports: []string{"bytes"},
	},
}

// embedded returns a skeleton that embeds the no-op observer for surfaces
// without a builder; the author overrides the hooks they need.
func embedded(surface string) string {
	return fmt.Sprintf(`var obs struct {
		hbasecop.Unimplemented%sObserver // embed defaults; override the hooks you need
	}`, surface)
}

// runInit scaffolds a buildable Go observer/endpoint (main.go + README) for the surface.
func runInit(args []string) error {
	fs := flag.NewFlagSet("hbasecop-build init", flag.ContinueOnError)
	surface := fs.String("surface", "region", "surface: region|master|regionserver|wal|bulkload|endpoint")
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
		return fmt.Errorf("--surface %q invalid; want region|master|regionserver|wal|bulkload|endpoint", *surface)
	}
	out := *dir
	if out == "" {
		out = name
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}

	// Assemble the import set: log/slog + os always, context if used, plus any extras, sorted.
	std := []string{"log/slog", "os"}
	if sc.ctx {
		std = append(std, "context")
	}
	std = append(std, sc.extraImports...)
	sort.Strings(std)
	var imports strings.Builder
	for _, s := range std {
		fmt.Fprintf(&imports, "\t%q\n", s)
	}

	declBlock := ""
	if sc.decl != "" {
		declBlock = "\n" + sc.decl + "\n"
	}
	bodyLine := ""
	if sc.body != "" {
		bodyLine = "\t" + sc.body + "\n"
	}

	main := fmt.Sprintf(`// Command %s is a hbasecop %s scaffold.
package main

import (
%s
	"github.com/virogg/go-hbase/pkg/hbasecop"
)
%s
func main() {
%s	if err := %s; err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
`, name, surfaceLabel(*surface), imports.String(), declBlock, bodyLine, sc.run)

	// An endpoint is hosted by GenericRegionObserver, so it packages with --surface region.
	pkgSurface := *surface
	if pkgSurface == "endpoint" {
		pkgSurface = "region"
	}
	readme := fmt.Sprintf("# %s\n\nhbasecop %s.\n\n"+
		"```bash\n"+
		"# build + package (cross-compiles, embeds the stock %s delegate)\n"+
		"hbasecop-build package --src . --surface %s --out %s.jar\n\n"+
		"# deploy onto an existing table\n"+
		"hbasecop-build admin deploy --table T \\\n"+
		"  --jar file:///coproc-jars/%s.jar \\\n"+
		"  --class %s\n"+
		"```\n", name, surfaceLabel(*surface), pkgSurface, pkgSurface, name, name, delegateFor(pkgSurface))

	if err := writeNew(filepath.Join(out, "main.go"), main); err != nil {
		return err
	}
	if err := writeNew(filepath.Join(out, "README.md"), readme); err != nil {
		return err
	}
	fmt.Printf("hbasecop-build: scaffolded %s in %s/\n", surfaceLabel(*surface), out)
	return nil
}

// surfaceLabel is the human description used in the scaffold comment and README.
func surfaceLabel(surface string) string {
	if surface == "endpoint" {
		return "endpoint coprocessor"
	}
	return surface + " observer"
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
