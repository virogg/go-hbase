// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type configKey struct {
	name     string
	kind     string
	def      string
	accepted string
}

var configKeys = []configKey{
	{"hbasecop.policy.<hook>", "prefix-policy", "pre*=strict, post*=best-effort", "strict | best-effort"},
	{"hbasecop.timeout.<hook>", "prefix-duration", "(falls back to timeout.default)", "duration with unit, e.g. 500ms, 2s"},
	{"hbasecop.timeout.default", "duration", "5s", "duration with unit"},
	{"hbasecop.heartbeat.period", "duration", "500ms", "duration with unit"},
	{"hbasecop.heartbeat.miss-threshold", "posint", "3", "positive integer"},
	{"hbasecop.restart.initial-delay", "duration", "200ms", "duration with unit"},
	{"hbasecop.restart.max-delay", "duration", "5s", "duration with unit"},
	{"hbasecop.restart.max-fails", "posint", "5", "positive integer"},
	{"hbasecop.restart.probe-interval", "duration", "30s", "duration with unit"},
	{"hbasecop.restart.deadline", "duration", "3s", "duration with unit"},
	{"hbasecop.ring.capacity", "posint", "16", "positive integer"},
	{"hbasecop.ring.max-object-size", "posint", "1048576", "positive integer (bytes)"},
	{"hbasecop.shutdown.graceful-timeout", "duration", "2s", "duration with unit"},
	{"hbasecop.endpoint.timeout", "duration", "30s", "duration with unit"},
	{"hbasecop.endpoint.servicing-pool-size", "posint", "8", "positive integer"},
	{"hbasecop.endpoint.servicing-queue-depth", "posint", "64", "positive integer"},
	{"hbasecop.endpoint.servicing-timeout", "duration", "30s", "duration with unit"},
	{"hbasecop.endpoint.bulk-ring.capacity", "posint", "(falls back to ring.capacity)", "positive integer"},
	{"hbasecop.endpoint.bulk-ring.max-object-size", "posint", "(falls back to ring.max-object-size)", "positive integer (bytes)"},
	{"hbasecop.endpoint.allow-mutate", "bool", "false", "true | false"},
	{"hbasecop.endpoint.max-concurrent-calls", "posint", "8", "positive integer"},
	{"hbasecop.endpoint.max-scanners-per-call", "posint", "16", "positive integer"},
	{"hbasecop.endpoint.max-bytes-per-resp", "posint", "1048576", "positive integer (bytes)"},
	{"hbasecop.endpoint.max-rows-per-next", "posint", "1000", "positive integer"},
	{"hbasecop.endpoint.scanner-idle-lease", "duration", "2m", "duration with unit"},
}

var durationRe = regexp.MustCompile(`^\d+\s*(ns|us|ms|s|m|h|d)$`)

var knownHooks = func() map[string]bool {
	names := hbasecop.HookNames()
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[lowerFirst(n)] = true
	}
	return m
}()

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func runConfig(args []string) error {
	fs := flag.NewFlagSet("hbasecop-build config", flag.ContinueOnError)
	list := fs.Bool("list", false, "print every hbasecop.* config key with default and accepted values")
	check := fs.String("check", "", "validate an hbase-site.xml; exit non-zero on a malformed hbasecop.* value")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch {
	case *check != "":
		return checkSiteFile(*check)
	case *list:
		printConfigKeys()
		return nil
	default:
		return fmt.Errorf("config: pass --list or --check <hbase-site.xml>")
	}
}

func printConfigKeys() {
	fmt.Printf("%-38s %-26s %s\n", "KEY", "DEFAULT", "ACCEPTED")
	for _, k := range configKeys {
		fmt.Printf("%-38s %-26s %s\n", k.name, k.def, k.accepted)
	}
}

type hadoopSite struct {
	Properties []struct {
		Name  string `xml:"name"`
		Value string `xml:"value"`
	} `xml:"property"`
}

func checkSiteFile(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var site hadoopSite
	if err := xml.Unmarshal(raw, &site); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	var bad, notices []string
	for _, p := range site.Properties {
		if !strings.HasPrefix(p.Name, "hbasecop.") {
			continue
		}
		if msg := validateKey(p.Name, p.Value); msg != "" {
			bad = append(bad, msg)
		}
		if hook := unknownHookSuffix(p.Name); hook != "" {
			notices = append(notices, fmt.Sprintf("%s names unknown hook %q (ignored at runtime)", p.Name, hook))
		} else if !knownKey(p.Name) {
			notices = append(notices, fmt.Sprintf("unknown key %s (ignored at runtime)", p.Name))
		}
	}
	sort.Strings(notices)
	for _, n := range notices {
		fmt.Println("notice:", n)
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		for _, b := range bad {
			fmt.Fprintln(os.Stderr, "error:", b)
		}
		return fmt.Errorf("%d malformed hbasecop.* value(s)", len(bad))
	}
	fmt.Printf("ok: %s has no malformed hbasecop.* values\n", path)
	return nil
}

func validateKey(name, value string) string {
	switch {
	case strings.HasPrefix(name, "hbasecop.policy."):
		if value != "strict" && value != "best-effort" {
			return name + "=" + value + " (want strict|best-effort)"
		}
	case name == "hbasecop.timeout.default", strings.HasPrefix(name, "hbasecop.timeout."):
		if !durationRe.MatchString(strings.TrimSpace(value)) {
			return name + "=" + value + " (want a duration with a unit, e.g. 500ms)"
		}
	default:
		for _, k := range configKeys {
			if k.name != name {
				continue
			}
			switch k.kind {
			case "duration":
				if !durationRe.MatchString(strings.TrimSpace(value)) {
					return name + "=" + value + " (want a duration with a unit)"
				}
			case "posint":
				if n, err := strconv.Atoi(strings.TrimSpace(value)); err != nil || n <= 0 {
					return name + "=" + value + " (want a positive integer)"
				}
			case "bool":
				if v := strings.TrimSpace(value); !strings.EqualFold(v, "true") && !strings.EqualFold(v, "false") {
					return name + "=" + value + " (want true|false)"
				}
			}
		}
	}
	return ""
}

func knownKey(name string) bool {
	if strings.HasPrefix(name, "hbasecop.policy.") || strings.HasPrefix(name, "hbasecop.timeout.") {
		return true
	}
	for _, k := range configKeys {
		if k.name == name {
			return true
		}
	}
	return false
}

func unknownHookSuffix(name string) string {
	for _, prefix := range []string{"hbasecop.policy.", "hbasecop.timeout."} {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if name == "hbasecop.timeout.default" {
			return ""
		}
		hook := name[len(prefix):]
		if hook != "" && !knownHooks[hook] {
			return hook
		}
		return ""
	}
	return ""
}
