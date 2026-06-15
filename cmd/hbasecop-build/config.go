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
)

// configKey documents one hbasecop.* setting for `config --list` and drives
// `config --check`. kind is one of: duration, posint, policy, prefix-duration,
// prefix-policy. The validation rules mirror the Java ConfigPreflight.
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
}

var durationRe = regexp.MustCompile(`^\d+\s*(ns|us|ms|s|m|h|d)$`)

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

// checkSiteFile validates hbasecop.* values in an hbase-site.xml against the
// same rules as the Java startup preflight. Malformed values are errors;
// unknown hbasecop.* keys are reported as notices.
func checkSiteFile(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var site hadoopSite
	if err := xml.Unmarshal(raw, &site); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	var bad, unknown []string
	for _, p := range site.Properties {
		if !strings.HasPrefix(p.Name, "hbasecop.") {
			continue
		}
		if msg := validateKey(p.Name, p.Value); msg != "" {
			bad = append(bad, msg)
		} else if !knownKey(p.Name) {
			unknown = append(unknown, p.Name)
		}
	}
	sort.Strings(unknown)
	for _, u := range unknown {
		fmt.Printf("notice: unknown key %s (ignored at runtime)\n", u)
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

// validateKey returns "" if name/value is well-formed, else a message. Unknown
// keys are not malformed (handled separately as notices).
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
