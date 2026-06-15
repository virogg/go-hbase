// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"log/slog"
	"testing"
)

// TestLogLevelFromEnv pins SPEC §6 / R-STYLE-6: the slog level is taken
// from HBASECOP_LOG_LEVEL. Regression for the defect where every Run*
// built slog.NewJSONHandler(os.Stderr, nil) and the env var was never
// read, so the level was permanently Info.
func TestLogLevelFromEnv(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
	}{
		{"", slog.LevelInfo},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"debug", slog.LevelDebug},
		{"  Debug  ", slog.LevelDebug},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
		{"nonsense", slog.LevelInfo},
	}
	for _, c := range cases {
		t.Setenv("HBASECOP_LOG_LEVEL", c.in)
		if got := logLevelFromEnv(); got != c.want {
			t.Errorf("logLevelFromEnv(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestNewLoggerHonorsLevel proves the level actually reaches the handler:
// with HBASECOP_LOG_LEVEL=debug a debug record is enabled, and at the
// default (info) it is not. Before the fix the env had no effect and
// debug was never enabled.
func TestNewLoggerHonorsLevel(t *testing.T) {
	ctx := context.Background()

	t.Setenv("HBASECOP_LOG_LEVEL", "debug")
	if !newLogger().Handler().Enabled(ctx, slog.LevelDebug) {
		t.Fatal("debug not enabled when HBASECOP_LOG_LEVEL=debug")
	}

	t.Setenv("HBASECOP_LOG_LEVEL", "")
	if newLogger().Handler().Enabled(ctx, slog.LevelDebug) {
		t.Fatal("debug enabled at default level - HBASECOP_LOG_LEVEL not applied")
	}
	if !newLogger().Handler().Enabled(ctx, slog.LevelInfo) {
		t.Fatal("info not enabled at default level")
	}
}
