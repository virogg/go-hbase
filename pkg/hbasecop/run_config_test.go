// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"strings"
	"testing"
	"time"
)

// setEnv sets the shmem env vars the Run* entrypoints read; unset values are
// cleared (t.Setenv restores them after the test).
func setShmemEnv(t *testing.T, in, out, capacity, maxObj, hb string) {
	t.Helper()
	t.Setenv("HBASECOP_SHMEM_IN_PATH", in)
	t.Setenv("HBASECOP_SHMEM_OUT_PATH", out)
	t.Setenv("HBASECOP_RING_CAPACITY", capacity)
	t.Setenv("HBASECOP_RING_MAX_OBJECT_SIZE", maxObj)
	t.Setenv("HBASECOP_HEARTBEAT_MS", hb)
}

func TestLoadShmemConfigFromEnv(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		setShmemEnv(t, "/tmp/in", "/tmp/out", "32", "8192", "250")
		c, err := loadShmemConfigFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.inPath != "/tmp/in" || c.outPath != "/tmp/out" {
			t.Fatalf("paths wrong: %+v", c)
		}
		if c.capacity != 32 || c.maxObjectSize != 8192 {
			t.Fatalf("sizes wrong: %+v", c)
		}
		if c.heartbeat != 250*time.Millisecond {
			t.Fatalf("heartbeat = %v, want 250ms", c.heartbeat)
		}
	})

	t.Run("heartbeat sentinels", func(t *testing.T) {
		setShmemEnv(t, "/tmp/in", "/tmp/out", "16", "4096", "-1")
		c, _ := loadShmemConfigFromEnv()
		if c.heartbeat != -1 {
			t.Fatalf("hb<0 should map to -1 (disabled), got %v", c.heartbeat)
		}
		t.Setenv("HBASECOP_HEARTBEAT_MS", "0")
		c, _ = loadShmemConfigFromEnv()
		if c.heartbeat != 0 {
			t.Fatalf("hb==0 should map to 0 (default), got %v", c.heartbeat)
		}
	})

	t.Run("missing/invalid", func(t *testing.T) {
		cases := []struct {
			name             string
			in, out, c, m, h string
			wantSubstr       string
		}{
			{"no in", "", "/o", "16", "4096", "0", "HBASECOP_SHMEM_IN_PATH"},
			{"no out", "/i", "", "16", "4096", "0", "HBASECOP_SHMEM_OUT_PATH"},
			{"no capacity", "/i", "/o", "", "4096", "0", "HBASECOP_RING_CAPACITY"},
			{"bad capacity", "/i", "/o", "x", "4096", "0", "HBASECOP_RING_CAPACITY"},
			{"no maxobj", "/i", "/o", "16", "", "0", "HBASECOP_RING_MAX_OBJECT_SIZE"},
			{"bad maxobj", "/i", "/o", "16", "x", "0", "HBASECOP_RING_MAX_OBJECT_SIZE"},
			{"bad heartbeat", "/i", "/o", "16", "4096", "abc", "HBASECOP_HEARTBEAT_MS"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				setShmemEnv(t, tc.in, tc.out, tc.c, tc.m, tc.h)
				_, err := loadShmemConfigFromEnv()
				if err == nil {
					t.Fatalf("expected error mentioning %s", tc.wantSubstr)
				}
				if !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("error %q does not mention %q", err.Error(), tc.wantSubstr)
				}
			})
		}
	})
}

func TestEnvIntHelpers(t *testing.T) {
	t.Setenv("HBASECOP_X", "")
	if _, err := mustEnvInt("HBASECOP_X"); err == nil {
		t.Fatal("mustEnvInt on empty should error")
	}
	if v, err := optionalEnvInt("HBASECOP_X", 7); err != nil || v != 7 {
		t.Fatalf("optionalEnvInt default: v=%d err=%v, want 7/nil", v, err)
	}
	t.Setenv("HBASECOP_X", "notanint")
	if _, err := optionalEnvInt("HBASECOP_X", 7); err == nil {
		t.Fatal("optionalEnvInt on non-int should error")
	}
	t.Setenv("HBASECOP_X", "42")
	if v, err := mustEnvInt("HBASECOP_X"); err != nil || v != 42 {
		t.Fatalf("mustEnvInt: v=%d err=%v, want 42/nil", v, err)
	}
}

// TestRunSurfaceGuards covers the argument-validation guards at the top of
// every Run* entrypoint without standing up a shmem channel.
func TestRunSurfaceGuards(t *testing.T) {
	region := UnimplementedRegionObserver{}
	master := UnimplementedMasterObserver{}
	rs := UnimplementedRegionServerObserver{}
	wal := UnimplementedWALObserver{}
	bulk := UnimplementedBulkLoadObserver{}

	guards := []struct {
		name string
		none func() error
		dup  func() error
	}{
		{"Run", func() error { return Run() }, func() error { return Run(region, region) }},
		{"RunMaster", func() error { return RunMaster() }, func() error { return RunMaster(master, master) }},
		{"RunRegionServer", func() error { return RunRegionServer() }, func() error { return RunRegionServer(rs, rs) }},
		{"RunWAL", func() error { return RunWAL() }, func() error { return RunWAL(wal, wal) }},
		{"RunBulkLoad", func() error { return RunBulkLoad() }, func() error { return RunBulkLoad(bulk, bulk) }},
	}
	for _, g := range guards {
		t.Run(g.name, func(t *testing.T) {
			if err := g.none(); err == nil {
				t.Errorf("%s() with no observers: expected error", g.name)
			}
			if err := g.dup(); err == nil {
				t.Errorf("%s() with multiple observers: expected error", g.name)
			}
		})
	}
}

// TestRunSurfaceConfigError drives each Run* past its guards into
// loadShmemConfigFromEnv with a missing env, exercising the config-load
// failure path of every surface entrypoint.
func TestRunSurfaceConfigError(t *testing.T) {
	t.Setenv("HBASECOP_SHMEM_IN_PATH", "") // force loadShmemConfigFromEnv to fail
	runs := []struct {
		name string
		fn   func() error
	}{
		{"RunMaster", func() error { return RunMaster(UnimplementedMasterObserver{}) }},
		{"RunRegionServer", func() error { return RunRegionServer(UnimplementedRegionServerObserver{}) }},
		{"RunWAL", func() error { return RunWAL(UnimplementedWALObserver{}) }},
		{"RunBulkLoad", func() error { return RunBulkLoad(UnimplementedBulkLoadObserver{}) }},
	}
	for _, r := range runs {
		t.Run(r.name, func(t *testing.T) {
			if err := r.fn(); err == nil {
				t.Errorf("%s should fail when shmem env is missing", r.name)
			}
		})
	}
}
