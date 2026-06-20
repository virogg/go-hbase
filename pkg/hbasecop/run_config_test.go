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

	t.Run("bulk ring absent disables reverse path", func(t *testing.T) {
		setShmemEnv(t, "/tmp/in", "/tmp/out", "16", "4096", "0")
		t.Setenv("HBASECOP_SHMEM_BULK_PATH", "")
		c, err := loadShmemConfigFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.bulkPath != "" {
			t.Fatalf("bulkPath = %q, want empty (reverse path off)", c.bulkPath)
		}
	})

	t.Run("bulk ring defaults to control ring sizes", func(t *testing.T) {
		setShmemEnv(t, "/tmp/in", "/tmp/out", "16", "4096", "0")
		t.Setenv("HBASECOP_SHMEM_BULK_PATH", "/tmp/bulk")
		t.Setenv("HBASECOP_BULK_RING_CAPACITY", "")
		t.Setenv("HBASECOP_BULK_RING_MAX_OBJECT_SIZE", "")
		c, err := loadShmemConfigFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.bulkPath != "/tmp/bulk" {
			t.Fatalf("bulkPath = %q, want /tmp/bulk", c.bulkPath)
		}
		if c.bulkCapacity != 16 || c.bulkMaxObjectSize != 4096 {
			t.Fatalf("bulk sizes = %d/%d, want 16/4096 (control-ring fallback)", c.bulkCapacity, c.bulkMaxObjectSize)
		}
	})

	t.Run("bulk ring explicit sizes", func(t *testing.T) {
		setShmemEnv(t, "/tmp/in", "/tmp/out", "16", "4096", "0")
		t.Setenv("HBASECOP_SHMEM_BULK_PATH", "/tmp/bulk")
		t.Setenv("HBASECOP_BULK_RING_CAPACITY", "8")
		t.Setenv("HBASECOP_BULK_RING_MAX_OBJECT_SIZE", "1048576")
		c, err := loadShmemConfigFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.bulkCapacity != 8 || c.bulkMaxObjectSize != 1048576 {
			t.Fatalf("bulk sizes = %d/%d, want 8/1048576", c.bulkCapacity, c.bulkMaxObjectSize)
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

// TestRunSurfaceGuards covers the no-observer guard at the top of every Run*
// entrypoint without standing up a shmem channel. Multiple observers are now
// valid (chained); see TestDispatchRegionChain.
func TestRunSurfaceGuards(t *testing.T) {
	guards := []struct {
		name string
		none func() error
	}{
		{"Run", func() error { return Run() }},
		{"RunMaster", func() error { return RunMaster() }},
		{"RunRegionServer", func() error { return RunRegionServer() }},
		{"RunWAL", func() error { return RunWAL() }},
		{"RunBulkLoad", func() error { return RunBulkLoad() }},
		{"RunAll", func() error { return RunAll() }},
	}
	for _, g := range guards {
		t.Run(g.name, func(t *testing.T) {
			if err := g.none(); err == nil {
				t.Errorf("%s() with no observers: expected error", g.name)
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
