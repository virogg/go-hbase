// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * T63 — refcounted lifecycle: first {@link SharedRuntime#acquire} on a key spawns the Go process;
 * subsequent acquires share the existing runtime/channel; the last {@link
 * SharedRuntime.Handle#release} sends SHUTDOWN and waits for the process to exit. No ELF-leak
 * across repeated start/stop cycles.
 */
final class SharedRuntimeTest {

  @Test
  void firstAcquireSpawnsAndIsSharedAcrossHandles(@TempDir Path tmp) throws Exception {
    Path in = tmp.resolve("in.mmap");
    Path out = tmp.resolve("out.mmap");
    String key = "shared-rt-share-" + System.nanoTime();

    SharedRuntime.Handle h1 =
        SharedRuntime.acquire(key, () -> SharedRuntime.Spec.of(baseConfig(in, out)));
    try {
      assertEquals(1, SharedRuntime.refcountForTesting(key));
      assertNotNull(h1.getRegionObserver(), "handle must expose RegionObserver");
      assertTrue(h1.runtimeForTesting().isAlive(), "Go process must be alive after first acquire");
      long pid1 = h1.runtimeForTesting().goProcessPidForTesting();

      SharedRuntime.Handle h2 =
          SharedRuntime.acquire(key, () -> SharedRuntime.Spec.of(baseConfig(in, out)));
      try {
        assertEquals(2, SharedRuntime.refcountForTesting(key));
        assertSame(
            h1.runtimeForTesting(),
            h2.runtimeForTesting(),
            "subsequent acquire on same key must share the same runtime");
        assertEquals(pid1, h2.runtimeForTesting().goProcessPidForTesting());
      } finally {
        h2.release();
      }

      // Releasing h2 must not stop the runtime — h1 still holds a ref.
      assertEquals(1, SharedRuntime.refcountForTesting(key));
      assertTrue(
          h1.runtimeForTesting().isAlive(), "Go process must still be alive on refcount > 0");
    } finally {
      h1.release();
    }

    assertEquals(0, SharedRuntime.refcountForTesting(key));
  }

  @Test
  void fiveStartStopCyclesHaveNoElfLeak(@TempDir Path tmp) throws Exception {
    Path in = tmp.resolve("in.mmap");
    Path out = tmp.resolve("out.mmap");
    String key = "shared-rt-cycles-" + System.nanoTime();

    Path sysTmp = Path.of(System.getProperty("java.io.tmpdir"));
    Set<Path> before = elfFingerprint(sysTmp);

    int[] cleanupCalls = {0};
    for (int i = 0; i < 5; i++) {
      SharedRuntime.Handle h =
          SharedRuntime.acquire(
              key, () -> SharedRuntime.Spec.of(baseConfig(in, out), () -> cleanupCalls[0]++));
      assertTrue(h.runtimeForTesting().isAlive(), "iteration " + i + ": process must be alive");
      h.release();
      assertEquals(
          0, SharedRuntime.refcountForTesting(key), "iteration " + i + ": refcount must drain");
      assertFalse(
          h.runtimeForTesting().isAlive(),
          "iteration " + i + ": Go process must have exited after final release");
    }

    Set<Path> after = elfFingerprint(sysTmp);
    after.removeAll(before);
    assertTrue(
        after.isEmpty(),
        "ELF-leak: extracted runtime binaries left over after 5 start/stop cycles: " + after);
    assertEquals(5, cleanupCalls[0], "Spec.onStop must run once per refcount→0 transition");
  }

  @Test
  void releaseIsIdempotent(@TempDir Path tmp) throws Exception {
    String key = "shared-rt-idem-" + System.nanoTime();
    SharedRuntime.Handle h =
        SharedRuntime.acquire(
            key,
            () ->
                SharedRuntime.Spec.of(baseConfig(tmp.resolve("in.mmap"), tmp.resolve("out.mmap"))));
    h.release();
    h.release(); // must be a no-op, must not double-stop the runtime or NPE
    assertEquals(0, SharedRuntime.refcountForTesting(key));
  }

  @Test
  void acquireFailureDoesNotPoisonRegistry(@TempDir Path tmp) throws Exception {
    String key = "shared-rt-fail-" + System.nanoTime();

    // Supplier throws — the registry entry must not be created, so a subsequent acquire with a
    // good supplier succeeds (no stale half-built entry blocking the key).
    assertThrows(
        IOException.class,
        () ->
            SharedRuntime.acquire(
                key,
                () -> {
                  throw new IOException("synthetic config failure");
                }));
    assertEquals(0, SharedRuntime.refcountForTesting(key));

    SharedRuntime.Handle h =
        SharedRuntime.acquire(
            key,
            () ->
                SharedRuntime.Spec.of(baseConfig(tmp.resolve("in.mmap"), tmp.resolve("out.mmap"))));
    try {
      assertTrue(h.runtimeForTesting().isAlive());
    } finally {
      h.release();
    }
  }

  private static CoprocessorRuntime.Config baseConfig(Path in, Path out) {
    return CoprocessorRuntime.Config.builder()
        .javaToGoFile(in)
        .goToJavaFile(out)
        .ringCapacity(8)
        .ringMaxObjectSize(64 * 1024)
        .heartbeatPeriodMs(-1)
        .hookTimeout(Duration.ofSeconds(2))
        .gracefulShutdownTimeout(Duration.ofSeconds(2))
        .build();
  }

  private static Set<Path> elfFingerprint(Path dir) throws IOException {
    Set<Path> out = new HashSet<>();
    if (!Files.isDirectory(dir)) {
      return out;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "hbasecop-runtime-*")) {
      for (Path p : stream) {
        out.add(p);
      }
    }
    return out;
  }
}
