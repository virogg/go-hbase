// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.supervisor.RestartConfig;
import com.virogg.hbasecop.bridge.supervisor.RestartController;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Lifecycle plumbing for the in-RegionServer coproc bridge: spawn the Go ELF, open shmem rings,
 * stand up a Multiplexer-backed {@link com.virogg.hbasecop.bridge.observer.HookDispatcher}, and
 * expose a {@link RegionObserver} that the host coproc can delegate to.
 *
 * <p>This test verifies the wiring end-to-end using the default {@code hbasecop-runtime} ELF (built
 * by {@code make go-build-runtime}). It does <em>not</em> drive hooks — that's covered by {@code
 * RegionObserverAdapterTest} with a mocked dispatcher and by the {@code PrePutCounterIT}
 * integration test against live HBase.
 */
final class CoprocessorRuntimeTest {

  @Test
  void startStopRoundTrip(@TempDir Path tmp) throws Exception {
    Path inFile = tmp.resolve("in.mmap");
    Path outFile = tmp.resolve("out.mmap");

    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(inFile)
            .goToJavaFile(outFile)
            .ringCapacity(8)
            .ringMaxObjectSize(64 * 1024)
            .heartbeatPeriodMs(-1) // disable heartbeats in test
            .hookTimeout(Duration.ofSeconds(2))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
            .build();

    try (CoprocessorRuntime rt = new CoprocessorRuntime(cfg)) {
      rt.start();

      RegionObserver observer = rt.getRegionObserver();
      assertNotNull(observer, "runtime must expose a RegionObserver after start");
      assertTrue(rt.isAlive(), "Go process must be alive after start");
      assertTrue(Files.exists(inFile), "Java->Go mmap file must exist");
      assertTrue(Files.exists(outFile), "Go->Java mmap file must exist");

      rt.stop();

      assertFalse(rt.isAlive(), "Go process must have exited after stop");
      assertFalse(hasReaderThreadAlive(rt), "reader thread must be joined after stop");
    }
  }

  @Test
  void startIsIdempotentRefusal() throws Exception {
    Path tmp = Files.createTempDirectory("hbasecop-rt-double-start-");
    try {
      Path inFile = tmp.resolve("in.mmap");
      Path outFile = tmp.resolve("out.mmap");
      CoprocessorRuntime.Config cfg =
          CoprocessorRuntime.Config.builder()
              .javaToGoFile(inFile)
              .goToJavaFile(outFile)
              .ringCapacity(4)
              .ringMaxObjectSize(8 * 1024)
              .heartbeatPeriodMs(-1)
              .hookTimeout(Duration.ofSeconds(1))
              .gracefulShutdownTimeout(Duration.ofSeconds(2))
              .build();

      try (CoprocessorRuntime rt = new CoprocessorRuntime(cfg)) {
        rt.start();
        try {
          rt.start();
          throw new AssertionError("second start() must throw");
        } catch (IllegalStateException expected) {
          // ok
        } finally {
          rt.stop();
        }
      }
    } finally {
      deleteRecursive(tmp);
    }
  }

  @Test
  void watchdogStaysQuietWhileGoSendsHeartbeats(@TempDir Path tmp) throws Exception {
    Path inFile = tmp.resolve("in.mmap");
    Path outFile = tmp.resolve("out.mmap");

    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(inFile)
            .goToJavaFile(outFile)
            .ringCapacity(8)
            .ringMaxObjectSize(64 * 1024)
            // Heartbeat every 100ms — well below the 3-miss=300ms threshold over a 600ms window.
            .heartbeatPeriodMs(100)
            .hookTimeout(Duration.ofSeconds(2))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
            .build();

    try (CoprocessorRuntime rt = new CoprocessorRuntime(cfg)) {
      rt.start();
      // Give the Go side enough wall time to ship ≥ 4 heartbeats — the watchdog must observe them
      // via the reader and stay quiet.
      Thread.sleep(600);
      assertFalse(rt.isUnhealthy(), "watchdog must not fire while heartbeats flow");
      assertTrue(rt.isAlive(), "Go process must still be alive");
      rt.stop();
    }
  }

  @Test
  void restartsGoProcessAfterCrash(@TempDir Path tmp) throws Exception {
    Path inFile = tmp.resolve("in.mmap");
    Path outFile = tmp.resolve("out.mmap");

    RestartConfig restart =
        RestartConfig.builder()
            .initialDelayMs(50L)
            .maxDelayMs(200L)
            .multiplier(2.0)
            .jitterRatio(0.0)
            .maxConsecutiveFails(3)
            .probeIntervalMs(500L)
            .build();

    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(inFile)
            .goToJavaFile(outFile)
            .ringCapacity(8)
            .ringMaxObjectSize(64 * 1024)
            .heartbeatPeriodMs(50)
            .hookTimeout(Duration.ofSeconds(2))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
            .restartConfig(restart)
            .build();

    try (CoprocessorRuntime rt = new CoprocessorRuntime(cfg)) {
      rt.start();
      assertTrue(rt.isAlive(), "Go process must be alive after start");
      long origPid = rt.goProcessPidForTesting();
      assertNotEquals(-1L, origPid);

      // Crash the Go process. The watchdog scheduler will detect the dead
      // process (via the next liveness check), notify the controller, and the
      // controller will fire a restart attempt after the configured initial
      // backoff.
      rt.crashGoProcessForTesting();

      long deadline = System.currentTimeMillis() + 5_000L;
      while (System.currentTimeMillis() < deadline) {
        long pid = rt.goProcessPidForTesting();
        if (rt.isAlive() && pid != origPid && pid != -1L) {
          break;
        }
        Thread.sleep(20);
      }

      assertTrue(rt.isAlive(), "Go process must be restarted after crash");
      assertNotEquals(origPid, rt.goProcessPidForTesting(), "pid must change after restart");
      assertFalse(rt.isUnhealthy(), "after successful restart, runtime must be healthy");
      assertEquals(
          RestartController.State.HEALTHY,
          rt.restartControllerForTesting().state(),
          "controller must return to HEALTHY after a successful restart");

      rt.stop();
    }
  }

  private static boolean hasReaderThreadAlive(CoprocessorRuntime rt) {
    Thread reader = rt.readerThreadForTesting();
    return reader != null && reader.isAlive();
  }

  private static void deleteRecursive(Path p) throws java.io.IOException {
    if (!Files.exists(p)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(p)) {
      walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (java.io.IOException ignored) {
                  // best effort
                }
              });
    }
  }
}
