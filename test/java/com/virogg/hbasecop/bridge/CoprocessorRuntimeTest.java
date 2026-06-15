// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.config.HookPolicy;
import com.virogg.hbasecop.bridge.config.Policy;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.observer.RegionObserverAdapter;
import com.virogg.hbasecop.bridge.supervisor.RestartConfig;
import com.virogg.hbasecop.bridge.supervisor.RestartController;
import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.multiplex.GoSideCrashedException;
import com.virogg.hbasecop.multiplex.Multiplexer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Lifecycle plumbing for the in-RegionServer coproc bridge: spawn the Go ELF, open shmem rings,
 * stand up a Multiplexer-backed {@link com.virogg.hbasecop.bridge.observer.HookDispatcher}, and
 * expose a {@link RegionObserver} that the host coproc can delegate to.
 *
 * <p>Verifies wiring end-to-end using the default {@code hbasecop-runtime} ELF (built by {@code
 * make go-build-runtime}). Does not drive hooks; that's covered by {@code
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
            // Heartbeat every 100ms, well below the 3-miss=300ms threshold over a 600ms window.
            .heartbeatPeriodMs(100)
            .hookTimeout(Duration.ofSeconds(2))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
            .build();

    try (CoprocessorRuntime rt = new CoprocessorRuntime(cfg)) {
      rt.start();
      // Allow the Go side to ship >= 4 heartbeats; watchdog must observe them via the reader and
      // stay quiet.
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

      // Watchdog scheduler detects the dead process on the next liveness check, notifies the
      // controller, which fires a restart attempt after the configured initial backoff.
      rt.crashGoProcessForTesting();

      // Await the TERMINAL restart state, not the intermediate observable: attemptRestart() swaps
      // in the fresh GoProcess (pid changes, isAlive flips true) before tick() stores
      // state=HEALTHY, so polling on the pid alone races the final transition on slow runners.
      long deadline = System.currentTimeMillis() + 5_000L;
      while (System.currentTimeMillis() < deadline) {
        long pid = rt.goProcessPidForTesting();
        if (rt.isAlive()
            && pid != origPid
            && pid != -1L
            && rt.restartControllerForTesting().state() == RestartController.State.HEALTHY) {
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

  // Regression for C2: disabling heartbeats must NOT disable crash detection or auto-restart. The
  // supervisor scheduler is created unconditionally and, with heartbeats off, ticks at the
  // crash-probe cadence (DEFAULT_CRASH_PROBE_MS). Pre-fix, the scheduler only started when a
  // watchdog existed, so a crash with heartbeats off went unnoticed and the runtime hung forever.
  // Mirrors restartsGoProcessAfterCrash but with heartbeatPeriodMs(-1).
  @Test
  void restartsGoProcessAfterCrashWithHeartbeatsDisabled(@TempDir Path tmp) throws Exception {
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
            .heartbeatPeriodMs(-1) // heartbeats DISABLED: the C2 scenario
            .hookTimeout(Duration.ofSeconds(2))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
            .restartConfig(restart)
            .build();

    try (CoprocessorRuntime rt = new CoprocessorRuntime(cfg)) {
      rt.start();
      assertTrue(rt.isAlive(), "Go process must be alive after start");
      long origPid = rt.goProcessPidForTesting();
      assertNotEquals(-1L, origPid);

      rt.crashGoProcessForTesting();

      // Detection rides the crash-probe cadence (~500ms) not the 50ms heartbeat, so allow a wider
      // deadline than the heartbeats-on test.
      long deadline = System.currentTimeMillis() + 8_000L;
      while (System.currentTimeMillis() < deadline) {
        long pid = rt.goProcessPidForTesting();
        if (rt.isAlive()
            && pid != origPid
            && pid != -1L
            && rt.restartControllerForTesting().state() == RestartController.State.HEALTHY) {
          break;
        }
        Thread.sleep(20);
      }

      assertTrue(rt.isAlive(), "crash must be detected and restarted even with heartbeats off");
      assertNotEquals(origPid, rt.goProcessPidForTesting(), "pid must change after restart");
      assertFalse(rt.isUnhealthy(), "after successful restart, runtime must be healthy");
      assertEquals(
          RestartController.State.HEALTHY,
          rt.restartControllerForTesting().state(),
          "controller must return to HEALTHY after a heartbeats-off restart");

      rt.stop();
    }
  }

  @Test
  void crashFailsInflightCallsAndDefersNewOnesPastRestartDeadline(@TempDir Path tmp)
      throws Exception {
    Path inFile = tmp.resolve("in.mmap");
    Path outFile = tmp.resolve("out.mmap");

    // Aggressive watchdog so the scheduler runs frequently, and a small restart-deadline so the
    // deferred-fail path lands quickly within the test budget.
    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(inFile)
            .goToJavaFile(outFile)
            .ringCapacity(64)
            .ringMaxObjectSize(64 * 1024)
            .heartbeatPeriodMs(50)
            .hookTimeout(Duration.ofSeconds(5))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
            // Block restart so deferred calls actually wait + fail (T34 stays HEALTHY-side here).
            .restartConfig(
                RestartConfig.builder()
                    .initialDelayMs(10_000L)
                    .maxDelayMs(10_000L)
                    .multiplier(2.0)
                    .jitterRatio(0.0)
                    .maxConsecutiveFails(5)
                    .probeIntervalMs(30_000L)
                    .build())
            .restartDeadline(Duration.ofMillis(300))
            .build();

    try (CoprocessorRuntime rt = new CoprocessorRuntime(cfg)) {
      rt.start();
      Multiplexer mux = rt.multiplexerForTesting();
      assertNotNull(mux);

      // Issue 100 prePut-shaped calls into the mux. The Go side won't respond (HOOK_PRE_PUT isn't
      // registered on the bare hbasecop-runtime), so they sit pending.
      final int n = 100;
      List<CompletableFuture<Message>> futures = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        futures.add(mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 1, new byte[] {})));
      }

      // Watchdog scheduler must observe the dead process and pause the mux, failing every pending
      // future with GoSideCrashedException.
      rt.crashGoProcessForTesting();

      long deadline = System.currentTimeMillis() + 1_000L;
      for (CompletableFuture<Message> f : futures) {
        long remaining = deadline - System.currentTimeMillis();
        ExecutionException ee =
            assertThrows(
                ExecutionException.class,
                () -> f.get(Math.max(remaining, 0L) + 200L, TimeUnit.MILLISECONDS));
        assertTrue(
            ee.getCause() instanceof GoSideCrashedException,
            () -> "expected GoSideCrashedException, got " + ee.getCause());
      }

      // A new call during the paused window must wait up to restart-deadline and then fail
      // with GoSideCrashedException; restart was configured with a 10s initial delay so the
      // process won't have come back yet.
      long t0 = System.currentTimeMillis();
      CompletableFuture<Message> deferred =
          mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 1, new byte[] {}));
      ExecutionException ee =
          assertThrows(ExecutionException.class, () -> deferred.get(2, TimeUnit.SECONDS));
      long elapsed = System.currentTimeMillis() - t0;
      assertTrue(
          ee.getCause() instanceof GoSideCrashedException,
          () -> "expected GoSideCrashedException, got " + ee.getCause());
      assertTrue(
          elapsed >= 250L && elapsed < 1_500L,
          () -> "deferred call must fail near restart-deadline; elapsed=" + elapsed + "ms");

      rt.stop();
    }
  }

  @Test
  void extraEnvDefaultsToEmpty() {
    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(Path.of("/tmp/in"))
            .goToJavaFile(Path.of("/tmp/out"))
            .build();
    assertTrue(cfg.extraEnv().isEmpty(), "default extraEnv must be empty");
  }

  @Test
  void extraEnvRoundTripsAndIsDefensivelyCopied() {
    Map<String, String> env = new java.util.HashMap<>();
    env.put("HBASECOP_FAULT_MODE", "exit-1");

    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(Path.of("/tmp/in"))
            .goToJavaFile(Path.of("/tmp/out"))
            .extraEnv(env)
            .build();

    assertEquals(Map.of("HBASECOP_FAULT_MODE", "exit-1"), cfg.extraEnv());

    env.put("HBASECOP_FAULT_MODE", "kill-9");
    env.put("HBASECOP_OBSERVER_TAG", "after-build");
    assertEquals(
        "exit-1",
        cfg.extraEnv().get("HBASECOP_FAULT_MODE"),
        "post-build mutations of source map must not leak in");
    assertEquals(1, cfg.extraEnv().size());

    assertThrows(
        UnsupportedOperationException.class,
        () -> cfg.extraEnv().put("HBASECOP_OBSERVER_TAG", "x"),
        "extraEnv() must return an unmodifiable view");
  }

  /**
   * Regression for an HBase live-cluster bug found during T36 IT run: HBase wraps the region-coproc
   * env in a {@code CompoundConfiguration} that merges {@code TableDescriptor.setValue} keys
   * dynamically inside {@code get()} but does <em>not</em> copy them into the base properties map.
   * The previous {@code new Configuration(src)} clone in {@code buildPolicyConfig} dropped those
   * merged values silently: per-table {@code hbasecop.policy.prePut} overrides were lost and policy
   * always defaulted to STRICT for prePut.
   *
   * <p>The fake simulates exactly that: an inner map only the overridden iterator and get()
   * consult; the standard {@code Properties}-based clone path would miss it.
   */
  @Test
  void policyResolvesAcrossCompoundConfigurationLikeWrapper(@TempDir Path tmp) throws Exception {
    Map<String, String> dynamic = new LinkedHashMap<>();
    dynamic.put("hbasecop.policy.prePut", "best-effort");
    Configuration compoundLike =
        new Configuration(false) {
          @Override
          public String get(String name) {
            String v = dynamic.get(name);
            return v != null ? v : super.get(name);
          }

          @Override
          public Iterator<Map.Entry<String, String>> iterator() {
            return dynamic.entrySet().iterator();
          }
        };

    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(tmp.resolve("in.mmap"))
            .goToJavaFile(tmp.resolve("out.mmap"))
            .configuration(compoundLike)
            .build();

    // Runtime never starts here; only exercises the policy-resolution path that
    // {@link CoprocessorRuntime#buildPolicyConfig(Config)} drives.
    PolicyConfig pc = invokeBuildPolicyConfig(cfg); // reflection wrapper below
    HookPolicy resolved = pc.forHook(RegionObserverAdapter.HOOK_PRE_PUT);
    assertEquals(
        Policy.BEST_EFFORT,
        resolved.policy(),
        "dynamically-merged per-table policy override must survive buildPolicyConfig");
  }

  private static PolicyConfig invokeBuildPolicyConfig(CoprocessorRuntime.Config cfg)
      throws Exception {
    java.lang.reflect.Method m =
        CoprocessorRuntime.class.getDeclaredMethod(
            "buildPolicyConfig", CoprocessorRuntime.Config.class);
    m.setAccessible(true);
    return (PolicyConfig) m.invoke(null, cfg);
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
