// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.fault;

import com.virogg.hbasecop.bridge.CoprocessorRuntime;
import com.virogg.hbasecop.bridge.SharedRuntime;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

/**
 * T36 fault-injection RegionCoprocessor. Acquires a shared {@link CoprocessorRuntime} via {@link
 * SharedRuntime} keyed on this class — the embedded Go binary is the {@code fault-observer} ELF
 * which dispatches a configurable fault on every prePut. The fault mode is selected by the {@code
 * HBASECOP_FAULT_MODE} env var on the Go side, propagated via {@link
 * CoprocessorRuntime.Config#extraEnv()} from the per-table {@link #KEY_FAULT_MODE} config.
 *
 * <p>Heartbeats and the restart controller (T33–T35) are wired with aggressive defaults so the
 * fault matrix observes the supervisor's recovery within the test's time budget.
 */
public final class FaultRegionObserver implements RegionCoprocessor {

  private static final Logger LOG = System.getLogger(FaultRegionObserver.class.getName());

  /**
   * Configuration key (per-table or per-coprocessor) selecting the fault mode. The value is
   * forwarded verbatim to the spawned Go process via the {@code HBASECOP_FAULT_MODE} environment
   * variable, where the {@code fault.ParseMode} parser validates it. Valid tokens: {@code none},
   * {@code kill-9}, {@code hang}, {@code exit-1}, {@code protocol-error}, {@code oom}.
   */
  public static final String KEY_FAULT_MODE = "hbasecop.fault.mode";

  private static final String SHARED_KEY = FaultRegionObserver.class.getName();

  private SharedRuntime.Handle handle;

  /**
   * Maps the {@link #KEY_FAULT_MODE} value in {@code conf} to the env-var map consumed by {@link
   * CoprocessorRuntime.Config#extraEnv()}. A missing or blank value returns the empty map.
   *
   * <p>Visible-for-testing as a pure helper so the mapping can be exercised without driving a
   * RegionCoprocessor lifecycle.
   */
  public static Map<String, String> envFromConfig(Configuration conf) {
    Objects.requireNonNull(conf, "conf");
    String mode = conf.get(KEY_FAULT_MODE);
    if (mode == null || mode.trim().isEmpty()) {
      return Map.of();
    }
    return Map.of("HBASECOP_FAULT_MODE", mode);
  }

  public FaultRegionObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    handle =
        SharedRuntime.acquire(
            SHARED_KEY,
            () -> {
              Path tmpDir = Files.createTempDirectory("hbasecop-fault-");
              CoprocessorRuntime.Config cfg =
                  CoprocessorRuntime.Config.builder()
                      .javaToGoFile(tmpDir.resolve("in.mmap"))
                      .goToJavaFile(tmpDir.resolve("out.mmap"))
                      .ringCapacity(16)
                      .ringMaxObjectSize(1 << 20)
                      .heartbeatPeriodMs(200)
                      .hookTimeout(Duration.ofSeconds(2))
                      .gracefulShutdownTimeout(Duration.ofSeconds(2))
                      .restartDeadline(Duration.ofSeconds(3))
                      .configuration(env.getConfiguration())
                      .extraEnv(envFromConfig(env.getConfiguration()))
                      .build();
              return SharedRuntime.Spec.of(cfg, () -> cleanupTmpDir(tmpDir));
            });
    LOG.log(Level.INFO, "FaultRegionObserver: handle acquired for env {0}", env);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (handle != null) {
      handle.release();
      handle = null;
    }
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return handle == null ? Optional.empty() : Optional.ofNullable(handle.getRegionObserver());
  }

  private static void cleanupTmpDir(Path tmpDir) {
    try (Stream<Path> walk = Files.walk(tmpDir)) {
      walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException ignored) {
                  // best effort
                }
              });
    } catch (IOException ignored) {
      // best effort
    }
  }
}
