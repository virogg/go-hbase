// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.fault;

import com.virogg.hbasecop.bridge.CoprocessorRuntime;
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
 * T36 fault-injection RegionCoprocessor. Mirrors {@code CounterRegionObserver}'s layout — spawns a
 * {@link CoprocessorRuntime} per region — but the embedded Go binary is the {@code fault-observer}
 * ELF which dispatches a configurable fault on every prePut. The fault mode is selected by the
 * {@code HBASECOP_FAULT_MODE} env var on the Go side; it can be set on the JVM (and thus on the
 * spawned child) via {@code -Dhbase.regionserver.executorService.handlers.count} style standard
 * RegionServer config, or simpler: by exporting it before {@code make hbase-up}.
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

  private CoprocessorRuntime runtime;
  private Path tmpDir;

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
    tmpDir = Files.createTempDirectory("hbasecop-fault-");
    Path inFile = tmpDir.resolve("in.mmap");
    Path outFile = tmpDir.resolve("out.mmap");

    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(inFile)
            .goToJavaFile(outFile)
            .ringCapacity(16)
            .ringMaxObjectSize(1 << 20)
            .heartbeatPeriodMs(200)
            .hookTimeout(Duration.ofSeconds(2))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
            .restartDeadline(Duration.ofSeconds(3))
            .configuration(env.getConfiguration())
            .extraEnv(envFromConfig(env.getConfiguration()))
            .build();

    runtime = new CoprocessorRuntime(cfg);
    try {
      runtime.start();
    } catch (IOException e) {
      cleanupTmpDir();
      runtime = null;
      throw e;
    }
    LOG.log(
        Level.INFO,
        "FaultRegionObserver: runtime started for env {0}, shmem dir {1}",
        env,
        tmpDir);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (runtime != null) {
      try {
        runtime.stop();
      } catch (IOException | InterruptedException e) {
        LOG.log(Level.WARNING, "FaultRegionObserver: runtime stop failed", e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
      runtime = null;
    }
    cleanupTmpDir();
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return runtime == null ? Optional.empty() : Optional.ofNullable(runtime.getRegionObserver());
  }

  private void cleanupTmpDir() {
    if (tmpDir == null) {
      return;
    }
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
    tmpDir = null;
  }
}
