// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.filter;

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
 * T43 read-path RegionCoprocessor. Mirrors {@code CounterRegionObserver}'s layout — spawns a
 * {@link CoprocessorRuntime} per region — but the embedded Go binary is the {@code filter-observer}
 * ELF which inspects every Get / Scan and bypasses those whose target row carries the configured
 * blocked prefix. The prefix is read from the {@link #KEY_BLOCKED_PREFIX} table-level config and
 * forwarded to the Go side via the {@code HBASECOP_FILTER_BLOCKED_PREFIX} environment variable.
 */
public final class FilterRegionObserver implements RegionCoprocessor {

  private static final Logger LOG = System.getLogger(FilterRegionObserver.class.getName());

  /**
   * Configuration key (per-table or per-coprocessor) selecting the blocked row-key prefix. The
   * value is forwarded verbatim to the spawned Go process via the
   * {@code HBASECOP_FILTER_BLOCKED_PREFIX} environment variable; an unset or blank value leaves the
   * env var unset (Go defaults to {@code "block-"}).
   */
  public static final String KEY_BLOCKED_PREFIX = "hbasecop.filter.blocked_prefix";

  private CoprocessorRuntime runtime;
  private Path tmpDir;

  /**
   * Maps the {@link #KEY_BLOCKED_PREFIX} value in {@code conf} to the env-var map consumed by
   * {@link CoprocessorRuntime.Config#extraEnv()}. A missing or blank value returns the empty map.
   *
   * <p>Visible-for-testing as a pure helper so the mapping can be exercised without driving a
   * RegionCoprocessor lifecycle.
   */
  public static Map<String, String> envFromConfig(Configuration conf) {
    Objects.requireNonNull(conf, "conf");
    String prefix = conf.get(KEY_BLOCKED_PREFIX);
    if (prefix == null || prefix.trim().isEmpty()) {
      return Map.of();
    }
    return Map.of("HBASECOP_FILTER_BLOCKED_PREFIX", prefix);
  }

  public FilterRegionObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    tmpDir = Files.createTempDirectory("hbasecop-filter-");
    Path inFile = tmpDir.resolve("in.mmap");
    Path outFile = tmpDir.resolve("out.mmap");

    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(inFile)
            .goToJavaFile(outFile)
            .ringCapacity(16)
            .ringMaxObjectSize(1 << 20)
            .hookTimeout(Duration.ofSeconds(5))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
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
        "FilterRegionObserver: runtime started for env {0}, shmem dir {1}",
        env,
        tmpDir);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (runtime != null) {
      try {
        runtime.stop();
      } catch (IOException | InterruptedException e) {
        LOG.log(Level.WARNING, "FilterRegionObserver: runtime stop failed", e);
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
