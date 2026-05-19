// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.filter;

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
 * T43 read-path RegionCoprocessor. Acquires a shared {@link CoprocessorRuntime} via {@link
 * SharedRuntime} keyed on this class — the embedded Go binary is the {@code filter-observer} ELF
 * which inspects every Get / Scan and bypasses those whose target row carries the configured
 * blocked prefix. The prefix is read from the {@link #KEY_BLOCKED_PREFIX} table-level config and
 * forwarded to the Go side via the {@code HBASECOP_FILTER_BLOCKED_PREFIX} environment variable.
 */
public final class FilterRegionObserver implements RegionCoprocessor {

  private static final Logger LOG = System.getLogger(FilterRegionObserver.class.getName());

  /**
   * Configuration key (per-table or per-coprocessor) selecting the blocked row-key prefix. The
   * value is forwarded verbatim to the spawned Go process via the {@code
   * HBASECOP_FILTER_BLOCKED_PREFIX} environment variable; an unset or blank value leaves the env
   * var unset (Go defaults to {@code "block-"}).
   */
  public static final String KEY_BLOCKED_PREFIX = "hbasecop.filter.blocked_prefix";

  private static final String SHARED_KEY = FilterRegionObserver.class.getName();

  private SharedRuntime.Handle handle;

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
    handle =
        SharedRuntime.acquire(
            SHARED_KEY,
            () -> {
              Path tmpDir = Files.createTempDirectory("hbasecop-filter-");
              CoprocessorRuntime.Config cfg =
                  CoprocessorRuntime.Config.builder()
                      .javaToGoFile(tmpDir.resolve("in.mmap"))
                      .goToJavaFile(tmpDir.resolve("out.mmap"))
                      .ringCapacity(16)
                      .ringMaxObjectSize(1 << 20)
                      .hookTimeout(Duration.ofSeconds(5))
                      .gracefulShutdownTimeout(Duration.ofSeconds(2))
                      .configuration(env.getConfiguration())
                      .extraEnv(envFromConfig(env.getConfiguration()))
                      .build();
              return SharedRuntime.Spec.of(cfg, () -> cleanupTmpDir(tmpDir));
            });
    LOG.log(Level.INFO, "FilterRegionObserver: handle acquired for env {0}", env);
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
