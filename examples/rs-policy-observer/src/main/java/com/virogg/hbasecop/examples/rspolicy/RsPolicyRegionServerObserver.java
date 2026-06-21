// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.rspolicy;

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
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;

public final class RsPolicyRegionServerObserver implements RegionServerCoprocessor {

  private static final Logger LOG = System.getLogger(RsPolicyRegionServerObserver.class.getName());

  public static final String KEY_VETO_WAL_ROLL = "hbasecop.policy.veto_wal_roll";

  private static final String SHARED_KEY = RsPolicyRegionServerObserver.class.getName();

  private SharedRuntime.Handle handle;

  public static Map<String, String> envFromConfig(Configuration conf) {
    Objects.requireNonNull(conf, "conf");
    if ("true".equals(conf.get(KEY_VETO_WAL_ROLL))) {
      return Map.of("HBASECOP_RS_POLICY_VETO_WAL_ROLL", "true");
    }
    return Map.of();
  }

  public RsPolicyRegionServerObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    handle =
        SharedRuntime.acquire(
            SHARED_KEY,
            () -> {
              Path tmpDir = Files.createTempDirectory("hbasecop-rs-policy-");
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
    LOG.log(Level.INFO, "RsPolicyRegionServerObserver: handle acquired for env {0}", env);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (handle != null) {
      handle.release();
      handle = null;
    }
  }

  @Override
  public Optional<RegionServerObserver> getRegionServerObserver() {
    return handle == null
        ? Optional.empty()
        : Optional.ofNullable(handle.getRegionServerObserver());
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
