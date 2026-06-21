// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.masterpolicy;

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
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;

public final class PolicyMasterObserver implements MasterCoprocessor {

  private static final Logger LOG = System.getLogger(PolicyMasterObserver.class.getName());

  public static final String KEY_BLOCKED_PREFIX = "hbasecop.policy.blocked_prefix";

  private static final String SHARED_KEY = PolicyMasterObserver.class.getName();

  private SharedRuntime.Handle handle;

  public static Map<String, String> envFromConfig(Configuration conf) {
    Objects.requireNonNull(conf, "conf");
    String prefix = conf.get(KEY_BLOCKED_PREFIX);
    if (prefix == null || prefix.trim().isEmpty()) {
      return Map.of();
    }
    return Map.of("HBASECOP_POLICY_BLOCKED_PREFIX", prefix);
  }

  public PolicyMasterObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    handle =
        SharedRuntime.acquire(
            SHARED_KEY,
            () -> {
              Path tmpDir = Files.createTempDirectory("hbasecop-master-policy-");
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
    LOG.log(Level.INFO, "PolicyMasterObserver: handle acquired for env {0}", env);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (handle != null) {
      handle.release();
      handle = null;
    }
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return handle == null ? Optional.empty() : Optional.ofNullable(handle.getMasterObserver());
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
