// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.masterpolicy;

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
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;

/**
 * T51 master-side coprocessor. Mirrors {@code FilterRegionObserver}'s layout — spawns a {@link
 * CoprocessorRuntime} and delegates to its {@link MasterObserver} adapter — but attaches at the
 * HMaster instead of a RegionServer. The embedded Go binary is the {@code master-policy-observer}
 * ELF which rejects table-lifecycle operations on tables whose qualifier carries the configured
 * blocked prefix; rejection travels back as an IOException to the HBase admin client because the
 * {@code preCreateTable} / {@code preDeleteTable} hooks default to the STRICT failure policy.
 *
 * <p>The prefix is read from the {@link #KEY_BLOCKED_PREFIX} coprocessor config and forwarded to
 * the Go side via the {@code HBASECOP_POLICY_BLOCKED_PREFIX} environment variable.
 */
public final class PolicyMasterObserver implements MasterCoprocessor {

  private static final Logger LOG = System.getLogger(PolicyMasterObserver.class.getName());

  /**
   * Configuration key selecting the blocked table-qualifier prefix. The value is forwarded
   * verbatim to the spawned Go process via the {@code HBASECOP_POLICY_BLOCKED_PREFIX} environment
   * variable; an unset or blank value leaves the env var unset (Go defaults to {@code
   * "forbidden-"}).
   */
  public static final String KEY_BLOCKED_PREFIX = "hbasecop.policy.blocked_prefix";

  private CoprocessorRuntime runtime;
  private Path tmpDir;

  /**
   * Maps the {@link #KEY_BLOCKED_PREFIX} value in {@code conf} to the env-var map consumed by
   * {@link CoprocessorRuntime.Config#extraEnv()}. A missing or blank value returns the empty map.
   *
   * <p>Visible-for-testing as a pure helper so the mapping can be exercised without driving a
   * MasterCoprocessor lifecycle.
   */
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
    tmpDir = Files.createTempDirectory("hbasecop-master-policy-");
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
        "PolicyMasterObserver: runtime started for env {0}, shmem dir {1}",
        env,
        tmpDir);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (runtime != null) {
      try {
        runtime.stop();
      } catch (IOException | InterruptedException e) {
        LOG.log(Level.WARNING, "PolicyMasterObserver: runtime stop failed", e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
      runtime = null;
    }
    cleanupTmpDir();
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return runtime == null ? Optional.empty() : Optional.ofNullable(runtime.getMasterObserver());
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
