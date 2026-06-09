// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.ttl;

import com.virogg.hbasecop.bridge.CoprocessorRuntime;
import com.virogg.hbasecop.bridge.SharedRuntime;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

/**
 * T73 TTL-validation example, RegionServer side: delegates every RegionObserver hook to the Go
 * runtime via a {@link CoprocessorRuntime}. The Go observer rejects Puts whose cell values lack the
 * {@code ttl=<seconds>;} envelope; under the default <b>strict</b> policy for pre-hooks the
 * rejection reaches the HBase client as an {@code IOException} and the write is aborted.
 *
 * <p>The runtime is acquired via {@link SharedRuntime} keyed on this class, so all regions on one
 * RegionServer share one Go process / one shmem pair (T63).
 */
public final class TtlValidatorRegionObserver implements RegionCoprocessor {

  private static final Logger LOG = System.getLogger(TtlValidatorRegionObserver.class.getName());

  private static final String SHARED_KEY = TtlValidatorRegionObserver.class.getName();

  private SharedRuntime.Handle handle;

  public TtlValidatorRegionObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    handle =
        SharedRuntime.acquire(
            SHARED_KEY,
            () -> {
              Path tmpDir = Files.createTempDirectory("hbasecop-ttl-");
              CoprocessorRuntime.Config cfg =
                  CoprocessorRuntime.Config.builder()
                      .javaToGoFile(tmpDir.resolve("in.mmap"))
                      .goToJavaFile(tmpDir.resolve("out.mmap"))
                      .ringCapacity(16)
                      .ringMaxObjectSize(1 << 20)
                      .hookTimeout(Duration.ofSeconds(5))
                      .gracefulShutdownTimeout(Duration.ofSeconds(2))
                      .configuration(env.getConfiguration())
                      .build();
              return SharedRuntime.Spec.of(cfg, () -> cleanupTmpDir(tmpDir));
            });
    LOG.log(Level.INFO, "TtlValidatorRegionObserver: handle acquired for env {0}", env);
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
