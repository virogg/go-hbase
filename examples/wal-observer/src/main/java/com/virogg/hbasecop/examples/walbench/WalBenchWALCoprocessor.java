// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.walbench;

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
import org.apache.hadoop.hbase.coprocessor.WALCoprocessor;
import org.apache.hadoop.hbase.coprocessor.WALObserver;

/**
 * T82 WAL coprocessor. Acquires a shared {@link CoprocessorRuntime} via {@link SharedRuntime}
 * keyed on this class and delegates to its {@link WALObserver} adapter. The embedded Go binary is
 * the {@code wal-observer} ELF, a deliberate no-op: every {@code preWALWrite}/{@code postWALWrite}
 * crosses the shmem rings and returns the zero result, so the {@code WalThroughputBenchIT} bench
 * measures pure bridge overhead on the WAL append hot path against a baseline cluster without
 * this coprocessor registered.
 *
 * <p>Registered cluster-wide via {@code hbase.coprocessor.wal.classes} (no per-table attachment
 * exists for WAL coprocessors); the T82 entrypoint block wires that when {@code
 * HBASECOP_WAL_COPROC_CLASS} is set.
 */
public final class WalBenchWALCoprocessor implements WALCoprocessor {

  private static final Logger LOG = System.getLogger(WalBenchWALCoprocessor.class.getName());

  private static final String SHARED_KEY = WalBenchWALCoprocessor.class.getName();

  private SharedRuntime.Handle handle;

  public WalBenchWALCoprocessor() {}

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    handle =
        SharedRuntime.acquire(
            SHARED_KEY,
            () -> {
              Path tmpDir = Files.createTempDirectory("hbasecop-wal-bench-");
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
    LOG.log(Level.INFO, "WalBenchWALCoprocessor: handle acquired for env {0}", env);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (handle != null) {
      handle.release();
      handle = null;
    }
  }

  @Override
  public Optional<WALObserver> getWALObserver() {
    return handle == null ? Optional.empty() : Optional.ofNullable(handle.getWALObserver());
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
