// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.counter;

import com.virogg.hbasecop.bridge.CoprocessorRuntime;
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
 * RegionServer-side coprocessor: spawns a {@link CoprocessorRuntime} per region, delegates {@code
 * prePut}/{@code postPut} to its {@link RegionObserver} (the bridge adapter), and tears it down on
 * unload.
 *
 * <p>HBase instantiates one of these per attached region via the standard no-arg constructor; each
 * instance owns its own Go process and shmem ring pair. Multi-region sharing on a single
 * RegionServer is T61 work.
 */
public final class CounterRegionObserver implements RegionCoprocessor {

  private static final Logger LOG = System.getLogger(CounterRegionObserver.class.getName());

  private CoprocessorRuntime runtime;
  private Path tmpDir;

  public CounterRegionObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    tmpDir = Files.createTempDirectory("hbasecop-counter-");
    Path inFile = tmpDir.resolve("in.mmap");
    Path outFile = tmpDir.resolve("out.mmap");

    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(inFile)
            .goToJavaFile(outFile)
            .ringCapacity(16)
            .ringMaxObjectSize(1 << 20) // 1 MiB — covers max Mutation size in tests
            .hookTimeout(Duration.ofSeconds(5))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
            .configuration(env.getConfiguration())
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
        "CounterRegionObserver: runtime started for env {0}, shmem dir {1}",
        env,
        tmpDir);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (runtime != null) {
      try {
        runtime.stop();
      } catch (IOException | InterruptedException e) {
        LOG.log(Level.WARNING, "CounterRegionObserver: runtime stop failed", e);
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
