// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bench;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.CoprocessorRuntime;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LatencyBenchIT {

  private static final int[] BATCH_SIZES = {1, 100, 1000};
  private static final int OPS = Integer.getInteger("bench.ops", 10_000);
  private static final int WARMUP = Integer.getInteger("bench.warmup", 2_000);
  private static final long P50_MAX_US = Long.getLong("bench.prePut.p50.max.us", 100);

  private static Path tmpDir;
  private static CoprocessorRuntime runtime;
  private static RegionObserver bridgeObserver;
  private static ObserverContext<RegionCoprocessorEnvironment> ctx;

  private static final RegionObserver JAVA_ONLY = new RegionObserver() {};

  @BeforeAll
  @SuppressWarnings("unchecked")
  static void startRuntime() throws Exception {
    assertNotNull(
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("bench/linux-amd64/noop-runtime"),
        "no-op bench ELF missing from test classpath - run `make bench-latency` (it stages the"
            + " ELF via go-build-bench-noop before invoking this test)");

    tmpDir = Files.createTempDirectory("hbasecop-bench-");
    CoprocessorRuntime.Config cfg =
        CoprocessorRuntime.Config.builder()
            .javaToGoFile(tmpDir.resolve("in.mmap"))
            .goToJavaFile(tmpDir.resolve("out.mmap"))
            .ringCapacity(16)
            .ringMaxObjectSize(1 << 20)
            .hookTimeout(Duration.ofSeconds(5))
            .gracefulShutdownTimeout(Duration.ofSeconds(2))
            .binaryResourcePath("bench/linux-amd64/noop-runtime")
            .build();
    runtime = new CoprocessorRuntime(cfg);
    runtime.start();
    bridgeObserver = runtime.getRegionObserver();

    ctx = (ObserverContext<RegionCoprocessorEnvironment>) mock(ObserverContext.class);
    RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
    Region region = mock(Region.class);
    RegionInfo regionInfo = mock(RegionInfo.class);
    when(ctx.getEnvironment()).thenReturn(env);
    when(env.getRegion()).thenReturn(region);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    when(regionInfo.getTable()).thenReturn(TableName.valueOf("default", "bench"));
    when(regionInfo.getEncodedName()).thenReturn("bench1234");
    when(regionInfo.getEncodedNameAsBytes()).thenReturn("bench1234".getBytes());
  }

  @AfterAll
  static void stopRuntime() throws Exception {
    if (runtime != null) {
      runtime.stop();
    }
  }

  @Test
  void prePutP50OverheadUnderTarget() throws Exception {
    runLeg(bridgeObserver, true, WARMUP, WARMUP, false);
    runLeg(JAVA_ONLY, true, WARMUP, WARMUP, false);

    long gatedOverheadUs = -1;
    for (int batch : BATCH_SIZES) {
      boolean gapped = batch > 1;
      long[] bridgePre = runLeg(bridgeObserver, true, OPS, batch, gapped);
      long[] basePre = runLeg(JAVA_ONLY, true, OPS, batch, gapped);
      long[] bridgePost = runLeg(bridgeObserver, false, OPS, batch, gapped);
      long[] basePost = runLeg(JAVA_ONLY, false, OPS, batch, gapped);

      report("prePut", "bridge", batch, bridgePre);
      report("prePut", "java-only", batch, basePre);
      report("postPut", "bridge", batch, bridgePost);
      report("postPut", "java-only", batch, basePost);

      long overheadUs = (percentile(bridgePre, 0.50) - percentile(basePre, 0.50)) / 1_000;
      System.out.printf(
          Locale.ROOT,
          "LATENCY_BENCH_OVERHEAD hook=prePut batch=%d p50_overhead_us=%d%n",
          batch,
          overheadUs);
      if (batch == 1) {
        gatedOverheadUs = overheadUs;
      }
    }

    long[] sparse = runLeg(bridgeObserver, true, OPS / 4, 1, true);
    System.out.printf(
        Locale.ROOT,
        "LATENCY_BENCH_SPARSE hook=prePut n=%d p50_us=%.1f p95_us=%.1f p99_us=%.1f (not gated)%n",
        sparse.length,
        percentile(sparse, 0.50) / 1_000.0,
        percentile(sparse, 0.95) / 1_000.0,
        percentile(sparse, 0.99) / 1_000.0);

    assertTrue(
        gatedOverheadUs < P50_MAX_US,
        String.format(
            Locale.ROOT,
            "T81 gate: prePut p50 overhead %dµs >= %dµs target (SPEC §7.6)",
            gatedOverheadUs,
            P50_MAX_US));
  }

  private static long[] runLeg(
      RegionObserver observer, boolean pre, int ops, int batch, boolean gapBetweenBursts)
      throws Exception {
    long[] latencies = new long[ops];
    WALEdit edit = new WALEdit();
    int done = 0;
    while (done < ops) {
      int burst = Math.min(batch, ops - done);
      for (int i = 0; i < burst; i++) {
        Put put = benchPut(done);
        long t0 = System.nanoTime();
        if (pre) {
          observer.prePut(ctx, put, edit, Durability.USE_DEFAULT);
        } else {
          observer.postPut(ctx, put, edit, Durability.USE_DEFAULT);
        }
        latencies[done++] = System.nanoTime() - t0;
      }
      if (gapBetweenBursts && done < ops) {
        Thread.sleep(1);
      }
    }
    return latencies;
  }

  private static Put benchPut(int seq) {
    Put p = new Put(("row-" + seq).getBytes());
    p.addColumn("cf".getBytes(), "q1".getBytes(), 1_700_000_000_000L, "hello-bench".getBytes());
    return p;
  }

  private static void report(String hook, String variant, int batch, long[] latenciesNs) {
    System.out.printf(
        Locale.ROOT,
        "LATENCY_BENCH_RESULT hook=%s variant=%s batch=%d n=%d p50_us=%.1f p95_us=%.1f"
            + " p99_us=%.1f max_us=%.1f%n",
        hook,
        variant,
        batch,
        latenciesNs.length,
        percentile(latenciesNs, 0.50) / 1_000.0,
        percentile(latenciesNs, 0.95) / 1_000.0,
        percentile(latenciesNs, 0.99) / 1_000.0,
        percentile(latenciesNs, 1.0) / 1_000.0);
  }

  private static long percentile(long[] latenciesNs, double q) {
    long[] sorted = latenciesNs.clone();
    Arrays.sort(sorted);
    int idx = Math.min(sorted.length - 1, (int) (sorted.length * q));
    return sorted[idx];
  }
}
