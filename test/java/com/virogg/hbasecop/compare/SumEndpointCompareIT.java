// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.compare;

import static com.virogg.hbasecop.compare.CompareSupport.CF;
import static com.virogg.hbasecop.compare.CompareSupport.GENERIC_FQCN;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_JAR;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_JAR_HOST;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_SUM_FQCN;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.Test;

/**
 * Native-vs-Go comparison for an AGGREGATING ENDPOINT (read path). Both arms expose the SAME {@code
 * GoEndpointService} surface, so the client call path ({@link
 * com.virogg.hbasecop.client.EndpointClient}) is byte identical — only the server-side {@code sum}
 * implementation differs:
 *
 * <ul>
 *   <li>native arm: {@code NativeSumEndpoint} scans the region and sums in the RegionServer JVM;
 *   <li>Go arm: the stock {@code GenericRegionObserver} forwards the call over the shmem bridge to
 *       the {@code endpoint-observer} Go process, which scans via the reverse channel and sums.
 * </ul>
 *
 * <p>Equivalence: both must return the exact arithmetic SUM of the seeded {@code cf:n} column.
 * Performance: per-arm sum-call latency, interleaved A/B over many rounds, reported (not gated).
 */
final class SumEndpointCompareIT {

  private static final byte[] N = "n".getBytes(StandardCharsets.UTF_8);
  private static final int ROWS = 2_000;
  private static final int ROUNDS = 120;
  private static final int WARMUP = 10;

  @Test
  void nativeAndGoEndpointsAgreeAndArePerfComparable() throws Throwable {
    CompareSupport.requireStagedJar(NATIVE_JAR_HOST);
    CompareSupport.requireStagedJar("test/integration/coproc-jars/endpoint-observer.jar");

    TableName nativeTn = TableName.valueOf("cmp_sum_native");
    TableName goTn = TableName.valueOf("cmp_sum_go");

    try (Connection conn = ConnectionFactory.createConnection(CompareSupport.clientConfig());
        Admin admin = conn.getAdmin()) {
      CompareSupport.waitForClusterReady(admin, Duration.ofSeconds(300));
      CompareSupport.dropTable(admin, nativeTn);
      CompareSupport.dropTable(admin, goTn);
      CompareSupport.createTableWithCoproc(admin, nativeTn, NATIVE_SUM_FQCN, NATIVE_JAR);
      CompareSupport.createTableWithCoproc(
          admin, goTn, GENERIC_FQCN, "file:///coproc-jars/endpoint-observer.jar");

      try (Table nativeTable = conn.getTable(nativeTn);
          Table goTable = conn.getTable(goTn)) {
        long expected = seedNumeric(nativeTable, goTable);

        // --- Equivalence (hard pass/fail) ---------------------------------
        long nativeSum = CompareSupport.sumEndpoint(nativeTable, N);
        long goSum = CompareSupport.sumEndpoint(goTable, N);
        assertEquals(expected, nativeSum, "native endpoint SUM must equal the arithmetic total");
        assertEquals(
            nativeSum, goSum, "Go endpoint SUM must equal the native endpoint SUM (equivalence)");

        // --- Performance (report-only) ------------------------------------
        List<Long> nativeSamples = new ArrayList<>();
        List<Long> goSamples = new ArrayList<>();
        for (int r = 0; r < WARMUP + ROUNDS; r++) {
          boolean nativeFirst = (r & 1) == 0; // alternate order to cancel drift
          long nt;
          long gt;
          if (nativeFirst) {
            nt = CompareSupport.timeNanos(() -> CompareSupport.sumEndpoint(nativeTable, N));
            gt = CompareSupport.timeNanos(() -> CompareSupport.sumEndpoint(goTable, N));
          } else {
            gt = CompareSupport.timeNanos(() -> CompareSupport.sumEndpoint(goTable, N));
            nt = CompareSupport.timeNanos(() -> CompareSupport.sumEndpoint(nativeTable, N));
          }
          if (r >= WARMUP) {
            nativeSamples.add(nt);
            goSamples.add(gt);
          }
        }
        CompareSupport.printResult("sum", "native", nativeSamples);
        CompareSupport.printResult("sum", "go", goSamples);
        CompareSupport.printSummary("sum", nativeSamples, goSamples, nativeSum == goSum);
      } finally {
        CompareSupport.dropTable(admin, nativeTn);
        CompareSupport.dropTable(admin, goTn);
      }
    }
  }

  /** Seeds rows 1..ROWS with cf:n=i into both tables; returns the arithmetic total. */
  private static long seedNumeric(Table nativeTable, Table goTable) throws Exception {
    List<Put> nativePuts = new ArrayList<>(ROWS);
    List<Put> goPuts = new ArrayList<>(ROWS);
    long expected = 0;
    for (int i = 1; i <= ROWS; i++) {
      expected += i;
      byte[] row = ("row-" + i).getBytes(StandardCharsets.UTF_8);
      byte[] val = Long.toString(i).getBytes(StandardCharsets.UTF_8);
      nativePuts.add(new Put(row).addColumn(CF, N, val));
      goPuts.add(new Put(row).addColumn(CF, N, val));
    }
    nativeTable.put(nativePuts);
    goTable.put(goPuts);
    return expected;
  }
}
