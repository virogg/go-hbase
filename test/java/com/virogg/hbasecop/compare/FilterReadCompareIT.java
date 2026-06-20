// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.compare;

import static com.virogg.hbasecop.compare.CompareSupport.CF;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_FILTER_FQCN;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_JAR;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_JAR_HOST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.Test;

/**
 * Native-vs-Go comparison for a READ-PATH bypass OBSERVER. Both arms bypass any Get whose row
 * carries the blocked prefix {@code "block-"} (the Get returns empty):
 *
 * <ul>
 *   <li>native arm: {@code NativeFilterObserver.preGetOp} calls {@code ctx.bypass()} in the
 *       RegionServer JVM;
 *   <li>Go arm: {@code FilterRegionObserver} forwards preGetOp over the bridge to the Go {@code
 *       filter-observer} process, which returns {@code HookResult.Bypass=true}.
 * </ul>
 *
 * <p>Equivalence: for the same rows, both arms must return the SAME visible result (blocked rows
 * empty, allowed rows present). Performance: per-arm Get latency over a blocked/allowed mix,
 * interleaved A/B, reported (not gated). The coproc is attached AFTER seeding
 * (disable/modify/enable) so the seed Puts never traverse the read-path observer.
 */
final class FilterReadCompareIT {

  private static final byte[] Q = "q".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VAL = "v".getBytes(StandardCharsets.UTF_8);
  private static final String BLOCKED = "block-";
  private static final String ALLOWED = "ok-";
  private static final int N_PER_GROUP = 3;
  private static final int PERF_ROUNDS = 200;
  private static final int PERF_WARMUP = 20;

  @Test
  void nativeAndGoFilterAgreeAndArePerfComparable() throws Throwable {
    CompareSupport.requireStagedJar(NATIVE_JAR_HOST);
    CompareSupport.requireStagedJar("test/integration/coproc-jars/filter-observer.jar");

    TableName nativeTn = TableName.valueOf("cmp_filter_native");
    TableName goTn = TableName.valueOf("cmp_filter_go");

    try (Connection conn = ConnectionFactory.createConnection(CompareSupport.clientConfig());
        Admin admin = conn.getAdmin()) {
      CompareSupport.waitForClusterReady(admin, Duration.ofSeconds(300));
      CompareSupport.dropTable(admin, nativeTn);
      CompareSupport.dropTable(admin, goTn);

      // Seed BEFORE attaching (the Go arm also vetoes block-* writes via preBatchMutate).
      CompareSupport.createPlainTable(admin, nativeTn);
      CompareSupport.createPlainTable(admin, goTn);
      try (Table seedN = conn.getTable(nativeTn);
          Table seedG = conn.getTable(goTn)) {
        seed(seedN);
        seed(seedG);
      }
      CompareSupport.attachCoproc(admin, nativeTn, NATIVE_FILTER_FQCN, NATIVE_JAR);
      CompareSupport.attachCoproc(
          admin,
          goTn,
          "com.virogg.hbasecop.examples.filter.FilterRegionObserver",
          "file:///coproc-jars/filter-observer.jar");

      try (Table nativeTable = conn.getTable(nativeTn);
          Table goTable = conn.getTable(goTn)) {

        // --- Equivalence (hard pass/fail) ---------------------------------
        for (int i = 0; i < N_PER_GROUP; i++) {
          byte[] blockedRow = (BLOCKED + i).getBytes(StandardCharsets.UTF_8);
          byte[] allowedRow = (ALLOWED + i).getBytes(StandardCharsets.UTF_8);

          Result nb = nativeTable.get(new Get(blockedRow));
          Result gb = goTable.get(new Get(blockedRow));
          assertTrue(nb.isEmpty(), "native: blocked Get must be bypassed (empty)");
          assertTrue(gb.isEmpty(), "Go: blocked Get must be bypassed (empty)");
          assertEquals(nb.isEmpty(), gb.isEmpty(), "blocked Get equivalence");

          Result na = nativeTable.get(new Get(allowedRow));
          Result ga = goTable.get(new Get(allowedRow));
          assertFalse(na.isEmpty(), "native: allowed Get must pass through");
          assertFalse(ga.isEmpty(), "Go: allowed Get must pass through");
          assertEquals(
              new String(na.getValue(CF, Q), StandardCharsets.UTF_8),
              new String(ga.getValue(CF, Q), StandardCharsets.UTF_8),
              "allowed Get value equivalence");
        }

        // --- Performance (report-only): mixed blocked/allowed Get latency --
        List<Long> nativeSamples = new ArrayList<>();
        List<Long> goSamples = new ArrayList<>();
        for (int r = 0; r < PERF_WARMUP + PERF_ROUNDS; r++) {
          boolean nativeFirst = (r & 1) == 0;
          long nt;
          long gt;
          if (nativeFirst) {
            nt = CompareSupport.timeNanos(() -> getMix(nativeTable));
            gt = CompareSupport.timeNanos(() -> getMix(goTable));
          } else {
            gt = CompareSupport.timeNanos(() -> getMix(goTable));
            nt = CompareSupport.timeNanos(() -> getMix(nativeTable));
          }
          if (r >= PERF_WARMUP) {
            nativeSamples.add(nt);
            goSamples.add(gt);
          }
        }
        CompareSupport.printResult("filter", "native", nativeSamples);
        CompareSupport.printResult("filter", "go", goSamples);
        CompareSupport.printSummary("filter", nativeSamples, goSamples, true);
      } finally {
        CompareSupport.dropTable(admin, nativeTn);
        CompareSupport.dropTable(admin, goTn);
      }
    }
  }

  /** One blocked Get + one allowed Get (the workload unit timed per round). */
  private static void getMix(Table table) throws Exception {
    table.get(new Get((BLOCKED + "1").getBytes(StandardCharsets.UTF_8)));
    table.get(new Get((ALLOWED + "1").getBytes(StandardCharsets.UTF_8)));
  }

  private static void seed(Table table) throws Exception {
    for (int i = 0; i < N_PER_GROUP; i++) {
      table.put(new Put((BLOCKED + i).getBytes(StandardCharsets.UTF_8)).addColumn(CF, Q, VAL));
      table.put(new Put((ALLOWED + i).getBytes(StandardCharsets.UTF_8)).addColumn(CF, Q, VAL));
    }
  }
}
