// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.compare;

import static com.virogg.hbasecop.compare.CompareSupport.CF;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_JAR;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_JAR_HOST;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_TTL_FQCN;
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
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.Test;

final class TtlObserverCompareIT {

  private static final byte[] Q = "q".getBytes(StandardCharsets.UTF_8);
  private static final int PERF_BATCH = 500;
  private static final int PERF_ROUNDS = 20;
  private static final int PERF_WARMUP = 3;

  private static final class Case {
    final String label;
    final byte[] value;
    final boolean expectAccept;

    Case(String label, String value, boolean expectAccept) {
      this.label = label;
      this.value = value.getBytes(StandardCharsets.UTF_8);
      this.expectAccept = expectAccept;
    }
  }

  private static final List<Case> CORPUS =
      List.of(
          new Case("valid-1h", "ttl=3600;payload", true),
          new Case("valid-min", "ttl=1;", true),
          new Case("valid-9digits", "ttl=123456789;x", true),
          new Case("no-envelope", "payload-without-ttl", false),
          new Case("no-digits", "ttl=;x", false),
          new Case("zero", "ttl=0;x", false),
          new Case("non-digit", "ttl=12a;x", false),
          new Case("too-long-10digits", "ttl=1234567890;x", false),
          new Case("no-terminator", "ttl=3600", false),
          new Case("too-short", "ttl=", false));

  @Test
  void nativeAndGoTtlAgreeAndArePerfComparable() throws Throwable {
    CompareSupport.requireStagedJar(NATIVE_JAR_HOST);
    CompareSupport.requireStagedJar("test/integration/coproc-jars/ttl-validator.jar");

    TableName nativeTn = TableName.valueOf("cmp_ttl_native");
    TableName goTn = TableName.valueOf("cmp_ttl_go");

    try (Connection conn = ConnectionFactory.createConnection(CompareSupport.clientConfig());
        Admin admin = conn.getAdmin()) {
      CompareSupport.waitForClusterReady(admin, Duration.ofSeconds(300));
      CompareSupport.dropTable(admin, nativeTn);
      CompareSupport.dropTable(admin, goTn);
      CompareSupport.createTableWithCoproc(admin, nativeTn, NATIVE_TTL_FQCN, NATIVE_JAR);
      CompareSupport.createTableWithCoproc(
          admin,
          goTn,
          "com.virogg.hbasecop.examples.ttl.TtlValidatorRegionObserver",
          "file:///coproc-jars/ttl-validator.jar");

      try (Table nativeTable = conn.getTable(nativeTn);
          Table goTable = conn.getTable(goTn)) {

        for (Case c : CORPUS) {
          byte[] row = ("case-" + c.label).getBytes(StandardCharsets.UTF_8);
          boolean nativeAccepted = tryPut(nativeTable, row, c.value);
          boolean goAccepted = tryPut(goTable, row, c.value);
          assertEquals(
              c.expectAccept, nativeAccepted, "native TTL decision mismatch for case " + c.label);
          assertEquals(
              nativeAccepted,
              goAccepted,
              "Go TTL decision must match native for case " + c.label + " (equivalence)");
          assertEquals(nativeAccepted, !nativeTable.get(new Get(row)).isEmpty(), c.label);
          assertEquals(goAccepted, !goTable.get(new Get(row)).isEmpty(), c.label);
        }

        List<Long> nativeSamples = new ArrayList<>();
        List<Long> goSamples = new ArrayList<>();
        for (int r = 0; r < PERF_WARMUP + PERF_ROUNDS; r++) {
          List<Put> nativeBatch = validBatch("perf-n-r" + r + "-");
          List<Put> goBatch = validBatch("perf-g-r" + r + "-");
          boolean nativeFirst = (r & 1) == 0;
          long nt;
          long gt;
          if (nativeFirst) {
            nt = CompareSupport.timeNanos(() -> nativeTable.put(nativeBatch));
            gt = CompareSupport.timeNanos(() -> goTable.put(goBatch));
          } else {
            gt = CompareSupport.timeNanos(() -> goTable.put(goBatch));
            nt = CompareSupport.timeNanos(() -> nativeTable.put(nativeBatch));
          }
          if (r >= PERF_WARMUP) {
            nativeSamples.add(nt);
            goSamples.add(gt);
          }
        }
        CompareSupport.printResult("ttl", "native", nativeSamples);
        CompareSupport.printResult("ttl", "go", goSamples);
        CompareSupport.printSummary("ttl", nativeSamples, goSamples, true);

        assertFalse(nativeSamples.isEmpty(), "expected timed rounds");
        assertTrue(goSamples.size() == PERF_ROUNDS, "expected ROUNDS timed go samples");
      } finally {
        CompareSupport.dropTable(admin, nativeTn);
        CompareSupport.dropTable(admin, goTn);
      }
    }
  }

  private static boolean tryPut(Table table, byte[] row, byte[] value) {
    try {
      table.put(new Put(row).addColumn(CF, Q, value));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static List<Put> validBatch(String prefix) {
    List<Put> puts = new ArrayList<>(PERF_BATCH);
    for (int i = 0; i < PERF_BATCH; i++) {
      byte[] row = (prefix + i).getBytes(StandardCharsets.UTF_8);
      byte[] value = ("ttl=3600;payload-" + i).getBytes(StandardCharsets.UTF_8);
      puts.add(new Put(row).addColumn(CF, Q, value));
    }
    return puts;
  }
}
