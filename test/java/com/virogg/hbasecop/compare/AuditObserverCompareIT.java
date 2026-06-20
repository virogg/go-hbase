// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.compare;

import static com.virogg.hbasecop.compare.CompareSupport.CF;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_AUDIT_FQCN;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_JAR;
import static com.virogg.hbasecop.compare.CompareSupport.NATIVE_JAR_HOST;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.Test;

/**
 * Native-vs-Go comparison for an AUDITING post-hook OBSERVER (write path, CPU-heavier). Both arms
 * emit one audit record per Put carrying the SHA-256 row digest:
 *
 * <ul>
 *   <li>native arm: {@code NativeAuditObserver.postPut} hashes + logs in the RegionServer JVM
 *       (marker {@code native-audit: audit});
 *   <li>Go arm: {@code AuditRegionObserver} forwards postPut over the bridge to the Go {@code
 *       audit-observer} process (marker {@code audit-observer: audit}).
 * </ul>
 *
 * <p>Equivalence: for the same rows, both arms must emit the SAME {@code row_digest} (and it must
 * match an independent Java oracle). Performance: per-arm write throughput with auditing on,
 * interleaved A/B, reported (not gated).
 */
final class AuditObserverCompareIT {

  private static final byte[] Q = "q".getBytes(StandardCharsets.UTF_8);
  private static final int EQUIV_ROWS = 16;
  private static final int PERF_BATCH = 500;
  private static final int PERF_ROUNDS = 20;
  private static final int PERF_WARMUP = 3;

  private static final Pattern NATIVE_DIGEST =
      Pattern.compile("native-audit: audit.*?row_digest=([0-9a-f]{16})");
  // Go side logs via slog; tolerate either JSON ("row_digest":"..") or logfmt (row_digest=..).
  private static final Pattern GO_DIGEST =
      Pattern.compile("audit-observer: audit.*?row_digest[\"=:\\s]+\"?([0-9a-f]{16})");

  @Test
  void nativeAndGoAuditAgreeAndArePerfComparable() throws Throwable {
    CompareSupport.requireStagedJar(NATIVE_JAR_HOST);
    CompareSupport.requireStagedJar("test/integration/coproc-jars/audit-observer.jar");

    TableName nativeTn = TableName.valueOf("cmp_audit_native");
    TableName goTn = TableName.valueOf("cmp_audit_go");

    try (Connection conn = ConnectionFactory.createConnection(CompareSupport.clientConfig());
        Admin admin = conn.getAdmin()) {
      CompareSupport.waitForClusterReady(admin, Duration.ofSeconds(300));
      CompareSupport.dropTable(admin, nativeTn);
      CompareSupport.dropTable(admin, goTn);
      CompareSupport.createTableWithCoproc(admin, nativeTn, NATIVE_AUDIT_FQCN, NATIVE_JAR);
      CompareSupport.createTableWithCoproc(
          admin,
          goTn,
          "com.virogg.hbasecop.examples.audit.AuditRegionObserver",
          "file:///coproc-jars/audit-observer.jar");

      try (Table nativeTable = conn.getTable(nativeTn);
          Table goTable = conn.getTable(goTn)) {

        // --- Equivalence (hard pass/fail) ---------------------------------
        Set<String> expected = new LinkedHashSet<>();
        for (int i = 0; i < EQUIV_ROWS; i++) {
          byte[] row = ("audit-eq-" + i).getBytes(StandardCharsets.UTF_8);
          expected.add(rowDigest(row));
          nativeTable.put(
              new Put(row).addColumn(CF, Q, ("v" + i).getBytes(StandardCharsets.UTF_8)));
          goTable.put(new Put(row).addColumn(CF, Q, ("v" + i).getBytes(StandardCharsets.UTF_8)));
        }

        Set<String> nativeDigests = awaitDigests(NATIVE_DIGEST, expected, Duration.ofSeconds(60));
        Set<String> goDigests = awaitDigests(GO_DIGEST, expected, Duration.ofSeconds(60));
        assertTrue(
            nativeDigests.containsAll(expected),
            "native audit must emit every expected row digest; missing="
                + minus(expected, nativeDigests));
        assertTrue(
            goDigests.containsAll(expected),
            "Go audit must emit every expected row digest; missing=" + minus(expected, goDigests));
        // The two arms agree on the digest for every audited row (the core equivalence).
        Set<String> bothForRows = new HashSet<>(expected);
        bothForRows.retainAll(nativeDigests);
        bothForRows.retainAll(goDigests);
        assertTrue(
            bothForRows.containsAll(expected),
            "native and Go must agree on the digest of every audited row");

        // --- Performance (report-only): audited write throughput ----------
        List<Long> nativeSamples = new ArrayList<>();
        List<Long> goSamples = new ArrayList<>();
        for (int r = 0; r < PERF_WARMUP + PERF_ROUNDS; r++) {
          List<Put> nativeBatch = batch("perf-n-r" + r + "-");
          List<Put> goBatch = batch("perf-g-r" + r + "-");
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
        CompareSupport.printResult("audit", "native", nativeSamples);
        CompareSupport.printResult("audit", "go", goSamples);
        CompareSupport.printSummary("audit", nativeSamples, goSamples, true);
      } finally {
        CompareSupport.dropTable(admin, nativeTn);
        CompareSupport.dropTable(admin, goTn);
      }
    }
  }

  /**
   * Polls {@code docker logs} until every expected digest has appeared (or the deadline lapses).
   */
  private static Set<String> awaitDigests(Pattern pattern, Set<String> expected, Duration deadline)
      throws Exception {
    long cutoff = System.nanoTime() + deadline.toNanos();
    Set<String> found = new HashSet<>();
    while (System.nanoTime() < cutoff) {
      found = scrape(pattern);
      if (found.containsAll(expected)) {
        return found;
      }
      Thread.sleep(1_000);
    }
    return found;
  }

  private static Set<String> scrape(Pattern pattern) throws Exception {
    Set<String> out = new HashSet<>();
    Matcher m = pattern.matcher(CompareSupport.dockerLogs());
    while (m.find()) {
      out.add(m.group(1));
    }
    return out;
  }

  private static Set<String> minus(Set<String> a, Set<String> b) {
    Set<String> r = new LinkedHashSet<>(a);
    r.removeAll(b);
    return r;
  }

  private static List<Put> batch(String prefix) {
    List<Put> puts = new ArrayList<>(PERF_BATCH);
    for (int i = 0; i < PERF_BATCH; i++) {
      byte[] row = (prefix + i).getBytes(StandardCharsets.UTF_8);
      puts.add(new Put(row).addColumn(CF, Q, ("v" + i).getBytes(StandardCharsets.UTF_8)));
    }
    return puts;
  }

  /** Independent oracle: first 8 bytes of SHA-256(row), hex (matches both arms' RowDigest). */
  private static String rowDigest(byte[] row) throws Exception {
    byte[] sum = MessageDigest.getInstance("SHA-256").digest(row);
    StringBuilder sb = new StringBuilder(16);
    for (int i = 0; i < 8; i++) {
      sb.append(Character.forDigit((sum[i] >> 4) & 0xf, 16));
      sb.append(Character.forDigit(sum[i] & 0xf, 16));
    }
    return sb.toString();
  }
}
