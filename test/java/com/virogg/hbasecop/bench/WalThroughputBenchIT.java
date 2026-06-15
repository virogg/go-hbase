// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bench;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * T82 WAL-throughput measurement driver: times a sequential batched-Put workload against a live
 * HBase 2.5 standalone cluster (the T26 docker-compose target) and prints one machine-readable
 * result line. A measurement, not an assertion test: the only assertion is the coproc-presence
 * sanity check below. The regression gate comparing a baseline run against one with the no-op
 * {@code WalBenchWALCoprocessor} registered lives in {@code make bench-wal}, which drives two
 * compose cycles and invokes this driver once per cycle.
 *
 * <p>Not part of {@code mvn test} (name doesn't match Surefire's defaults); invoked explicitly via
 * {@code mvn test -Dtest=WalThroughputBenchIT -DfailIfNoTests=false}.
 *
 * <p>Knobs (system properties):
 *
 * <ul>
 *   <li>{@code bench.wal.ops}: total Puts to time (default 20000).
 *   <li>{@code bench.wal.batch}: Puts per {@code table.put(List)} call (default 100).
 *   <li>{@code bench.wal.expect.coproc}: when {@code "true"}, asserts a {@code hbasecop-runtime}
 *       process is alive in the container (entrypoint registered the WAL coprocessor); when {@code
 *       "false"} (default), asserts none is, guarding against a stale cluster leaking the coproc
 *       into the baseline measurement.
 * </ul>
 *
 * <p>The bench table carries no per-table coprocessor: WAL coprocessors are cluster-wide via {@code
 * hbase.coprocessor.wal.classes}, wired by the T82 entrypoint block when {@code
 * HBASECOP_WAL_COPROC_CLASS} is set on the container.
 */
final class WalThroughputBenchIT {

  private static final String CONTAINER_NAME = "go-hbase-dev";
  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);
  private static final byte[] QUAL = "q".getBytes(StandardCharsets.UTF_8);
  private static final int VALUE_SIZE = 100;
  private static final int WARMUP_BATCHES = 2;

  @Test
  void measureWalAppendThroughput() throws Exception {
    int ops = Integer.getInteger("bench.wal.ops", 20_000);
    int batchSize = Integer.getInteger("bench.wal.batch", 100);
    boolean expectCoproc =
        Boolean.parseBoolean(System.getProperty("bench.wal.expect.coproc", "false"));

    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "10");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");

    TableName tn = TableName.valueOf("walbench");

    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));

      if (admin.tableExists(tn)) {
        dropTable(admin, tn);
      }
      createTable(admin, tn);
      try (Table table = conn.getTable(tn)) {
        Random rnd = new Random(0x57414C42L); // fixed seed: same value stream every run

        // Warmup: prime client metadata caches and force WAL creation before the
        // presence check and the timed window.
        for (int b = 0; b < WARMUP_BATCHES; b++) {
          table.put(makeBatch("warm-" + b + "-", 0, batchSize, rnd));
        }

        assertCoprocPresence(expectCoproc);

        long t0 = System.nanoTime();
        for (int written = 0; written < ops; ) {
          int n = Math.min(batchSize, ops - written);
          table.put(makeBatch("row-", written, n, rnd));
          written += n;
        }
        long elapsedNanos = System.nanoTime() - t0;

        long wallMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
        double opsPerSec = ops / (elapsedNanos / 1e9);
        System.out.printf(
            Locale.ROOT,
            "WAL_BENCH_RESULT ops=%d wall_ms=%d ops_per_sec=%.1f%n",
            ops,
            wallMs,
            opsPerSec);
        System.out.printf(
            Locale.ROOT,
            "walbench: %d puts (batches of %d, %d-byte values) in %d ms -> %.1f ops/sec"
                + " (expect_coproc=%s)%n",
            ops,
            batchSize,
            VALUE_SIZE,
            wallMs,
            opsPerSec,
            expectCoproc);
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  /** Builds one batch of {@code n} Puts with distinct rows and random {@code VALUE_SIZE} values. */
  private static List<Put> makeBatch(String rowPrefix, int offset, int n, Random rnd) {
    List<Put> puts = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      byte[] value = new byte[VALUE_SIZE];
      rnd.nextBytes(value);
      String row = String.format(Locale.ROOT, "%s%08d", rowPrefix, offset + i);
      Put p = new Put(row.getBytes(StandardCharsets.UTF_8));
      p.addColumn(CF, QUAL, value);
      puts.add(p);
    }
    return puts;
  }

  private static void waitForClusterReady(Admin admin, Duration deadline) throws Exception {
    Instant cutoff = Instant.now().plus(deadline);
    Exception lastFailure = null;
    while (Instant.now().isBefore(cutoff)) {
      try {
        admin.listTableNames();
        return;
      } catch (Exception e) {
        lastFailure = e;
        Thread.sleep(1_000);
      }
    }
    throw new IllegalStateException(
        "HBase cluster not ready within " + deadline + ": " + lastFailure, lastFailure);
  }

  private static void createTable(Admin admin, TableName tn) throws IOException {
    // No per-table coprocessor on purpose; see the class doc.
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .build();
    admin.createTable(desc);
  }

  private static void dropTable(Admin admin, TableName tn) throws IOException {
    if (!admin.tableExists(tn)) {
      return;
    }
    if (admin.isTableEnabled(tn)) {
      admin.disableTable(tn);
    }
    admin.deleteTable(tn);
  }

  /**
   * Sanity-checks that the measured configuration matches the requested one: a coproc run must have
   * a live {@code hbasecop-runtime} inside the container, a baseline run must not.
   */
  private static void assertCoprocPresence(boolean expectCoproc)
      throws IOException, InterruptedException {
    List<String> pids = pgrepRuntimePids();
    if (expectCoproc) {
      assertFalse(
          pids.isEmpty(),
          "bench.wal.expect.coproc=true but no hbasecop-runtime process is alive in "
              + CONTAINER_NAME
              + " - was HBASECOP_WAL_COPROC_CLASS set on the container?");
    } else {
      assertTrue(
          pids.isEmpty(),
          () ->
              "bench.wal.expect.coproc=false but hbasecop-runtime is alive in "
                  + CONTAINER_NAME
                  + " (pids="
                  + pids
                  + ") - stale cluster leaking a coprocessor into the baseline; recreate the"
                  + " cluster");
    }
  }

  /** Returns the pids of every live {@code hbasecop-runtime} process inside the container. */
  private static List<String> pgrepRuntimePids() throws IOException, InterruptedException {
    ProcessBuilder pb =
        new ProcessBuilder(
            "sh", "-c", "docker exec " + CONTAINER_NAME + " pgrep -f hbasecop-runtime || true");
    pb.redirectErrorStream(true);
    Process proc = pb.start();
    List<String> pids = new ArrayList<>();
    try (BufferedReader r =
        new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = r.readLine()) != null) {
        String trimmed = line.trim();
        if (!trimmed.isEmpty() && trimmed.chars().allMatch(Character::isDigit)) {
          pids.add(trimmed);
        }
      }
    }
    if (!proc.waitFor(10, TimeUnit.SECONDS)) {
      proc.destroyForcibly();
      throw new IOException("docker exec pgrep timed out");
    }
    return pids;
  }
}
