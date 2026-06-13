// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * T45 integration test: exercises the storage hooks (preFlush/postFlush, preCompactSelection,
 * preCompact/postCompact) end-to-end through the {@code filter-observer} coproc-jar on a live HBase
 * 2.5 standalone cluster.
 *
 * <p>The observer's flush/compact handlers are passive recorders — they emit a uniquely-tagged
 * {@code "filter-observer: <hook>"} slog line per invocation. The IT pre-populates the table,
 * drives {@code admin.flush(tn)} twice (to materialise two HFiles), and then {@code
 * admin.majorCompact(tn)}, waiting for the compaction state to return to NONE. It then greps the
 * docker logs for the expected lines, proving the hooks fired end-to-end through the bridge.
 *
 * <p>Not part of {@code mvn test}; invoked by {@code make test-integration-storage}.
 */
final class StorageHooksIT {

  private static final String CONTAINER_NAME = "go-hbase-dev";
  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/filter-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/filter-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.examples.filter.FilterRegionObserver";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);
  private static final byte[] QUAL = "q".getBytes(StandardCharsets.UTF_8);
  private static final int N_PER_FLUSH = 8;
  private static final Duration LOG_GRACE = Duration.ofSeconds(30);
  private static final Duration COMPACT_WAIT = Duration.ofSeconds(60);

  @Test
  void flushAndCompactFireObserverHooks() throws Exception {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make filter-observer-jar` and copy into test/integration/coproc-jars/)");

    long baselinePreFlush = countLogLines("filter-observer: preFlush");
    long baselinePostFlush = countLogLines("filter-observer: postFlush");
    long baselinePreCompactSelection = countLogLines("filter-observer: preCompactSelection");
    long baselinePreCompact = countLogLines("filter-observer: preCompact");
    long baselinePostCompact = countLogLines("filter-observer: postCompact");

    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "10");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "60000");
    cfg.set("hbase.client.operation.timeout", "120000");
    cfg.set("hbase.client.meta.operation.timeout", "120000");

    TableName tn = TableName.valueOf("hbasecop_storage_it");

    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));

      if (admin.tableExists(tn)) {
        dropTable(admin, tn);
      }

      createTableWithCoproc(admin, tn);
      try (Table table = conn.getTable(tn)) {
        // First batch + flush — materialises HFile #1.
        populate(table, "row-a-", N_PER_FLUSH);
        admin.flush(tn);
        waitForLog(
            baselinePreFlush, 1, "filter-observer: preFlush", LOG_GRACE, "preFlush after flush #1");
        waitForLog(
            baselinePostFlush,
            1,
            "filter-observer: postFlush",
            LOG_GRACE,
            "postFlush after flush #1");

        // Second batch + flush — materialises HFile #2 so a major compaction
        // has at least two store-files to merge (otherwise HBase may no-op).
        populate(table, "row-b-", N_PER_FLUSH);
        admin.flush(tn);
        waitForLog(
            baselinePreFlush, 2, "filter-observer: preFlush", LOG_GRACE, "preFlush after flush #2");
        waitForLog(
            baselinePostFlush,
            2,
            "filter-observer: postFlush",
            LOG_GRACE,
            "postFlush after flush #2");

        admin.majorCompact(tn);
        waitForCompaction(admin, tn, COMPACT_WAIT);

        waitForLog(
            baselinePreCompactSelection,
            1,
            "filter-observer: preCompactSelection",
            LOG_GRACE,
            "preCompactSelection");
        waitForLog(baselinePreCompact, 1, "filter-observer: preCompact", LOG_GRACE, "preCompact");
        waitForLog(
            baselinePostCompact, 1, "filter-observer: postCompact", LOG_GRACE, "postCompact");
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  private static Path resolveJarOnHost() {
    Path here = Paths.get("").toAbsolutePath();
    while (here != null) {
      Path candidate = here.resolve(COPROC_JAR_HOST_RELATIVE);
      if (Files.exists(candidate)) {
        return candidate;
      }
      here = here.getParent();
    }
    return Paths.get(COPROC_JAR_HOST_RELATIVE).toAbsolutePath();
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

  private static void createTableWithCoproc(Admin admin, TableName tn) throws IOException {
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .setCoprocessor(
                CoprocessorDescriptorBuilder.newBuilder(COPROC_CLASSNAME)
                    .setJarPath(COPROC_JAR_IN_CONTAINER)
                    .setPriority(0)
                    .build())
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

  private static void populate(Table table, String prefix, int n) throws IOException {
    for (int i = 1; i <= n; i++) {
      byte[] row = (prefix + i).getBytes(StandardCharsets.UTF_8);
      Put p = new Put(row);
      p.addColumn(CF, QUAL, row);
      table.put(p);
    }
  }

  /**
   * Polls {@link Admin#getCompactionState(TableName)} until it returns to NONE or the deadline
   * elapses. HBase major-compaction is asynchronous on the server.
   */
  private static void waitForCompaction(Admin admin, TableName tn, Duration deadline)
      throws Exception {
    Instant cutoff = Instant.now().plus(deadline);
    while (Instant.now().isBefore(cutoff)) {
      CompactionState state = admin.getCompactionState(tn);
      if (state == CompactionState.NONE) {
        return;
      }
      Thread.sleep(500);
    }
    throw new IllegalStateException("compaction on " + tn + " did not complete within " + deadline);
  }

  private static void waitForLog(
      long baseline, int expectedDelta, String needle, Duration grace, String label)
      throws Exception {
    Instant deadline = Instant.now().plus(grace);
    while (Instant.now().isBefore(deadline)) {
      if (countLogLines(needle) - baseline >= expectedDelta) {
        return;
      }
      Thread.sleep(250);
    }
    long actual = countLogLines(needle) - baseline;
    assertTrue(
        actual >= expectedDelta,
        label + ": expected ≥ " + expectedDelta + " new '" + needle + "' lines, got " + actual);
  }

  /** Runs {@code docker logs {container} 2>&1 | grep -c '<needle>'}. */
  private static long countLogLines(String needle) throws IOException, InterruptedException {
    String safe = needle.replace("'", "'\\''");
    ProcessBuilder pb =
        new ProcessBuilder(
            "sh", "-c", "docker logs " + CONTAINER_NAME + " 2>&1 | grep -c '" + safe + "'");
    pb.redirectErrorStream(true);
    Process proc = pb.start();
    String out;
    try (BufferedReader r =
        new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = r.readLine()) != null) {
        sb.append(line);
      }
      out = sb.toString().trim();
    }
    boolean exited = proc.waitFor(10, TimeUnit.SECONDS);
    if (!exited) {
      proc.destroyForcibly();
      throw new IOException("docker logs grep timed out");
    }
    int code = proc.exitValue();
    if (code != 0 && code != 1) {
      throw new IOException(
          "docker logs grep exited " + code + " (is the container `" + CONTAINER_NAME + "` up?)");
    }
    try {
      return Long.parseLong(out);
    } catch (NumberFormatException e) {
      throw new IOException("unexpected docker-logs grep output: '" + out + "'", e);
    }
  }
}
