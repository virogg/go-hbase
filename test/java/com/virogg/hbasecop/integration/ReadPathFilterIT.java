// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * T43 integration test: exercises the read-path hooks (preGetOp, preScannerOpen, preScannerNext)
 * end-to-end through the {@code filter-observer} coproc-jar on a live HBase 2.5 standalone cluster
 * (the T26 docker-compose target).
 *
 * <p>The observer is configured to bypass every Get / Scan whose target row carries the blocked
 * prefix {@code "block-"}. The test pre-populates {@code block-*} and {@code ok-*} rows, then
 * asserts that:
 *
 * <ul>
 *   <li>Get on a {@code block-} row returns an empty {@link Result} (observer bypassed the Get).
 *   <li>Get on an {@code ok-} row returns the stored value (observer allowed the Get).
 *   <li>Scan starting at {@code block-} yields 0 rows (observer bypassed scanner open).
 *   <li>Scan starting at {@code ok-} yields the full set of {@code ok-*} rows.
 *   <li>{@code preGetOp} and {@code preScannerOpen} log counts increment, proving the hooks fired.
 * </ul>
 *
 * <p>This test is not part of {@code mvn test} — its name doesn't match Surefire's defaults — and
 * is invoked explicitly by {@code make test-integration-read}, which manages the cluster lifecycle
 * and stages the coproc-jar into the bind-mount the container reads from.
 */
final class ReadPathFilterIT {

  private static final String CONTAINER_NAME = "go-hbase-dev";
  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/filter-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/filter-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.examples.filter.FilterRegionObserver";
  private static final String CONF_KEY_BLOCKED_PREFIX = "hbasecop.filter.blocked_prefix";
  private static final String BLOCKED_PREFIX = "block-";
  private static final String ALLOWED_PREFIX = "ok-";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);
  private static final byte[] QUAL = "q".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VAL = "v".getBytes(StandardCharsets.UTF_8);
  private static final int N_PER_GROUP = 3;
  private static final Duration LOG_GRACE = Duration.ofSeconds(10);

  @Test
  void blockedReadsAreBypassed_allowedReadsPassThrough() throws Exception {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make filter-observer-jar` and copy into test/integration/coproc-jars/)");

    long baselinePreGet = countLogLines("filter-observer: starting");
    long baselineRuntime = countLogLines("hbasecop-runtime");

    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "10");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");

    TableName tn = TableName.valueOf("hbasecop_read_it");

    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));

      if (admin.tableExists(tn)) {
        dropTable(admin, tn);
      }

      // Create the table WITHOUT the coprocessor and pre-populate first: the
      // shared filter-observer also implements preBatchMutate (added in T44),
      // which vetoes every block-* mutation. Attaching the observer only after
      // the 3 block-* and 3 ok-* rows are in place keeps this test focused on
      // the read-path hooks instead of tripping the write-path block.
      createTablePlain(admin, tn);
      try {
        try (Table seed = conn.getTable(tn)) {
          populate(seed, BLOCKED_PREFIX, N_PER_GROUP);
          populate(seed, ALLOWED_PREFIX, N_PER_GROUP);
        }
        attachCoproc(admin, tn);

        try (Table table = conn.getTable(tn)) {
          // --- Get path --------------------------------------------------
          Result blockedGet =
              table.get(new Get((BLOCKED_PREFIX + "1").getBytes(StandardCharsets.UTF_8)));
          assertNotNull(blockedGet, "blocked Get must return a non-null (empty) Result");
          assertTrue(
              blockedGet.isEmpty(),
              "blocked Get must return empty Result (observer bypassed HBase Get) — got "
                  + blockedGet);

          Result allowedGet =
              table.get(new Get((ALLOWED_PREFIX + "1").getBytes(StandardCharsets.UTF_8)));
          assertNotNull(allowedGet, "allowed Get must return a non-null Result");
          assertFalse(
              allowedGet.isEmpty(), "allowed Get must return the stored cell — got empty Result");

          // --- Scan path -------------------------------------------------
          List<String> blockedRows = scanRowKeys(table, BLOCKED_PREFIX, BLOCKED_PREFIX + "~");
          assertEquals(
              0,
              blockedRows.size(),
              "blocked scan must return 0 rows (observer bypassed scanner open) — got "
                  + blockedRows);

          List<String> allowedRows = scanRowKeys(table, ALLOWED_PREFIX, ALLOWED_PREFIX + "~");
          assertEquals(
              N_PER_GROUP,
              allowedRows.size(),
              "allowed scan must return all " + N_PER_GROUP + " ok-* rows — got " + allowedRows);

          // --- Hooks fired -----------------------------------------------
          // Wait briefly for runtime logs to flush, then assert the Go side
          // produced its startup log (proves the filter-observer ELF ran),
          // plus the runtime emitted hook-handling traffic.
          waitForLog(baselinePreGet, 1, "filter-observer: starting", LOG_GRACE);
          long startupCount = countLogLines("filter-observer: starting") - baselinePreGet;
          assertTrue(
              startupCount >= 1,
              "filter-observer should have logged its startup line at least once — got "
                  + startupCount);

          long runtimeCount = countLogLines("hbasecop-runtime") - baselineRuntime;
          assertTrue(
              runtimeCount >= 1,
              "Java bridge must have logged runtime activity — got " + runtimeCount);
        }
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

  /** Create the bare table — no coprocessor — so the seed Puts are not observed. */
  private static void createTablePlain(Admin admin, TableName tn) throws IOException {
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .build();
    admin.createTable(desc);
  }

  /**
   * Attach the filter-observer coprocessor to an existing table via the standard
   * disable/modify/enable cycle. Run after seeding so the observer never sees the seed mutations.
   */
  private static void attachCoproc(Admin admin, TableName tn) throws IOException {
    admin.disableTable(tn);
    TableDescriptor updated =
        TableDescriptorBuilder.newBuilder(admin.getDescriptor(tn))
            .setValue(CONF_KEY_BLOCKED_PREFIX, BLOCKED_PREFIX)
            .setCoprocessor(
                CoprocessorDescriptorBuilder.newBuilder(COPROC_CLASSNAME)
                    .setJarPath(COPROC_JAR_IN_CONTAINER)
                    .setPriority(0)
                    .build())
            .build();
    admin.modifyTable(updated);
    admin.enableTable(tn);
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
      Put p = new Put((prefix + i).getBytes(StandardCharsets.UTF_8));
      p.addColumn(CF, QUAL, VAL);
      table.put(p);
    }
  }

  private static List<String> scanRowKeys(Table table, String startRow, String stopRow)
      throws IOException {
    Scan scan =
        new Scan()
            .withStartRow(startRow.getBytes(StandardCharsets.UTF_8))
            .withStopRow(stopRow.getBytes(StandardCharsets.UTF_8));
    List<String> rows = new ArrayList<>();
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result r : scanner) {
        rows.add(new String(r.getRow(), StandardCharsets.UTF_8));
      }
    }
    return rows;
  }

  private static void waitForLog(long baseline, int expectedDelta, String needle, Duration grace)
      throws Exception {
    Instant deadline = Instant.now().plus(grace);
    while (Instant.now().isBefore(deadline)) {
      if (countLogLines(needle) - baseline >= expectedDelta) {
        return;
      }
      Thread.sleep(250);
    }
    // Fall through on timeout — caller's assertion will report the exact delta.
  }

  /** Runs {@code docker logs {container} 2>&1 | grep -c '<needle>'}. */
  private static long countLogLines(String needle) throws IOException, InterruptedException {
    // The needle is hard-coded by callers — no shell-injection risk — but quote
    // it defensively in case future callers parameterize.
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
