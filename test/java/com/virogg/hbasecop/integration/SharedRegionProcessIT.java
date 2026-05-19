// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * T63 integration test: a single counter-observer coproc-jar attached to a 4-way pre-split table on
 * a one-RegionServer standalone cluster must spawn exactly one Go process. {@link
 * com.virogg.hbasecop.bridge.SharedRuntime} refcounts the process across all four {@code
 * CounterRegionObserver} instances; if that fell back to per-region spawning, we'd observe four
 * pids in the container.
 *
 * <p>Driven by {@code make test-integration-shared}: brings up the cluster, creates the pre-split
 * table, drives one Put per region, then checks both {@code pgrep} (live process count) and {@code
 * docker logs} (count of {@code GoProcess started:} lines during the test window) — both must be
 * exactly one.
 */
final class SharedRegionProcessIT {

  private static final String CONTAINER_NAME = "go-hbase-dev";
  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/counter-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/counter-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.examples.counter.CounterRegionObserver";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);
  private static final byte[] QUAL = "q".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VAL = "v".getBytes(StandardCharsets.UTF_8);
  private static final int N_REGIONS = 4;
  private static final Duration OP_TIMEOUT = Duration.ofMinutes(2);
  private static final Duration LOG_GRACE = Duration.ofSeconds(10);

  @Test
  void fourRegionsShareOneGoProcess() throws Exception {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make counter-observer-jar` and copy into test/integration/coproc-jars/)");

    long baselineStartedLogs = countStartedLogs();

    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "10");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");

    TableName tn = TableName.valueOf("hbasecop_shared_pid_it");

    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(60));

      if (admin.tableExists(tn)) {
        dropTable(admin, tn);
      }

      createPreSplitTableWithCoproc(admin, tn);
      try {
        waitForRegionCount(admin, tn, N_REGIONS, Duration.ofSeconds(60));
        runOnePutPerRegion(conn, tn);

        // Process check: only one hbasecop-runtime should be alive on the RS.
        List<String> pids = pgrepRuntimePids();
        assertEquals(
            1,
            pids.size(),
            () ->
                "expected exactly one hbasecop-runtime process across "
                    + N_REGIONS
                    + " regions, found pids="
                    + pids
                    + " (SharedRuntime refcount likely broken)");

        // Log check: only one `GoProcess started:` line should have been emitted in this test
        // window.
        waitForLogs(baselineStartedLogs, 1, LOG_GRACE);
        long startedDelta = countStartedLogs() - baselineStartedLogs;
        assertEquals(
            1,
            startedDelta,
            () ->
                "expected exactly one `GoProcess started:` line in this run, got "
                    + (countStartedLogsQuiet() - baselineStartedLogs)
                    + " (baseline="
                    + baselineStartedLogs
                    + ")");
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  private static byte[][] splitKeys(int n) {
    // n regions ⇒ n-1 split keys. Keys must sort: "row-1xx" < "row-2xx" < ... lexicographically.
    byte[][] keys = new byte[n - 1][];
    for (int i = 1; i < n; i++) {
      keys[i - 1] = ("row-" + (i * 100)).getBytes(StandardCharsets.UTF_8);
    }
    return keys;
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

  private static void createPreSplitTableWithCoproc(Admin admin, TableName tn) throws IOException {
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .setCoprocessor(
                CoprocessorDescriptorBuilder.newBuilder(COPROC_CLASSNAME)
                    .setJarPath(COPROC_JAR_IN_CONTAINER)
                    .setPriority(0)
                    .build())
            .build();
    admin.createTable(desc, splitKeys(N_REGIONS));
  }

  private static void waitForRegionCount(Admin admin, TableName tn, int expected, Duration deadline)
      throws Exception {
    Instant cutoff = Instant.now().plus(deadline);
    int last = -1;
    while (Instant.now().isBefore(cutoff)) {
      int count = admin.getRegions(tn).size();
      last = count;
      if (count >= expected) {
        return;
      }
      Thread.sleep(500);
    }
    fail("expected " + expected + " online regions within " + deadline + ", got " + last);
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

  private static void runOnePutPerRegion(Connection conn, TableName tn) throws IOException {
    // Row keys are chosen to land in each of the four regions (boundaries at row-100/200/300):
    // "row-050" ∈ region 0, "row-150" ∈ region 1, "row-250" ∈ region 2, "row-350" ∈ region 3.
    String[] rows = {"row-050", "row-150", "row-250", "row-350"};
    try (Table table = conn.getTable(tn)) {
      Instant deadline = Instant.now().plus(OP_TIMEOUT);
      for (String row : rows) {
        if (Instant.now().isAfter(deadline)) {
          fail("Put burst exceeded " + OP_TIMEOUT);
        }
        Put p = new Put(row.getBytes(StandardCharsets.UTF_8));
        p.addColumn(CF, QUAL, VAL);
        table.put(p);
      }
    }
  }

  private static void waitForLogs(long baseline, int expectedDelta, Duration grace)
      throws Exception {
    Instant deadline = Instant.now().plus(grace);
    while (Instant.now().isBefore(deadline)) {
      if (countStartedLogs() - baseline >= expectedDelta) {
        return;
      }
      Thread.sleep(250);
    }
  }

  /**
   * Counts {@code GoProcess started:} lines across the container's entire log history (emitted by
   * {@code GoProcess.start()} once per spawned process). A shared runtime should produce one such
   * line for the whole 4-region test; a per-region runtime would produce four.
   */
  private static long countStartedLogs() throws IOException, InterruptedException {
    return runDockerCount("docker logs " + CONTAINER_NAME + " 2>&1 | grep -c 'GoProcess started:'");
  }

  private static long countStartedLogsQuiet() {
    try {
      return countStartedLogs();
    } catch (Exception e) {
      return -1L;
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

  private static long runDockerCount(String shellCommand) throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder("sh", "-c", shellCommand);
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
    if (!proc.waitFor(10, TimeUnit.SECONDS)) {
      proc.destroyForcibly();
      throw new IOException("docker count helper timed out");
    }
    int code = proc.exitValue();
    if (code != 0 && code != 1) {
      throw new IOException("docker count helper exited " + code);
    }
    try {
      return Long.parseLong(out);
    } catch (NumberFormatException e) {
      throw new IOException("unexpected docker count output: '" + out + "'", e);
    }
  }
}
