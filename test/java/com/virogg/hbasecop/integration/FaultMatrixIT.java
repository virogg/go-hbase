// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * T36 fault-injection matrix: per-hook policy ({@code strict | best-effort}) crossed with fault
 * mode ({@code kill-9 | hang | exit-1 | protocol-error | oom}) against a live HBase 2.5 cluster
 * (T26 docker-compose target) wired to the {@code fault-observer} coproc-jar (T36 foundation). Per
 * case asserts:
 *
 * <ul>
 *   <li>(a) semantic correctness: {@code strict} makes the client see {@link IOException}; {@code
 *       best-effort} lets the {@code Put} complete normally;
 *   <li>(b) no data loss / no double-apply: post-state scan row count matches the policy (0 for
 *       strict, 1 for best-effort);
 *   <li>(c) supervisor recovery: a follow-up {@code Put} after the restart deadline completes in
 *       finite time with the same per-policy outcome (no hang).
 * </ul>
 *
 * <p>Like {@code PrePutCounterIT}, not part of {@code mvn test} (the {@code *IT} suffix keeps
 * Surefire's default includes from picking it up). Runs via {@code make test-fault}, which manages
 * the cluster lifecycle and stages {@code test/integration/coproc-jars/fault-observer.jar} into the
 * bind-mount the container reads from.
 */
final class FaultMatrixIT {

  private static final String CONTAINER_NAME = "go-hbase-dev";
  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/fault-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/fault-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.examples.fault.FaultRegionObserver";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);
  private static final byte[] QUAL = "q".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VAL = "v".getBytes(StandardCharsets.UTF_8);
  private static final Duration RESTART_DEADLINE = Duration.ofSeconds(3);
  private static final Duration RECOVERY_SLACK = Duration.ofSeconds(2);

  private static final List<Outcome> RESULTS = new CopyOnWriteArrayList<>();

  /** Cartesian product (policy x mode): 10 cases (2 x 5); satisfies T36's {@code >=10}. */
  static Stream<Arguments> matrix() {
    List<String> policies = List.of("strict", "best-effort");
    List<String> modes = List.of("kill-9", "hang", "exit-1", "protocol-error", "oom");
    List<Arguments> rows = new ArrayList<>();
    for (String p : policies) {
      for (String m : modes) {
        rows.add(Arguments.of(p, m));
      }
    }
    return rows.stream();
  }

  @ParameterizedTest(name = "{0} × {1}")
  @MethodSource("matrix")
  void faultCase(String policy, String mode) throws Exception {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make fault-observer-jar` and copy into test/integration/coproc-jars/)");

    TableName tn = caseTableName(policy, mode);
    Configuration cfg = baseConfig();

    Outcome outcome = new Outcome(policy, mode);
    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));

      if (admin.tableExists(tn)) {
        dropTable(admin, tn);
      }

      createTableWithFault(admin, tn, policy, mode);
      try {
        outcome.firstPut = runOnePut(conn, tn, "row-1");

        // Let the supervisor detect death and finish a restart cycle for crash modes.
        Thread.sleep(RESTART_DEADLINE.plus(RECOVERY_SLACK).toMillis());

        outcome.secondPut = runOnePut(conn, tn, "row-2");
        outcome.rowsAfter = countRows(conn, tn);

        assertSemantics(policy, mode, outcome);
        assertPostState(policy, outcome);
      } finally {
        dropTable(admin, tn);
        RESULTS.add(outcome);
      }
    }
  }

  @AfterAll
  static void printReport() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("+--------------+----------------+-------------+-------------+------+\n");
    sb.append("| policy       | mode           | put #1      | put #2      | rows |\n");
    sb.append("+--------------+----------------+-------------+-------------+------+\n");
    for (Outcome o : RESULTS) {
      sb.append(
          String.format(
              Locale.ROOT,
              "| %-12s | %-14s | %-11s | %-11s | %4d |%n",
              o.policy,
              o.mode,
              renderPut(o.firstPut),
              renderPut(o.secondPut),
              o.rowsAfter));
    }
    sb.append("+--------------+----------------+-------------+-------------+------+\n");
    System.out.println(sb);
  }

  private static String renderPut(PutOutcome p) {
    if (p == null) {
      return "-";
    }
    return p.threw ? "IOException" : "ok";
  }

  private static void assertSemantics(String policy, String mode, Outcome o) {
    PutOutcome first = o.firstPut;
    PutOutcome second = o.secondPut;
    switch (policy) {
      case "strict":
        assertTrue(first.threw, "strict × " + mode + ": Put #1 must throw, got " + first.summary());
        assertTrue(
            second.threw,
            "strict × "
                + mode
                + ": Put #2 must throw (fault still active), got "
                + second.summary());
        break;
      case "best-effort":
        assertTrue(
            !first.threw,
            "best-effort × " + mode + ": Put #1 must succeed, got " + first.summary());
        assertTrue(
            !second.threw,
            "best-effort × " + mode + ": Put #2 must succeed, got " + second.summary());
        break;
      default:
        fail("unknown policy: " + policy);
    }
  }

  private static void assertPostState(String policy, Outcome o) {
    int expected = "strict".equals(policy) ? 0 : 2;
    assertEquals(
        expected,
        o.rowsAfter,
        policy + " × " + o.mode + ": post-state row count mismatch (expected " + expected + ")");
  }

  private static TableName caseTableName(String policy, String mode) {
    String slug = (policy + "_" + mode).replace("-", "");
    return TableName.valueOf("hbasecop_fault_" + slug);
  }

  private static Configuration baseConfig() {
    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    // Keep client retries low so per-case wall time stays bounded.
    cfg.set("hbase.client.retries.number", "2");
    cfg.set("hbase.client.pause", "200");
    cfg.set("hbase.rpc.timeout", "15000");
    cfg.set("hbase.client.operation.timeout", "20000");
    cfg.set("hbase.client.meta.operation.timeout", "30000");
    return cfg;
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

  private static void createTableWithFault(Admin admin, TableName tn, String policy, String mode)
      throws IOException {
    TableDescriptorBuilder b =
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            // Per-table policy and fault-mode keys; the bridge / fault-observer read them
            // from env.getConfiguration() at coprocessor start.
            .setValue("hbasecop.policy.prePut", policy)
            .setValue("hbasecop.fault.mode", mode);
    b.setCoprocessor(
        CoprocessorDescriptorBuilder.newBuilder(COPROC_CLASSNAME)
            .setJarPath(COPROC_JAR_IN_CONTAINER)
            .setPriority(0)
            .build());
    admin.createTable(b.build());
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

  private static PutOutcome runOnePut(Connection conn, TableName tn, String rowKey) {
    try (Table table = conn.getTable(tn)) {
      Put p = new Put(rowKey.getBytes(StandardCharsets.UTF_8));
      p.addColumn(CF, QUAL, VAL);
      table.put(p);
      return new PutOutcome(false, null);
    } catch (Throwable t) {
      return new PutOutcome(true, t);
    }
  }

  private static int countRows(Connection conn, TableName tn) throws IOException {
    int n = 0;
    try (Table table = conn.getTable(tn);
        ResultScanner scanner = table.getScanner(new Scan().addFamily(CF))) {
      for (Result r : scanner) {
        if (!r.isEmpty()) {
          n++;
        }
      }
    }
    return n;
  }

  /** Per-Put outcome captured for both the report and the assertions. */
  private static final class PutOutcome {
    final boolean threw;
    final Throwable cause;

    PutOutcome(boolean threw, Throwable cause) {
      this.threw = threw;
      this.cause = cause;
    }

    String summary() {
      if (!threw) {
        return "ok";
      }
      return cause == null ? "threw" : cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
  }

  private static final class Outcome {
    final String policy;
    final String mode;
    PutOutcome firstPut;
    PutOutcome secondPut;
    int rowsAfter;

    Outcome(String policy, String mode) {
      this.policy = policy;
      this.mode = mode;
    }
  }
}
