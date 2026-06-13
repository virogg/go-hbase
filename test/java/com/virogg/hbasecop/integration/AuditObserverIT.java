// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * T72 integration test: 25 Puts + 25 Deletes through the audit-observer coproc on a live HBase 2.5
 * standalone cluster → exactly 50 JSON audit records in the RegionServer log, none of them leaking
 * the raw row key (SPEC §8).
 *
 * <p>Invoked by {@code make test-integration-audit}, which stages {@code audit-observer.jar} into
 * the container bind-mount and manages the cluster lifecycle. Not part of {@code mvn test}.
 */
final class AuditObserverIT {

  private static final String CONTAINER_NAME = "go-hbase-dev";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/audit-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/audit-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.examples.audit.AuditRegionObserver";
  private static final String AUDIT_MARKER = "audit-observer: audit";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);
  private static final byte[] QUAL = "q".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VAL = "v".getBytes(StandardCharsets.UTF_8);
  private static final int N_OPS_PER_KIND = 25; // 25 Puts + 25 Deletes = 50 audit records
  private static final Duration LOG_GRACE = Duration.ofSeconds(10);

  @Test
  void fiftyOpsProduceFiftyAuditRecords() throws Exception {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make audit-observer-jar` and copy into test/integration/coproc-jars/)");

    long baseline = countAuditLogs();

    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", "localhost");
    cfg.set("hbase.zookeeper.property.clientPort", "2181");
    cfg.set("hbase.client.retries.number", "10");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");

    TableName tn = TableName.valueOf("hbasecop_audit_it");

    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));
      dropTableIfExists(admin, tn);
      admin.createTable(
          TableDescriptorBuilder.newBuilder(tn)
              .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
              .setCoprocessor(
                  CoprocessorDescriptorBuilder.newBuilder(COPROC_CLASSNAME)
                      .setJarPath(COPROC_JAR_IN_CONTAINER)
                      .setPriority(0)
                      .build())
              .build());

      try (Table table = conn.getTable(tn)) {
        for (int i = 0; i < N_OPS_PER_KIND; i++) {
          Put p = new Put(("row-" + i).getBytes(StandardCharsets.UTF_8));
          p.addColumn(CF, QUAL, VAL);
          table.put(p);
        }
        for (int i = 0; i < N_OPS_PER_KIND; i++) {
          table.delete(new Delete(("row-" + i).getBytes(StandardCharsets.UTF_8)));
        }

        int expected = 2 * N_OPS_PER_KIND;
        waitForLogs(baseline, expected, LOG_GRACE);
        long delta = countAuditLogs() - baseline;
        assertEquals(
            expected,
            delta,
            () -> "expected exactly " + expected + " audit records this run, got " + delta);

        // SPEC §8: audit records must not leak raw row keys.
        assertEquals(
            0L,
            countLogMatches(AUDIT_MARKER + ".*row-0\""),
            "an audit record leaked a raw row key");
      } finally {
        dropTableIfExists(admin, tn);
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
    Exception last = null;
    while (Instant.now().isBefore(cutoff)) {
      try {
        admin.listTableNames();
        return;
      } catch (Exception e) {
        last = e;
        Thread.sleep(1_000);
      }
    }
    throw new IllegalStateException("HBase cluster not ready within " + deadline, last);
  }

  private static void dropTableIfExists(Admin admin, TableName tn) throws IOException {
    if (!admin.tableExists(tn)) {
      return;
    }
    if (admin.isTableEnabled(tn)) {
      admin.disableTable(tn);
    }
    admin.deleteTable(tn);
  }

  private static void waitForLogs(long baseline, int expectedDelta, Duration grace)
      throws Exception {
    Instant deadline = Instant.now().plus(grace);
    while (Instant.now().isBefore(deadline)) {
      if (countAuditLogs() - baseline >= expectedDelta) {
        return;
      }
      Thread.sleep(250);
    }
  }

  private static long countAuditLogs() throws IOException, InterruptedException {
    return countLogMatches(AUDIT_MARKER);
  }

  /** Runs `docker logs {container} 2>&1 | grep -cE '{pattern}'`. */
  private static long countLogMatches(String pattern) throws IOException, InterruptedException {
    ProcessBuilder pb =
        new ProcessBuilder(
            "sh", "-c", "docker logs " + CONTAINER_NAME + " 2>&1 | grep -cE '" + pattern + "'");
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
      throw new IOException("docker logs grep timed out");
    }
    int code = proc.exitValue();
    if (code != 0 && code != 1) { // grep -c exits 1 on zero matches
      throw new IOException("docker logs grep exited " + code);
    }
    return Long.parseLong(out);
  }
}
