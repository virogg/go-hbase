// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * T51 integration test: exercises the MasterObserver {@code preCreateTable} hook end-to-end through
 * the {@code master-policy-observer} coproc-jar on a live HBase 2.5 standalone cluster.
 *
 * <p>Unlike region/table coprocessors, a MasterObserver is registered cluster-wide via {@code
 * hbase.coprocessor.master.classes}; the docker entrypoint patches that into hbase-site.xml when
 * {@code make test-integration-master} exports {@code HBASECOP_MASTER_COPROC_CLASS}. The Go
 * observer rejects any table whose qualifier begins with {@code "forbidden-"} by returning an
 * error, which the strict-by-default {@code MasterObserverAdapter} surfaces as an {@link
 * IOException} back to the HBase admin client.
 *
 * <p>The test asserts:
 *
 * <ul>
 *   <li>{@code createTable("forbidden-…")} fails with an IOException carrying the observer's policy
 *       message, and the table is absent afterwards.
 *   <li>{@code createTable("ok-…")} succeeds (observer allowed it through).
 * </ul>
 *
 * <p>Not part of {@code mvn test}; invoked by {@code make test-integration-master}.
 */
final class MasterPolicyIT {

  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final byte[] CF = "cf".getBytes(java.nio.charset.StandardCharsets.UTF_8);
  private static final String BLOCKED_PREFIX = "forbidden-";
  private static final String ALLOWED_PREFIX = "ok-";

  @Test
  void blockedTableCreateRejected_allowedTableCreateSucceeds() throws Exception {
    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "6");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");

    TableName blocked = TableName.valueOf(BLOCKED_PREFIX + "users");
    TableName allowed = TableName.valueOf(ALLOWED_PREFIX + "users");

    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));

      dropQuietly(admin, blocked);
      dropQuietly(admin, allowed);

      // --- Blocked create: observer must reject it ------------------------
      IOException rejected =
          assertThrows(
              IOException.class,
              () -> admin.createTable(descriptor(blocked)),
              "createTable on a forbidden- table must fail with IOException");
      String msg = rootMessage(rejected);
      assertTrue(
          msg.contains("forbidden-users") || msg.contains("naming policy"),
          "rejection should carry the observer's policy message — got: " + msg);
      assertTrue(
          !admin.tableExists(blocked), "forbidden- table must not exist after a rejected create");

      // --- Allowed create: observer must let it through -------------------
      try {
        admin.createTable(descriptor(allowed));
        assertTrue(admin.tableExists(allowed), "ok- table must exist after an allowed create");
      } finally {
        dropQuietly(admin, allowed);
      }
    }
  }

  private static TableDescriptor descriptor(TableName tn) {
    return TableDescriptorBuilder.newBuilder(tn)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
        .build();
  }

  /** Flattens an exception chain into one searchable string (HBase wraps observer errors). */
  private static String rootMessage(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c.getMessage() != null) {
        sb.append(c.getMessage()).append(" | ");
      }
    }
    return sb.toString();
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

  private static void dropQuietly(Admin admin, TableName tn) {
    try {
      if (!admin.tableExists(tn)) {
        return;
      }
      if (admin.isTableEnabled(tn)) {
        admin.disableTable(tn);
      }
      admin.deleteTable(tn);
    } catch (IOException ignored) {
      // best effort — table may never have been created (rejected path)
    }
  }
}
