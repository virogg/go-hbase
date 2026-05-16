// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.Arrays;
import java.util.List;
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
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * T44 integration test: drives a {@code Table.batch(List<Put>)} of mixed {@code ok-*} / {@code
 * block-*} rows through the filter-observer coproc-jar on a live HBase 2.5 standalone cluster.
 * Asserts that the blocked rows fail individually (the observer's {@code preBatchMutate} marked
 * them via {@code HookResponse.blocked_indices}, which the Java adapter translated into per-index
 * {@code OperationStatus(SANITY_CHECK_FAILURE)}), while the {@code ok-*} rows succeed and are later
 * visible via {@code Get}.
 *
 * <p>Lifecycle managed by {@code make test-integration-batch}; not run by default {@code mvn test}.
 */
final class BatchPartialBlockIT {

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

  @Test
  void mixedBatchPartiallyBlocked() throws Exception {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make filter-observer-jar` and copy into test/integration/coproc-jars/)");

    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "10");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");

    TableName tn = TableName.valueOf("hbasecop_batch_it");

    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(60));

      if (admin.tableExists(tn)) {
        dropTable(admin, tn);
      }

      createTableWithCoproc(admin, tn);
      try (Table table = conn.getTable(tn)) {
        // Interleave 3 ok + 2 block. Indices: 0=ok, 1=ok, 2=block, 3=ok, 4=block.
        List<Row> batch = new ArrayList<>();
        batch.add(put(ALLOWED_PREFIX + "1"));
        batch.add(put(ALLOWED_PREFIX + "2"));
        batch.add(put(BLOCKED_PREFIX + "1"));
        batch.add(put(ALLOWED_PREFIX + "3"));
        batch.add(put(BLOCKED_PREFIX + "2"));

        Object[] results = new Object[batch.size()];
        try {
          table.batch(batch, results);
          fail(
              "Table.batch should have surfaced a retries-exhausted error for blocked indices — got"
                  + " no exception. results="
                  + Arrays.toString(results));
        } catch (IOException expected) {
          // Expected: blocked indices fail individually. The remaining "ok-*" indices
          // populated their results slot; the failed indices' slots are null.
        }

        // ok-* indices populate their slot with a non-null Result (success marker).
        // block-* indices stay null (failure marker).
        assertNotNull(results[0], "results[0] (ok-1) should be non-null on success");
        assertNotNull(results[1], "results[1] (ok-2) should be non-null on success");
        assertNull(results[2], "results[2] (block-1) should be null on per-index failure");
        assertNotNull(results[3], "results[3] (ok-3) should be non-null on success");
        assertNull(results[4], "results[4] (block-2) should be null on per-index failure");

        // Round-trip read-back: only the ok-* rows landed in storage. Note this exercises
        // the Get path too — and our filter-observer also bypasses Gets on block-* rows,
        // so we cannot distinguish "absent in storage" from "bypass returned empty" on
        // the block-* side. Asserting on the ok-* side is the load-bearing check.
        for (int i = 1; i <= 3; i++) {
          Result r = table.get(new Get((ALLOWED_PREFIX + i).getBytes(StandardCharsets.UTF_8)));
          assertNotNull(r, "Get(ok-" + i + ") should return a non-null Result");
          assertEquals(
              "v",
              new String(r.getValue(CF, QUAL), StandardCharsets.UTF_8),
              "ok-" + i + " should have been written by the batch");
        }
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  private static Put put(String row) {
    Put p = new Put(row.getBytes(StandardCharsets.UTF_8));
    p.addColumn(CF, QUAL, VAL);
    return p;
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
            .setValue(CONF_KEY_BLOCKED_PREFIX, BLOCKED_PREFIX)
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
}
