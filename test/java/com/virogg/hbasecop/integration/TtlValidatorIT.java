// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * T73 integration test: strict-mode validation on a live HBase 2.5 standalone cluster. A Put whose
 * value carries the {@code ttl=<seconds>;} envelope succeeds and is readable; a Put without it is
 * rejected by the Go observer - the client sees an {@link IOException} and no row is written.
 *
 * <p>Invoked by {@code make test-integration-ttl}, which stages {@code ttl-validator.jar} into the
 * container bind-mount and manages the cluster lifecycle. Not part of {@code mvn test}.
 */
final class TtlValidatorIT {

  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/ttl-validator.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/ttl-validator.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.examples.ttl.TtlValidatorRegionObserver";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);
  private static final byte[] QUAL = "q".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VALID_VALUE = "ttl=3600;hello".getBytes(StandardCharsets.UTF_8);
  private static final byte[] INVALID_VALUE = "no-ttl-envelope".getBytes(StandardCharsets.UTF_8);

  @Test
  void validPutSucceedsInvalidPutAbortsStrict() throws Exception {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make ttl-validator-jar` and copy into test/integration/coproc-jars/)");

    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", "localhost");
    cfg.set("hbase.zookeeper.property.clientPort", "2181");
    // The invalid Put fails deterministically on every attempt (validation,
    // not a transient fault) - keep retries low so the negative case is fast.
    cfg.set("hbase.client.retries.number", "2");
    cfg.set("hbase.client.pause", "300");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");

    TableName tn = TableName.valueOf("hbasecop_ttl_it");

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
        // Valid Put: envelope present → accepted, readable.
        byte[] validRow = "row-valid".getBytes(StandardCharsets.UTF_8);
        Put valid = new Put(validRow);
        valid.addColumn(CF, QUAL, VALID_VALUE);
        table.put(valid);

        Result r = table.get(new Get(validRow));
        assertArrayEquals(VALID_VALUE, r.getValue(CF, QUAL), "valid row must be written intact");

        // Invalid Put: no envelope → Go rejects, strict pre-hook policy →
        // IOException at the client (RetriesExhaustedWithDetailsException
        // after the client gives up retrying the deterministic failure).
        byte[] invalidRow = "row-invalid".getBytes(StandardCharsets.UTF_8);
        Put invalid = new Put(invalidRow);
        invalid.addColumn(CF, QUAL, INVALID_VALUE);
        assertThrows(IOException.class, () -> table.put(invalid), "invalid Put must abort");

        // The strict abort must leave NO partial state behind.
        assertTrue(
            table.get(new Get(invalidRow)).isEmpty(),
            "rejected Put must not write any cell (no data loss / no partial apply)");
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
}
