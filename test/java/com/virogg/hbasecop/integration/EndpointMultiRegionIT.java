// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.virogg.hbasecop.client.EndpointClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
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

final class EndpointMultiRegionIT {

  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/endpoint-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/endpoint-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver";
  private static final byte[] CF = "cf".getBytes(UTF_8);
  private static final byte[] N = "n".getBytes(UTF_8);
  private static final int N_REGIONS = 4;

  private static final String[] ROWS = {
    "row-010", "row-020", // region 0
    "row-110", "row-120", // region 1
    "row-210", "row-220", // region 2
    "row-310", "row-320", // region 3
  };
  private static final long TOTAL = 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8;

  @Test
  void fanOutSumReducesAcrossRegions() throws Throwable {
    requireStagedJar();
    TableName tn = TableName.valueOf("hbasecop_endpoint_multiregion_it");

    try (Connection conn = ConnectionFactory.createConnection(clientConfig());
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));
      dropTable(admin, tn);
      createPreSplitTableWithCoproc(admin, tn);
      try {
        waitForRegionCount(admin, tn, N_REGIONS, Duration.ofSeconds(60));
        try (Table table = conn.getTable(tn)) {
          for (int i = 0; i < ROWS.length; i++) {
            long v = i + 1;
            table.put(
                new Put(ROWS[i].getBytes(UTF_8))
                    .addColumn(CF, N, Long.toString(v).getBytes(UTF_8)));
          }

          Map<byte[], byte[]> perRegion = EndpointClient.callAllRegions(table, "sum", N);
          assertEquals(
              N_REGIONS,
              perRegion.size(),
              "sum endpoint must run once per region (true fan-out across all regions)");
          long sumOfPartials = 0;
          for (byte[] v : perRegion.values()) {
            long partial = Long.parseLong(new String(v, UTF_8));
            assertTrue(
                partial > 0 && partial < TOTAL,
                () -> "each region holds only part of the total, got partial that was not");
            sumOfPartials += partial;
          }
          assertEquals(TOTAL, sumOfPartials, "the per-region partials must add up to the total");

          long reduced =
              EndpointClient.callAndReduce(
                  table,
                  "sum",
                  N,
                  0L,
                  (acc, regionResult) -> acc + Long.parseLong(new String(regionResult, UTF_8)));
          assertEquals(TOTAL, reduced, "fan-out + reduce must yield the table-wide sum");
        }
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  private static void requireStagedJar() {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make endpoint-observer-jar` and copy into test/integration/coproc-jars/)");
  }

  private static Configuration clientConfig() {
    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "10");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");
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

  private static byte[][] splitKeys(int n) {
    byte[][] keys = new byte[n - 1][];
    for (int i = 1; i < n; i++) {
      keys[i - 1] = ("row-" + (i * 100)).getBytes(UTF_8);
    }
    return keys;
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
      last = admin.getRegions(tn).size();
      if (last >= expected) {
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
}
