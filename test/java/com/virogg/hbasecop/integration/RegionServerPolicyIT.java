// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

/**
 * T52 integration test: exercises the RegionServerObserver {@code preRollWALWriterRequest} hook
 * end-to-end through the {@code rs-policy-observer} coproc-jar on a live HBase 2.5 standalone
 * cluster.
 *
 * <p>A RegionServerObserver is registered cluster-wide via {@code
 * hbase.coprocessor.regionserver.classes}; the docker entrypoint patches that into hbase-site.xml
 * when {@code make test-integration-rs} exports {@code HBASECOP_RS_COPROC_CLASS}. The Go observer
 * rejects every WAL-writer roll (the {@code veto_wal_roll} policy is enabled for this run) by
 * returning an error, which the strict-by-default {@code RegionServerObserverAdapter} surfaces as
 * an {@link java.io.IOException} back to the HBase admin client.
 *
 * <p>The {@code preStopRegionServer} hook named in T52's AC cannot be triggered without tearing
 * down the standalone cluster's only RegionServer, so this test drives the region-server surface
 * through {@code Admin#rollWALWriter(ServerName)} - a clean, idempotent admin operation that
 * invokes {@code preRollWALWriterRequest} on the live RegionServer.
 *
 * <p>The test asserts that {@code rollWALWriter} fails with an exception carrying the observer's
 * policy message.
 *
 * <p>Not part of {@code mvn test}; invoked by {@code make test-integration-rs}.
 */
final class RegionServerPolicyIT {

  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";

  @Test
  void vetoedWalRollRejected() throws Exception {
    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "6");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");

    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));

      ServerName rs = firstRegionServer(admin);

      Exception rejected =
          assertThrows(
              Exception.class,
              () -> admin.rollWALWriter(rs),
              "rollWALWriter must fail while the region-server observer vetoes WAL rolls");
      String msg = rootMessage(rejected);
      assertTrue(
          msg.contains("region-server policy") || msg.contains("WAL writer roll rejected"),
          "rejection should carry the observer's policy message - got: " + msg);
    }
  }

  private static ServerName firstRegionServer(Admin admin) throws Exception {
    Collection<ServerName> servers = admin.getRegionServers();
    if (servers.isEmpty()) {
      throw new IllegalStateException("no live RegionServers reported by the cluster");
    }
    return servers.iterator().next();
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
}
