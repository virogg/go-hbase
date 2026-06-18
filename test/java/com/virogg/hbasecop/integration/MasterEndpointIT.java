// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.client.AdminEndpointClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

/**
 * TE43/TE52 integration test: the generic {@code GoEndpointService} exposed by the stock {@code
 * GenericMasterObserver} via {@code MasterCoprocessor.getServices()} (TE43) is invokable through
 * the TE52 {@link com.virogg.hbasecop.client.AdminEndpointClient#callMaster} helper over {@code
 * Admin.coprocessorService()} (master-scoped, no region), and the call round-trips to the Go
 * endpoint and back. This is the live verification for both the master-endpoint infrastructure
 * (TE43) and the single-call admin helper (TE52).
 *
 * <p>The master coprocessor is registered cluster-wide via {@code hbase.coprocessor.master.classes}
 * (the docker entrypoint patches hbase-site.xml when {@code make test-integration-master-endpoint}
 * exports {@code HBASECOP_MASTER_COPROC_CLASS=...GenericMasterObserver} + {@code
 * HBASECOP_MASTER_COPROC_JAR=...endpoint-observer.jar}). A master endpoint carries region_id 0, so
 * region-local reverse reads/writes are unavailable there (A-12); a stateless method ("upper") is
 * the canonical master-endpoint shape.
 *
 * <p>Not part of {@code mvn test}; invoked by {@code make test-integration-master-endpoint}.
 */
final class MasterEndpointIT {

  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/endpoint-observer.jar";

  @Test
  void masterEndpointCallRoundTripsToGo() throws Throwable {
    requireStagedJar();

    try (Connection conn = ConnectionFactory.createConnection(clientConfig());
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));

      // AdminEndpointClient.callMaster (TE52) makes a single call (one master, no fan-out) over
      // the master's unshaded protobuf channel from Admin.coprocessorService() — the channel the
      // proto2 GoEndpointService stub binds to — and unwraps the result.
      byte[] result =
          AdminEndpointClient.callMaster(admin, "upper", "master-hello".getBytes(UTF_8));
      assertEquals(
          "MASTER-HELLO",
          new String(result, UTF_8),
          "master endpoint must round-trip to the Go handler and upper-case the payload");
    }
  }

  private static void requireStagedJar() {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make test-integration-master-endpoint`)");
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
}
