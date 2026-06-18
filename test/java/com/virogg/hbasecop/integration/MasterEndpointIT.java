// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointResponse;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.junit.jupiter.api.Test;

/**
 * TE43 integration test: the generic {@code GoEndpointService} exposed by the stock {@code
 * GenericMasterObserver} via {@code MasterCoprocessor.getServices()} is invokable through {@code
 * Admin.coprocessorService()} (master-scoped, no region), and the call round-trips to the Go
 * endpoint and back.
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

      // Admin.coprocessorService() targets the active master; its CoprocessorRpcChannel is the
      // UNSHADED com.google.protobuf channel the proto2 GoEndpointService stub binds to.
      CoprocessorRpcChannel channel = admin.coprocessorService();
      GoEndpointService.Stub stub = GoEndpointService.newStub(channel);
      ServerRpcController controller = new ServerRpcController();
      AtomicReference<GoEndpointResponse> out = new AtomicReference<>();
      RpcCallback<GoEndpointResponse> done = out::set;

      stub.call(
          controller,
          GoEndpointRequest.newBuilder()
              .setMethod("upper")
              .setPayload(ByteString.copyFromUtf8("master-hello"))
              .build(),
          done);

      if (controller.failed()) {
        throw new IOException("master endpoint controller failed: " + controller.errorText());
      }
      GoEndpointResponse resp = out.get();
      assertTrue(resp != null, "master endpoint returned no response");
      assertTrue(resp.getError().isEmpty(), () -> "master endpoint error: " + resp.getError());
      assertEquals(
          "MASTER-HELLO",
          resp.getPayload().toStringUtf8(),
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
