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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.junit.jupiter.api.Test;

/**
 * TE22 integration test: a client invokes the generic {@code GoEndpointService} exposed by the
 * stock {@code GenericRegionObserver} via {@code Table.coprocessorService}, and the call
 * round-trips to a Go {@code Endpoint} (the endpoint-observer example, which upper-cases its
 * payload) and back.
 *
 * <p>Exercises the UNSHADED {@code com.google.protobuf} endpoint path end-to-end: an unshaded
 * {@link ServerRpcController} + an unshaded {@link RpcCallback}, matching HBase's {@code
 * Coprocessor.getServices()} contract. Not in {@code mvn test}; run via {@code make
 * test-integration-endpoint}, which manages the cluster and stages the coproc-jar.
 */
final class EndpointRoundTripIT {

  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/endpoint-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/endpoint-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);

  @Test
  void clientEndpointCallRoundTripsToGo() throws Throwable {
    requireStagedJar();
    TableName tn = TableName.valueOf("hbasecop_endpoint_it");

    try (Connection conn = ConnectionFactory.createConnection(clientConfig());
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));
      dropTable(admin, tn);
      createTableWithCoproc(admin, tn);
      try (Table table = conn.getTable(tn)) {
        byte[] result = callEndpoint(table, "upper", ByteString.copyFromUtf8("hello"));
        assertEquals(
            "HELLO",
            new String(result, StandardCharsets.UTF_8),
            "endpoint must round-trip to the Go handler and upper-case the payload");
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  /**
   * TE31: a client invokes the endpoint method "get", whose Go handler issues a reverse-RPC GET
   * back into the region for the row named by the payload and returns the cell value. Proves the
   * reverse channel end to end on a live cluster: seed a row, reverse-GET it, assert the value.
   */
  @Test
  void clientReverseGetRoundTrips() throws Throwable {
    requireStagedJar();
    TableName tn = TableName.valueOf("hbasecop_endpoint_revget_it");
    byte[] row = "row-1".getBytes(StandardCharsets.UTF_8);
    byte[] qualifier = "q".getBytes(StandardCharsets.UTF_8);
    byte[] value = "reverse-value".getBytes(StandardCharsets.UTF_8);

    try (Connection conn = ConnectionFactory.createConnection(clientConfig());
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));
      dropTable(admin, tn);
      createTableWithCoproc(admin, tn);
      try (Table table = conn.getTable(tn)) {
        table.put(new Put(row).addColumn(CF, qualifier, value));

        byte[] result = callEndpoint(table, "get", ByteString.copyFrom(row));
        assertEquals(
            new String(value, StandardCharsets.UTF_8),
            new String(result, StandardCharsets.UTF_8),
            "endpoint reverse GET must read the seeded row's value from the region");
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  /**
   * TE32: data-dependent reverse reads. The "follow" endpoint reads row A, takes its cf:next cell
   * as a pointer to row B, reads B, and returns B's cf:val — proving "read A → read B by a key from
   * A" over two correlated reverse round-trips within one endpoint call.
   */
  @Test
  void clientReverseGetDataDependent() throws Throwable {
    requireStagedJar();
    TableName tn = TableName.valueOf("hbasecop_endpoint_follow_it");
    byte[] rowA = "a".getBytes(StandardCharsets.UTF_8);
    byte[] rowB = "b".getBytes(StandardCharsets.UTF_8);
    byte[] next = "next".getBytes(StandardCharsets.UTF_8);
    byte[] val = "val".getBytes(StandardCharsets.UTF_8);
    byte[] deep = "deep-value".getBytes(StandardCharsets.UTF_8);

    try (Connection conn = ConnectionFactory.createConnection(clientConfig());
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));
      dropTable(admin, tn);
      createTableWithCoproc(admin, tn);
      try (Table table = conn.getTable(tn)) {
        table.put(new Put(rowA).addColumn(CF, next, rowB)); // a: cf:next -> "b"
        table.put(new Put(rowB).addColumn(CF, val, deep)); // b: cf:val -> "deep-value"

        byte[] result = callEndpoint(table, "follow", ByteString.copyFrom(rowA));
        assertEquals(
            new String(deep, StandardCharsets.UTF_8),
            new String(result, StandardCharsets.UTF_8),
            "follow must read A's cf:next pointer, then return B's cf:val (data-dependent)");
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

  /**
   * Invokes GoEndpointService.Call(method, payload) over every region, returning the sole result.
   */
  private static byte[] callEndpoint(Table table, String method, ByteString payload)
      throws Throwable {
    GoEndpointRequest request =
        GoEndpointRequest.newBuilder().setMethod(method).setPayload(payload).build();

    Map<byte[], byte[]> perRegion =
        table.coprocessorService(
            GoEndpointService.class,
            null,
            null,
            instance -> {
              // Unshaded controller + callback: HBase's BlockingRpcCallback is shaded and would
              // not bind to the unshaded com.google.protobuf.Service generated stub.
              ServerRpcController controller = new ServerRpcController();
              AtomicReference<GoEndpointResponse> out = new AtomicReference<>();
              RpcCallback<GoEndpointResponse> done = out::set;
              instance.call(controller, request, done);
              if (controller.failed()) {
                throw new IOException("endpoint controller failed: " + controller.errorText());
              }
              GoEndpointResponse resp = out.get();
              if (resp == null) {
                throw new IOException("endpoint returned no response");
              }
              if (!resp.getError().isEmpty()) {
                throw new IOException("endpoint error: " + resp.getError());
              }
              return resp.getPayload().toByteArray();
            });

    assertEquals(
        1, perRegion.size(), "single-region table should yield exactly one endpoint result");
    return perRegion.values().iterator().next();
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
