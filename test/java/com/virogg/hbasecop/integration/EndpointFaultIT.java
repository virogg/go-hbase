// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.junit.jupiter.api.Test;

/**
 * TE24 fault matrix for Tier 2 endpoints, end-to-end on a live cluster. Proves the two failure
 * modes surface as clean client errors (not hangs) and that the coprocessor stays usable:
 *
 * <ul>
 *   <li><b>panic</b>: a panic inside the Go endpoint handler is recovered by the SDK into an
 *       endpoint error to the client; the shared Go process stays alive (a follow-up call works).
 *   <li><b>exit</b>: the Go process crashes mid-call; the blocked client call fails promptly via
 *       the supervisor's crash path, and the process restarts so a later call succeeds again.
 * </ul>
 *
 * <p>Not in {@code mvn test}; run via {@code make test-integration-endpoint}.
 */
final class EndpointFaultIT {

  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/endpoint-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/endpoint-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);

  @Test
  void panicSurfacesAsClientErrorAndProcessSurvives() throws Throwable {
    withEndpointTable(
        (table, tn) -> {
          // A panicking endpoint must fail the call, not hang or crash the process.
          IOException err =
              assertThrows(
                  IOException.class, () -> callMethod(table, "panic", "x"), "panic must error");
          assertTrue(err.getMessage().toLowerCase().contains("panic"), err.getMessage());

          // The shared Go process survived: a normal call still works immediately.
          assertArrayEquals(
              "HELLO".getBytes(StandardCharsets.UTF_8), callMethod(table, "upper", "hello"));
        });
  }

  @Test
  void crashMidCallFailsPromptlyThenRecovers() throws Throwable {
    withEndpointTable(
        (table, tn) -> {
          // os.Exit(1) in the handler crashes the Go process; the blocked call must fail (not
          // hang).
          assertThrows(Exception.class, () -> callMethod(table, "exit", "x"), "crash must error");

          // The supervisor restarts the process; a normal call recovers within the deadline.
          Instant deadline = Instant.now().plus(Duration.ofSeconds(40));
          Throwable last = null;
          while (Instant.now().isBefore(deadline)) {
            try {
              byte[] out = callMethod(table, "upper", "hi");
              assertArrayEquals("HI".getBytes(StandardCharsets.UTF_8), out);
              return;
            } catch (Throwable t) {
              last = t;
              Thread.sleep(1_000);
            }
          }
          throw new AssertionError("endpoint did not recover after a crash", last);
        });
  }

  // --- harness --------------------------------------------------------------

  @FunctionalInterface
  private interface TableTest {
    void run(Table table, TableName tn) throws Throwable;
  }

  private static void withEndpointTable(TableTest test) throws Throwable {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make endpoint-observer-jar`)");

    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "10");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");

    TableName tn = TableName.valueOf("hbasecop_endpoint_fault_it");
    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {
      waitForClusterReady(admin, Duration.ofSeconds(300));
      dropTable(admin, tn);
      createTableWithCoproc(admin, tn);
      try (Table table = conn.getTable(tn)) {
        test.run(table, tn);
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  /** Invokes GoEndpointService.Call(method) over the single region; throws IOException on error. */
  private static byte[] callMethod(Table table, String method, String payload) throws Throwable {
    GoEndpointRequest request =
        GoEndpointRequest.newBuilder()
            .setMethod(method)
            .setPayload(ByteString.copyFromUtf8(payload))
            .build();
    Map<byte[], byte[]> perRegion =
        table.coprocessorService(
            GoEndpointService.class,
            null,
            null,
            instance -> {
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
