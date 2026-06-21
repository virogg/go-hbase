// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointResponse;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
          IOException err =
              assertThrows(
                  IOException.class, () -> callMethod(table, "panic", "x"), "panic must error");
          assertTrue(err.getMessage().toLowerCase().contains("panic"), err.getMessage());

          assertArrayEquals(
              "HELLO".getBytes(StandardCharsets.UTF_8), callMethod(table, "upper", "hello"));
        });
  }

  @Test
  void crashMidCallFailsPromptlyThenRecovers() throws Throwable {
    withEndpointTable(
        (table, tn) -> {
          assertThrows(Exception.class, () -> callMethod(table, "exit", "x"), "crash must error");

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

  @Test
  void scanCrashReapsScannerAndRecovers() throws Throwable {
    withEndpointTable(
        (table, tn) -> {
          int rows = 3;
          for (int i = 0; i < rows; i++) {
            table.put(
                new Put(("r-" + i).getBytes(StandardCharsets.UTF_8))
                    .addColumn(
                        CF,
                        "q".getBytes(StandardCharsets.UTF_8),
                        ("v" + i).getBytes(StandardCharsets.UTF_8)));
          }

          assertThrows(
              Exception.class, () -> callMethod(table, "scan-leak", ""), "crash must error");

          Instant deadline = Instant.now().plus(Duration.ofSeconds(40));
          Throwable last = null;
          while (Instant.now().isBefore(deadline)) {
            try {
              byte[] out = callMethod(table, "scan", "");
              assertEquals(Integer.toString(rows), new String(out, StandardCharsets.UTF_8));
              return;
            } catch (Throwable t) {
              last = t;
              Thread.sleep(1_000);
            }
          }
          throw new AssertionError("scan did not recover after a crash mid-scan", last);
        });
  }

  @Test
  void longEndpointDoesNotTripWatchdog() throws Throwable {
    withEndpointTable(
        (table, tn) -> {
          long startsBefore = goProcessStartCount();
          assertArrayEquals("ok".getBytes(StandardCharsets.UTF_8), callMethod(table, "slow", "5s"));
          assertArrayEquals(
              "HELLO".getBytes(StandardCharsets.UTF_8), callMethod(table, "upper", "hello"));
          assertEquals(
              startsBefore,
              goProcessStartCount(),
              "a long endpoint must not trigger a false heartbeat-miss restart");
        });
  }

  @Test
  void admissionCapRejectsSurplusConcurrentCalls() throws Throwable {
    withEndpointTable(
        Map.of(
            "hbasecop.endpoint.max-concurrent-calls", "1",
            "hbasecop.endpoint.timeout", "60s"),
        (table, tn) -> {
          long startsBefore = slowStartCount();
          ExecutorService pool = Executors.newSingleThreadExecutor();
          try {
            Future<byte[]> holder = pool.submit(() -> callQuietly(table, "slow", "20s"));
            waitForSlowStarts(startsBefore, 1, Duration.ofSeconds(30));

            IOException err =
                assertThrows(IOException.class, () -> callMethod(table, "upper", "x"));
            assertTrue(err.getMessage().contains("max-concurrent-calls"), err.getMessage());

            assertArrayEquals(
                "ok".getBytes(StandardCharsets.UTF_8), holder.get(60, TimeUnit.SECONDS));
          } finally {
            pool.shutdownNow();
          }
        });
  }

  @Test
  void scannerPerCallCapTripsCleanly() throws Throwable {
    withEndpointTable(
        Map.of("hbasecop.endpoint.max-scanners-per-call", "2"),
        (table, tn) -> {
          table.put(
              new Put("r".getBytes(StandardCharsets.UTF_8))
                  .addColumn(
                      CF,
                      "q".getBytes(StandardCharsets.UTF_8),
                      "v".getBytes(StandardCharsets.UTF_8)));
          IOException err =
              assertThrows(IOException.class, () -> callMethod(table, "manyscan", "3"));
          assertTrue(err.getMessage().contains("max scanners per call exceeded"), err.getMessage());
          assertEquals("1", new String(callMethod(table, "scan", ""), StandardCharsets.UTF_8));
        });
  }

  @Test
  void scanCapsBatchRatherThanTruncate() throws Throwable {
    int rows = 7;
    withEndpointTable(
        Map.of(
            "hbasecop.endpoint.max-rows-per-next", "2",
            "hbasecop.endpoint.max-bytes-per-resp", "4096"),
        (table, tn) -> {
          for (int i = 0; i < rows; i++) {
            table.put(
                new Put(("row-" + i).getBytes(StandardCharsets.UTF_8))
                    .addColumn(
                        CF,
                        "q".getBytes(StandardCharsets.UTF_8),
                        ("v" + i).getBytes(StandardCharsets.UTF_8)));
          }
          String[] stats =
              new String(callMethod(table, "scanstats", ""), StandardCharsets.UTF_8).split(",");
          assertEquals(
              rows, Integer.parseInt(stats[0]), "scan must return every row, not truncate");
          assertTrue(
              Integer.parseInt(stats[1]) > 1,
              () -> "max-rows-per-next=2 over 7 rows must force >1 SCAN_NEXT, got " + stats[1]);
        });
  }

  @Test
  void oversizedRowSurfacesCleanError() throws Throwable {
    withEndpointTable(
        (table, tn) -> {
          byte[] chunk = new byte[200 * 1024];
          Arrays.fill(chunk, (byte) 'x');
          for (int i = 0; i < 8; i++) {
            table.put(
                new Put("big".getBytes(StandardCharsets.UTF_8))
                    .addColumn(CF, ("c" + i).getBytes(StandardCharsets.UTF_8), chunk));
          }

          IOException err = assertThrows(IOException.class, () -> callMethod(table, "scan", ""));
          assertTrue(err.getMessage().contains("exceeds ring slot"), err.getMessage());

          assertArrayEquals(
              "OK".getBytes(StandardCharsets.UTF_8), callMethod(table, "upper", "ok"));
        });
  }

  @FunctionalInterface
  private interface TableTest {
    void run(Table table, TableName tn) throws Throwable;
  }

  private static void withEndpointTable(TableTest test) throws Throwable {
    withEndpointTable(Map.of(), test);
  }

  private static void withEndpointTable(Map<String, String> coprocProps, TableTest test)
      throws Throwable {
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
      createTableWithCoproc(admin, tn, coprocProps);
      try (Table table = conn.getTable(tn)) {
        test.run(table, tn);
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  private static byte[] callQuietly(Table table, String method, String payload) {
    try {
      return callMethod(table, method, payload);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private static long goProcessStartCount() throws IOException, InterruptedException {
    return dockerLogCount("GoProcess started:");
  }

  private static long slowStartCount() throws IOException, InterruptedException {
    return dockerLogCount("endpoint-observer: slow start");
  }

  private static void waitForSlowStarts(long baseline, int delta, Duration timeout)
      throws Exception {
    Instant cutoff = Instant.now().plus(timeout);
    while (Instant.now().isBefore(cutoff)) {
      if (slowStartCount() - baseline >= delta) {
        return;
      }
      Thread.sleep(250);
    }
    throw new AssertionError(
        "timed out waiting for " + delta + " 'slow start' log line(s) (the holder never acquired)");
  }

  private static long dockerLogCount(String needle) throws IOException, InterruptedException {
    ProcessBuilder pb =
        new ProcessBuilder("sh", "-c", "docker logs go-hbase-dev 2>&1 | grep -c '" + needle + "'");
    pb.redirectErrorStream(true);
    Process proc = pb.start();
    String out;
    try (BufferedReader r =
        new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
      out = r.readLine();
    }
    if (!proc.waitFor(10, TimeUnit.SECONDS)) {
      proc.destroyForcibly();
      throw new IOException("docker logs count timed out");
    }
    try {
      return Long.parseLong(out == null ? "0" : out.trim());
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

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

  private static void createTableWithCoproc(
      Admin admin, TableName tn, Map<String, String> coprocProps) throws IOException {
    CoprocessorDescriptorBuilder coproc =
        CoprocessorDescriptorBuilder.newBuilder(COPROC_CLASSNAME)
            .setJarPath(COPROC_JAR_IN_CONTAINER)
            .setPriority(0);
    coprocProps.forEach(coproc::setProperty);
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .setCoprocessor(coproc.build())
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
