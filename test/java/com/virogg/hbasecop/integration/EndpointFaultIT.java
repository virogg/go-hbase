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

/**
 * TE24/TE54 fault matrix for Tier 2 endpoints, end-to-end on a live cluster. Every case has a
 * deterministic outcome: a clean client error (never a hang) and a coprocessor that stays usable.
 *
 * <ul>
 *   <li><b>panic</b>: a panic in the Go handler is recovered by the SDK into an endpoint error; the
 *       shared Go process stays alive (a follow-up call works).
 *   <li><b>crash mid-invoke</b> (exit): the Go process crashes mid-call; the blocked call fails
 *       promptly via the supervisor crash path and the process restarts so a later call succeeds.
 *   <li><b>crash mid-scan</b> (scan-leak): a crash leaves a server-side scanner open; the bridge
 *       reaps it on the crash path so the region is not wedged, and a later scan works.
 *   <li><b>long endpoint vs watchdog</b>: a call far longer than the heartbeat-miss deadline does
 *       NOT trigger a false restart (the heartbeat runs on its own goroutine; handler-pinning).
 *   <li><b>admission cap</b>: over {@code max-concurrent-calls} concurrent calls fail fast, the
 *       in-flight ones still complete.
 *   <li><b>scanner-per-call cap</b>: over {@code max-scanners-per-call} scanners in one call is a
 *       clean error; the region stays usable.
 *   <li><b>scan caps batch</b>: {@code max-rows-per-next}/{@code max-bytes-per-resp} batch a scan
 *       across SCAN_NEXT rather than truncate it (the full count still comes back).
 *   <li><b>oversized row</b>: a row larger than the ring slot surfaces a clean error, not a crash
 *       loop.
 * </ul>
 *
 * <p>idle-lease eviction and the narrow scanner crash-vs-register races stay unit-proven (a
 * deterministic live IT for either would be timing-racy); see {@code tasks/tier2-endpoints.md}.
 *
 * <p>Not in {@code mvn test}; run via {@code make test-integration-endpoint} / {@code make
 * test-integration-endpoint-all}.
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

          // scan-leak opens a server-side scanner, pulls one batch, then crashes WITHOUT closing
          // it.
          assertThrows(
              Exception.class, () -> callMethod(table, "scan-leak", ""), "crash must error");

          // The bridge reaps the orphaned RegionScanner on the crash path (so no read point leaks
          // to block compaction). After restart a full scan must work — proving the region is not
          // wedged by a leaked scanner.
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
          // 5s is far longer than the heartbeat-miss deadline (3 x 500ms = 1.5s). The heartbeat
          // runs on its own Go goroutine, independent of the per-invoke handler, so the watchdog
          // must NOT false-restart: the in-flight call returns ok rather than dying on a crash.
          assertArrayEquals("ok".getBytes(StandardCharsets.UTF_8), callMethod(table, "slow", "5s"));
          // Process healthy: a normal call works immediately.
          assertArrayEquals(
              "HELLO".getBytes(StandardCharsets.UTF_8), callMethod(table, "upper", "hello"));
          // And no restart happened during the long call (start-log count unchanged).
          assertEquals(
              startsBefore,
              goProcessStartCount(),
              "a long endpoint must not trigger a false heartbeat-miss restart");
        });
  }

  @Test
  void admissionCapRejectsSurplusConcurrentCalls() throws Throwable {
    withEndpointTable(
        Map.of("hbasecop.endpoint.max-concurrent-calls", "2"),
        (table, tn) -> {
          ExecutorService pool = Executors.newFixedThreadPool(4);
          try {
            // Hold both admission permits with two slow calls.
            Future<byte[]> h1 = pool.submit(() -> callQuietly(table, "slow", "6s"));
            Future<byte[]> h2 = pool.submit(() -> callQuietly(table, "slow", "6s"));
            Thread.sleep(1_500); // let both acquire permits and be in-flight

            // A third concurrent call must be rejected fast by admission, not queued/hung.
            IOException err =
                assertThrows(IOException.class, () -> callMethod(table, "upper", "x"));
            assertTrue(err.getMessage().contains("max-concurrent-calls"), err.getMessage());

            // The two held calls still complete successfully.
            assertArrayEquals("ok".getBytes(StandardCharsets.UTF_8), h1.get(30, TimeUnit.SECONDS));
            assertArrayEquals("ok".getBytes(StandardCharsets.UTF_8), h2.get(30, TimeUnit.SECONDS));
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
          // manyscan opens scanners until the per-call cap (2) is exceeded -> clean error.
          IOException err =
              assertThrows(IOException.class, () -> callMethod(table, "manyscan", "3"));
          assertTrue(err.getMessage().contains("max scanners per call exceeded"), err.getMessage());
          // Region still usable: a normal single-scanner scan works.
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
          // A 2-row-per-NEXT cap + small byte ceiling must BATCH the scan across multiple
          // SCAN_NEXT (has_more) and still count every row — caps batch, they do not truncate.
          assertEquals(
              Integer.toString(rows),
              new String(callMethod(table, "scan", ""), StandardCharsets.UTF_8));
        });
  }

  @Test
  void oversizedRowSurfacesCleanError() throws Throwable {
    withEndpointTable(
        (table, tn) -> {
          // Build ONE row larger than the 1 MiB ring slot, as several sub-slot cells: each Put
          // forwards through the observer pipeline fine (200 KiB < slot), but the accumulated row
          // exceeds the slot when a server-side scan tries to ship it in one SCAN_NEXT reply.
          byte[] chunk = new byte[200 * 1024];
          Arrays.fill(chunk, (byte) 'x');
          for (int i = 0; i < 8; i++) { // 8 x 200 KiB = 1.6 MiB row > 1 MiB slot
            table.put(
                new Put("big".getBytes(StandardCharsets.UTF_8))
                    .addColumn(CF, ("c" + i).getBytes(StandardCharsets.UTF_8), chunk));
          }

          // A server-side scan of it must surface a clean "exceeds ring slot" error, not hang.
          IOException err = assertThrows(IOException.class, () -> callMethod(table, "scan", ""));
          assertTrue(err.getMessage().contains("exceeds ring slot"), err.getMessage());

          // The process is not crash-looped: a non-scanning call still works.
          assertArrayEquals(
              "OK".getBytes(StandardCharsets.UTF_8), callMethod(table, "upper", "ok"));
        });
  }

  // --- harness --------------------------------------------------------------

  @FunctionalInterface
  private interface TableTest {
    void run(Table table, TableName tn) throws Throwable;
  }

  private static void withEndpointTable(TableTest test) throws Throwable {
    withEndpointTable(Map.of(), test);
  }

  /**
   * Boots a single-region endpoint table whose coprocessor carries {@code coprocProps} (e.g.
   * per-test endpoint caps), runs the test, and drops the table. Each test owns its table, so its
   * caps drive a fresh shared runtime.
   */
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

  /** Invokes {@link #callMethod} but wraps any throwable so it can run inside an executor task. */
  private static byte[] callQuietly(Table table, String method, String payload) {
    try {
      return callMethod(table, method, payload);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /**
   * Counts {@code GoProcess started:} lines in the container log — one per spawned/respawned Go
   * runtime. An unchanged count across a call window proves no restart occurred.
   */
  private static long goProcessStartCount() throws IOException, InterruptedException {
    ProcessBuilder pb =
        new ProcessBuilder(
            "sh", "-c", "docker logs go-hbase-dev 2>&1 | grep -c 'GoProcess started:'");
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

  private static void createTableWithCoproc(
      Admin admin, TableName tn, Map<String, String> coprocProps) throws IOException {
    CoprocessorDescriptorBuilder coproc =
        CoprocessorDescriptorBuilder.newBuilder(COPROC_CLASSNAME)
            .setJarPath(COPROC_JAR_IN_CONTAINER)
            .setPriority(0);
    // Endpoint caps (e.g. max-concurrent-calls) are read into the shared runtime config from the
    // coprocessor properties; setting them per-table here gives each test its own caps.
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
