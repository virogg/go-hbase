// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

/**
 * T84 soak driver: paced mixed Put/Get load against a live HBase 2.5 standalone cluster (the T26
 * docker-compose target) with the counter-observer coproc-jar attached. Proves the data-loss
 * invariant: every client-acked Put rowkey must appear in a full table scan at end of run. Table
 * registers the coprocessor with {@code hbasecop.policy.prePut = best-effort} so kill-9 chaos
 * windows (injected externally by {@code test/integration/scripts/soak.sh}) degrade the hook
 * instead of rejecting client writes.
 *
 * <p>Knobs (system properties):
 *
 * <ul>
 *   <li>{@code soak.duration.s}: load duration in seconds (default 60 smoke run; soak target passes
 *       3600).
 *   <li>{@code soak.rate}: target combined ops/sec across all writers (default 1000).
 *   <li>{@code soak.writers}: number of writer threads (default 4).
 *   <li>{@code soak.read.fraction}: fraction of ops that are Gets of previously-acked rows (default
 *       0.2).
 * </ul>
 *
 * <p>Put failures are tolerated (best-effort policy plus supervisor restart windows surface
 * transient RPC errors) and only reported; the test fails solely on {@code lost > 0}, an acked row
 * missing from the final scan. The summary includes one machine-readable line, {@code SOAK_RESULT
 * ...}, parsed by the soak orchestrator script.
 *
 * <p>Like {@code PrePutCounterIT}, not part of {@code mvn test} (the {@code *IT} suffix keeps
 * Surefire's default include patterns from picking it up); invoked explicitly via {@code soak.sh} /
 * {@code make soak}, which manages the cluster lifecycle and stages {@code
 * test/integration/coproc-jars/counter-observer.jar} into the bind-mount the container reads from.
 */
final class SoakIT {

  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/counter-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/counter-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.examples.counter.CounterRegionObserver";
  private static final String POLICY_KEY = "hbasecop.policy.prePut";
  private static final String POLICY_BEST_EFFORT = "best-effort";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);
  private static final byte[] QUAL = "q".getBytes(StandardCharsets.UTF_8);
  private static final Duration JOIN_SLACK = Duration.ofMinutes(2);
  private static final int SCAN_ATTEMPTS = 3;
  private static final long SCAN_RETRY_PAUSE_MS = 5_000;

  private static final int DURATION_S = Integer.getInteger("soak.duration.s", 60);
  private static final int RATE = Integer.getInteger("soak.rate", 1000);
  private static final int WRITERS = Integer.getInteger("soak.writers", 4);
  private static final double READ_FRACTION =
      Double.parseDouble(System.getProperty("soak.read.fraction", "0.2"));

  private final LongAdder putsAcked = new LongAdder();
  private final LongAdder putsFailed = new LongAdder();
  private final LongAdder getsOk = new LongAdder();
  private final LongAdder getsFailed = new LongAdder();

  @Test
  void sustainedLoadLosesNoAckedWrites() throws Exception {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make counter-observer-jar` and copy into test/integration/coproc-jars/)");

    Configuration cfg = baseConfig();
    TableName tn = TableName.valueOf("soak");

    try (Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));

      if (admin.tableExists(tn)) {
        dropTable(admin, tn);
      }

      createSoakTable(admin, tn);
      try {
        List<List<String>> ledgers = runLoad(conn, tn);

        ScanState scanned = scanAll(conn, tn);

        long lost = 0;
        List<String> lostSample = new ArrayList<>();
        for (List<String> ledger : ledgers) {
          for (String key : ledger) {
            if (!scanned.keys.contains(key)) {
              lost++;
              if (lostSample.size() < 20) {
                lostSample.add(key);
              }
            }
          }
        }

        printSummary(scanned.rows, lost);

        // Throughput floor: lost==0 is vacuous if writers acked almost nothing
        // (e.g. every put failed). 20% of nominal write volume tolerates restart
        // windows and pacing drift while still proving sustained load.
        long writeShare = (long) (RATE * DURATION_S * (1.0 - READ_FRACTION));
        long minAcked = writeShare / 5;
        assertTrue(
            putsAcked.sum() >= minAcked,
            "soak load too thin to be meaningful: "
                + putsAcked.sum()
                + " acked puts < floor "
                + minAcked
                + " (20% of nominal "
                + writeShare
                + ")");

        long lostFinal = lost;
        assertEquals(
            0,
            lostFinal,
            () ->
                "data loss: "
                    + lostFinal
                    + " client-acked rowkey(s) missing from final scan, e.g. "
                    + lostSample);
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  /**
   * Spawns the writer threads, lets them run for the configured duration and joins them. Returns
   * the per-thread ledgers of client-acked rowkeys.
   */
  private List<List<String>> runLoad(Connection conn, TableName tn) throws Exception {
    List<List<String>> ledgers = new ArrayList<>(WRITERS);
    List<Thread> threads = new ArrayList<>(WRITERS);
    long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(DURATION_S);

    for (int i = 0; i < WRITERS; i++) {
      List<String> ledger = new ArrayList<>();
      ledgers.add(ledger);
      int threadIdx = i;
      Thread t = new Thread(() -> writerLoop(conn, tn, threadIdx, endNanos, ledger));
      t.setName("soak-writer-" + i);
      t.setDaemon(true);
      threads.add(t);
    }
    threads.forEach(Thread::start);

    // Per-op latency is bounded by hbase.client.operation.timeout, so a writer can overshoot the
    // deadline by at most one op; the join slack covers that plus restart windows.
    long joinDeadlineMs = System.currentTimeMillis() + DURATION_S * 1_000L + JOIN_SLACK.toMillis();
    for (Thread t : threads) {
      long remaining = joinDeadlineMs - System.currentTimeMillis();
      t.join(Math.max(1, remaining));
      assertTrue(!t.isAlive(), "writer thread did not finish within duration + slack: " + t);
    }
    return ledgers;
  }

  /**
   * One writer: paced at {@code RATE / WRITERS} ops/sec. Put {@code soak-<thread>-<seq>} or, with
   * probability {@code READ_FRACTION}, Get a random previously-acked row. Acked Put keys go into
   * {@code ledger} (single-writer list, read by the main thread only after join).
   */
  private void writerLoop(
      Connection conn, TableName tn, int threadIdx, long endNanos, List<String> ledger) {
    long intervalNanos = Math.max(1, (long) (1_000_000_000.0 * WRITERS / RATE));
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    long seq = 0;
    long next = System.nanoTime();

    try (Table table = conn.getTable(tn)) {
      while (System.nanoTime() < endNanos && !Thread.currentThread().isInterrupted()) {
        if (rnd.nextDouble() < READ_FRACTION && !ledger.isEmpty()) {
          String key = ledger.get(rnd.nextInt(ledger.size()));
          try {
            Result r = table.get(new Get(key.getBytes(StandardCharsets.UTF_8)));
            if (r != null && !r.isEmpty()) {
              getsOk.increment();
            } else {
              // Acked row not (yet) visible: reported, not fatal; the final scan is the gate.
              getsFailed.increment();
            }
          } catch (Throwable t) {
            getsFailed.increment();
          }
        } else {
          String key = "soak-" + threadIdx + "-" + seq;
          Put p = new Put(key.getBytes(StandardCharsets.UTF_8));
          p.addColumn(CF, QUAL, Long.toString(seq).getBytes(StandardCharsets.UTF_8));
          try {
            table.put(p);
            putsAcked.increment();
            ledger.add(key);
          } catch (Throwable t) {
            putsFailed.increment();
          }
          seq++; // Advance even on failure so rowkeys stay unique per thread.
        }

        next += intervalNanos;
        long sleepNanos = next - System.nanoTime();
        if (sleepNanos > 0) {
          try {
            TimeUnit.NANOSECONDS.sleep(sleepNanos);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        } else {
          // Fell behind (slow op / restart window): reset the schedule, don't burst to catch up.
          next = System.nanoTime();
        }
      }
    } catch (IOException e) {
      // getTable/close failure: per-op outcomes were already counted; nothing else to record.
    }
  }

  private void printSummary(long rowsScanned, long lost) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n=== SOAK SUMMARY ===\n");
    sb.append(
        String.format(
            Locale.ROOT,
            "config: duration_s=%d rate=%d writers=%d read_fraction=%s%n",
            DURATION_S,
            RATE,
            WRITERS,
            READ_FRACTION));
    sb.append(
        String.format(
            Locale.ROOT,
            "SOAK_RESULT puts_acked=%d puts_failed=%d gets_ok=%d gets_failed=%d"
                + " rows_scanned=%d lost=%d%n",
            putsAcked.sum(),
            putsFailed.sum(),
            getsOk.sum(),
            getsFailed.sum(),
            rowsScanned,
            lost));
    sb.append("====================\n");
    System.out.println(sb);
  }

  private static Configuration baseConfig() {
    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    // Generous retries so ops ride out kill-9 restart windows instead of failing fast; the
    // operation timeout still bounds each op so a hung bridge can't stall a writer forever.
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

  private static void createSoakTable(Admin admin, TableName tn) throws IOException {
    TableDescriptorBuilder b =
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            // Best-effort prePut: kill-9 windows degrade the hook instead of rejecting writes.
            .setValue(POLICY_KEY, POLICY_BEST_EFFORT);
    b.setCoprocessor(
        CoprocessorDescriptorBuilder.newBuilder(COPROC_CLASSNAME)
            .setJarPath(COPROC_JAR_IN_CONTAINER)
            .setPriority(0)
            .build());
    admin.createTable(b.build());
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

  /** Full scan; retried a few times so a restart window right at the end can't fail the gate. */
  private static ScanState scanAll(Connection conn, TableName tn) throws Exception {
    IOException last = null;
    for (int attempt = 1; attempt <= SCAN_ATTEMPTS; attempt++) {
      try {
        ScanState state = new ScanState();
        try (Table table = conn.getTable(tn);
            ResultScanner scanner = table.getScanner(new Scan().addFamily(CF))) {
          for (Result r : scanner) {
            if (!r.isEmpty()) {
              state.rows++;
              state.keys.add(new String(r.getRow(), StandardCharsets.UTF_8));
            }
          }
        }
        return state;
      } catch (IOException e) {
        last = e;
        Thread.sleep(SCAN_RETRY_PAUSE_MS);
      }
    }
    throw last;
  }

  /** Final-scan output: row count plus the key set the data-loss gate checks against. */
  private static final class ScanState {
    long rows;
    final Set<String> keys = new HashSet<>();
  }
}
