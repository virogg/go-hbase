// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.compare;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.client.EndpointClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

final class CompareSupport {

  static final String CONTAINER_NAME = "go-hbase-dev";
  static final String ZK_QUORUM = "localhost";
  static final String ZK_PORT = "2181";
  static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);

  static final String NATIVE_JAR = "file:///coproc-jars/native-coproc.jar";

  static final String NATIVE_JAR_HOST = "test/integration/coproc-jars/native-coproc.jar";
  static final String NATIVE_SUM_FQCN =
      "com.virogg.hbasecop.examples.nativecoproc.NativeSumEndpoint";
  static final String NATIVE_TTL_FQCN =
      "com.virogg.hbasecop.examples.nativecoproc.NativeTtlObserver";
  static final String NATIVE_AUDIT_FQCN =
      "com.virogg.hbasecop.examples.nativecoproc.NativeAuditObserver";
  static final String NATIVE_FILTER_FQCN =
      "com.virogg.hbasecop.examples.nativecoproc.NativeFilterObserver";

  static final String GENERIC_FQCN = "com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver";

  private CompareSupport() {}

  static Configuration clientConfig() {
    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "2");
    cfg.set("hbase.client.pause", "200");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");
    return cfg;
  }

  static void waitForClusterReady(Admin admin, Duration deadline) throws Exception {
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

  static void requireStagedJar(String hostRelativePath) {
    Path jar = resolveOnHost(hostRelativePath);
    assertTrue(
        Files.isReadable(jar),
        "coproc-jar not staged on host bind-mount: " + jar + " (run the matching make target)");
  }

  private static Path resolveOnHost(String hostRelativePath) {
    Path here = Paths.get("").toAbsolutePath();
    while (here != null) {
      Path candidate = here.resolve(hostRelativePath);
      if (Files.exists(candidate)) {
        return candidate;
      }
      here = here.getParent();
    }
    return Paths.get(hostRelativePath).toAbsolutePath();
  }

  static void createPlainTable(Admin admin, TableName tn) throws IOException {
    admin.createTable(
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .build());
  }

  static void createTableWithCoproc(Admin admin, TableName tn, String fqcn, String jarInContainer)
      throws IOException {
    admin.createTable(
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .setCoprocessor(
                CoprocessorDescriptorBuilder.newBuilder(fqcn)
                    .setJarPath(jarInContainer)
                    .setPriority(0)
                    .build())
            .build());
  }

  static void attachCoproc(Admin admin, TableName tn, String fqcn, String jarInContainer)
      throws IOException {
    admin.disableTable(tn);
    TableDescriptor updated =
        TableDescriptorBuilder.newBuilder(admin.getDescriptor(tn))
            .setCoprocessor(
                CoprocessorDescriptorBuilder.newBuilder(fqcn)
                    .setJarPath(jarInContainer)
                    .setPriority(0)
                    .build())
            .build();
    admin.modifyTable(updated);
    admin.enableTable(tn);
  }

  static void dropTable(Admin admin, TableName tn) throws IOException {
    if (!admin.tableExists(tn)) {
      return;
    }
    if (admin.isTableEnabled(tn)) {
      admin.disableTable(tn);
    }
    admin.deleteTable(tn);
  }

  static long sumEndpoint(Table table, byte[] qualifier) throws Throwable {
    return EndpointClient.callAndReduce(
        table,
        "sum",
        qualifier,
        0L,
        (acc, partial) -> acc + Long.parseLong(new String(partial, StandardCharsets.UTF_8)));
  }

  static String dockerLogs() throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder("sh", "-c", "docker logs " + CONTAINER_NAME + " 2>&1");
    pb.redirectErrorStream(true);
    Process p = pb.start();
    StringBuilder sb = new StringBuilder();
    try (BufferedReader r =
        new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = r.readLine()) != null) {
        sb.append(line).append('\n');
      }
    }
    if (!p.waitFor(60, TimeUnit.SECONDS)) {
      p.destroyForcibly();
      throw new IOException("docker logs timed out");
    }
    return sb.toString();
  }

  static long timeNanos(ThrowingRunnable r) throws Throwable {
    long t0 = System.nanoTime();
    r.run();
    return System.nanoTime() - t0;
  }

  @FunctionalInterface
  interface ThrowingRunnable {
    void run() throws Throwable;
  }

  static long medianNanos(List<Long> samples) {
    long[] a = samples.stream().mapToLong(Long::longValue).toArray();
    Arrays.sort(a);
    return a.length == 0 ? 0 : a[a.length / 2];
  }

  static long percentileNanos(List<Long> samples, double p) {
    long[] a = samples.stream().mapToLong(Long::longValue).toArray();
    Arrays.sort(a);
    if (a.length == 0) {
      return 0;
    }
    int idx = (int) Math.min(a.length - 1L, Math.round(p / 100.0 * (a.length - 1)));
    return a[idx];
  }

  static long minNanos(List<Long> samples) {
    return samples.stream().mapToLong(Long::longValue).min().orElse(0);
  }

  private static double us(long nanos) {
    return nanos / 1_000.0;
  }

  static void printResult(String bench, String arm, List<Long> samples) {
    System.out.printf(
        Locale.ROOT,
        "COMPARE_RESULT bench=%s arm=%s unit=us median=%.1f min=%.1f p90=%.1f rounds=%d%n",
        bench,
        arm,
        us(medianNanos(samples)),
        us(minNanos(samples)),
        us(percentileNanos(samples, 90)),
        samples.size());
  }

  static void printSummary(
      String bench, List<Long> nativeSamples, List<Long> goSamples, boolean equivalent) {
    double nat = us(medianNanos(nativeSamples));
    double go = us(medianNanos(goSamples));
    double ratio = nat > 0 ? go / nat : 0;
    System.out.printf(
        Locale.ROOT,
        "COMPARE_SUMMARY bench=%s native_median_us=%.1f go_median_us=%.1f go_over_native=%.2f"
            + " equivalent=%s%n",
        bench,
        nat,
        go,
        ratio,
        equivalent);
  }

  static List<byte[]> rowKeys(String prefix, int from, int count) {
    List<byte[]> out = new ArrayList<>(count);
    for (int i = from; i < from + count; i++) {
      out.add(String.format(Locale.ROOT, "%s%08d", prefix, i).getBytes(StandardCharsets.UTF_8));
    }
    return out;
  }

  static void dropAll(Admin admin, Map<String, TableName> names) throws IOException {
    for (TableName tn : names.values()) {
      dropTable(admin, tn);
    }
  }
}
