// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.e2e;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.shmem.Channel;
import com.virogg.hbasecop.bridge.shmem.Config;
import com.virogg.hbasecop.bridge.shmem.Role;
import com.virogg.hbasecop.bridge.supervisor.GoProcess;
import com.virogg.hbasecop.bridge.supervisor.GoProcessConfig;
import com.virogg.hbasecop.bridge.wire.Decoder;
import com.virogg.hbasecop.bridge.wire.Encoder;
import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * T19 acceptance: 10 000 PING/PONG roundtrips through a real spawned Go runtime over real shmem
 * rings, with payloads spanning the wire chunking boundary.
 *
 * <p>Payload sizes cycle through {@code {0, 1 KiB, 64 KiB, 1 MiB}}: 0 and 1 KiB exercise the
 * single-chunk path, 64 KiB straddles {@code MAX_PAYLOAD_BYTES = 65509} forcing a 2-chunk encode,
 * and 1 MiB forces ~17 chunks. The serial send/recv loop times each roundtrip and emits a
 * p50/p99/p999/max distribution to {@code stderr}.
 *
 * <p>Each shmem ring slot carries one full wire-encoded message - including all of its chunks
 * back-to-back - so {@code maxObjectSize} is sized to fit the 1 MiB-payload encoding.
 */
class PingPongE2ETest {

  private static final int CAPACITY = 8;
  private static final int MAX_OBJECT_SIZE = 2 * 1024 * 1024;
  private static final byte HOOK_PING = (byte) 0xFF;
  private static final int N = 10_000;
  private static final int[] PAYLOAD_SIZES = {0, 1024, 65_536, 1_048_576};
  private static final Duration RECV_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration STOP_BUDGET = Duration.ofSeconds(2);

  @Test
  @DisplayName("10k PING/PONG over spawned Go runtime, payload sizes 0/1KB/64KB/1MB")
  void pingPong10kCrossLanguage(@TempDir Path tmp) throws Exception {
    runE2E(tmp);
  }

  /** Entry point for {@code make demo-ping}: identical loop with a self-created tmpdir. */
  public static void main(String[] args) throws Exception {
    Path tmp = Files.createTempDirectory("hbasecop-ping-e2e-");
    try {
      runE2E(tmp);
    } finally {
      try (Stream<Path> walk = Files.walk(tmp)) {
        walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
            .forEach(
                p -> {
                  try {
                    Files.deleteIfExists(p);
                  } catch (IOException ignored) {
                    // Best-effort cleanup.
                  }
                });
      }
    }
  }

  private static void runE2E(Path tmp) throws Exception {
    Path inFile = tmp.resolve("in.mmap");
    Path outFile = tmp.resolve("out.mmap");

    try (Channel javaToGo = openChannel(inFile, Role.PRODUCER);
        Channel goToJava = openChannel(outFile, Role.CONSUMER)) {

      GoProcessConfig cfg =
          GoProcessConfig.builder()
              .binaryResourcePath("bin/linux-amd64/hbasecop-runtime")
              .javaToGoFile(inFile)
              .goToJavaFile(outFile)
              .capacity(CAPACITY)
              .maxObjectSize(MAX_OBJECT_SIZE)
              .heartbeatPeriodMs(-1) // disable heartbeats so they don't contend with latencies
              .gracefulShutdownTimeout(STOP_BUDGET)
              .build();

      try (GoProcess go = new GoProcess(cfg, javaToGo)) {
        go.start();
        assertTrue(go.isAlive(), "Go runtime must be alive after start()");

        Encoder encoder = new Encoder();
        Decoder decoder = new Decoder();
        long[] latencyNs = new long[N];

        Instant t0 = Instant.now();
        for (int i = 0; i < N; i++) {
          int size = PAYLOAD_SIZES[i % PAYLOAD_SIZES.length];
          byte[] payload = buildPayload(i, size);
          long reqId = (long) i + 1L;
          byte[] frame = encodeFrame(encoder, reqId, payload);

          long started = System.nanoTime();
          send(javaToGo, frame);
          Message pong = awaitRecv(goToJava, decoder, RECV_TIMEOUT);
          long elapsedNs = System.nanoTime() - started;
          latencyNs[i] = elapsedNs;

          assertEquals(FrameType.RESPONSE, pong.type(), () -> "iter type mismatch");
          assertEquals(reqId, pong.reqId(), () -> "iter req_id mismatch");
          assertEquals(HOOK_PING, pong.hookId(), () -> "iter hook_id mismatch");
          assertArrayEquals(payload, pong.payload(), () -> "iter payload mismatch");
        }
        Duration wall = Duration.between(t0, Instant.now());

        reportLatencies(latencyNs, wall);

        Instant stopStart = Instant.now();
        go.stop();
        Duration stopElapsed = Duration.between(stopStart, Instant.now());
        assertFalse(go.isAlive(), "Go runtime must have exited after stop()");
        assertEquals(0, go.exitCode(), "Go runtime must exit cleanly");
        assertTrue(
            stopElapsed.compareTo(STOP_BUDGET.plus(Duration.ofMillis(500))) < 0,
            "stop() within budget; took " + stopElapsed.toMillis() + "ms");
      }
    }
  }

  private static byte[] buildPayload(int iter, int size) {
    byte[] out = new byte[size];
    for (int k = 0; k < size; k++) {
      out[k] = (byte) ((iter * 31 + k) & 0xFF);
    }
    return out;
  }

  private static byte[] encodeFrame(Encoder enc, long reqId, byte[] payload) throws Exception {
    Message m = new Message(FrameType.REQUEST, reqId, 0, HOOK_PING, payload);
    ByteBuffer bb = enc.encode(m);
    byte[] out = new byte[bb.remaining()];
    bb.get(out);
    return out;
  }

  private static void send(Channel ch, byte[] frame) throws Exception {
    while (true) {
      try {
        ch.send(frame);
        return;
      } catch (com.virogg.hbasecop.bridge.shmem.RingFullException e) {
        Thread.onSpinWait();
      }
    }
  }

  private static Message awaitRecv(Channel ch, Decoder dec, Duration timeout) throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      Optional<byte[]> raw = ch.recv();
      if (raw.isPresent()) {
        Message m = dec.decode(ByteBuffer.wrap(raw.get()));
        if (m != null) {
          return m;
        }
      } else {
        Thread.onSpinWait();
      }
    }
    throw new AssertionError("no inbound frame within " + timeout);
  }

  private static void reportLatencies(long[] latencyNs, Duration wall) {
    long[] sorted = latencyNs.clone();
    Arrays.sort(sorted);
    System.err.printf(
        "T19 ping/pong: N=%d wall=%dms throughput=%.0f msg/s%n",
        latencyNs.length,
        wall.toMillis(),
        latencyNs.length * 1000.0 / Math.max(1, wall.toMillis()));
    System.err.printf(
        "  overall: min=%s p50=%s p99=%s p999=%s max=%s%n",
        fmt(sorted[0]),
        fmt(sorted[sorted.length / 2]),
        fmt(sorted[(int) (sorted.length * 0.99)]),
        fmt(sorted[(int) (sorted.length * 0.999)]),
        fmt(sorted[sorted.length - 1]));

    Map<Integer, long[]> perSize = bucketByPayloadSize(latencyNs);
    for (Map.Entry<Integer, long[]> e : perSize.entrySet()) {
      long[] bucket = e.getValue();
      Arrays.sort(bucket);
      System.err.printf(
          "  payload=%-8d n=%d min=%s p50=%s p99=%s max=%s%n",
          e.getKey(),
          bucket.length,
          fmt(bucket[0]),
          fmt(bucket[bucket.length / 2]),
          fmt(bucket[(int) (bucket.length * 0.99)]),
          fmt(bucket[bucket.length - 1]));
    }
  }

  private static Map<Integer, long[]> bucketByPayloadSize(long[] latencyNs) {
    int[] counts = new int[PAYLOAD_SIZES.length];
    for (int i = 0; i < latencyNs.length; i++) {
      counts[i % PAYLOAD_SIZES.length]++;
    }
    Map<Integer, long[]> buckets = new LinkedHashMap<>();
    int[] cursors = new int[PAYLOAD_SIZES.length];
    for (int j = 0; j < PAYLOAD_SIZES.length; j++) {
      buckets.put(PAYLOAD_SIZES[j], new long[counts[j]]);
    }
    for (int i = 0; i < latencyNs.length; i++) {
      int b = i % PAYLOAD_SIZES.length;
      buckets.get(PAYLOAD_SIZES[b])[cursors[b]++] = latencyNs[i];
    }
    return buckets;
  }

  private static String fmt(long ns) {
    if (ns < 10_000L) {
      return ns + "ns";
    }
    if (ns < 10_000_000L) {
      return String.format("%.1fus", ns / 1_000.0);
    }
    return String.format("%.2fms", ns / 1_000_000.0);
  }

  private static Channel openChannel(Path file, Role role) {
    return Channel.open(
        Config.builder()
            .filename(file.toString())
            .capacity(CAPACITY)
            .maxObjectSize(MAX_OBJECT_SIZE)
            .role(role)
            .build());
  }
}
