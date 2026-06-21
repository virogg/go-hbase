// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.shmem.Channel;
import com.virogg.hbasecop.bridge.shmem.Config;
import com.virogg.hbasecop.bridge.shmem.Role;
import com.virogg.hbasecop.bridge.wire.Decoder;
import com.virogg.hbasecop.bridge.wire.Encoder;
import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GoProcessTest {

  private static final int CAPACITY = 16;
  private static final int MAX_OBJECT_SIZE = 4096;
  private static final byte HOOK_PING = (byte) 0xFF;

  @Test
  @DisplayName("PING/PONG roundtrip via spawned Go runtime, then graceful stop()")
  void pingPongAndGracefulShutdown(@TempDir Path tmp) throws Exception {
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
              .heartbeatPeriodMs(-1)
              .build();

      try (GoProcess go = new GoProcess(cfg, javaToGo)) {
        go.start();

        assertTrue(go.isAlive(), "process should be alive after start()");
        assertTrue(go.pid() > 0, "pid must be > 0");

        // Send PING
        long reqId = 42L;
        byte[] payload = new byte[] {1, 2, 3, 4};
        Message ping = new Message(FrameType.REQUEST, reqId, 0, HOOK_PING, payload);
        ByteBuffer encoded = new Encoder().encode(ping);
        byte[] frame = new byte[encoded.remaining()];
        encoded.get(frame);
        javaToGo.send(frame);

        Message pong = awaitRecv(goToJava, Duration.ofSeconds(5));
        assertEquals(FrameType.RESPONSE, pong.type(), "expected RESPONSE");
        assertEquals(reqId, pong.reqId(), "req_id must match");
        assertEquals(HOOK_PING, pong.hookId(), "hook_id must match");
        assertArrayEquals(payload, pong.payload(), "payload must be echoed verbatim");

        Instant t0 = Instant.now();
        go.stop();
        Duration elapsed = Duration.between(t0, Instant.now());

        assertFalse(go.isAlive(), "process must have exited after stop()");
        assertTrue(
            elapsed.toMillis() < 1500,
            "stop() should complete in ≤ 1s (took " + elapsed.toMillis() + "ms)");
        assertEquals(0, go.exitCode(), "expected clean exit code");
      }
    }
  }

  @Test
  @DisplayName("isAlive()/pid() report sensible defaults before start()")
  void preStartState(@TempDir Path tmp) throws Exception {
    Path inFile = tmp.resolve("in.mmap");
    try (Channel javaToGo = openChannel(inFile, Role.PRODUCER)) {
      GoProcessConfig cfg =
          GoProcessConfig.builder()
              .binaryResourcePath("bin/linux-amd64/hbasecop-runtime")
              .javaToGoFile(inFile)
              .goToJavaFile(tmp.resolve("out.mmap"))
              .capacity(CAPACITY)
              .maxObjectSize(MAX_OBJECT_SIZE)
              .heartbeatPeriodMs(-1)
              .build();

      GoProcess go = new GoProcess(cfg, javaToGo);
      assertFalse(go.isAlive(), "process is not alive before start()");
    }
  }

  private Channel openChannel(Path file, Role role) {
    return Channel.open(
        Config.builder()
            .filename(file.toString())
            .capacity(CAPACITY)
            .maxObjectSize(MAX_OBJECT_SIZE)
            .role(role)
            .build());
  }

  private Message awaitRecv(Channel ch, Duration timeout) throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      Optional<byte[]> raw = ch.recv();
      if (raw.isPresent()) {
        return new Decoder().decode(ByteBuffer.wrap(raw.get()));
      }
      Thread.sleep(1);
    }
    throw new AssertionError("no inbound frame within " + timeout);
  }
}
