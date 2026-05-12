// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * T16 acceptance: mirror of {@code internal/shmem.TestChannelRoundtrip1000} on the Java side. The
 * wrapper is end-to-end exercised in a single thread so any cross-process visibility issues remain
 * outside the surface tested here (those land in T18/T19).
 */
class ChannelTest {

  private static final int CAPACITY = 8;
  private static final int MAX_OBJECT_SIZE = 256; // → max payload 252 bytes

  private Config configFor(Path file, Role role) {
    return Config.builder()
        .filename(file.toString())
        .capacity(CAPACITY)
        .maxObjectSize(MAX_OBJECT_SIZE)
        .role(role)
        .build();
  }

  @Test
  @DisplayName("1000-frame round-trip with varying payload sizes (forces ring wrap)")
  void roundtrip1000(@TempDir Path tmp) throws Exception {
    Path file = tmp.resolve("channel.mmap");
    try (Channel prod = Channel.open(configFor(file, Role.PRODUCER));
        Channel cons = Channel.open(configFor(file, Role.CONSUMER))) {

      int maxPayload = MAX_OBJECT_SIZE - 4;
      for (int i = 0; i < 1000; i++) {
        int size = i % (maxPayload + 1);
        byte[] in = new byte[size];
        for (int k = 0; k < size; k++) {
          in[k] = (byte) (i + k);
        }

        prod.send(in);
        Optional<byte[]> out = cons.recv();
        assertTrue(out.isPresent(), "recv at iteration " + i + " (size=" + size + ")");
        assertArrayEquals(in, out.get(), "frame " + i + " content");
      }
    }
  }

  @Test
  @DisplayName("recv on empty ring returns Optional.empty()")
  void recvEmpty(@TempDir Path tmp) throws Exception {
    Path file = tmp.resolve("channel.mmap");
    try (Channel prod = Channel.open(configFor(file, Role.PRODUCER));
        Channel cons = Channel.open(configFor(file, Role.CONSUMER))) {
      // Touch the producer so the region is fully initialized.
      assertEquals(CAPACITY, prod.capacity());
      assertFalse(cons.recv().isPresent());
    }
  }

  @Test
  @DisplayName("send of oversize frame throws FrameTooLargeException")
  void sendOversize(@TempDir Path tmp) throws Exception {
    Path file = tmp.resolve("channel.mmap");
    try (Channel prod = Channel.open(configFor(file, Role.PRODUCER))) {
      byte[] tooBig = new byte[MAX_OBJECT_SIZE - 4 + 1];
      assertThrows(FrameTooLargeException.class, () -> prod.send(tooBig));
    }
  }

  @Test
  @DisplayName("send on consumer or recv on producer throws IllegalStateException")
  void wrongRole(@TempDir Path tmp) throws Exception {
    Path file = tmp.resolve("channel.mmap");
    try (Channel prod = Channel.open(configFor(file, Role.PRODUCER));
        Channel cons = Channel.open(configFor(file, Role.CONSUMER))) {
      assertThrows(IllegalStateException.class, () -> cons.send(new byte[] {1}));
      assertThrows(IllegalStateException.class, prod::recv);
    }
  }

  @Test
  @DisplayName("send returns RingFullException when ring fills before flush is observed")
  void ringFull(@TempDir Path tmp) throws Exception {
    Path file = tmp.resolve("channel.mmap");
    try (Channel prod = Channel.open(configFor(file, Role.PRODUCER))) {
      // No consumer ack, so capacity sends must succeed then the next fails.
      for (int i = 0; i < CAPACITY; i++) {
        prod.send(new byte[] {(byte) i});
      }
      assertThrows(RingFullException.class, () -> prod.send(new byte[] {0}));
    }
  }

  @Test
  @DisplayName("Config validation rejects missing/invalid fields")
  void configValidation(@TempDir Path tmp) {
    Path file = tmp.resolve("x.mmap");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            Channel.open(
                Config.builder().capacity(8).maxObjectSize(64).role(Role.PRODUCER).build()),
        "missing filename");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            Channel.open(
                Config.builder()
                    .filename(file.toString())
                    .capacity(0)
                    .maxObjectSize(64)
                    .role(Role.PRODUCER)
                    .build()),
        "capacity=0");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            Channel.open(
                Config.builder()
                    .filename(file.toString())
                    .capacity(8)
                    .maxObjectSize(0)
                    .role(Role.PRODUCER)
                    .build()),
        "maxObjectSize=0");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            Channel.open(
                Config.builder().filename(file.toString()).capacity(8).maxObjectSize(64).build()),
        "role null");
  }
}
