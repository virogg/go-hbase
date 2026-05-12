// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Java half of the T13 cross-language byte-level verification. Pairs with
 * internal/wire/wire_golden_test.go on the Go side: both load the same fixtures.tsv and the same
 * {@code <name>.bin} files, then assert that encode→bytes and bytes→decode produce identical
 * results.
 *
 * <p>Test working directory at runtime is the Maven project basedir, so the fixtures path is
 * relative to repo root.
 */
class WireGoldenTest {

  private static final Path CORPUS_DIR = Paths.get("test/golden/wire/v1");

  static List<Fixture> fixtures() throws IOException {
    List<String> lines = Files.readAllLines(CORPUS_DIR.resolve("fixtures.tsv"));
    List<Fixture> out = new ArrayList<>();
    for (String raw : lines) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      String[] f = line.split("\\|", -1);
      if (f.length != 8) {
        throw new IllegalStateException("expected 8 fields, got " + f.length + ": " + line);
      }
      FrameType type = FrameType.valueOf(f[1]);
      long reqId = Long.parseUnsignedLong(f[2]);
      int regionId = Integer.parseUnsignedInt(f[3]);
      byte hookId = (byte) Integer.parseUnsignedInt(f[4]);
      byte[] payload = parsePayload(f[5], f[6]);
      int chunkSize = Integer.parseInt(f[7]);
      out.add(new Fixture(f[0], new Message(type, reqId, regionId, hookId, payload), chunkSize));
    }
    return out;
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("fixtures")
  @DisplayName("encode→.bin matches; .bin→decode matches")
  void goldenRoundTrip(Fixture fx) throws IOException {
    byte[] want = Files.readAllBytes(CORPUS_DIR.resolve(fx.name + ".bin"));

    // (1) Encode → bytes
    Encoder enc = fx.chunkSize > 0 ? new Encoder(fx.chunkSize) : new Encoder();
    ByteBuffer encoded = enc.encode(fx.message);
    byte[] got = new byte[encoded.remaining()];
    encoded.get(got);
    assertArrayEquals(want, got, fx.name + ": encode bytes mismatch");

    // (2) bytes → Decode
    Message decoded = new Decoder().decode(ByteBuffer.wrap(want));
    assertEquals(fx.message.type(), decoded.type(), fx.name + ": type");
    assertEquals(fx.message.reqId(), decoded.reqId(), fx.name + ": reqId");
    assertEquals(fx.message.regionId(), decoded.regionId(), fx.name + ": regionId");
    assertEquals(fx.message.hookId(), decoded.hookId(), fx.name + ": hookId");
    assertArrayEquals(fx.message.payload(), decoded.payload(), fx.name + ": payload");
  }

  private static byte[] parsePayload(String kind, String value) {
    switch (kind) {
      case "HEX":
        return parseHex(value);
      case "ASCENDING":
        int n = Integer.parseInt(value);
        byte[] out = new byte[n];
        for (int i = 0; i < n; i++) {
          out[i] = (byte) (i % 251);
        }
        return out;
      default:
        throw new IllegalArgumentException("unknown payload_kind: " + kind);
    }
  }

  // Hand-rolled hex parser; java.util.HexFormat is Java 17+, we target 11.
  private static byte[] parseHex(String hex) {
    if (hex.isEmpty()) {
      return new byte[0];
    }
    if ((hex.length() & 1) != 0) {
      throw new IllegalArgumentException("odd-length hex: " + hex);
    }
    byte[] out = new byte[hex.length() / 2];
    for (int i = 0; i < out.length; i++) {
      int hi = Character.digit(hex.charAt(i * 2), 16);
      int lo = Character.digit(hex.charAt(i * 2 + 1), 16);
      if (hi < 0 || lo < 0) {
        throw new IllegalArgumentException("non-hex char near index " + (i * 2));
      }
      out[i] = (byte) ((hi << 4) | lo);
    }
    return out;
  }

  // Sanity assertion that the corpus directory is reachable from the
  // Maven working directory. Catches accidental cwd drift before any
  // parameterized test fails with a less obvious file-not-found.
  @org.junit.jupiter.api.Test
  void corpusDirectoryExists() {
    assertFalse(
        !Files.isRegularFile(CORPUS_DIR.resolve("fixtures.tsv")),
        "missing " + CORPUS_DIR.resolve("fixtures.tsv").toAbsolutePath());
  }

  static final class Fixture {
    final String name;
    final Message message;
    final int chunkSize;

    Fixture(String name, Message message, int chunkSize) {
      this.name = name;
      this.message = message;
      this.chunkSize = chunkSize;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
