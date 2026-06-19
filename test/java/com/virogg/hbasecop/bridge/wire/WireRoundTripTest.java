// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Mirror of internal/wire/wire_test.go on the Go side; same scenarios, same assertions. */
class WireRoundTripTest {

  // --- Single-chunk round-trip --------------------------------------------

  static Stream<Arguments> singleChunkCases() {
    return Stream.of(
        Arguments.of("empty_heartbeat", new Message(FrameType.HEARTBEAT, 0, 0, (byte) 0, null)),
        Arguments.of(
            "small_request", new Message(FrameType.REQUEST, 7L, 3, (byte) 12, "hello".getBytes())),
        Arguments.of(
            "exact_max_frame",
            new Message(
                FrameType.REQUEST,
                1L,
                0,
                (byte) 0,
                repeat((byte) 0xCC, WireFormat.MAX_PAYLOAD_BYTES))),
        Arguments.of(
            "response_with_bytes",
            new Message(FrameType.RESPONSE, 9L, 0, (byte) 0, new byte[] {1, 2})),
        Arguments.of(
            "error_strict_path", new Message(FrameType.ERROR, 10L, 0, (byte) 0, "boom".getBytes())),
        Arguments.of(
            "shutdown_rs", new Message(FrameType.SHUTDOWN, 0, 0, (byte) 0, "bye".getBytes())),
        Arguments.of("log_info", new Message(FrameType.LOG, 0, 0, (byte) 0, "info".getBytes())));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("singleChunkCases")
  @DisplayName("single-chunk encode→decode==input")
  void roundTripSingleChunk(String name, Message msg) throws IOException {
    ByteBuffer encoded = new Encoder().encode(msg);
    Message decoded = new Decoder().decode(encoded);
    assertMessageEquals(msg, decoded);
  }

  // --- Tier 2 (wire v2) endpoint / reverse-RPC frame types ----------------

  static Stream<Arguments> endpointTypeCases() {
    return Stream.of(
        Arguments.of(
            "endpoint_invoke",
            new Message(FrameType.ENDPOINT_INVOKE, 11L, 4, (byte) 0, "invoke".getBytes())),
        Arguments.of(
            "endpoint_result",
            new Message(FrameType.ENDPOINT_RESULT, 11L, 0, (byte) 0, "result".getBytes())),
        Arguments.of(
            "rpc_request",
            new Message(FrameType.RPC_REQUEST, 12L, 4, (byte) 0, "scan-open".getBytes())),
        Arguments.of(
            "rpc_response",
            new Message(FrameType.RPC_RESPONSE, 12L, 0, (byte) 0, new byte[] {1, 2, 3})));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("endpointTypeCases")
  @DisplayName("v2 endpoint/reverse-RPC types encode→decode==input")
  void roundTripEndpointTypes(String name, Message msg) throws IOException {
    ByteBuffer encoded = new Encoder().encode(msg);
    Message decoded = new Decoder().decode(encoded);
    assertMessageEquals(msg, decoded);
  }

  @Test
  @DisplayName("v2 RpcResponse is not a control frame: may span multiple chunks")
  void roundTripRpcResponseMultiChunk() throws IOException {
    Message msg =
        new Message(
            FrameType.RPC_RESPONSE,
            13L,
            0,
            (byte) 0,
            ascending(WireFormat.MAX_PAYLOAD_BYTES * 2 + 7));
    ByteBuffer encoded = new Encoder().encode(msg);
    Message decoded = new Decoder().decode(encoded);
    assertMessageEquals(msg, decoded);
  }

  // --- Multi-chunk round-trip ---------------------------------------------

  @Test
  @DisplayName("default 64KiB chunk: 2/3 chunks via real payload sizes")
  void roundTripMultiChunk() throws IOException {
    int[] sizes = {
      WireFormat.MAX_PAYLOAD_BYTES + 1, // 2 chunks
      WireFormat.MAX_PAYLOAD_BYTES * 2 + 5, // 3 chunks
      WireFormat.MAX_PAYLOAD_BYTES * 2 // exactly 2 chunks
    };
    for (int sz : sizes) {
      byte[] payload = ascending(sz);
      Message msg = new Message(FrameType.REQUEST, 42L, 1, (byte) 5, payload);
      ByteBuffer encoded = new Encoder().encode(msg);
      Message decoded = new Decoder().decode(encoded);
      assertMessageEquals(msg, decoded);
    }
  }

  @Test
  @DisplayName("WithChunkSize allows small per-chunk windows for testing")
  void roundTripChunkSizeOverride() throws IOException {
    Message msg =
        new Message(FrameType.REQUEST, 5L, 0, (byte) 1, "abcdefghijklmnopqrstuvwxyz".getBytes());
    ByteBuffer encoded = new Encoder(10).encode(msg);
    Message decoded = new Decoder().decode(encoded);
    assertMessageEquals(msg, decoded);
  }

  @Test
  @DisplayName("out-of-order chunk arrival reassembles by chunk_idx")
  void decodeOutOfOrderChunks() throws IOException {
    // Build 3 chunks for req_id=11 in arrival order [2, 0, 1].
    ByteBuffer buf = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
    writeRaw(buf, FrameType.REQUEST.value(), 11L, 4, (byte) 2, 2, 3, "ccc".getBytes());
    writeRaw(buf, FrameType.REQUEST.value(), 11L, 4, (byte) 2, 0, 3, "aaa".getBytes());
    writeRaw(buf, FrameType.REQUEST.value(), 11L, 4, (byte) 2, 1, 3, "bbb".getBytes());
    buf.flip();

    Message decoded = new Decoder().decode(buf);
    Message want = new Message(FrameType.REQUEST, 11L, 4, (byte) 2, "aaabbbccc".getBytes());
    assertMessageEquals(want, decoded);
  }

  // --- Error paths ---------------------------------------------------------

  @Test
  void decodeRejectsOversizedLen() {
    ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(1 << 30);
    buf.flip();
    assertThrows(FrameTooLargeException.class, () -> new Decoder().decode(buf));
  }

  @Test
  void decodeRejectsShorterThanHeader() {
    ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(10); // shorter than 23-byte header
    buf.flip();
    assertThrows(FrameTooLargeException.class, () -> new Decoder().decode(buf));
  }

  @Test
  void decodeRejectsUnknownType() {
    ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    writeRaw(buf, (byte) 99, 1L, 0, (byte) 0, 0, 1, new byte[] {'x'});
    buf.flip();
    assertThrows(UnknownTypeException.class, () -> new Decoder().decode(buf));
  }

  @Test
  @DisplayName("type byte one past RPC_RESPONSE is rejected (lockstep with Go)")
  void decodeRejectsTypeOnePastCeiling() {
    byte past = (byte) (FrameType.RPC_RESPONSE.value() + 1); // 11: a future v3 type
    assertEquals(FrameType.UNKNOWN, FrameType.fromByte(past));
    ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    writeRaw(buf, past, 1L, 0, (byte) 0, 0, 1, new byte[] {'x'});
    buf.flip();
    assertThrows(UnknownTypeException.class, () -> new Decoder().decode(buf));
  }

  @Test
  void decodeRejectsInvalidChunkBounds() {
    int[][] cases = {{0, 0}, {2, 2}, {5, 2}};
    for (int[] c : cases) {
      ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
      writeRaw(buf, FrameType.REQUEST.value(), 1L, 0, (byte) 0, c[0], c[1], new byte[0]);
      buf.flip();
      assertThrows(InvalidChunkException.class, () -> new Decoder().decode(buf));
    }
  }

  @Test
  void decodeRejectsDuplicateChunk() {
    ByteBuffer buf = ByteBuffer.allocate(128).order(ByteOrder.BIG_ENDIAN);
    writeRaw(buf, FrameType.REQUEST.value(), 7L, 0, (byte) 0, 0, 2, "aa".getBytes());
    writeRaw(buf, FrameType.REQUEST.value(), 7L, 0, (byte) 0, 0, 2, "aa".getBytes());
    buf.flip();
    assertThrows(InvalidChunkException.class, () -> new Decoder().decode(buf));
  }

  @Test
  void decodeRejectsHeaderDriftAcrossChunks() {
    ByteBuffer buf = ByteBuffer.allocate(128).order(ByteOrder.BIG_ENDIAN);
    writeRaw(buf, FrameType.REQUEST.value(), 7L, 0, (byte) 1, 0, 2, "aa".getBytes());
    writeRaw(buf, FrameType.REQUEST.value(), 7L, 0, (byte) 9, 1, 2, "bb".getBytes());
    buf.flip();
    assertThrows(InvalidChunkException.class, () -> new Decoder().decode(buf));
  }

  @Test
  void decodeRejectsControlMultiChunk() {
    ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    writeRaw(buf, FrameType.HEARTBEAT.value(), 0L, 0, (byte) 0, 0, 2, "a".getBytes());
    buf.flip();
    assertThrows(ControlMultiChunkException.class, () -> new Decoder().decode(buf));
  }

  @Test
  void decodeRejectsZeroReqIdMultiChunk() {
    ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    writeRaw(buf, FrameType.REQUEST.value(), 0L, 0, (byte) 0, 0, 2, "a".getBytes());
    buf.flip();
    assertThrows(InvalidChunkException.class, () -> new Decoder().decode(buf));
  }

  @Test
  void decodeCleanEOFReturnsNull() throws IOException {
    ByteBuffer empty = ByteBuffer.allocate(0).order(ByteOrder.BIG_ENDIAN);
    assertNull(new Decoder().decode(empty));
  }

  @Test
  void encodeRejectsUnknownType() {
    assertThrows(
        UnknownTypeException.class,
        () -> new Encoder().encode(new Message(FrameType.UNKNOWN, 0, 0, (byte) 0, null)));
  }

  @Test
  void encodeRejectsOversizedControlPayload() {
    Message msg = new Message(FrameType.HEARTBEAT, 0, 0, (byte) 0, repeat((byte) 1, 20));
    assertThrows(ControlMultiChunkException.class, () -> new Encoder(10).encode(msg));
  }

  @Test
  @DisplayName("multiple messages back-to-back round-trip in order")
  void streamRoundTrip() throws IOException {
    List<Message> msgs =
        List.of(
            new Message(FrameType.HEARTBEAT, 0, 0, (byte) 0, null),
            new Message(FrameType.REQUEST, 1L, 0, (byte) 0, "first".getBytes()),
            new Message(
                FrameType.REQUEST, 2L, 0, (byte) 0, ascending(WireFormat.MAX_PAYLOAD_BYTES + 100)),
            new Message(FrameType.RESPONSE, 1L, 0, (byte) 0, "ok".getBytes()));

    ByteBuffer buf = ByteBuffer.allocate(WireFormat.MAX_FRAME_SIZE * 3).order(ByteOrder.BIG_ENDIAN);
    Encoder enc = new Encoder();
    for (Message m : msgs) {
      ByteBuffer chunk = enc.encode(m);
      buf.put(chunk);
    }
    buf.flip();

    Decoder dec = new Decoder();
    for (Message want : msgs) {
      assertMessageEquals(want, dec.decode(buf));
    }
    assertNull(dec.decode(buf));
  }

  // --- helpers -------------------------------------------------------------

  private static void assertMessageEquals(Message want, Message got) {
    assertEquals(want.type(), got.type(), "type");
    assertEquals(want.reqId(), got.reqId(), "reqId");
    assertEquals(want.regionId(), got.regionId(), "regionId");
    assertEquals(want.hookId(), got.hookId(), "hookId");
    assertArrayEquals(want.payload(), got.payload(), "payload");
  }

  private static byte[] repeat(byte b, int n) {
    byte[] out = new byte[n];
    java.util.Arrays.fill(out, b);
    return out;
  }

  private static byte[] ascending(int n) {
    byte[] out = new byte[n];
    for (int i = 0; i < n; i++) {
      out[i] = (byte) (i % 251);
    }
    return out;
  }

  // Hand-crafts a chunk frame (length-prefixed, no chunking) for tests that
  // need to inject bytes the Encoder would never produce: out-of-order
  // chunks, illegal type bytes, duplicate chunk_idx, etc.
  private static void writeRaw(
      ByteBuffer buf,
      byte type,
      long reqId,
      int regionId,
      byte hookId,
      int chunkIdx,
      int chunkTotal,
      byte[] payload) {
    int len = 23 + payload.length;
    buf.putInt(len);
    buf.put(type);
    buf.putLong(reqId);
    buf.putInt(regionId);
    buf.put(hookId);
    buf.put((byte) 0); // chunk_flags reserved
    buf.putInt(chunkIdx);
    buf.putInt(chunkTotal);
    buf.put(payload);
  }
}
