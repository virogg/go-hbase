// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire.pb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * T11 acceptance: PB encode→decode==input for every Frame oneof variant plus representative header
 * shapes (single-chunk, multi-chunk, non-zero region_id, control frames with req_id=0).
 *
 * <p>Mirror of internal/wire/wirepb/wire_test.go on the Go side. T13 will fold both into a shared
 * golden corpus for cross-language verification.
 */
class FrameRoundTripTest {

  static List<Frame> goldenFrames() {
    return List.of(
        // request_single_chunk
        Frame.newBuilder()
            .setHeader(
                FrameHeader.newBuilder()
                    .setReqId(0x0123456789ABCDEFL)
                    .setRegionId(0)
                    .setHookId(1)
                    .setChunkIdx(0)
                    .setChunkTotal(1))
            .setRequest(
                Request.newBuilder().setHookCtx(ByteString.copyFromUtf8("opaque-hook-context")))
            .build(),
        // response_empty_payload
        Frame.newBuilder()
            .setHeader(FrameHeader.newBuilder().setReqId(42).setHookId(1).setChunkTotal(1))
            .setResponse(Response.getDefaultInstance())
            .build(),
        // heartbeat_control_frame
        Frame.newBuilder()
            .setHeader(FrameHeader.newBuilder().setChunkTotal(1))
            .setHeartbeat(Heartbeat.newBuilder().setMonotonicNanos(1_700_000_000_000_000_000L))
            .build(),
        // error_strict_path
        Frame.newBuilder()
            .setHeader(FrameHeader.newBuilder().setReqId(7).setHookId(2).setChunkTotal(1))
            .setError(Error.newBuilder().setCode(42).setMessage("user observer panicked"))
            .build(),
        // shutdown
        Frame.newBuilder()
            .setHeader(FrameHeader.newBuilder().setChunkTotal(1))
            .setShutdown(Shutdown.newBuilder().setReason("rs-stop"))
            .build(),
        // log_info
        Frame.newBuilder()
            .setHeader(FrameHeader.newBuilder().setChunkTotal(1))
            .setLog(Log.newBuilder().setLevel(Log.Level.INFO).setMessage("go runtime started"))
            .build(),
        // multi_chunk_request
        Frame.newBuilder()
            .setHeader(
                FrameHeader.newBuilder()
                    .setReqId(99)
                    .setRegionId(7)
                    .setHookId(3)
                    .setChunkIdx(2)
                    .setChunkTotal(5))
            .setRequest(Request.newBuilder().setHookCtx(ByteString.copyFromUtf8("chunk-2-of-5")))
            .build());
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("goldenFrames")
  @DisplayName("Frame encode→decode==input")
  void roundTrip(Frame original) throws InvalidProtocolBufferException {
    byte[] wire = original.toByteArray();
    Frame decoded = Frame.parseFrom(wire);

    assertEquals(original, decoded, "round-trip equality");
    assertNotNull(decoded.getHeader(), "header preserved");
    assertEquals(original.getPayloadCase(), decoded.getPayloadCase(), "payload variant preserved");
  }

  @org.junit.jupiter.api.Test
  @DisplayName("empty oneof stays unset across encode→decode")
  void emptyOneofPreserved() throws InvalidProtocolBufferException {
    Frame empty = Frame.newBuilder().setHeader(FrameHeader.newBuilder().setChunkTotal(1)).build();

    Frame decoded = Frame.parseFrom(empty.toByteArray());

    assertEquals(Frame.PayloadCase.PAYLOAD_NOT_SET, decoded.getPayloadCase());
    assertEquals(empty, decoded);
    assertTrue(decoded.hasHeader(), "header still present on payload-less frame");
  }
}
