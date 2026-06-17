// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.multiplex.Multiplexer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * TE12: the Go→Java reader demux ({@link CoprocessorRuntime#routeFrame}) routes the Tier 2
 * reverse-RPC request to the stub sink keyed by the wire-header req_id, while the existing
 * RESPONSE→multiplexer path is unchanged. Endpoints are otherwise OFF: a frame type with no E1
 * route (e.g. ENDPOINT_RESULT, wired in TE22) reaches neither sink.
 */
class CoprocessorRuntimeRouteFrameTest {

  private static Multiplexer noopMux() {
    return Multiplexer.builder(m -> {}).build();
  }

  @Test
  void routesRpcRequestToReverseSink() {
    AtomicReference<Message> captured = new AtomicReference<>();
    Message m = new Message(FrameType.RPC_REQUEST, 7L, 4, (byte) 0, "scan-open".getBytes());

    CoprocessorRuntime.routeFrame(m, noopMux(), null, captured::set);

    Message got = captured.get();
    assertEquals(FrameType.RPC_REQUEST, got.type());
    assertEquals(7L, got.reqId(), "correlated by wire-header req_id");
    assertArrayEquals("scan-open".getBytes(), got.payload());
  }

  @Test
  void routesResponseToMultiplexer() throws Exception {
    // A pending call assigns a req_id; routing a RESPONSE for it must complete the future.
    ConcurrentLinkedQueue<Message> sent = new ConcurrentLinkedQueue<>();
    Multiplexer mux = Multiplexer.builder(sent::add).build();

    CompletableFuture<Message> pending =
        mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, new byte[0]));
    long assignedReqId = sent.poll().reqId();

    AtomicReference<Message> reverse = new AtomicReference<>();
    CoprocessorRuntime.routeFrame(
        new Message(FrameType.RESPONSE, assignedReqId, 0, (byte) 0, new byte[] {0x09}),
        mux,
        null,
        reverse::set);

    Message resp = pending.get(2, TimeUnit.SECONDS);
    assertArrayEquals(new byte[] {0x09}, resp.payload());
    assertNull(reverse.get(), "RESPONSE must not reach the reverse-RPC sink");
  }

  @Test
  void endpointResultHasNoE1Route() {
    // ENDPOINT_RESULT routing is wired in TE22; in E1 it reaches neither sink (default WARN path).
    AtomicReference<Message> reverse = new AtomicReference<>();
    CoprocessorRuntime.routeFrame(
        new Message(FrameType.ENDPOINT_RESULT, 11L, 0, (byte) 0, "result".getBytes()),
        noopMux(),
        null,
        reverse::set);
    assertNull(reverse.get(), "ENDPOINT_RESULT must not reach the reverse-RPC sink in E1");
  }
}
