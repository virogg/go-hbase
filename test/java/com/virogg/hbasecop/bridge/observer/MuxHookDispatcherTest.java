// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.multiplex.Multiplexer;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

class MuxHookDispatcherTest {

  private static final byte HOOK = (byte) 22;

  @Test
  void responseFrameUnwrapsHookResponseBytes() throws Exception {
    byte[] hookRespBytes = HookResponse.newBuilder().setBypass(true).build().toByteArray();
    Multiplexer[] holder = new Multiplexer[1];
    Multiplexer mux =
        new Multiplexer(
            msg -> {
              com.virogg.hbasecop.bridge.wire.pb.Response wire =
                  com.virogg.hbasecop.bridge.wire.pb.Response.newBuilder()
                      .setHookResp(ByteString.copyFrom(hookRespBytes))
                      .build();
              holder[0].deliver(
                  new Message(
                      FrameType.RESPONSE,
                      msg.reqId(),
                      msg.regionId(),
                      msg.hookId(),
                      wire.toByteArray()));
            });
    holder[0] = mux;

    MuxHookDispatcher dispatcher = new MuxHookDispatcher(mux);
    byte[] got = dispatcher.dispatchHook(0, HOOK, new byte[0], Duration.ofSeconds(2));

    assertNotNull(got);
    HookResponse parsed = HookResponse.parseFrom(got);
    assertTrue(parsed.getBypass(), "unwrapped HookResponse should carry bypass=true");
  }

  @Test
  void errorFrameMapsToIOException() {
    Multiplexer[] holder = new Multiplexer[1];
    Multiplexer mux =
        new Multiplexer(
            msg -> {
              com.virogg.hbasecop.bridge.wire.pb.Error err =
                  com.virogg.hbasecop.bridge.wire.pb.Error.newBuilder()
                      .setCode(7)
                      .setMessage("go-side boom")
                      .build();
              holder[0].deliver(
                  new Message(
                      FrameType.ERROR,
                      msg.reqId(),
                      msg.regionId(),
                      msg.hookId(),
                      err.toByteArray()));
            });
    holder[0] = mux;

    MuxHookDispatcher dispatcher = new MuxHookDispatcher(mux);
    IOException ex =
        assertThrows(
            IOException.class,
            () -> dispatcher.dispatchHook(0, HOOK, new byte[0], Duration.ofSeconds(2)));
    assertTrue(ex.getMessage().contains("go-side boom"), () -> "msg=" + ex.getMessage());
  }

  @Test
  void timeoutCancelsPendingEntry() {
    Multiplexer mux = new Multiplexer(msg -> {});
    MuxHookDispatcher dispatcher = new MuxHookDispatcher(mux);

    assertThrows(
        TimeoutException.class,
        () -> dispatcher.dispatchHook(0, HOOK, new byte[0], Duration.ofMillis(100)));

    assertEquals(0, mux.pendingCountForTesting(), "timed-out request must be cancelled");
  }
}
