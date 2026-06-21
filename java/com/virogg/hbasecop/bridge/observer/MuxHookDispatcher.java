// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.pb.Request;
import com.virogg.hbasecop.bridge.wire.pb.Response;
import com.virogg.hbasecop.multiplex.ChannelClosedException;
import com.virogg.hbasecop.multiplex.Multiplexer;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

public final class MuxHookDispatcher implements HookDispatcher {

  private static final long RESPONSE_SPIN_NANOS = 150_000;

  private final Multiplexer mux;

  public MuxHookDispatcher(Multiplexer mux) {
    this.mux = Objects.requireNonNull(mux, "mux");
  }

  @Override
  public byte[] dispatchHook(int regionId, byte hookId, byte[] hookCtxBytes, Duration timeout)
      throws IOException, InterruptedException, TimeoutException {
    Objects.requireNonNull(hookCtxBytes, "hookCtxBytes");
    Objects.requireNonNull(timeout, "timeout");

    byte[] requestPayload =
        Request.newBuilder().setHookCtx(ByteString.copyFrom(hookCtxBytes)).build().toByteArray();
    Message req = new Message(FrameType.REQUEST, 0L, regionId, hookId, requestPayload);
    Multiplexer.Call call = mux.callTracked(req);
    CompletableFuture<Message> fut = call.future;

    long spinDeadline = System.nanoTime() + RESPONSE_SPIN_NANOS;
    while (!fut.isDone() && System.nanoTime() - spinDeadline < 0) {
      Thread.onSpinWait();
    }

    final Message resp;
    try {
      resp = fut.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fut.cancel(false);
      mux.cancel(call.reqId);
      throw e;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ChannelClosedException) {
        throw new IOException("hbasecop: channel closed mid-call for hook " + hookId, cause);
      }
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("hbasecop: hook " + hookId + " dispatch failed", cause);
    }

    if (resp.type() == FrameType.ERROR) {
      try {
        com.virogg.hbasecop.bridge.wire.pb.Error err =
            com.virogg.hbasecop.bridge.wire.pb.Error.parseFrom(resp.payload());
        throw new IOException(
            "hbasecop: hook "
                + hookId
                + " returned error (code="
                + err.getCode()
                + "): "
                + err.getMessage());
      } catch (InvalidProtocolBufferException e) {
        throw new IOException("hbasecop: malformed wirepb.Error payload for hook " + hookId, e);
      }
    }

    final Response wireResp;
    try {
      wireResp = Response.parseFrom(resp.payload());
    } catch (InvalidProtocolBufferException e) {
      throw new IOException("hbasecop: malformed wirepb.Response payload for hook " + hookId, e);
    }
    return wireResp.getHookResp().toByteArray();
  }
}
