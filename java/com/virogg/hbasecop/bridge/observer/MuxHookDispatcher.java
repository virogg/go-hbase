// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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

/**
 * {@link HookDispatcher} backed by a {@link Multiplexer}: every hook invocation becomes one {@code
 * REQUEST} frame on the Java→Go channel, and the matching {@code RESPONSE} (correlated by the mux
 * on req_id) returns the payload bytes the adapter then parses as a {@code HookResponse}.
 *
 * <p>This is the production wiring used by {@link com.virogg.hbasecop.bridge.CoprocessorRuntime};
 * unit tests of {@link com.virogg.hbasecop.bridge.observer.RegionObserverAdapter} use a manual mock
 * instead.
 */
public final class MuxHookDispatcher implements HookDispatcher {

  private final Multiplexer mux;

  public MuxHookDispatcher(Multiplexer mux) {
    this.mux = Objects.requireNonNull(mux, "mux");
  }

  @Override
  public byte[] dispatchHook(int regionId, byte hookId, byte[] hookCtxBytes, Duration timeout)
      throws IOException, InterruptedException, TimeoutException {
    Objects.requireNonNull(hookCtxBytes, "hookCtxBytes");
    Objects.requireNonNull(timeout, "timeout");

    // Wrap the per-hook context bytes in the wirepb.Request envelope the Go
    // dispatcher unmarshals from Message.payload (see pkg/hbasecop/dispatch.go).
    byte[] requestPayload =
        Request.newBuilder().setHookCtx(ByteString.copyFrom(hookCtxBytes)).build().toByteArray();
    // regionId is the T61 multi-region routing key, allocated by the adapter
    // via RegionIdAllocator on RegionObserver.start(env). 0 = no region scope.
    Message req = new Message(FrameType.REQUEST, 0L, regionId, hookId, requestPayload);
    Multiplexer.Call call = mux.callTracked(req);
    CompletableFuture<Message> fut = call.future;

    final Message resp;
    try {
      resp = fut.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      // Drop the waiter so its future + pending-map entry are reclaimed.
      // CompletableFuture.get(timeout) does NOT remove the registration; without
      // this, every timed-out hook leaks one entry in Multiplexer.pending for the
      // life of the channel. A late RESPONSE for this id is then ignored.
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

    // The Go side may answer with either a RESPONSE (wirepb.Response carrying
    // the HookResponse bytes) or an ERROR (wirepb.Error). Map the latter to
    // an IOException so the adapter surfaces it as a strict-mode failure.
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

    // The Go side wraps the HookResponse bytes in wirepb.Response.hook_resp;
    // unwrap before handing back to the adapter, which parses HookResponse.
    final Response wireResp;
    try {
      wireResp = Response.parseFrom(resp.payload());
    } catch (InvalidProtocolBufferException e) {
      throw new IOException("hbasecop: malformed wirepb.Response payload for hook " + hookId, e);
    }
    return wireResp.getHookResp().toByteArray();
  }
}
