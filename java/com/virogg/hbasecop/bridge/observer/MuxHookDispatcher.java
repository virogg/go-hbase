// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
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
  public byte[] dispatchHook(byte hookId, byte[] hookCtxBytes, Duration timeout)
      throws IOException, InterruptedException, TimeoutException {
    Objects.requireNonNull(hookCtxBytes, "hookCtxBytes");
    Objects.requireNonNull(timeout, "timeout");

    // regionId is reserved for T61 multi-region routing; passing 0 keeps the wire shape stable.
    Message req = new Message(FrameType.REQUEST, 0L, 0, hookId, hookCtxBytes);
    CompletableFuture<Message> fut = mux.call(req);

    final Message resp;
    try {
      resp = fut.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
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
    return resp.payload();
  }
}
