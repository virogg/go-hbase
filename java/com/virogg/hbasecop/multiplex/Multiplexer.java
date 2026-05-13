// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Correlates outbound Requests with inbound Responses over a single shared channel.
 *
 * <p>Each {@link #call} allocates a strictly monotonic {@code uint64} req_id, registers a pending
 * {@link CompletableFuture}, rewrites the supplied {@link Message} into a {@link FrameType#REQUEST}
 * carrying that id and hands it to the {@link Sender}. The future completes when {@link #deliver}
 * receives a Response with the same req_id, or fails with {@link ChannelClosedException} on {@link
 * #close}.
 *
 * <p>One {@code Multiplexer} instance corresponds to one (in,out) channel pair; region/hook fan-out
 * lives in higher layers (T61).
 */
public final class Multiplexer implements AutoCloseable {

  /** Outbound writer. Implementations must be safe for concurrent invocation. */
  @FunctionalInterface
  public interface Sender {
    void send(Message msg) throws Exception;
  }

  private final Sender sender;
  private final AtomicLong nextId = new AtomicLong();

  private final Object lock = new Object();
  private final Map<Long, CompletableFuture<Message>> pending = new HashMap<>();
  private boolean closed;

  public Multiplexer(Sender sender) {
    if (sender == null) {
      throw new NullPointerException("sender");
    }
    this.sender = sender;
  }

  /**
   * Allocate a fresh req_id, register a waiter, mark {@code request} as {@link FrameType#REQUEST}
   * and dispatch via the {@link Sender}. The returned future completes when a matching Response is
   * delivered, fails with {@link ChannelClosedException} on {@link #close}, or fails with the
   * sender's exception if dispatch throws.
   *
   * <p>The supplied {@code request} is not mutated; a new {@link Message} carrying the allocated
   * req_id and {@code REQUEST} type is forwarded to the sender.
   */
  public CompletableFuture<Message> call(Message request) {
    if (request == null) {
      throw new NullPointerException("request");
    }

    long id = nextId.incrementAndGet();
    CompletableFuture<Message> fut = new CompletableFuture<>();

    synchronized (lock) {
      if (closed) {
        fut.completeExceptionally(new ChannelClosedException());
        return fut;
      }
      pending.put(id, fut);
    }

    Message outbound =
        new Message(FrameType.REQUEST, id, request.regionId(), request.hookId(), request.payload());

    try {
      sender.send(outbound);
    } catch (Throwable t) {
      synchronized (lock) {
        pending.remove(id);
      }
      fut.completeExceptionally(t);
    }
    return fut;
  }

  /**
   * Route {@code response} to the waiter that issued the matching {@link #call}.
   *
   * @return {@code true} when a waiter was found and completed, {@code false} when no pending entry
   *     exists for {@code response.reqId()} — typically a late arrival after timeout/close.
   */
  public boolean deliver(Message response) {
    if (response == null) {
      throw new NullPointerException("response");
    }
    CompletableFuture<Message> fut;
    synchronized (lock) {
      fut = pending.remove(response.reqId());
    }
    if (fut == null) {
      return false;
    }
    fut.complete(response);
    return true;
  }

  /**
   * Transition to a terminal state: every pending {@link #call} fails with {@link
   * ChannelClosedException} and subsequent calls fail the same way. Idempotent.
   */
  @Override
  public void close() {
    List<CompletableFuture<Message>> drained;
    synchronized (lock) {
      if (closed) {
        return;
      }
      closed = true;
      drained = new ArrayList<>(pending.values());
      pending.clear();
    }
    ChannelClosedException reason = new ChannelClosedException();
    for (CompletableFuture<Message> f : drained) {
      f.completeExceptionally(reason);
    }
  }
}
