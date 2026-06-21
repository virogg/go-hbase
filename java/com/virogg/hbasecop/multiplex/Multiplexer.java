// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

import com.virogg.hbasecop.bridge.wire.Message;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class Multiplexer implements AutoCloseable {

  @FunctionalInterface
  public interface Sender {
    void send(Message msg) throws Exception;
  }

  public static final int DEFAULT_MAX_PENDING = 100_000;

  private final Sender sender;
  private final long restartDeadlineMs;
  private final ScheduledExecutorService scheduler;
  private final int maxPending;
  private final AtomicLong nextId = new AtomicLong();

  private final Object lock = new Object();
  private final Map<Long, CompletableFuture<Message>> pending = new HashMap<>();
  private final Map<Long, Deferred> deferred = new LinkedHashMap<>();
  private boolean closed;
  private boolean paused;
  private Throwable pauseReason;

  public Multiplexer(Sender sender) {
    this(sender, 0L, null, DEFAULT_MAX_PENDING);
  }

  private Multiplexer(
      Sender sender, long restartDeadlineMs, ScheduledExecutorService scheduler, int maxPending) {
    this.sender = Objects.requireNonNull(sender, "sender");
    if (restartDeadlineMs < 0L) {
      throw new IllegalArgumentException("restartDeadlineMs must be ≥ 0, got " + restartDeadlineMs);
    }
    if (maxPending <= 0) {
      throw new IllegalArgumentException("maxPending must be > 0, got " + maxPending);
    }
    this.restartDeadlineMs = restartDeadlineMs;
    this.scheduler = scheduler;
    this.maxPending = maxPending;
  }

  public static Builder builder(Sender sender) {
    return new Builder(sender);
  }

  public CompletableFuture<Message> call(Message request) {
    return callTracked(request).future;
  }

  public Call callTracked(Message request) {
    Objects.requireNonNull(request, "request");

    long id = nextId.incrementAndGet();
    CompletableFuture<Message> fut = new CompletableFuture<>();
    Message outbound =
        new Message(request.type(), id, request.regionId(), request.hookId(), request.payload());

    synchronized (lock) {
      if (closed) {
        fut.completeExceptionally(new ChannelClosedException());
        return new Call(id, fut);
      }
      if (paused) {
        if (scheduler == null || restartDeadlineMs <= 0L) {
          fut.completeExceptionally(
              pauseReason != null ? pauseReason : new GoSideCrashedException("multiplex: paused"));
          return new Call(id, fut);
        }
        if (deferred.size() + pending.size() >= maxPending) {
          fut.completeExceptionally(
              new GoSideCrashedException("multiplex: pending overflow (max=" + maxPending + ")"));
          return new Call(id, fut);
        }
        Deferred d = new Deferred(id, outbound, fut);
        deferred.put(id, d);
        d.timeoutTask =
            scheduler.schedule(() -> failDeferred(id), restartDeadlineMs, TimeUnit.MILLISECONDS);
        return new Call(id, fut);
      }
      if (pending.size() >= maxPending) {
        fut.completeExceptionally(
            new GoSideCrashedException("multiplex: pending overflow (max=" + maxPending + ")"));
        return new Call(id, fut);
      }
      pending.put(id, fut);
    }

    try {
      sender.send(outbound);
    } catch (Throwable t) {
      synchronized (lock) {
        pending.remove(id);
      }
      fut.completeExceptionally(t);
    }
    return new Call(id, fut);
  }

  public void cancel(long reqId) {
    Deferred d;
    synchronized (lock) {
      pending.remove(reqId);
      d = deferred.remove(reqId);
    }
    if (d != null && d.timeoutTask != null) {
      d.timeoutTask.cancel(false);
    }
  }

  public static final class Call {
    public final long reqId;
    public final CompletableFuture<Message> future;

    Call(long reqId, CompletableFuture<Message> future) {
      this.reqId = reqId;
      this.future = future;
    }
  }

  public boolean deliver(Message response) {
    Objects.requireNonNull(response, "response");
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

  public void pauseInflightFailing(Throwable reason) {
    Objects.requireNonNull(reason, "reason");
    List<CompletableFuture<Message>> drained;
    synchronized (lock) {
      if (closed || paused) {
        return;
      }
      paused = true;
      pauseReason = reason;
      drained = new ArrayList<>(pending.values());
      pending.clear();
    }
    for (CompletableFuture<Message> f : drained) {
      f.completeExceptionally(reason);
    }
  }

  public void resume() {
    List<Deferred> drained;
    synchronized (lock) {
      if (!paused) {
        return;
      }
      paused = false;
      pauseReason = null;
      drained = new ArrayList<>(deferred.values());
      deferred.clear();
      for (Deferred d : drained) {
        if (d.timeoutTask != null) {
          d.timeoutTask.cancel(false);
        }
        pending.put(d.id, d.future);
      }
    }
    for (Deferred d : drained) {
      try {
        sender.send(d.outbound);
      } catch (Throwable t) {
        synchronized (lock) {
          pending.remove(d.id);
        }
        d.future.completeExceptionally(t);
      }
    }
  }

  @Override
  public void close() {
    List<CompletableFuture<Message>> drainedPending;
    List<Deferred> drainedDeferred;
    synchronized (lock) {
      if (closed) {
        return;
      }
      closed = true;
      drainedPending = new ArrayList<>(pending.values());
      pending.clear();
      drainedDeferred = new ArrayList<>(deferred.values());
      deferred.clear();
    }
    ChannelClosedException reason = new ChannelClosedException();
    for (CompletableFuture<Message> f : drainedPending) {
      f.completeExceptionally(reason);
    }
    for (Deferred d : drainedDeferred) {
      if (d.timeoutTask != null) {
        d.timeoutTask.cancel(false);
      }
      d.future.completeExceptionally(reason);
    }
  }

  public int pendingCountForTesting() {
    synchronized (lock) {
      return pending.size();
    }
  }

  public int deferredCountForTesting() {
    synchronized (lock) {
      return deferred.size();
    }
  }

  private void failDeferred(long id) {
    Deferred d;
    synchronized (lock) {
      d = deferred.remove(id);
    }
    if (d == null) {
      return;
    }
    d.future.completeExceptionally(
        new GoSideCrashedException(
            "multiplex: restart deadline exceeded (" + restartDeadlineMs + "ms)"));
  }

  private static final class Deferred {
    final long id;
    final Message outbound;
    final CompletableFuture<Message> future;
    ScheduledFuture<?> timeoutTask;

    Deferred(long id, Message outbound, CompletableFuture<Message> future) {
      this.id = id;
      this.outbound = outbound;
      this.future = future;
    }
  }

  public static final class Builder {
    private final Sender sender;
    private long restartDeadlineMs;
    private ScheduledExecutorService scheduler;
    private int maxPending = DEFAULT_MAX_PENDING;

    private Builder(Sender sender) {
      this.sender = Objects.requireNonNull(sender, "sender");
    }

    public Builder maxPending(int n) {
      this.maxPending = n;
      return this;
    }

    public Builder restartDeadlineMs(long ms) {
      this.restartDeadlineMs = ms;
      return this;
    }

    public Builder scheduler(ScheduledExecutorService scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    public Multiplexer build() {
      return new Multiplexer(sender, restartDeadlineMs, scheduler, maxPending);
    }
  }
}
