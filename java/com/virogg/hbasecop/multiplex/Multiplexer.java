// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

import com.virogg.hbasecop.bridge.wire.FrameType;
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

/**
 * Correlates outbound Requests with inbound Responses over a single shared channel.
 *
 * <p>Each {@link #call} allocates a strictly monotonic {@code uint64} req_id, registers a pending
 * {@link CompletableFuture}, rewrites the supplied {@link Message} into a {@link FrameType#REQUEST}
 * carrying that id and hands it to the {@link Sender}. The future completes when {@link #deliver}
 * receives a Response with the same req_id, or fails with {@link ChannelClosedException} on {@link
 * #close}.
 *
 * <p>Crash-restart semantics (T35): {@link #pauseInflightFailing(Throwable)} fails every pending
 * future and parks subsequent {@link #call}s as <em>deferred</em>; {@link #resume()} adopts the
 * deferred queue into pending and dispatches each REQUEST. If a {@link ScheduledExecutorService}
 * and a positive {@code restartDeadlineMs} are configured via {@link #builder(Sender)}, every
 * deferred call has a per-call timer that fails it with {@link GoSideCrashedException} when the
 * deadline elapses without a resume. With neither wired in, calls during pause fail immediately
 * with {@link GoSideCrashedException} carrying the pause-time reason.
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
  private final long restartDeadlineMs;
  private final ScheduledExecutorService scheduler;
  private final AtomicLong nextId = new AtomicLong();

  private final Object lock = new Object();
  private final Map<Long, CompletableFuture<Message>> pending = new HashMap<>();
  private final Map<Long, Deferred> deferred = new LinkedHashMap<>();
  private boolean closed;
  private boolean paused;
  private Throwable pauseReason;

  public Multiplexer(Sender sender) {
    this(sender, 0L, null);
  }

  private Multiplexer(Sender sender, long restartDeadlineMs, ScheduledExecutorService scheduler) {
    this.sender = Objects.requireNonNull(sender, "sender");
    if (restartDeadlineMs < 0L) {
      throw new IllegalArgumentException("restartDeadlineMs must be ≥ 0, got " + restartDeadlineMs);
    }
    this.restartDeadlineMs = restartDeadlineMs;
    this.scheduler = scheduler;
  }

  public static Builder builder(Sender sender) {
    return new Builder(sender);
  }

  /**
   * Allocate a fresh req_id, register a waiter, mark {@code request} as {@link FrameType#REQUEST}
   * and dispatch via the {@link Sender}. The returned future completes when a matching Response is
   * delivered, fails with {@link ChannelClosedException} on {@link #close}, with {@link
   * GoSideCrashedException} when the mux is paused (or becomes paused while deferred), or with the
   * sender's exception if dispatch throws.
   *
   * <p>The supplied {@code request} is not mutated; a new {@link Message} carrying the allocated
   * req_id and {@code REQUEST} type is forwarded to the sender.
   */
  public CompletableFuture<Message> call(Message request) {
    Objects.requireNonNull(request, "request");

    long id = nextId.incrementAndGet();
    CompletableFuture<Message> fut = new CompletableFuture<>();
    Message outbound =
        new Message(FrameType.REQUEST, id, request.regionId(), request.hookId(), request.payload());

    synchronized (lock) {
      if (closed) {
        fut.completeExceptionally(new ChannelClosedException());
        return fut;
      }
      if (paused) {
        if (scheduler == null || restartDeadlineMs <= 0L) {
          // No deferred-wait wiring — fail fast with the pause reason.
          fut.completeExceptionally(
              pauseReason != null ? pauseReason : new GoSideCrashedException("multiplex: paused"));
          return fut;
        }
        Deferred d = new Deferred(id, outbound, fut);
        deferred.put(id, d);
        d.timeoutTask =
            scheduler.schedule(() -> failDeferred(id), restartDeadlineMs, TimeUnit.MILLISECONDS);
        return fut;
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
    return fut;
  }

  /**
   * Route {@code response} to the waiter that issued the matching {@link #call}.
   *
   * @return {@code true} when a waiter was found and completed, {@code false} when no pending entry
   *     exists for {@code response.reqId()} — typically a late arrival after timeout/close.
   */
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

  /**
   * Crash-restart entry: fail every {@link #call} currently pending a response with {@code reason}
   * and transition the mux to the {@code paused} state. Subsequent {@link #call}s are parked as
   * deferred (with a {@code restartDeadlineMs} timer when one is wired) until {@link #resume()} is
   * invoked or {@link #close()} is called. Idempotent when already paused — the existing {@code
   * reason} is retained.
   */
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

  /**
   * Leave the paused state and dispatch every deferred call. Each deferred entry is moved into
   * pending and its REQUEST is forwarded to the {@link Sender} (any pending deadline timer is
   * cancelled). A no-op when not paused.
   */
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

  /**
   * Transition to a terminal state: every pending and deferred {@link #call} fails with {@link
   * ChannelClosedException} and subsequent calls fail the same way. Idempotent.
   */
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

  /** Visible-for-testing: number of pending (in-flight, awaiting response) calls. */
  public int pendingCountForTesting() {
    synchronized (lock) {
      return pending.size();
    }
  }

  /** Visible-for-testing: number of deferred (paused, awaiting resume or deadline) calls. */
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

  /** Builder for the deferred-wait variant. */
  public static final class Builder {
    private final Sender sender;
    private long restartDeadlineMs;
    private ScheduledExecutorService scheduler;

    private Builder(Sender sender) {
      this.sender = Objects.requireNonNull(sender, "sender");
    }

    /**
     * Window during which a call issued while paused waits for {@link Multiplexer#resume()} before
     * being failed with {@link GoSideCrashedException}. {@code 0} → no waiting (fail fast).
     */
    public Builder restartDeadlineMs(long ms) {
      this.restartDeadlineMs = ms;
      return this;
    }

    /**
     * Scheduler that owns the per-deferred-call timeout tasks. When omitted (or {@code null}),
     * calls during pause fail immediately regardless of {@link #restartDeadlineMs(long)}.
     */
    public Builder scheduler(ScheduledExecutorService scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    public Multiplexer build() {
      return new Multiplexer(sender, restartDeadlineMs, scheduler);
    }
  }
}
