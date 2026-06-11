// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MultiplexerTest {

  /** Echo responder that reflects every Request as a TypeResponse with identical payload. */
  private static Thread startEchoResponder(
      ConcurrentLinkedQueue<Message> outbox, Multiplexer mux, AtomicBoolean stop) {
    Thread t =
        new Thread(
            () -> {
              while (!stop.get() || !outbox.isEmpty()) {
                Message req = outbox.poll();
                if (req == null) {
                  Thread.yield();
                  continue;
                }
                Message resp =
                    new Message(
                        FrameType.RESPONSE,
                        req.reqId(),
                        req.regionId(),
                        req.hookId(),
                        Arrays.copyOf(req.payload(), req.payload().length));
                mux.deliver(resp);
              }
            },
            "echo-responder");
    t.setDaemon(true);
    t.start();
    return t;
  }

  /**
   * T24 acceptance test: 1000 parallel call() invocations against a single Multiplexer all observe
   * their own matching Response.
   */
  @Test
  void concurrentCallsAllMatched() throws Exception {
    ConcurrentLinkedQueue<Message> outbox = new ConcurrentLinkedQueue<>();
    Multiplexer mux = new Multiplexer(outbox::add);

    AtomicBoolean stop = new AtomicBoolean(false);
    Thread responder = startEchoResponder(outbox, mux, stop);

    final int n = 1000;
    ExecutorService pool = Executors.newFixedThreadPool(32);
    try {
      List<CompletableFuture<Message>> futures = new ArrayList<>(n);
      List<byte[]> expected = new ArrayList<>(n);

      for (int i = 0; i < n; i++) {
        byte[] payload = ("payload-" + i).getBytes(StandardCharsets.UTF_8);
        expected.add(payload);
        final int idx = i;
        CompletableFuture<Message> f = new CompletableFuture<>();
        pool.submit(
            () -> {
              try {
                Message req = new Message(FrameType.REQUEST, 0, idx, (byte) 0, payload);
                mux.call(req)
                    .whenComplete(
                        (m, e) -> {
                          if (e != null) {
                            f.completeExceptionally(e);
                          } else {
                            f.complete(m);
                          }
                        });
              } catch (Throwable t) {
                f.completeExceptionally(t);
              }
            });
        futures.add(f);
      }

      for (int i = 0; i < n; i++) {
        Message resp = futures.get(i).get(5, TimeUnit.SECONDS);
        assertEquals(FrameType.RESPONSE, resp.type());
        assertEquals(i, resp.regionId());
        assertTrue(Arrays.equals(expected.get(i), resp.payload()), "payload mismatch for caller");
      }
    } finally {
      stop.set(true);
      responder.join(2000);
      pool.shutdownNow();
      mux.close();
    }
  }

  @Test
  void allocatesMonotonicReqIds() throws Exception {
    ConcurrentLinkedQueue<Message> outbox = new ConcurrentLinkedQueue<>();
    Multiplexer mux = new Multiplexer(outbox::add);

    final int n = 256;
    List<CompletableFuture<Message>> futures = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      futures.add(mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null)));
    }

    Set<Long> ids = new HashSet<>();
    long max = 0;
    for (Message m : outbox) {
      assertTrue(m.reqId() > 0, "reqId must be > 0");
      assertTrue(ids.add(m.reqId()), "duplicate reqId " + m.reqId());
      max = Math.max(max, m.reqId());
    }
    assertEquals(n, ids.size());
    assertEquals(n, max);

    mux.close();
    for (CompletableFuture<Message> f : futures) {
      ExecutionException ee =
          assertThrows(ExecutionException.class, () -> f.get(1, TimeUnit.SECONDS));
      assertTrue(ee.getCause() instanceof ChannelClosedException);
    }
  }

  @Test
  void closeFailsPendingCalls() throws Exception {
    Multiplexer mux = new Multiplexer(msg -> {});

    List<CompletableFuture<Message>> futures = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      futures.add(mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null)));
    }

    mux.close();

    for (CompletableFuture<Message> f : futures) {
      ExecutionException ee =
          assertThrows(ExecutionException.class, () -> f.get(1, TimeUnit.SECONDS));
      assertTrue(ee.getCause() instanceof ChannelClosedException, () -> "cause=" + ee.getCause());
    }
  }

  @Test
  void callAfterCloseFailsWithoutSenderInvocation() {
    AtomicBoolean sent = new AtomicBoolean(false);
    Multiplexer mux = new Multiplexer(msg -> sent.set(true));
    mux.close();

    CompletableFuture<Message> f = mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null));
    assertTrue(f.isCompletedExceptionally());
    ExecutionException ee = assertThrows(ExecutionException.class, f::get);
    assertTrue(ee.getCause() instanceof ChannelClosedException);
    assertFalse(sent.get(), "send must not be invoked after close");
  }

  @Test
  void deliverUnknownReqIdReturnsFalse() {
    Multiplexer mux = new Multiplexer(msg -> {});
    boolean delivered = mux.deliver(new Message(FrameType.RESPONSE, 42L, 0, (byte) 0, new byte[0]));
    assertFalse(delivered);
  }

  @Test
  void senderThrowsCompletesFutureExceptionally() {
    IOException boom = new IOException("send boom");
    Multiplexer mux =
        new Multiplexer(
            msg -> {
              throw boom;
            });

    CompletableFuture<Message> f = mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null));
    assertTrue(f.isCompletedExceptionally());
    ExecutionException ee = assertThrows(ExecutionException.class, f::get);
    assertSame(boom, ee.getCause());

    // Slot must be freed: a late Deliver for that reqId returns false.
    assertFalse(mux.deliver(new Message(FrameType.RESPONSE, 1L, 0, (byte) 0, new byte[0])));
  }

  @Test
  void requestRewrittenWithAllocatedReqIdAndRequestType() throws Exception {
    ConcurrentLinkedQueue<Message> outbox = new ConcurrentLinkedQueue<>();
    Multiplexer mux = new Multiplexer(outbox::add);

    // Caller passes RESPONSE type with non-zero reqId — mux should
    // rewrite both fields before handing to the sender.
    mux.call(new Message(FrameType.RESPONSE, 999L, 7, (byte) 3, new byte[] {1, 2, 3}));

    assertEquals(1, outbox.size());
    Message sent = outbox.poll();
    assertNotNull(sent);
    assertEquals(FrameType.REQUEST, sent.type());
    assertEquals(1L, sent.reqId());
    assertEquals(7, sent.regionId());
    assertEquals((byte) 3, sent.hookId());
    assertTrue(Arrays.equals(new byte[] {1, 2, 3}, sent.payload()));
  }

  @Test
  void closeIsIdempotent() {
    Multiplexer mux = new Multiplexer(msg -> {});
    mux.close();
    mux.close(); // must not throw
  }

  @Test
  void responseAfterCallCompletesFuture() throws Exception {
    ConcurrentLinkedQueue<Message> outbox = new ConcurrentLinkedQueue<>();
    Multiplexer mux = new Multiplexer(outbox::add);

    CompletableFuture<Message> f =
        mux.call(
            new Message(FrameType.REQUEST, 0, 1, (byte) 9, "hi".getBytes(StandardCharsets.UTF_8)));
    Message sent = outbox.poll();
    assertNotNull(sent);

    assertTrue(
        mux.deliver(new Message(FrameType.RESPONSE, sent.reqId(), 1, (byte) 9, "ok".getBytes())));

    Message got = f.get(1, TimeUnit.SECONDS);
    assertEquals(FrameType.RESPONSE, got.type());
    assertEquals(sent.reqId(), got.reqId());

    // Second deliver for the same reqId is an orphan.
    assertFalse(
        mux.deliver(new Message(FrameType.RESPONSE, sent.reqId(), 1, (byte) 9, new byte[0])));
  }

  @Test
  void pauseInflightFailsPendingWithGoSideCrashedException() throws Exception {
    Multiplexer mux = new Multiplexer(msg -> {});

    List<CompletableFuture<Message>> futures = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      futures.add(mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null)));
    }

    GoSideCrashedException reason = new GoSideCrashedException("pid 1234 exited");
    mux.pauseInflightFailing(reason);

    for (CompletableFuture<Message> f : futures) {
      ExecutionException ee =
          assertThrows(ExecutionException.class, () -> f.get(1, TimeUnit.SECONDS));
      assertSame(reason, ee.getCause(), () -> "cause=" + ee.getCause());
    }
    mux.close();
  }

  @Test
  void pauseIsIdempotent() {
    Multiplexer mux = new Multiplexer(msg -> {});
    mux.pauseInflightFailing(new GoSideCrashedException("first"));
    // Second pause must not throw and is a no-op when already paused.
    mux.pauseInflightFailing(new GoSideCrashedException("second"));
    mux.close();
  }

  @Test
  void callDuringPauseDefersAndDoesNotInvokeSender() {
    AtomicInteger sends = new AtomicInteger();
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "mux-test-scheduler");
              t.setDaemon(true);
              return t;
            });
    try {
      Multiplexer mux =
          Multiplexer.builder(msg -> sends.incrementAndGet())
              .restartDeadlineMs(1_000L)
              .scheduler(scheduler)
              .build();
      mux.pauseInflightFailing(new GoSideCrashedException("paused"));

      CompletableFuture<Message> f = mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null));
      assertFalse(f.isDone(), "deferred call must not complete while paused");
      assertEquals(0, sends.get(), "sender must not be invoked during pause");

      mux.close();
      ExecutionException ee = assertThrows(ExecutionException.class, () -> f.get(1, SECONDS));
      assertTrue(ee.getCause() instanceof ChannelClosedException);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  void resumeDispatchesDeferredCalls() throws Exception {
    ConcurrentLinkedQueue<Message> outbox = new ConcurrentLinkedQueue<>();
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "mux-test-scheduler");
              t.setDaemon(true);
              return t;
            });
    try {
      Multiplexer mux =
          Multiplexer.builder(outbox::add).restartDeadlineMs(5_000L).scheduler(scheduler).build();
      mux.pauseInflightFailing(new GoSideCrashedException("paused"));

      CompletableFuture<Message> f =
          mux.call(new Message(FrameType.REQUEST, 0, 7, (byte) 9, new byte[] {1, 2, 3}));
      assertFalse(f.isDone(), "deferred while paused");
      assertTrue(outbox.isEmpty(), "no send yet");

      mux.resume();

      // After resume, the queued call must reach the sender as REQUEST with the
      // mux-assigned reqId.
      Message sent = null;
      long deadline = System.currentTimeMillis() + 1_000L;
      while (System.currentTimeMillis() < deadline && sent == null) {
        sent = outbox.poll();
        if (sent == null) Thread.sleep(5);
      }
      assertNotNull(sent, "resume must dispatch deferred calls");
      assertEquals(FrameType.REQUEST, sent.type());
      assertEquals(7, sent.regionId());
      assertEquals((byte) 9, sent.hookId());

      assertTrue(
          mux.deliver(new Message(FrameType.RESPONSE, sent.reqId(), 7, (byte) 9, new byte[0])));
      assertNotNull(f.get(1, SECONDS));
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  void deferredCallFailsAfterRestartDeadline() throws Exception {
    AtomicInteger sends = new AtomicInteger();
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "mux-test-scheduler");
              t.setDaemon(true);
              return t;
            });
    try {
      Multiplexer mux =
          Multiplexer.builder(msg -> sends.incrementAndGet())
              .restartDeadlineMs(100L)
              .scheduler(scheduler)
              .build();
      mux.pauseInflightFailing(new GoSideCrashedException("paused"));

      long t0 = System.currentTimeMillis();
      CompletableFuture<Message> f = mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null));

      ExecutionException ee = assertThrows(ExecutionException.class, () -> f.get(2, SECONDS));
      long elapsed = System.currentTimeMillis() - t0;
      assertTrue(
          ee.getCause() instanceof GoSideCrashedException,
          () -> "expected GoSideCrashedException, got " + ee.getCause());
      assertTrue(
          elapsed >= 100L && elapsed < 1_000L,
          () -> "deferred fail should fire near deadline; elapsed=" + elapsed + "ms");
      assertEquals(0, sends.get(), "sender must not be invoked for deferred-then-failed call");
      mux.close();
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  void callDuringPauseFailsImmediatelyWhenNoSchedulerWired() {
    AtomicInteger sends = new AtomicInteger();
    Multiplexer mux = new Multiplexer(msg -> sends.incrementAndGet()); // no scheduler / deadline
    GoSideCrashedException reason = new GoSideCrashedException("kill -9");
    mux.pauseInflightFailing(reason);

    CompletableFuture<Message> f = mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null));
    assertTrue(f.isCompletedExceptionally());
    ExecutionException ee = assertThrows(ExecutionException.class, f::get);
    assertSame(reason, ee.getCause());
    assertEquals(0, sends.get(), "sender must not run during pause");
    mux.close();
  }

  @Test
  void closeDuringPauseFailsDeferredCallsWithChannelClosed() throws Exception {
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "mux-test-scheduler");
              t.setDaemon(true);
              return t;
            });
    try {
      Multiplexer mux =
          Multiplexer.builder(msg -> {}).restartDeadlineMs(5_000L).scheduler(scheduler).build();
      mux.pauseInflightFailing(new GoSideCrashedException("paused"));

      CompletableFuture<Message> f = mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null));
      assertFalse(f.isDone());

      mux.close();
      ExecutionException ee = assertThrows(ExecutionException.class, () -> f.get(1, SECONDS));
      assertTrue(
          ee.getCause() instanceof ChannelClosedException,
          () -> "expected ChannelClosedException, got " + ee.getCause());
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  void getWithTimeoutBlocksUntilDeliver() {
    Multiplexer mux = new Multiplexer(msg -> {});
    CompletableFuture<Message> f = mux.call(new Message(FrameType.REQUEST, 0, 0, (byte) 0, null));
    assertThrows(TimeoutException.class, () -> f.get(50, TimeUnit.MILLISECONDS));
    mux.close();
  }
}
