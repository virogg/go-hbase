// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.shmem.RingFullException;
import com.virogg.hbasecop.bridge.shmem.ShmemException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Regression for the P1 liveness defect: {@code sendOnChannel} used an UNBOUNDED busy-spin over a
 * non-blocking {@code Channel.send} under the shared sendLock, so a full/hung/dead Go side pinned
 * the RegionServer RPC-handler thread at 100% CPU forever. {@link
 * CoprocessorRuntime#sendWithDeadline} must (a) return once a slot frees, (b) give up at the
 * deadline with a clear ShmemException rather than spin forever, and (c) honor interruption. A fake
 * nanoClock keeps the test instant.
 */
class SendWithDeadlineTest {

  @Test
  void returnsOnceSendSucceeds() throws Exception {
    AtomicLong clock = new AtomicLong();
    int[] calls = {0};
    CoprocessorRuntime.sendWithDeadline(
        () -> {
          calls[0]++;
          if (calls[0] < 3) {
            throw new RingFullException(); // ring full for the first two tries
          }
        },
        1_000L,
        () -> clock.addAndGet(TimeUnit.MICROSECONDS.toNanos(10)));
    assertEquals(3, calls[0], "should retry until the send succeeds");
  }

  @Test
  void throwsAtDeadlineInsteadOfSpinningForever() {
    // Clock jumps 1ms per reading; with a 5ms deadline the loop gives up in a handful of
    // iterations instead of spinning unboundedly against an always-full ring.
    AtomicLong clock = new AtomicLong();
    ShmemException ex =
        assertThrows(
            ShmemException.class,
            () ->
                CoprocessorRuntime.sendWithDeadline(
                    () -> {
                      throw new RingFullException(); // never drains
                    },
                    5L,
                    () -> clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1))));
    assertTrue(ex.getMessage().contains("not draining"), "message should explain the stall");
  }

  @Test
  void honorsInterruption() {
    Thread.currentThread().interrupt(); // pre-set the flag
    assertThrows(
        InterruptedException.class,
        () ->
            CoprocessorRuntime.sendWithDeadline(
                () -> {
                  throw new RingFullException();
                },
                10_000L,
                System::nanoTime));
    // assertThrows consumed the exception; the flag was cleared by Thread.interrupted().
    assertTrue(!Thread.currentThread().isInterrupted());
  }
}
