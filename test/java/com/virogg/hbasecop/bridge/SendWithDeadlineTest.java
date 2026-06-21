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

class SendWithDeadlineTest {

  @Test
  void returnsOnceSendSucceeds() throws Exception {
    AtomicLong clock = new AtomicLong();
    int[] calls = {0};
    CoprocessorRuntime.sendWithDeadline(
        () -> {
          calls[0]++;
          if (calls[0] < 3) {
            throw new RingFullException();
          }
        },
        1_000L,
        () -> clock.addAndGet(TimeUnit.MICROSECONDS.toNanos(10)));
    assertEquals(3, calls[0], "should retry until the send succeeds");
  }

  @Test
  void throwsAtDeadlineInsteadOfSpinningForever() {
    AtomicLong clock = new AtomicLong();
    ShmemException ex =
        assertThrows(
            ShmemException.class,
            () ->
                CoprocessorRuntime.sendWithDeadline(
                    () -> {
                      throw new RingFullException();
                    },
                    5L,
                    () -> clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1))));
    assertTrue(ex.getMessage().contains("not draining"), "message should explain the stall");
  }

  @Test
  void honorsInterruption() {
    Thread.currentThread().interrupt();
    assertThrows(
        InterruptedException.class,
        () ->
            CoprocessorRuntime.sendWithDeadline(
                () -> {
                  throw new RingFullException();
                },
                10_000L,
                System::nanoTime));
    assertTrue(!Thread.currentThread().isInterrupted());
  }
}
