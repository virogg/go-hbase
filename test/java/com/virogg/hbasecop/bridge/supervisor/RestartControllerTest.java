// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * T34 — verifies the {@link RestartController} state machine: exponential-backoff scheduling,
 * consecutive-fail counting, transition to {@code UNHEALTHY} after {@code maxConsecutiveFails}, and
 * periodic probing thereafter. Drives time and attempt outcomes deterministically.
 */
final class RestartControllerTest {

  private static final RestartConfig CFG =
      RestartConfig.builder()
          .initialDelayMs(200L)
          .maxDelayMs(5_000L)
          .multiplier(2.0)
          .jitterRatio(0.2)
          .maxConsecutiveFails(5)
          .probeIntervalMs(30_000L)
          .build();

  private AtomicLong nowMs;
  private AtomicInteger attempts;
  private Deque<Boolean> outcomes;
  private RestartController ctl;

  @BeforeEach
  void setUp() {
    nowMs = new AtomicLong(1_000L);
    attempts = new AtomicInteger(0);
    outcomes = new ArrayDeque<>();
    ctl =
        new RestartController(
            CFG,
            nowMs::get,
            () -> {
              attempts.incrementAndGet();
              Boolean next = outcomes.pollFirst();
              return next != null && next;
            },
            () -> 0.5); // jitter source returns 0.5 → no offset (2*0.5-1=0)
  }

  @Test
  void freshControllerIsHealthy() {
    assertEquals(RestartController.State.HEALTHY, ctl.state());
    assertEquals(0, ctl.consecutiveFails());
    assertFalse(ctl.tick(), "tick on HEALTHY must be a no-op");
    assertEquals(0, attempts.get());
  }

  @Test
  void notifyDeadSchedulesFirstAttemptAfterInitialDelay() {
    ctl.notifyDead();
    assertEquals(RestartController.State.AWAITING_RESTART, ctl.state());

    // Just before the initial delay — no attempt yet.
    nowMs.addAndGet(199L);
    assertFalse(ctl.tick());
    assertEquals(0, attempts.get());

    // At the deadline — attempt fires.
    nowMs.addAndGet(1L);
    outcomes.add(true);
    assertTrue(ctl.tick());
    assertEquals(1, attempts.get());
    assertEquals(RestartController.State.HEALTHY, ctl.state());
    assertEquals(0, ctl.consecutiveFails());
  }

  @Test
  void backoffDelayDoublesUpToCap() {
    // Pure delay math, jitter pinned to 0 via 0.5 supplier.
    assertEquals(200L, ctl.backoffDelayMs(0));
    assertEquals(400L, ctl.backoffDelayMs(1));
    assertEquals(800L, ctl.backoffDelayMs(2));
    assertEquals(1_600L, ctl.backoffDelayMs(3));
    assertEquals(3_200L, ctl.backoffDelayMs(4));
    assertEquals(5_000L, ctl.backoffDelayMs(5));
    assertEquals(5_000L, ctl.backoffDelayMs(6));
    assertEquals(5_000L, ctl.backoffDelayMs(20));
  }

  @Test
  void jitterAppliedAtBothExtremes() {
    RestartController low = new RestartController(CFG, nowMs::get, () -> true, () -> 0.0); // -20%
    RestartController high =
        new RestartController(CFG, nowMs::get, () -> true, () -> 0.999999); // +20%
    assertEquals(160L, low.backoffDelayMs(0)); // 200 * 0.8
    assertEquals(239L, high.backoffDelayMs(0)); // 200 * 1.2 minus rounding (200*1.19999...)
  }

  @Test
  void failuresUseExponentialBackoff() {
    ctl.notifyDead();

    // Sequence: 5 failures, each at the scheduled deadline.
    long[] expectedDelays = {200L, 400L, 800L, 1_600L, 3_200L};
    for (int i = 0; i < expectedDelays.length; i++) {
      long due = ctl.nextAttemptDueMs();
      assertEquals(nowMs.get() + expectedDelays[i], due, "delay[" + i + "]");
      nowMs.set(due);
      outcomes.add(false);
      assertTrue(ctl.tick(), "tick should run attempt at deadline");
      assertEquals(i + 1, attempts.get());
      assertEquals(i + 1, ctl.consecutiveFails());
    }
    // 5 failures → UNHEALTHY now.
    assertEquals(RestartController.State.UNHEALTHY, ctl.state());
  }

  @Test
  void exceedingMaxFailsTransitionsToUnhealthyAndProbes() {
    ctl.notifyDead();
    for (int i = 0; i < 5; i++) {
      nowMs.set(ctl.nextAttemptDueMs());
      outcomes.add(false);
      ctl.tick();
    }
    assertEquals(RestartController.State.UNHEALTHY, ctl.state());
    long unhealthyAt = nowMs.get();
    assertEquals(unhealthyAt + 30_000L, ctl.nextAttemptDueMs());

    // Within the probe interval — no attempt.
    nowMs.set(unhealthyAt + 29_999L);
    assertFalse(ctl.tick());
    assertEquals(5, attempts.get());

    // At probe interval — attempt fires.
    nowMs.set(unhealthyAt + 30_000L);
    outcomes.add(false);
    assertTrue(ctl.tick());
    assertEquals(6, attempts.get());
    // Still unhealthy, fail counter unchanged (we don't keep ratcheting forever).
    assertEquals(RestartController.State.UNHEALTHY, ctl.state());
    // Next probe scheduled another probe-interval out.
    assertEquals(unhealthyAt + 30_000L + 30_000L, ctl.nextAttemptDueMs());
  }

  @Test
  void probeSuccessReturnsToHealthy() {
    // Cross threshold to UNHEALTHY.
    ctl.notifyDead();
    for (int i = 0; i < 5; i++) {
      nowMs.set(ctl.nextAttemptDueMs());
      outcomes.add(false);
      ctl.tick();
    }
    assertEquals(RestartController.State.UNHEALTHY, ctl.state());

    // Probe succeeds.
    nowMs.set(ctl.nextAttemptDueMs());
    outcomes.add(true);
    assertTrue(ctl.tick());
    assertEquals(RestartController.State.HEALTHY, ctl.state());
    assertEquals(0, ctl.consecutiveFails(), "successful probe resets counter");
  }

  @Test
  void successPartwayThroughResetsCounter() {
    ctl.notifyDead();
    // 2 fails…
    nowMs.set(ctl.nextAttemptDueMs());
    outcomes.add(false);
    ctl.tick();
    nowMs.set(ctl.nextAttemptDueMs());
    outcomes.add(false);
    ctl.tick();
    assertEquals(2, ctl.consecutiveFails());
    // …then success.
    nowMs.set(ctl.nextAttemptDueMs());
    outcomes.add(true);
    ctl.tick();
    assertEquals(RestartController.State.HEALTHY, ctl.state());
    assertEquals(0, ctl.consecutiveFails());

    // A subsequent crash starts the backoff at 200 again.
    ctl.notifyDead();
    assertEquals(nowMs.get() + 200L, ctl.nextAttemptDueMs());
  }

  @Test
  void notifyDeadWhileAwaitingIsNoOp() {
    ctl.notifyDead();
    long firstDue = ctl.nextAttemptDueMs();
    nowMs.addAndGet(50L);
    ctl.notifyDead(); // duplicate signal
    assertEquals(firstDue, ctl.nextAttemptDueMs(), "duplicate notifyDead must not reschedule");
    assertEquals(RestartController.State.AWAITING_RESTART, ctl.state());
  }

  @Test
  void notifyDeadWhileUnhealthyIsNoOp() {
    ctl.notifyDead();
    for (int i = 0; i < 5; i++) {
      nowMs.set(ctl.nextAttemptDueMs());
      outcomes.add(false);
      ctl.tick();
    }
    long probeDue = ctl.nextAttemptDueMs();
    ctl.notifyDead();
    assertEquals(probeDue, ctl.nextAttemptDueMs());
    assertEquals(RestartController.State.UNHEALTHY, ctl.state());
  }

  @Test
  void stopCancelsScheduledAttempts() {
    ctl.notifyDead();
    ctl.stop();
    assertEquals(RestartController.State.STOPPED, ctl.state());

    nowMs.addAndGet(10_000L);
    assertFalse(ctl.tick());
    assertEquals(0, attempts.get());

    // notifyDead in STOPPED is a no-op.
    ctl.notifyDead();
    assertEquals(RestartController.State.STOPPED, ctl.state());
  }

  @Test
  void attemptThrowingIsTreatedAsFailure() {
    RestartController throwing =
        new RestartController(
            CFG,
            nowMs::get,
            () -> {
              attempts.incrementAndGet();
              throw new RuntimeException("boom");
            },
            () -> 0.5);
    throwing.notifyDead();
    nowMs.set(throwing.nextAttemptDueMs());
    assertTrue(throwing.tick(), "exception still counts as an attempt");
    assertEquals(1, attempts.get());
    assertEquals(1, throwing.consecutiveFails());
    assertEquals(RestartController.State.AWAITING_RESTART, throwing.state());
  }

  @Test
  void constructorRejectsNulls() {
    assertThrows(
        NullPointerException.class,
        () -> new RestartController(null, nowMs::get, () -> true, () -> 0.5));
    assertThrows(
        NullPointerException.class, () -> new RestartController(CFG, null, () -> true, () -> 0.5));
    assertThrows(
        NullPointerException.class, () -> new RestartController(CFG, nowMs::get, null, () -> 0.5));
    assertThrows(
        NullPointerException.class, () -> new RestartController(CFG, nowMs::get, () -> true, null));
  }

  @Test
  void backoffDelayRejectsNegative() {
    assertThrows(IllegalArgumentException.class, () -> ctl.backoffDelayMs(-1));
  }
}
