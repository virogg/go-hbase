// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * T33 acceptance - verifies the {@link HeartbeatWatchdog} detection logic in isolation using a
 * deterministic fake clock. Wiring into {@code CoprocessorRuntime} is covered separately.
 */
final class HeartbeatWatchdogTest {

  private static final Duration PERIOD = Duration.ofMillis(100);
  private static final int MISS_THRESHOLD = 3;

  private AtomicLong nowMs;
  private AtomicInteger hungCalls;
  private AtomicLong lastReportedElapsed;
  private HeartbeatWatchdog wd;

  @BeforeEach
  void setUp() {
    nowMs = new AtomicLong(1_000L);
    hungCalls = new AtomicInteger(0);
    lastReportedElapsed = new AtomicLong(-1L);
    wd =
        new HeartbeatWatchdog(
            PERIOD,
            MISS_THRESHOLD,
            nowMs::get,
            elapsed -> {
              hungCalls.incrementAndGet();
              lastReportedElapsed.set(elapsed);
            });
  }

  @Test
  void tickWithinThresholdDoesNotFire() {
    // 2 periods elapsed - under the 3-miss threshold.
    nowMs.addAndGet(2 * PERIOD.toMillis());
    assertFalse(wd.tick(), "tick must not fire below threshold");
    assertEquals(0, hungCalls.get());
    assertFalse(wd.hungReported());
  }

  @Test
  void tickAtThresholdFires() {
    // Exactly missThreshold * period elapsed.
    nowMs.addAndGet(MISS_THRESHOLD * PERIOD.toMillis());
    assertTrue(wd.tick(), "tick must fire at/after threshold");
    assertEquals(1, hungCalls.get());
    assertTrue(wd.hungReported());
    assertEquals(MISS_THRESHOLD * PERIOD.toMillis(), lastReportedElapsed.get());
  }

  @Test
  void firedOnlyOncePerHangSpell() {
    nowMs.addAndGet(MISS_THRESHOLD * PERIOD.toMillis());
    assertTrue(wd.tick());
    // Subsequent ticks while still hung must not re-fire.
    nowMs.addAndGet(PERIOD.toMillis());
    assertFalse(wd.tick());
    nowMs.addAndGet(PERIOD.toMillis() * 10);
    assertFalse(wd.tick());
    assertEquals(1, hungCalls.get());
  }

  @Test
  void heartbeatAfterFireAllowsRefire() {
    // First hang.
    nowMs.addAndGet(MISS_THRESHOLD * PERIOD.toMillis());
    assertTrue(wd.tick());

    // Recovery: a heartbeat arrives, watchdog re-arms.
    nowMs.addAndGet(PERIOD.toMillis());
    wd.recordHeartbeat();
    assertFalse(wd.hungReported(), "recordHeartbeat must clear hung flag");

    // Stale again → fires a second time.
    nowMs.addAndGet(MISS_THRESHOLD * PERIOD.toMillis());
    assertTrue(wd.tick());
    assertEquals(2, hungCalls.get());
  }

  @Test
  void recordHeartbeatBeforeThresholdPreventsFiring() {
    // Miss 2 periods.
    nowMs.addAndGet(2 * PERIOD.toMillis());
    // Heartbeat arrives just before the third.
    wd.recordHeartbeat();
    // Tick one more period - only 1 period since last heartbeat.
    nowMs.addAndGet(PERIOD.toMillis());
    assertFalse(wd.tick(), "fresh heartbeat must reset the deadline");
    assertEquals(0, hungCalls.get());
  }

  @Test
  void customMissThresholdRespected() {
    AtomicInteger calls = new AtomicInteger();
    HeartbeatWatchdog w =
        new HeartbeatWatchdog(
            Duration.ofMillis(50), 5, nowMs::get, elapsed -> calls.incrementAndGet());

    // 4 missed: under threshold.
    nowMs.addAndGet(4 * 50L);
    assertFalse(w.tick());
    // 5 missed: fires.
    nowMs.addAndGet(50L);
    assertTrue(w.tick());
    assertEquals(1, calls.get());
  }

  @Test
  void constructorRejectsNonPositivePeriod() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HeartbeatWatchdog(Duration.ZERO, 3, nowMs::get, elapsed -> {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HeartbeatWatchdog(Duration.ofMillis(-1), 3, nowMs::get, elapsed -> {}));
  }

  @Test
  void constructorRejectsMissThresholdBelowOne() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HeartbeatWatchdog(PERIOD, 0, nowMs::get, elapsed -> {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HeartbeatWatchdog(PERIOD, -1, nowMs::get, elapsed -> {}));
  }

  @Test
  void constructorRejectsNulls() {
    assertThrows(
        NullPointerException.class,
        () -> new HeartbeatWatchdog(null, 3, nowMs::get, elapsed -> {}));
    assertThrows(
        NullPointerException.class, () -> new HeartbeatWatchdog(PERIOD, 3, null, elapsed -> {}));
    assertThrows(
        NullPointerException.class, () -> new HeartbeatWatchdog(PERIOD, 3, nowMs::get, null));
  }

  @Test
  void accessorsExposeConfiguredValues() {
    assertEquals(PERIOD, wd.period());
    assertEquals(MISS_THRESHOLD, wd.missThreshold());
  }
}
