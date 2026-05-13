// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Heartbeat liveness detector. Owns no threads of its own — callers feed it heartbeats via {@link
 * #recordHeartbeat()} and periodically poll {@link #tick()}; when no heartbeat has been recorded
 * for at least {@code missThreshold × period} the watchdog fires the {@code onHung} callback
 * exactly once (until the next {@link #recordHeartbeat()} re-arms it).
 *
 * <p>The clock is injected as a {@link LongSupplier} returning monotonic-ish milliseconds, so unit
 * tests drive time deterministically and production wires {@code System::currentTimeMillis}.
 *
 * <p>{@link #recordHeartbeat()} and {@link #tick()} are safe to call concurrently from different
 * threads — typical use is a reader thread recording heartbeats and a scheduler ticking.
 */
public final class HeartbeatWatchdog {

  private final Duration period;
  private final int missThreshold;
  private final LongSupplier clockMs;
  private final LongConsumer onHung;

  private volatile long lastHeartbeatMs;
  private volatile boolean hungReported;

  /**
   * @param period interval between expected heartbeats (matches the Go-side period)
   * @param missThreshold number of consecutive missed periods that trips the watchdog (≥ 1)
   * @param clockMs monotonic-ish ms clock
   * @param onHung invoked with the elapsed-since-last-heartbeat (ms) when the watchdog fires
   */
  public HeartbeatWatchdog(
      Duration period, int missThreshold, LongSupplier clockMs, LongConsumer onHung) {
    this.period = Objects.requireNonNull(period, "period");
    if (period.isZero() || period.isNegative()) {
      throw new IllegalArgumentException("period must be positive, got " + period);
    }
    if (missThreshold < 1) {
      throw new IllegalArgumentException("missThreshold must be ≥ 1, got " + missThreshold);
    }
    this.missThreshold = missThreshold;
    this.clockMs = Objects.requireNonNull(clockMs, "clockMs");
    this.onHung = Objects.requireNonNull(onHung, "onHung");
    this.lastHeartbeatMs = clockMs.getAsLong();
  }

  public Duration period() {
    return period;
  }

  public int missThreshold() {
    return missThreshold;
  }

  /** Re-arm the watchdog with a fresh heartbeat timestamp. */
  public void recordHeartbeat() {
    lastHeartbeatMs = clockMs.getAsLong();
    hungReported = false;
  }

  /**
   * Evaluate the watchdog against the current clock. Returns {@code true} iff this call fired the
   * {@code onHung} callback; subsequent ticks during the same hang are silent until {@link
   * #recordHeartbeat()} re-arms the detector.
   */
  public boolean tick() {
    if (hungReported) {
      return false;
    }
    long elapsed = clockMs.getAsLong() - lastHeartbeatMs;
    long deadlineMs = period.toMillis() * (long) missThreshold;
    if (elapsed >= deadlineMs) {
      hungReported = true;
      onHung.accept(elapsed);
      return true;
    }
    return false;
  }

  /** Visible for testing. */
  public boolean hungReported() {
    return hungReported;
  }

  /** Visible for testing. */
  public long lastHeartbeatMs() {
    return lastHeartbeatMs;
  }
}
