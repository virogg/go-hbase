// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public final class HeartbeatWatchdog {

  private final Duration period;
  private final int missThreshold;
  private final LongSupplier clockMs;
  private final LongConsumer onHung;

  private volatile long lastHeartbeatMs;
  private volatile boolean hungReported;

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

  public void recordHeartbeat() {
    lastHeartbeatMs = clockMs.getAsLong();
    hungReported = false;
  }

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

  public boolean hungReported() {
    return hungReported;
  }

  public long lastHeartbeatMs() {
    return lastHeartbeatMs;
  }
}
