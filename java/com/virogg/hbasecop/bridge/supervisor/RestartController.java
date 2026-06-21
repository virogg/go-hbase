// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

public final class RestartController {

  private static final Logger LOG = System.getLogger(RestartController.class.getName());

  public enum State {
    HEALTHY,
    AWAITING_RESTART,
    UNHEALTHY,
    STOPPED
  }

  @FunctionalInterface
  public interface RestartAttempt {
    boolean attempt();
  }

  private final RestartConfig cfg;
  private final LongSupplier clockMs;
  private final RestartAttempt attempt;
  private final DoubleSupplier jitterSource;

  private State state = State.HEALTHY;
  private int consecutiveFails = 0;
  private long nextAttemptDueMs = Long.MIN_VALUE;

  public RestartController(
      RestartConfig cfg,
      LongSupplier clockMs,
      RestartAttempt attempt,
      DoubleSupplier jitterSource) {
    this.cfg = Objects.requireNonNull(cfg, "cfg");
    this.clockMs = Objects.requireNonNull(clockMs, "clockMs");
    this.attempt = Objects.requireNonNull(attempt, "attempt");
    this.jitterSource = Objects.requireNonNull(jitterSource, "jitterSource");
  }

  public synchronized State state() {
    return state;
  }

  public synchronized int consecutiveFails() {
    return consecutiveFails;
  }

  public synchronized long nextAttemptDueMs() {
    return nextAttemptDueMs;
  }

  public synchronized boolean isHealthy() {
    return state == State.HEALTHY;
  }

  public synchronized boolean isUnhealthy() {
    return state == State.UNHEALTHY;
  }

  public long backoffDelayMs(int fails) {
    if (fails < 0) {
      throw new IllegalArgumentException("fails must be ≥ 0, got " + fails);
    }
    long base = cfg.initialDelayMs();
    for (int i = 0; i < fails && base < cfg.maxDelayMs(); i++) {
      base = (long) (base * cfg.multiplier());
    }
    base = Math.min(base, cfg.maxDelayMs());
    return applyJitter(base);
  }

  private long applyJitter(long baseMs) {
    double r = jitterSource.getAsDouble();
    if (r < 0.0 || r >= 1.0) {
      throw new IllegalStateException("jitterSource must return a value in [0, 1), got " + r);
    }
    double offset = (2.0 * r - 1.0) * cfg.jitterRatio();
    long jittered = (long) (baseMs * (1.0 + offset));
    return Math.max(0L, jittered);
  }

  public synchronized void notifyDead() {
    if (state != State.HEALTHY) {
      return;
    }
    state = State.AWAITING_RESTART;
    consecutiveFails = 0;
    nextAttemptDueMs = clockMs.getAsLong() + backoffDelayMs(0);
    LOG.log(
        Level.WARNING,
        "RestartController: notified dead, first attempt at +{0}ms",
        nextAttemptDueMs - clockMs.getAsLong());
  }

  public boolean tick() {
    synchronized (this) {
      if (state != State.AWAITING_RESTART && state != State.UNHEALTHY) {
        return false;
      }
      if (clockMs.getAsLong() < nextAttemptDueMs) {
        return false;
      }
    }

    boolean ok;
    try {
      ok = attempt.attempt();
    } catch (RuntimeException e) {
      LOG.log(Level.WARNING, "RestartController: attempt threw, counting as failure", e);
      ok = false;
    }

    synchronized (this) {
      if (state == State.STOPPED) {
        return true; // attempt ran, but state was torn down concurrently
      }
      if (ok) {
        state = State.HEALTHY;
        consecutiveFails = 0;
        nextAttemptDueMs = Long.MIN_VALUE;
        LOG.log(Level.INFO, "RestartController: attempt succeeded, back to HEALTHY");
        return true;
      }
      if (state == State.UNHEALTHY) {
        nextAttemptDueMs = clockMs.getAsLong() + cfg.probeIntervalMs();
        LOG.log(
            Level.WARNING,
            "RestartController: probe failed, next probe in {0}ms",
            cfg.probeIntervalMs());
        return true;
      }
      consecutiveFails++;
      if (consecutiveFails >= cfg.maxConsecutiveFails()) {
        state = State.UNHEALTHY;
        nextAttemptDueMs = clockMs.getAsLong() + cfg.probeIntervalMs();
        LOG.log(
            Level.ERROR,
            "RestartController: {0} consecutive failures → UNHEALTHY; probing every {1}ms",
            consecutiveFails,
            cfg.probeIntervalMs());
      } else {
        nextAttemptDueMs = clockMs.getAsLong() + backoffDelayMs(consecutiveFails);
        LOG.log(
            Level.WARNING,
            "RestartController: attempt #{0} failed, retry in {1}ms",
            consecutiveFails,
            nextAttemptDueMs - clockMs.getAsLong());
      }
      return true;
    }
  }

  public synchronized void stop() {
    state = State.STOPPED;
    nextAttemptDueMs = Long.MIN_VALUE;
  }
}
