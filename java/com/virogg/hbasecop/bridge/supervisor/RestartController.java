// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

/**
 * Tick-driven restart state machine for the supervised Go process. Owns no threads of its own;
 * callers signal lifecycle events via {@link #notifyDead()} and drive the clock by polling {@link
 * #tick()}; the controller fires the injected {@link RestartAttempt} when the next scheduled
 * deadline elapses.
 *
 * <p>State transitions:
 *
 * <ul>
 *   <li>{@link State#HEALTHY} → {@link State#AWAITING_RESTART} on {@link #notifyDead()}, with the
 *       first attempt scheduled after the initial backoff slot.
 *   <li>{@link State#AWAITING_RESTART}: each failed attempt increments {@code consecutiveFails} and
 *       schedules the next attempt at {@code now + backoffDelayMs(fails)}; a successful attempt
 *       resets the counter and returns to {@link State#HEALTHY}.
 *   <li>{@link State#UNHEALTHY}: reached once {@code consecutiveFails == maxConsecutiveFails}.
 *       Further attempts run on {@code probeIntervalMs} cadence; a successful probe resets the
 *       counter and returns to {@link State#HEALTHY}.
 *   <li>{@link State#STOPPED}: terminal; reached via {@link #stop()}. No further attempts run.
 * </ul>
 *
 * <p>The injected {@link DoubleSupplier} returns a value in {@code [0, 1)} used to draw the jitter
 * offset; production wires {@code ThreadLocalRandom.current()::nextDouble} and tests pin it to
 * {@code 0.5} for deterministic delays.
 *
 * <p>All mutating methods are {@code synchronized}; attempt invocation runs outside the lock to
 * keep restart work (potentially several hundred ms of process spawn / shmem handshake) off the
 * critical section.
 */
public final class RestartController {

  private static final Logger LOG = System.getLogger(RestartController.class.getName());

  /** Lifecycle state. */
  public enum State {
    HEALTHY,
    AWAITING_RESTART,
    UNHEALTHY,
    STOPPED
  }

  /** Callback invoked from {@link #tick()} when a restart attempt is due. */
  @FunctionalInterface
  public interface RestartAttempt {
    /**
     * Try to bring the supervised resource back up. Implementations should not throw; if they do,
     * the throw is logged at {@code WARNING} and counted as a failure.
     *
     * @return {@code true} iff the resource is healthy after this call
     */
    boolean attempt();
  }

  private final RestartConfig cfg;
  private final LongSupplier clockMs;
  private final RestartAttempt attempt;
  private final DoubleSupplier jitterSource;

  private State state = State.HEALTHY;
  private int consecutiveFails = 0;
  private long nextAttemptDueMs = Long.MIN_VALUE;

  /**
   * @param cfg backoff + probe tunables
   * @param clockMs monotonic-ish ms clock (production: {@code System::currentTimeMillis})
   * @param attempt callback that performs one restart try
   * @param jitterSource returns a value in {@code [0, 1)} used as the jitter draw
   */
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

  /**
   * Compute the (jittered) delay before the {@code (fails+1)}-th attempt. Public for direct unit
   * testing of the backoff curve.
   *
   * @param fails number of consecutive failed attempts so far ({@code 0} → initial delay)
   */
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

  /**
   * Signal that the supervised process has died. Transitions {@link State#HEALTHY} → {@link
   * State#AWAITING_RESTART} and schedules the first restart attempt after the initial backoff slot.
   * Calls in any other state are silently ignored (the existing schedule wins).
   */
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

  /**
   * Drive the state machine against the current clock. Invokes {@link RestartAttempt#attempt()} iff
   * the next scheduled deadline has elapsed. Returns {@code true} iff this call invoked the
   * callback.
   */
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
        // Probe failure: keep cadence, do not ratchet consecutiveFails.
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

  /** Terminal stop; cancels future attempts. Idempotent. */
  public synchronized void stop() {
    state = State.STOPPED;
    nextAttemptDueMs = Long.MIN_VALUE;
  }
}
