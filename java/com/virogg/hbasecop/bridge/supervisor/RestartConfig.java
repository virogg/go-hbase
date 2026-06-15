// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

/**
 * Tunables for {@link RestartController}: exponential-backoff parameters, consecutive-failure
 * threshold before the runtime is declared unhealthy, and the probe interval used once unhealthy.
 *
 * <p>Defaults follow {@code plan.md} T34: {@code 200ms → 400 → 800 → ... → 5s} doubling, jitter
 * ±20%, 5 consecutive failures before {@code UNHEALTHY}, probe every 30s thereafter.
 */
public final class RestartConfig {

  /** Default delay before the first restart attempt. */
  public static final long DEFAULT_INITIAL_DELAY_MS = 200L;

  /** Default upper bound on the per-attempt delay. */
  public static final long DEFAULT_MAX_DELAY_MS = 5_000L;

  /** Default multiplier applied after each failed attempt. */
  public static final double DEFAULT_MULTIPLIER = 2.0;

  /** Default jitter ratio (±20%). */
  public static final double DEFAULT_JITTER_RATIO = 0.2;

  /** Default consecutive-failure threshold before the runtime is declared unhealthy. */
  public static final int DEFAULT_MAX_CONSECUTIVE_FAILS = 5;

  /** Default probe interval once unhealthy. */
  public static final long DEFAULT_PROBE_INTERVAL_MS = 30_000L;

  private final long initialDelayMs;
  private final long maxDelayMs;
  private final double multiplier;
  private final double jitterRatio;
  private final int maxConsecutiveFails;
  private final long probeIntervalMs;

  private RestartConfig(Builder b) {
    if (b.initialDelayMs <= 0L) {
      throw new IllegalArgumentException("initialDelayMs must be > 0, got " + b.initialDelayMs);
    }
    if (b.maxDelayMs < b.initialDelayMs) {
      throw new IllegalArgumentException(
          "maxDelayMs must be ≥ initialDelayMs, got " + b.maxDelayMs);
    }
    if (b.multiplier <= 1.0) {
      throw new IllegalArgumentException("multiplier must be > 1.0, got " + b.multiplier);
    }
    if (b.jitterRatio < 0.0 || b.jitterRatio >= 1.0) {
      throw new IllegalArgumentException("jitterRatio must be in [0, 1), got " + b.jitterRatio);
    }
    if (b.maxConsecutiveFails < 1) {
      throw new IllegalArgumentException(
          "maxConsecutiveFails must be ≥ 1, got " + b.maxConsecutiveFails);
    }
    if (b.probeIntervalMs <= 0L) {
      throw new IllegalArgumentException("probeIntervalMs must be > 0, got " + b.probeIntervalMs);
    }
    this.initialDelayMs = b.initialDelayMs;
    this.maxDelayMs = b.maxDelayMs;
    this.multiplier = b.multiplier;
    this.jitterRatio = b.jitterRatio;
    this.maxConsecutiveFails = b.maxConsecutiveFails;
    this.probeIntervalMs = b.probeIntervalMs;
  }

  public long initialDelayMs() {
    return initialDelayMs;
  }

  public long maxDelayMs() {
    return maxDelayMs;
  }

  public double multiplier() {
    return multiplier;
  }

  public double jitterRatio() {
    return jitterRatio;
  }

  public int maxConsecutiveFails() {
    return maxConsecutiveFails;
  }

  public long probeIntervalMs() {
    return probeIntervalMs;
  }

  public static RestartConfig defaults() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Mutable builder; not thread-safe. */
  public static final class Builder {
    private long initialDelayMs = DEFAULT_INITIAL_DELAY_MS;
    private long maxDelayMs = DEFAULT_MAX_DELAY_MS;
    private double multiplier = DEFAULT_MULTIPLIER;
    private double jitterRatio = DEFAULT_JITTER_RATIO;
    private int maxConsecutiveFails = DEFAULT_MAX_CONSECUTIVE_FAILS;
    private long probeIntervalMs = DEFAULT_PROBE_INTERVAL_MS;

    public Builder initialDelayMs(long ms) {
      this.initialDelayMs = ms;
      return this;
    }

    public Builder maxDelayMs(long ms) {
      this.maxDelayMs = ms;
      return this;
    }

    public Builder multiplier(double m) {
      this.multiplier = m;
      return this;
    }

    public Builder jitterRatio(double r) {
      this.jitterRatio = r;
      return this;
    }

    public Builder maxConsecutiveFails(int n) {
      this.maxConsecutiveFails = n;
      return this;
    }

    public Builder probeIntervalMs(long ms) {
      this.probeIntervalMs = ms;
      return this;
    }

    public RestartConfig build() {
      return new RestartConfig(this);
    }
  }
}
