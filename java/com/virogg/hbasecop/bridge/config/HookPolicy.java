// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.config;

import java.time.Duration;
import java.util.Objects;

public final class HookPolicy {

  private final Policy policy;
  private final Duration timeout;

  public HookPolicy(Policy policy, Duration timeout) {
    this.policy = Objects.requireNonNull(policy, "policy");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative() || timeout.isZero()) {
      throw new IllegalArgumentException("timeout must be positive, got " + timeout);
    }
  }

  public Policy policy() {
    return policy;
  }

  public Duration timeout() {
    return timeout;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HookPolicy)) {
      return false;
    }
    HookPolicy other = (HookPolicy) o;
    return policy == other.policy && timeout.equals(other.timeout);
  }

  @Override
  public int hashCode() {
    return Objects.hash(policy, timeout);
  }

  @Override
  public String toString() {
    return "HookPolicy{policy=" + policy + ", timeout=" + timeout + '}';
  }
}
