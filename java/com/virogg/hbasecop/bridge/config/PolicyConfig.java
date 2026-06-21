// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.config;

import com.virogg.hbasecop.bridge.observer.HookId;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;

public final class PolicyConfig {

  public static final String KEY_POLICY_PREFIX = "hbasecop.policy.";
  public static final String KEY_TIMEOUT_PREFIX = "hbasecop.timeout.";
  public static final String KEY_TIMEOUT_DEFAULT = "hbasecop.timeout.default";

  public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  private final Configuration conf;

  public PolicyConfig(Configuration conf) {
    this.conf = Objects.requireNonNull(conf, "conf");
  }

  public HookPolicy forHook(String hookName) {
    Objects.requireNonNull(hookName, "hookName");
    if (hookName.isEmpty()) {
      throw new IllegalArgumentException("hookName must not be empty");
    }
    return new HookPolicy(resolvePolicy(hookName), resolveTimeout(hookName));
  }

  public HookPolicy forHook(byte hookId) {
    HookId id = HookId.byValue(hookId);
    if (id == null) {
      throw new IllegalArgumentException("unknown hookId=" + hookId);
    }
    return forHook(id.methodName());
  }

  public HookPolicy forHook(HookId id) {
    Objects.requireNonNull(id, "id");
    return forHook(id.methodName());
  }

  private Policy resolvePolicy(String hookName) {
    String raw = conf.get(KEY_POLICY_PREFIX + hookName);
    if (raw == null) {
      return defaultPolicyFor(hookName);
    }
    String normalized = raw.trim().toLowerCase(Locale.ROOT);
    switch (normalized) {
      case "strict":
        return Policy.STRICT;
      case "best-effort":
        return Policy.BEST_EFFORT;
      default:
        throw new IllegalArgumentException(
            "hbasecop: invalid policy '" + raw + "' for hook '" + hookName + "'");
    }
  }

  private Duration resolveTimeout(String hookName) {
    String perHookKey = KEY_TIMEOUT_PREFIX + hookName;
    if (conf.get(perHookKey) != null) {
      return Duration.ofNanos(conf.getTimeDuration(perHookKey, 0L, TimeUnit.NANOSECONDS));
    }
    if (conf.get(KEY_TIMEOUT_DEFAULT) != null) {
      return Duration.ofNanos(conf.getTimeDuration(KEY_TIMEOUT_DEFAULT, 0L, TimeUnit.NANOSECONDS));
    }
    return DEFAULT_TIMEOUT;
  }

  private static Policy defaultPolicyFor(String hookName) {
    if (hookName.startsWith("pre")) {
      return Policy.STRICT;
    }
    if (hookName.startsWith("post")) {
      return Policy.BEST_EFFORT;
    }
    return Policy.STRICT;
  }
}
