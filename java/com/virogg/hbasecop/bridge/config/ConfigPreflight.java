// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.config;

import com.virogg.hbasecop.bridge.observer.HookId;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;

public final class ConfigPreflight {

  private static final Set<String> DURATION_KEYS =
      Set.of(
          "hbasecop.heartbeat.period",
          "hbasecop.restart.initial-delay",
          "hbasecop.restart.max-delay",
          "hbasecop.restart.deadline",
          "hbasecop.restart.probe-interval",
          "hbasecop.shutdown.graceful-timeout",
          "hbasecop.endpoint.timeout",
          "hbasecop.endpoint.servicing-timeout",
          "hbasecop.endpoint.scanner-idle-lease",
          PolicyConfig.KEY_TIMEOUT_DEFAULT);

  private static final Set<String> POSITIVE_INT_KEYS =
      Set.of(
          "hbasecop.heartbeat.miss-threshold",
          "hbasecop.restart.max-fails",
          "hbasecop.ring.capacity",
          "hbasecop.ring.max-object-size",
          "hbasecop.endpoint.servicing-pool-size",
          "hbasecop.endpoint.servicing-queue-depth",
          "hbasecop.endpoint.bulk-ring.capacity",
          "hbasecop.endpoint.bulk-ring.max-object-size",
          "hbasecop.endpoint.max-concurrent-calls",
          "hbasecop.endpoint.max-scanners-per-call",
          "hbasecop.endpoint.max-bytes-per-resp",
          "hbasecop.endpoint.max-rows-per-next");

  private static final Set<String> BOOLEAN_KEYS = Set.of("hbasecop.endpoint.allow-mutate");

  private static final Pattern DURATION = Pattern.compile("\\d+\\s*(ns|us|ms|s|m|h|d)");

  private ConfigPreflight() {}

  public static void validate(Configuration conf, Logger log) {
    if (conf == null) {
      return;
    }
    List<String> errors = new ArrayList<>();
    for (Map.Entry<String, String> e : conf) {
      String key = e.getKey();
      if (!key.startsWith("hbasecop.")) {
        continue;
      }
      String val = e.getValue();
      if (key.startsWith(PolicyConfig.KEY_POLICY_PREFIX)) {
        if (knownHookKey(key, PolicyConfig.KEY_POLICY_PREFIX, log)
            && !val.equals("strict")
            && !val.equals("best-effort")) {
          errors.add(key + "=" + val + " (want strict|best-effort)");
        }
      } else if (key.equals(PolicyConfig.KEY_TIMEOUT_DEFAULT)) {
        checkDuration(key, val, errors);
      } else if (key.startsWith(PolicyConfig.KEY_TIMEOUT_PREFIX)) {
        if (knownHookKey(key, PolicyConfig.KEY_TIMEOUT_PREFIX, log)) {
          checkDuration(key, val, errors);
        }
      } else if (DURATION_KEYS.contains(key)) {
        checkDuration(key, val, errors);
      } else if (POSITIVE_INT_KEYS.contains(key)) {
        checkPositiveInt(key, val, errors);
      } else if (BOOLEAN_KEYS.contains(key)) {
        checkBoolean(key, val, errors);
      } else {
        log.log(Level.WARNING, "hbasecop: unknown config key {0} (ignored)", key);
      }
    }
    if (!errors.isEmpty()) {
      throw new IllegalArgumentException(
          "hbasecop: malformed config: " + String.join("; ", errors));
    }
  }

  private static boolean knownHookKey(String key, String prefix, Logger log) {
    String hook = key.substring(prefix.length());
    if (!hook.isEmpty() && HookId.byMethodName(hook) != null) {
      return true;
    }
    log.log(
        Level.WARNING,
        "hbasecop: config key {0} is not a recognized hbasecop hook key (ignored)",
        key);
    return false;
  }

  private static void checkDuration(String key, String val, List<String> errors) {
    if (!DURATION.matcher(val.trim()).matches()) {
      errors.add(key + "=" + val + " (want a duration with a unit, e.g. 500ms, 2s)");
    }
  }

  private static void checkPositiveInt(String key, String val, List<String> errors) {
    try {
      if (Integer.parseInt(val.trim()) <= 0) {
        errors.add(key + "=" + val + " (want a positive integer)");
      }
    } catch (NumberFormatException ex) {
      errors.add(key + "=" + val + " (want a positive integer)");
    }
  }

  private static void checkBoolean(String key, String val, List<String> errors) {
    String v = val.trim();
    if (!v.equalsIgnoreCase("true") && !v.equalsIgnoreCase("false")) {
      errors.add(key + "=" + val + " (want true|false)");
    }
  }
}
