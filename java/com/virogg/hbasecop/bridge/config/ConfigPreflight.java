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

/**
 * Validates {@code hbasecop.*} configuration at coprocessor start so a typo'd key or malformed
 * value fails fast at region-open instead of lazily on the first hook of a live write path.
 * Malformed values throw; unknown keys and unknown per-hook suffixes log a WARN.
 */
public final class ConfigPreflight {

  // Exact keys whose value is a Hadoop duration (a unit is required).
  private static final Set<String> DURATION_KEYS =
      Set.of(
          "hbasecop.heartbeat.period",
          "hbasecop.restart.initial-delay",
          "hbasecop.restart.max-delay",
          "hbasecop.restart.deadline",
          "hbasecop.restart.probe-interval",
          "hbasecop.shutdown.graceful-timeout",
          "hbasecop.endpoint.timeout",
          PolicyConfig.KEY_TIMEOUT_DEFAULT);

  // Exact keys whose value must be a positive integer.
  private static final Set<String> POSITIVE_INT_KEYS =
      Set.of(
          "hbasecop.heartbeat.miss-threshold",
          "hbasecop.restart.max-fails",
          "hbasecop.ring.capacity",
          "hbasecop.ring.max-object-size");

  private static final Pattern DURATION = Pattern.compile("\\d+\\s*(ns|us|ms|s|m|h|d)");

  private ConfigPreflight() {}

  /**
   * Validate every {@code hbasecop.*} entry in conf. Throws IllegalArgumentException listing all
   * malformed values; logs a WARN per unknown key or unknown per-hook suffix.
   */
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
        // Only a known hook suffix is a framework failure-policy key; value-check just those.
        // An unknown suffix (e.g. an observer's own hbasecop.policy.* app key) is a WARN, never
        // an error — else a stray key would abort coprocessor start and, for a cluster-wide
        // master/RS coprocessor, take the whole HMaster/RegionServer down with it.
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
      } else {
        log.log(Level.WARNING, "hbasecop: unknown config key {0} (ignored)", key);
      }
    }
    if (!errors.isEmpty()) {
      throw new IllegalArgumentException(
          "hbasecop: malformed config: " + String.join("; ", errors));
    }
  }

  /**
   * True iff {@code key}'s per-hook suffix names a known hook, so its value is a framework
   * policy/timeout key worth validating. Otherwise WARNs (an observer may read it as its own
   * config) and returns false so the caller skips the value check.
   */
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
}
