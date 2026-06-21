// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.System.Logger;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

final class ConfigPreflightTest {

  private static final Logger LOG = System.getLogger(ConfigPreflightTest.class.getName());

  private static Configuration conf(String... kv) {
    Configuration c = new Configuration(false);
    for (int i = 0; i < kv.length; i += 2) {
      c.set(kv[i], kv[i + 1]);
    }
    return c;
  }

  @Test
  void acceptsWellFormedConfig() {
    Configuration c =
        conf(
            "hbasecop.policy.prePut", "strict",
            "hbasecop.policy.postPut", "best-effort",
            "hbasecop.timeout.prePut", "500ms",
            "hbasecop.timeout.default", "2s",
            "hbasecop.heartbeat.period", "500ms",
            "hbasecop.ring.capacity", "16",
            "unrelated.key", "ignored");
    assertDoesNotThrow(() -> ConfigPreflight.validate(c, LOG));
  }

  @Test
  void rejectsBadPolicyValue() {
    Configuration c = conf("hbasecop.policy.prePut", "strct");
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> ConfigPreflight.validate(c, LOG));
    assertTrue(ex.getMessage().contains("hbasecop.policy.prePut"), ex.getMessage());
  }

  @Test
  void rejectsDurationWithoutUnit() {
    Configuration c = conf("hbasecop.timeout.prePut", "500");
    assertThrows(IllegalArgumentException.class, () -> ConfigPreflight.validate(c, LOG));
  }

  @Test
  void validatesEndpointTimeoutDuration() {
    assertDoesNotThrow(
        () -> ConfigPreflight.validate(conf("hbasecop.endpoint.timeout", "30s"), LOG));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConfigPreflight.validate(conf("hbasecop.endpoint.timeout", "30"), LOG));
    assertTrue(ex.getMessage().contains("hbasecop.endpoint.timeout"), ex.getMessage());
  }

  @Test
  void rejectsNonPositiveInt() {
    Configuration c = conf("hbasecop.ring.capacity", "0");
    assertThrows(IllegalArgumentException.class, () -> ConfigPreflight.validate(c, LOG));
  }

  @Test
  void validatesAllowMutateBoolean() {
    assertDoesNotThrow(
        () -> ConfigPreflight.validate(conf("hbasecop.endpoint.allow-mutate", "true"), LOG));
    assertDoesNotThrow(
        () -> ConfigPreflight.validate(conf("hbasecop.endpoint.allow-mutate", "false"), LOG));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConfigPreflight.validate(conf("hbasecop.endpoint.allow-mutate", "yes"), LOG));
    assertTrue(ex.getMessage().contains("hbasecop.endpoint.allow-mutate"), ex.getMessage());
  }

  @Test
  void validatesTe42Limits() {
    assertDoesNotThrow(
        () ->
            ConfigPreflight.validate(
                conf(
                    "hbasecop.endpoint.max-concurrent-calls", "16",
                    "hbasecop.endpoint.max-scanners-per-call", "8",
                    "hbasecop.endpoint.max-bytes-per-resp", "1048576",
                    "hbasecop.endpoint.max-rows-per-next", "500",
                    "hbasecop.endpoint.scanner-idle-lease", "2m"),
                LOG));
    assertThrows(
        IllegalArgumentException.class,
        () -> ConfigPreflight.validate(conf("hbasecop.endpoint.max-rows-per-next", "0"), LOG));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ConfigPreflight.validate(conf("hbasecop.endpoint.scanner-idle-lease", "120"), LOG));
    assertTrue(ex.getMessage().contains("hbasecop.endpoint.scanner-idle-lease"), ex.getMessage());
  }

  @Test
  void unknownKeyAndUnknownHookAreWarnNotError() {
    Configuration c =
        conf(
            "hbasecop.bogus.key", "whatever",
            "hbasecop.policy.preNotAHook", "strict");
    assertDoesNotThrow(() -> ConfigPreflight.validate(c, LOG));
  }

  @Test
  void observerAppKeysInPolicyTimeoutNamespaceDoNotAbortStart() {
    Configuration c =
        conf(
            "hbasecop.policy.blocked_prefix", "forbidden-",
            "hbasecop.policy.veto_wal_roll", "true",
            "hbasecop.timeout.blocked_prefix", "not-a-duration");
    assertDoesNotThrow(() -> ConfigPreflight.validate(c, LOG));
  }

  @Test
  void reportsAllMalformedValuesAtOnce() {
    Configuration c =
        conf(
            "hbasecop.policy.prePut", "nope",
            "hbasecop.ring.capacity", "-1");
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> ConfigPreflight.validate(c, LOG));
    assertTrue(ex.getMessage().contains("prePut"), ex.getMessage());
    assertTrue(ex.getMessage().contains("ring.capacity"), ex.getMessage());
  }
}
