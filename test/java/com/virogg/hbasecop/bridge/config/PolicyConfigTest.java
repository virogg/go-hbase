// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.observer.RegionObserverAdapter;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

final class PolicyConfigTest {

  @Test
  void defaultsPrePutStrictPostPutBestEffort() {
    PolicyConfig cfg = new PolicyConfig(new Configuration(false));

    HookPolicy pre = cfg.forHook("prePut");
    HookPolicy post = cfg.forHook("postPut");

    assertEquals(Policy.STRICT, pre.policy());
    assertEquals(Policy.BEST_EFFORT, post.policy());
  }

  @Test
  void defaultTimeoutIsFiveSeconds() {
    PolicyConfig cfg = new PolicyConfig(new Configuration(false));

    assertEquals(Duration.ofSeconds(5), cfg.forHook("prePut").timeout());
    assertEquals(Duration.ofSeconds(5), cfg.forHook("postPut").timeout());
  }

  @Test
  void perHookPolicyOverridesDefault() {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.prePut", "best-effort");
    conf.set("hbasecop.policy.postPut", "strict");

    PolicyConfig cfg = new PolicyConfig(conf);

    assertEquals(Policy.BEST_EFFORT, cfg.forHook("prePut").policy());
    assertEquals(Policy.STRICT, cfg.forHook("postPut").policy());
  }

  @Test
  void policyParsingIsCaseInsensitiveAndTrimmed() {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.prePut", "  BEST-EFFORT  ");
    conf.set("hbasecop.policy.postPut", "Strict");

    PolicyConfig cfg = new PolicyConfig(conf);

    assertEquals(Policy.BEST_EFFORT, cfg.forHook("prePut").policy());
    assertEquals(Policy.STRICT, cfg.forHook("postPut").policy());
  }

  @Test
  void unknownPolicyValueRejectedEagerlyOnLookup() {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.prePut", "panic");

    PolicyConfig cfg = new PolicyConfig(conf);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> cfg.forHook("prePut"));
    assertTrue(ex.getMessage().contains("panic"), () -> "msg=" + ex.getMessage());
  }

  @Test
  void defaultTimeoutOverridableGlobally() {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.timeout.default", "250ms");

    PolicyConfig cfg = new PolicyConfig(conf);

    assertEquals(Duration.ofMillis(250), cfg.forHook("prePut").timeout());
    assertEquals(Duration.ofMillis(250), cfg.forHook("postPut").timeout());
  }

  @Test
  void perHookTimeoutOverridesDefault() {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.timeout.default", "250ms");
    conf.set("hbasecop.timeout.prePut", "2s");

    PolicyConfig cfg = new PolicyConfig(conf);

    assertEquals(Duration.ofSeconds(2), cfg.forHook("prePut").timeout());
    assertEquals(Duration.ofMillis(250), cfg.forHook("postPut").timeout());
  }

  @Test
  void unknownPrefixDefaultsToStrict() {
    PolicyConfig cfg = new PolicyConfig(new Configuration(false));

    HookPolicy weird = cfg.forHook("flushStore");

    assertEquals(Policy.STRICT, weird.policy(), "unknown prefix must default to strict");
  }

  @Test
  void forHookByIdMapsToCanonicalName() {
    PolicyConfig cfg = new PolicyConfig(new Configuration(false));

    HookPolicy byId = cfg.forHook(RegionObserverAdapter.HOOK_PRE_PUT);
    HookPolicy byName = cfg.forHook("prePut");

    assertEquals(byName.policy(), byId.policy());
    assertEquals(byName.timeout(), byId.timeout());
  }

  @Test
  void forHookByIdRejectsUnknown() {
    PolicyConfig cfg = new PolicyConfig(new Configuration(false));
    assertThrows(IllegalArgumentException.class, () -> cfg.forHook((byte) 99));
  }

  @Test
  void nullConfigurationRejected() {
    assertThrows(NullPointerException.class, () -> new PolicyConfig(null));
  }

  @Test
  void hookNameRequired() {
    PolicyConfig cfg = new PolicyConfig(new Configuration(false));
    assertThrows(NullPointerException.class, () -> cfg.forHook((String) null));
    assertThrows(IllegalArgumentException.class, () -> cfg.forHook(""));
  }

  @Test
  void hookPolicyExposesValues() {
    HookPolicy p = new HookPolicy(Policy.STRICT, Duration.ofMillis(123));
    assertEquals(Policy.STRICT, p.policy());
    assertEquals(Duration.ofMillis(123), p.timeout());
    assertNotNull(p.toString());
  }
}
