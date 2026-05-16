// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.masterpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** Pure-helper tests for {@link PolicyMasterObserver#envFromConfig(Configuration)}. */
final class PolicyMasterObserverTest {

  @Test
  void envFromConfig_returnsEnvVarWhenPrefixSet() {
    Configuration conf = new Configuration(false);
    conf.set(PolicyMasterObserver.KEY_BLOCKED_PREFIX, "forbidden-");

    Map<String, String> env = PolicyMasterObserver.envFromConfig(conf);

    assertEquals(Map.of("HBASECOP_POLICY_BLOCKED_PREFIX", "forbidden-"), env);
  }

  @Test
  void envFromConfig_returnsEmptyMapWhenPrefixUnset() {
    assertTrue(PolicyMasterObserver.envFromConfig(new Configuration(false)).isEmpty());
  }

  @Test
  void envFromConfig_returnsEmptyMapWhenPrefixBlank() {
    Configuration conf = new Configuration(false);
    conf.set(PolicyMasterObserver.KEY_BLOCKED_PREFIX, "   ");

    assertTrue(PolicyMasterObserver.envFromConfig(conf).isEmpty());
  }

  @Test
  void envFromConfig_rejectsNullConf() {
    assertThrows(NullPointerException.class, () -> PolicyMasterObserver.envFromConfig(null));
  }
}
