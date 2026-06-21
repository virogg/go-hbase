// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.rspolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

final class RsPolicyRegionServerObserverTest {

  @Test
  void envFromConfig_returnsEnvVarWhenVetoEnabled() {
    Configuration conf = new Configuration(false);
    conf.set(RsPolicyRegionServerObserver.KEY_VETO_WAL_ROLL, "true");

    Map<String, String> env = RsPolicyRegionServerObserver.envFromConfig(conf);

    assertEquals(Map.of("HBASECOP_RS_POLICY_VETO_WAL_ROLL", "true"), env);
  }

  @Test
  void envFromConfig_returnsEmptyMapWhenVetoUnset() {
    assertTrue(RsPolicyRegionServerObserver.envFromConfig(new Configuration(false)).isEmpty());
  }

  @Test
  void envFromConfig_returnsEmptyMapWhenVetoNotTrue() {
    Configuration conf = new Configuration(false);
    conf.set(RsPolicyRegionServerObserver.KEY_VETO_WAL_ROLL, "false");

    assertTrue(RsPolicyRegionServerObserver.envFromConfig(conf).isEmpty());
  }

  @Test
  void envFromConfig_rejectsNullConf() {
    assertThrows(
        NullPointerException.class, () -> RsPolicyRegionServerObserver.envFromConfig(null));
  }
}
