// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** Pure-helper tests for {@link FilterRegionObserver#envFromConfig(Configuration)}. */
final class FilterRegionObserverTest {

  @Test
  void envFromConfig_returnsEnvVarWhenPrefixSet() {
    Configuration conf = new Configuration(false);
    conf.set(FilterRegionObserver.KEY_BLOCKED_PREFIX, "block-");

    Map<String, String> env = FilterRegionObserver.envFromConfig(conf);

    assertEquals(Map.of("HBASECOP_FILTER_BLOCKED_PREFIX", "block-"), env);
  }

  @Test
  void envFromConfig_returnsEmptyMapWhenPrefixUnset() {
    Configuration conf = new Configuration(false);

    assertTrue(FilterRegionObserver.envFromConfig(conf).isEmpty());
  }

  @Test
  void envFromConfig_returnsEmptyMapWhenPrefixBlank() {
    Configuration conf = new Configuration(false);
    conf.set(FilterRegionObserver.KEY_BLOCKED_PREFIX, "   ");

    assertTrue(FilterRegionObserver.envFromConfig(conf).isEmpty());
  }

  @Test
  void envFromConfig_rejectsNullConf() {
    assertThrows(NullPointerException.class, () -> FilterRegionObserver.envFromConfig(null));
  }
}
