// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.rpc.RegionRegistry;
import com.virogg.hbasecop.multiplex.RegionIdAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * TE31: the region lifecycle registers the live region under its wire region_id in a shared {@link
 * RegionRegistry}, so the reverse-RPC servicing pool can resolve a Go-initiated GET's target, and
 * drops it on close.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RegionObserverAdapterRegionRegistryTest {

  @Mock private HookDispatcher dispatcher;
  @Mock private RegionCoprocessorEnvironment env;
  @Mock private Region region;
  @Mock private RegionInfo regionInfo;

  private RegionIdAllocator allocator;
  private RegionRegistry registry;
  private RegionObserverAdapter adapter;

  @BeforeEach
  void setUp() {
    when(env.getRegion()).thenReturn(region);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    when(regionInfo.getEncodedName()).thenReturn("abc1234");

    allocator = new RegionIdAllocator();
    registry = new RegionRegistry();
    adapter =
        new RegionObserverAdapter(
            dispatcher, new PolicyConfig(new Configuration(false)), allocator, registry);
  }

  @Test
  void startRegistersLiveRegionUnderItsId() {
    adapter.start(env);

    int id = allocator.idFor("abc1234");
    assertTrue(id > 0, "region_id should be allocated");
    assertSame(region, registry.lookup(id), "live region must be resolvable by its region_id");
  }

  @Test
  void stopReleasesRegionFromRegistry() {
    adapter.start(env);
    int id = allocator.idFor("abc1234");

    adapter.stop(env);

    assertNull(registry.lookup(id), "region must be dropped from the registry on close");
  }
}
