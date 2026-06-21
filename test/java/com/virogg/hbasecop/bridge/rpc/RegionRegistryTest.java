// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hadoop.hbase.regionserver.Region;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RegionRegistryTest {

  @Mock private Region regionA;
  @Mock private Region regionB;

  @Test
  void registerThenLookupReturnsRegion() {
    RegionRegistry reg = new RegionRegistry();
    reg.register(1, regionA);
    reg.register(2, regionB);
    assertSame(regionA, reg.lookup(1));
    assertSame(regionB, reg.lookup(2));
    assertEquals(2, reg.size());
  }

  @Test
  void lookupUnknownIdReturnsNull() {
    RegionRegistry reg = new RegionRegistry();
    assertNull(reg.lookup(42));
  }

  @Test
  void releaseRemovesMapping() {
    RegionRegistry reg = new RegionRegistry();
    reg.register(1, regionA);
    reg.release(1);
    assertNull(reg.lookup(1));
    assertEquals(0, reg.size());
    reg.release(1); // idempotent
  }

  @Test
  void zeroIdAndNullRegionAreNotStored() {
    RegionRegistry reg = new RegionRegistry();
    reg.register(0, regionA);
    reg.register(5, null);
    assertNull(reg.lookup(0));
    assertNull(reg.lookup(5));
    assertEquals(0, reg.size());
  }
}
