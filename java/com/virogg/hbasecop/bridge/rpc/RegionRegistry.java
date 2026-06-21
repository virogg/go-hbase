// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.regionserver.Region;

public final class RegionRegistry {

  private final ConcurrentHashMap<Integer, Region> regions = new ConcurrentHashMap<>();

  public void register(int regionId, Region region) {
    if (regionId == 0 || region == null) {
      return;
    }
    regions.put(regionId, region);
  }

  public void release(int regionId) {
    regions.remove(regionId);
  }

  public Region lookup(int regionId) {
    return regions.get(regionId);
  }

  public int size() {
    return regions.size();
  }
}
