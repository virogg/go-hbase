// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class RegionIdAllocator {

  private final ConcurrentHashMap<String, Integer> ids = new ConcurrentHashMap<>();
  private final AtomicInteger next = new AtomicInteger(0);

  public int allocate(String encodedRegionName) {
    Objects.requireNonNull(encodedRegionName, "encodedRegionName");
    return ids.computeIfAbsent(encodedRegionName, k -> next.incrementAndGet());
  }

  public int idFor(String encodedRegionName) {
    if (encodedRegionName == null) {
      return 0;
    }
    Integer id = ids.get(encodedRegionName);
    return id == null ? 0 : id;
  }

  public void release(String encodedRegionName) {
    ids.remove(encodedRegionName);
  }

  public int size() {
    return ids.size();
  }
}
