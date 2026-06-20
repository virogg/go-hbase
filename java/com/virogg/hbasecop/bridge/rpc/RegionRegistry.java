// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.regionserver.Region;

/**
 * Per-process map from the wire {@code region_id} to the live {@link Region}, so the reverse-RPC
 * servicing pool (Tier 2, TE31) can resolve the target region of a Go-initiated GET. Entries are
 * added/removed by the region coprocessor's open/close lifecycle (alongside {@link
 * com.virogg.hbasecop.multiplex.RegionIdAllocator}); region_id 0 — the "no region scope" sentinel —
 * is never stored.
 *
 * <p>Thread-safe: backed by a {@link ConcurrentHashMap}. {@link #lookup(int)} returns {@code null}
 * for an unknown or already-released id, which the servicer turns into a clean error rather than an
 * NPE.
 */
public final class RegionRegistry {

  private final ConcurrentHashMap<Integer, Region> regions = new ConcurrentHashMap<>();

  /** Register the live region for {@code regionId}. No-op for id 0 or a null region. */
  public void register(int regionId, Region region) {
    if (regionId == 0 || region == null) {
      return;
    }
    regions.put(regionId, region);
  }

  /** Drop the mapping for {@code regionId}. No-op for an unknown id. */
  public void release(int regionId) {
    regions.remove(regionId);
  }

  /** The live region for {@code regionId}, or {@code null} if unknown or released. */
  public Region lookup(int regionId) {
    return regions.get(regionId);
  }

  /** Number of currently-registered regions. Intended for tests and diagnostics. */
  public int size() {
    return regions.size();
  }
}
