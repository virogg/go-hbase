// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-process registry mapping HBase encoded region names to the unsigned 32-bit {@code region_id}
 * the wire frame header (see {@code internal/wire/frame.go}) uses to route hook invocations from
 * the Java supervisor to the Go runtime.
 *
 * <p>{@link #allocate(String)} returns the existing id for a known region and otherwise mints a
 * fresh id from a monotonic counter. {@link #release(String)} drops the mapping but never recycles
 * the freed id, so logs and metrics keyed by region_id stay unambiguous across region open/close
 * cycles within one Go-process lifetime.
 *
 * <p>The id space is local to one Go-process lifetime (one supervisor, one shmem ring pair). After
 * a Go-process restart or RegionServer restart the counter resets — the Java adapter re-allocates
 * ids on the next {@code RegionObserver.start(env)}. The id is therefore opaque to user observer
 * code; treat it as a sharding key, not a stable identifier.
 *
 * <p>Thread-safe: backed by {@link ConcurrentHashMap} and an {@link AtomicInteger}. Concurrent
 * {@link #allocate(String)} on the same region name converges to a single id; concurrent allocates
 * on distinct names yield distinct ids.
 */
public final class RegionIdAllocator {

  private final ConcurrentHashMap<String, Integer> ids = new ConcurrentHashMap<>();
  private final AtomicInteger next = new AtomicInteger(0);

  /**
   * Return the id assigned to {@code encodedRegionName}, allocating a fresh one if none exists.
   * Idempotent: repeated allocation of the same region returns the same id.
   *
   * @throws NullPointerException if {@code encodedRegionName} is null
   */
  public int allocate(String encodedRegionName) {
    Objects.requireNonNull(encodedRegionName, "encodedRegionName");
    return ids.computeIfAbsent(encodedRegionName, k -> next.incrementAndGet());
  }

  /**
   * Return the id previously allocated for {@code encodedRegionName}, or {@code 0} if the region
   * has not been registered (or has been released). {@code 0} is reserved as the "no region scope"
   * sentinel — it is never assigned by {@link #allocate(String)}.
   */
  public int idFor(String encodedRegionName) {
    if (encodedRegionName == null) {
      return 0;
    }
    Integer id = ids.get(encodedRegionName);
    return id == null ? 0 : id;
  }

  /**
   * Drop the mapping for {@code encodedRegionName}. No-op for unknown regions. The freed id is
   * <em>not</em> reused; the next {@link #allocate(String)} call (for any name) yields a new id
   * strictly greater than every id ever returned by this allocator.
   */
  public void release(String encodedRegionName) {
    ids.remove(encodedRegionName);
  }

  /** Number of currently-registered regions. Intended for tests and diagnostics. */
  public int size() {
    return ids.size();
  }
}
