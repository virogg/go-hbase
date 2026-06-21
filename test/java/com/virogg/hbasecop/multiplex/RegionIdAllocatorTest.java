// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RegionIdAllocatorTest {

  @Test
  void allocateAssignsMonotonicIdsStartingAtOne() {
    RegionIdAllocator alloc = new RegionIdAllocator();
    assertEquals(1, alloc.allocate("region-a"));
    assertEquals(2, alloc.allocate("region-b"));
    assertEquals(3, alloc.allocate("region-c"));
  }

  @Test
  void allocateIsIdempotentForSameRegion() {
    RegionIdAllocator alloc = new RegionIdAllocator();
    int first = alloc.allocate("region-a");
    int second = alloc.allocate("region-a");
    assertEquals(first, second);
  }

  @Test
  void idForReturnsZeroForUnknownRegion() {
    RegionIdAllocator alloc = new RegionIdAllocator();
    assertEquals(0, alloc.idFor("never-allocated"));
  }

  @Test
  void idForReturnsAllocatedIdAfterAllocate() {
    RegionIdAllocator alloc = new RegionIdAllocator();
    int id = alloc.allocate("region-a");
    assertEquals(id, alloc.idFor("region-a"));
  }

  @Test
  void releaseRemovesMappingWithoutReusingId() {
    RegionIdAllocator alloc = new RegionIdAllocator();
    int aFirst = alloc.allocate("region-a");
    alloc.release("region-a");
    assertEquals(0, alloc.idFor("region-a"));

    int aSecond = alloc.allocate("region-a");
    assertNotEquals(aFirst, aSecond, "freed id must not be recycled");
    assertTrue(aSecond > aFirst, "monotonic counter must keep advancing");
  }

  @Test
  void releaseOfUnknownRegionIsNoop() {
    RegionIdAllocator alloc = new RegionIdAllocator();
    alloc.release("never-allocated");
    assertEquals(1, alloc.allocate("region-a"));
  }

  @Test
  void concurrentAllocationsAreDistinctPerRegion() throws Exception {
    final int regionCount = 64;
    RegionIdAllocator alloc = new RegionIdAllocator();
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      CountDownLatch start = new CountDownLatch(1);
      Future<?>[] futures = new Future[regionCount];
      Integer[] ids = new Integer[regionCount];
      for (int i = 0; i < regionCount; i++) {
        final int idx = i;
        futures[i] =
            pool.submit(
                () -> {
                  start.await();
                  ids[idx] = alloc.allocate("region-" + idx);
                  return null;
                });
      }
      start.countDown();
      for (Future<?> f : futures) {
        f.get(5, TimeUnit.SECONDS);
      }
      Set<Integer> seen = new HashSet<>();
      for (Integer id : ids) {
        assertTrue(id >= 1 && id <= regionCount, () -> "id out of range: " + id);
        assertTrue(seen.add(id), () -> "duplicate id: " + id);
      }
      assertEquals(regionCount, seen.size());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void concurrentAllocationsOfSameRegionConverge() throws Exception {
    final int workers = 16;
    RegionIdAllocator alloc = new RegionIdAllocator();
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      CountDownLatch start = new CountDownLatch(1);
      Future<Integer>[] futures = new Future[workers];
      for (int i = 0; i < workers; i++) {
        futures[i] =
            pool.submit(
                () -> {
                  start.await();
                  return alloc.allocate("hot-region");
                });
      }
      start.countDown();
      Integer canonical = futures[0].get(5, TimeUnit.SECONDS);
      for (int i = 1; i < workers; i++) {
        assertEquals(canonical, futures[i].get(5, TimeUnit.SECONDS));
      }
    } finally {
      pool.shutdownNow();
    }
  }
}
