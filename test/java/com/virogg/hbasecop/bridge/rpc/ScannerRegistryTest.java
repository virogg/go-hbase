// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ScannerRegistryTest {

  @Mock private RegionScanner s1;
  @Mock private RegionScanner s2;

  @Test
  void registerAssignsMonotonicIdsAndLooksUp() {
    ScannerRegistry reg = new ScannerRegistry();
    long id1 = reg.register(7, s1);
    long id2 = reg.register(7, s2);
    assertNotEquals(id1, id2);
    assertSame(s1, reg.lookup(7, id1));
    assertSame(s2, reg.lookup(7, id2));
    assertEquals(2, reg.size());
  }

  @Test
  void lookupUnknownReturnsNull() {
    ScannerRegistry reg = new ScannerRegistry();
    assertNull(reg.lookup(1, 1));
    long id = reg.register(1, s1);
    assertNull(reg.lookup(2, id), "different call id must not resolve");
  }

  @Test
  void removeReturnsAndDropsWithoutClosing() throws Exception {
    ScannerRegistry reg = new ScannerRegistry();
    long id = reg.register(1, s1);
    assertSame(s1, reg.remove(1, id));
    assertNull(reg.lookup(1, id));
    assertEquals(0, reg.size());
    verify(s1, org.mockito.Mockito.never()).close();
  }

  @Test
  void closeForCallClosesOnlyThatCallsScanners() throws Exception {
    ScannerRegistry reg = new ScannerRegistry();
    reg.register(1, s1);
    long id2 = reg.register(2, s2);
    assertEquals(1, reg.closeForCall(1));
    verify(s1).close();
    assertEquals(1, reg.size());
    assertSame(s2, reg.lookup(2, id2), "call 2's scanner must survive closing call 1");
  }

  @Test
  void closeAllReapsEveryScanner() throws Exception {
    ScannerRegistry reg = new ScannerRegistry();
    reg.register(1, s1);
    reg.register(2, s2);
    int reaped = reg.closeAll();
    assertEquals(2, reaped);
    verify(s1).close();
    verify(s2).close();
    assertEquals(0, reg.size());
  }

  @Test
  void registerAfterReapRejectsAndClosesScanner() throws Exception {
    ScannerRegistry reg = new ScannerRegistry();
    reg.register(1, s1);
    reg.closeAll();
    verify(s1).close();

    long id = reg.register(1, s2);
    assertEquals(ScannerRegistry.REJECTED, id, "register racing a reap must be rejected");
    verify(s2).close();
    assertEquals(0, reg.size());
    assertNull(reg.lookup(1, id));
  }

  @Test
  void concurrentRegisterAndCloseAllNeverLeaksAScanner() throws Exception {
    ScannerRegistry reg = new ScannerRegistry();
    int threads = 6;
    int perThread = 50;
    List<RegionScanner> all = Collections.synchronizedList(new ArrayList<>());
    ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean stop = new AtomicBoolean(false);
    try {
      List<Future<?>> registrars = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        long callId = t % 3;
        registrars.add(
            pool.submit(
                () -> {
                  awaitQuietly(start);
                  for (int i = 0; i < perThread; i++) {
                    RegionScanner s = Mockito.mock(RegionScanner.class);
                    all.add(s);
                    reg.register(callId, s);
                  }
                }));
      }
      Future<?> reaper =
          pool.submit(
              () -> {
                awaitQuietly(start);
                while (!stop.get()) {
                  reg.closeAll();
                }
              });
      start.countDown();
      for (Future<?> f : registrars) {
        f.get(30, TimeUnit.SECONDS);
      }
      stop.set(true);
      reaper.get(30, TimeUnit.SECONDS);
      reg.closeAll();
    } finally {
      pool.shutdownNow();
    }

    assertEquals(0, reg.size(), "no scanner may survive a register-vs-reap race");
    for (RegionScanner s : all) {
      verify(s, Mockito.atLeastOnce()).close();
    }
  }

  @Test
  void registerRejectsBeyondMaxPerCall() throws Exception {
    ScannerRegistry reg = new ScannerRegistry(2, Long.MAX_VALUE, System::currentTimeMillis);
    assertNotEquals(ScannerRegistry.AT_CAPACITY, reg.register(1, s1));
    assertNotEquals(ScannerRegistry.AT_CAPACITY, reg.register(1, s2));
    RegionScanner s3 = Mockito.mock(RegionScanner.class);
    assertEquals(ScannerRegistry.AT_CAPACITY, reg.register(1, s3));
    verify(s3).close();
    assertEquals(2, reg.size());
  }

  @Test
  void evictIdleClosesOnlyStaleScanners() throws Exception {
    AtomicLong now = new AtomicLong(0);
    ScannerRegistry reg = new ScannerRegistry(Integer.MAX_VALUE, 1000, now::get);
    reg.register(1, s1);
    now.set(2000);
    long id2 = reg.register(1, s2);

    int reaped = reg.evictIdle();
    assertEquals(1, reaped);
    verify(s1).close();
    assertEquals(1, reg.size());
    assertSame(s2, reg.lookup(1, id2), "a freshly-touched scanner survives the sweep");
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
