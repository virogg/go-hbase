// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** TE33 - {@link ScannerRegistry}: id assignment, lookup, and reaping (the leak guarantee). */
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
    verify(s1, org.mockito.Mockito.never()).close(); // remove does not close
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
}
