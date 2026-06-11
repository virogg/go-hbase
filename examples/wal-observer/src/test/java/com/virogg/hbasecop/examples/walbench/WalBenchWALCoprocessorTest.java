// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.walbench;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Lifecycle-edge tests for {@link WalBenchWALCoprocessor} that need no runtime: before {@code
 * start()} acquires a {@link com.virogg.hbasecop.bridge.SharedRuntime} handle the coprocessor must
 * expose no observer and tolerate {@code stop()}.
 */
final class WalBenchWALCoprocessorTest {

  @Test
  void getWALObserver_isEmptyBeforeStart() {
    assertTrue(new WalBenchWALCoprocessor().getWALObserver().isEmpty());
  }

  @Test
  void stop_beforeStartIsSafe() {
    WalBenchWALCoprocessor coproc = new WalBenchWALCoprocessor();
    assertDoesNotThrow(() -> coproc.stop(null));
    assertTrue(coproc.getWALObserver().isEmpty());
  }
}
