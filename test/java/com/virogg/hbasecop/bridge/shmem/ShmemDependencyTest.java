// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.jgshmem.ring.WaitingRingConsumer;
import com.jgshmem.ring.WaitingRingProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ShmemDependencyTest {

  @Test
  @DisplayName("WaitingRingProducer/Consumer classes are reachable")
  void shmemClassesReachable() {
    assertNotNull(WaitingRingProducer.class.getCanonicalName());
    assertNotNull(WaitingRingConsumer.class.getCanonicalName());
  }
}
