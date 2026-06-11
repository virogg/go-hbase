// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.jgshmem.ring.WaitingRingConsumer;
import com.jgshmem.ring.WaitingRingProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * T14 hello-world: proves the locally-installed {@code com.jgshmem:java-go-shmem} artifact is on
 * the bridge's classpath. We only reference the class objects — instantiating a producer requires
 * an mmap region and is exercised in T16 (the real wrapper).
 */
class ShmemDependencyTest {

  @Test
  @DisplayName("WaitingRingProducer/Consumer classes are reachable")
  void shmemClassesReachable() {
    assertNotNull(WaitingRingProducer.class.getCanonicalName());
    assertNotNull(WaitingRingConsumer.class.getCanonicalName());
  }
}
