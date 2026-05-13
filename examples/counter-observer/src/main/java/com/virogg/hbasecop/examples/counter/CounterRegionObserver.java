// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.counter;

import java.util.Optional;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

/**
 * Reference coprocessor class registered on the HBase RegionServer for the counter-observer demo.
 * Loaded by HBase via the standard {@code RegionCoprocessor} reflection contract (no-arg ctor).
 *
 * <p>T25 deliverable is the coproc-jar packaging: this class + the bridge classes shaded in + the
 * Go ELF embedded at {@code bin/linux-amd64/hbasecop-runtime}. Full lifecycle wiring (spawning the
 * {@code GoProcess}, opening shmem rings, instantiating {@link
 * com.virogg.hbasecop.bridge.observer.RegionObserverAdapter}) lands in T27 once docker-compose
 * HBase 2.5 is in place.
 */
public final class CounterRegionObserver implements RegionCoprocessor {

  public CounterRegionObserver() {}

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.empty();
  }
}
