// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.nativecoproc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

public final class NativeFilterObserver implements RegionCoprocessor, RegionObserver {

  static final String BLOCKED_PREFIX_KEY = "hbasecop.filter.blocked_prefix";

  static final String DEFAULT_BLOCKED_PREFIX = "block-";

  private volatile byte[] blocked = DEFAULT_BLOCKED_PREFIX.getBytes(StandardCharsets.UTF_8);

  private final AtomicLong preGetOps = new AtomicLong();
  private final AtomicLong blockedGets = new AtomicLong();

  public NativeFilterObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) {
    if (env instanceof RegionCoprocessorEnvironment) {
      String configured =
          ((RegionCoprocessorEnvironment) env)
              .getRegion()
              .getTableDescriptor()
              .getValue(BLOCKED_PREFIX_KEY);
      if (configured != null && !configured.isEmpty()) {
        blocked = configured.getBytes(StandardCharsets.UTF_8);
      }
    }
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void preGetOp(
      ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result)
      throws IOException {
    preGetOps.incrementAndGet();
    if (matchesBlocked(get.getRow())) {
      blockedGets.incrementAndGet();
      c.bypass();
    }
  }

  private boolean matchesBlocked(byte[] row) {
    if (blocked.length == 0 || row == null || row.length < blocked.length) {
      return false;
    }
    for (int i = 0; i < blocked.length; i++) {
      if (row[i] != blocked[i]) {
        return false;
      }
    }
    return true;
  }

  long preGetCount() {
    return preGetOps.get();
  }

  long blockedGetCount() {
    return blockedGets.get();
  }
}
