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

/**
 * Native (pure-Java) twin of the Go {@code examples/filter-observer} read-path observer. {@code
 * preGetOp} inspects every Get's row key: those carrying the configured blocked prefix are bypassed
 * ({@link ObserverContext#bypass()}), so HBase skips its own read and the Get returns empty — the
 * same observable effect the Go arm produces via {@code HookResult.Bypass=true}.
 *
 * <p>The blocked prefix matches the Go arm's default ({@code "block-"}; override via the {@code
 * hbasecop.compare.filter.blocked-prefix} coprocessor config). Match logic mirrors {@code
 * examples/filter-observer/filter/filter.go} {@code matchesBlocked} ({@code bytes.HasPrefix}).
 */
public final class NativeFilterObserver implements RegionCoprocessor, RegionObserver {

  /** Table-level config key — same key the Go arm's FilterRegionObserver reads. */
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
      c.bypass(); // skip the core read; result stays empty
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
