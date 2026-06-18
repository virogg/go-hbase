// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

/**
 * Per-process registry of open server-side scanners (Tier 2, TE33), keyed by {@code (call_id,
 * scanner_id)}. It mints monotonic scanner ids on SCAN_OPEN, resolves them for SCAN_NEXT/CLOSE, and
 * — critically — closes scanners that outlive their owner so no {@link RegionScanner} leaks.
 *
 * <p>A leaked RegionScanner pins an MVCC read point, which blocks store-file compaction cleanup, so
 * {@link #closeAll()} is invoked on the Go-process crash path (alongside {@code
 * Multiplexer.pauseInflightFailing}) to reap every scanner the dead process owned. {@link
 * #closeForCall(long)} reaps a single endpoint call's scanners.
 *
 * <p>Thread-safe: a {@link ConcurrentHashMap} of per-call maps. Scanner ids are globally monotonic
 * (never reused) so logs/metrics correlated by id stay unambiguous across open/close cycles.
 */
public final class ScannerRegistry {

  private static final Logger LOG = System.getLogger(ScannerRegistry.class.getName());

  private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, RegionScanner>> byCall =
      new ConcurrentHashMap<>();
  private final AtomicLong nextScannerId = new AtomicLong();

  /** Register a freshly opened scanner under {@code callId}; returns its assigned scanner id. */
  public long register(long callId, RegionScanner scanner) {
    long id = nextScannerId.incrementAndGet();
    byCall.computeIfAbsent(callId, k -> new ConcurrentHashMap<>()).put(id, scanner);
    return id;
  }

  /** The scanner for {@code (callId, scannerId)}, or {@code null} if unknown/closed. */
  public RegionScanner lookup(long callId, long scannerId) {
    ConcurrentHashMap<Long, RegionScanner> m = byCall.get(callId);
    return m == null ? null : m.get(scannerId);
  }

  /**
   * Remove {@code (callId, scannerId)} from the registry and return it so the caller can close it,
   * or {@code null} if unknown. Does not close the scanner.
   */
  public RegionScanner remove(long callId, long scannerId) {
    ConcurrentHashMap<Long, RegionScanner> m = byCall.get(callId);
    if (m == null) {
      return null;
    }
    RegionScanner s = m.remove(scannerId);
    if (m.isEmpty()) {
      byCall.remove(callId, m);
    }
    return s;
  }

  /** Close and drop every scanner owned by {@code callId}; returns how many were reaped. */
  public int closeForCall(long callId) {
    ConcurrentHashMap<Long, RegionScanner> m = byCall.remove(callId);
    if (m == null) {
      return 0;
    }
    int n = 0;
    for (RegionScanner s : m.values()) {
      closeQuietly(s);
      n++;
    }
    return n;
  }

  /** Close and drop every open scanner (crash/teardown reaping); returns how many were reaped. */
  public int closeAll() {
    int n = 0;
    for (Map.Entry<Long, ConcurrentHashMap<Long, RegionScanner>> e : byCall.entrySet()) {
      ConcurrentHashMap<Long, RegionScanner> m = byCall.remove(e.getKey());
      if (m == null) {
        continue;
      }
      for (RegionScanner s : m.values()) {
        closeQuietly(s);
        n++;
      }
    }
    return n;
  }

  /** Number of currently-open scanners. Intended for tests and diagnostics. */
  public int size() {
    int n = 0;
    for (ConcurrentHashMap<Long, RegionScanner> m : byCall.values()) {
      n += m.size();
    }
    return n;
  }

  private static void closeQuietly(RegionScanner s) {
    try {
      s.close();
    } catch (IOException e) {
      LOG.log(Level.WARNING, "hbasecop: closing a reaped RegionScanner threw", e);
    }
  }
}
