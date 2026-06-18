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
 * #closeForCall(long)} reaps a single endpoint call's scanners on normal completion.
 *
 * <p><b>Reaping race (TE42).</b> The servicing pool stays live during a crash sweep, so a queued
 * SCAN_OPEN can {@link #register} concurrently with {@link #closeAll}. Each per-call entry carries
 * a {@code closing} tombstone; register, lookup, remove, and the sweeps all mutate the per-call
 * bucket through {@link ConcurrentHashMap#compute}, which is atomic per key — so a register either
 * lands before the sweep (and is drained by it) or sees {@code closing} and is rejected ({@link
 * #REJECTED}), closing its own freshly-opened scanner. {@link #closeAll} keeps the drained
 * tombstone so a late register for a reaped call still rejects; {@link #closeForCall} (no
 * concurrent register on a finished call) drops the entry. The narrow residue — a register that
 * creates a brand-new entry after {@code closeAll} already iterated its key set — is reaped by the
 * idle lease ({@code evictIdle}, landing with TE42 scanner-idle-lease), the standard orphan backstop.
 *
 * <p>Scanner ids are globally monotonic (never reused) so logs/metrics correlated by id stay
 * unambiguous across open/close cycles; call ids likewise never recur, so a tombstone is only ever
 * revisited by a late register racing its own call's reap.
 */
public final class ScannerRegistry {

  /** {@link #register} return value when the call was being reaped: the scanner was closed here. */
  public static final long REJECTED = -1L;

  private static final Logger LOG = System.getLogger(ScannerRegistry.class.getName());

  /** A call's open scanners plus a tombstone marking that a reap has begun for the call. */
  private static final class CallScanners {
    final ConcurrentHashMap<Long, RegionScanner> scanners = new ConcurrentHashMap<>();
    volatile boolean closing;
  }

  private final ConcurrentHashMap<Long, CallScanners> byCall = new ConcurrentHashMap<>();
  private final AtomicLong nextScannerId = new AtomicLong();

  /**
   * Register a freshly opened scanner under {@code callId}; returns its assigned scanner id, or
   * {@link #REJECTED} if the call is being reaped (in which case {@code scanner} has been closed).
   */
  public long register(long callId, RegionScanner scanner) {
    long id = nextScannerId.incrementAndGet();
    boolean[] rejected = {false};
    byCall.compute(
        callId,
        (k, call) -> {
          if (call != null && call.closing) {
            rejected[0] = true; // a reap is in progress for this call; keep the tombstone
            return call;
          }
          if (call == null) {
            call = new CallScanners();
          }
          call.scanners.put(id, scanner);
          return call;
        });
    if (rejected[0]) {
      // The just-opened scanner outlives its reaped call and Go will never CLOSE it — close it now.
      closeQuietly(scanner);
      return REJECTED;
    }
    return id;
  }

  /** The scanner for {@code (callId, scannerId)}, or {@code null} if unknown/closed/reaping. */
  public RegionScanner lookup(long callId, long scannerId) {
    CallScanners call = byCall.get(callId);
    if (call == null || call.closing) {
      return null;
    }
    return call.scanners.get(scannerId);
  }

  /**
   * Remove {@code (callId, scannerId)} from the registry and return it so the caller can close it,
   * or {@code null} if unknown. Does not close the scanner.
   */
  public RegionScanner remove(long callId, long scannerId) {
    RegionScanner[] out = {null};
    byCall.compute(
        callId,
        (k, call) -> {
          if (call == null) {
            return null;
          }
          out[0] = call.scanners.remove(scannerId);
          // Drop a fully-drained live entry; keep tombstones for the reaping-race window.
          return call.scanners.isEmpty() && !call.closing ? null : call;
        });
    return out[0];
  }

  /**
   * Close and drop every scanner owned by {@code callId} (normal completion); returns the count.
   */
  public int closeForCall(long callId) {
    int[] n = {0};
    byCall.compute(
        callId,
        (k, call) -> {
          if (call == null) {
            return null;
          }
          call.closing = true;
          n[0] = drain(call.scanners);
          // A finished call issues no further SCAN_OPEN, so drop the entry rather than tombstone
          // it.
          return null;
        });
    return n[0];
  }

  /** Close and drop every open scanner (crash/teardown reaping); returns how many were reaped. */
  public int closeAll() {
    int n = 0;
    for (Long callId : byCall.keySet()) {
      n += reapOne(callId);
    }
    return n;
  }

  private int reapOne(long callId) {
    int[] n = {0};
    byCall.compute(
        callId,
        (k, call) -> {
          if (call == null) {
            return null;
          }
          call.closing = true; // tombstone BEFORE draining so a racing register rejects
          n[0] = drain(call.scanners);
          // Keep the drained tombstone: a late register for this reaped call must still reject.
          return call;
        });
    return n[0];
  }

  /** Number of currently-open scanners. Intended for tests and diagnostics. */
  public int size() {
    int n = 0;
    for (CallScanners call : byCall.values()) {
      n += call.scanners.size();
    }
    return n;
  }

  private static int drain(Map<Long, RegionScanner> scanners) {
    int n = 0;
    for (RegionScanner s : scanners.values()) {
      closeQuietly(s);
      n++;
    }
    scanners.clear();
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
