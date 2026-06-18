// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
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
 * concurrent register on a finished call) drops the entry.
 *
 * <p><b>Limits (TE42).</b> {@code maxScannersPerCall} bounds concurrent open scanners per call
 * ({@link #AT_CAPACITY} on overflow). The idle lease reaps any scanner not touched (by SCAN_NEXT)
 * within {@code idleLeaseMs} — the backstop for an endpoint that opens a scanner and never closes
 * it, and for the narrow new-entry-after-sweep reaping residue. {@link #evictIdle()} runs on the
 * runtime's scheduler tick and also prunes drained tombstones, bounding their accumulation.
 *
 * <p>Scanner ids are globally monotonic (never reused); call ids likewise never recur.
 */
public final class ScannerRegistry {

  /** {@link #register} return value when the call was being reaped: the scanner was closed here. */
  public static final long REJECTED = -1L;

  /** {@link #register} return value when the call is at its scanner cap: the scanner was closed. */
  public static final long AT_CAPACITY = -2L;

  private static final Logger LOG = System.getLogger(ScannerRegistry.class.getName());

  /** A single open scanner plus its last-touch time (for the idle lease). */
  private static final class ScannerHolder {
    final RegionScanner scanner;
    volatile long lastTouchMs;

    ScannerHolder(RegionScanner scanner, long nowMs) {
      this.scanner = scanner;
      this.lastTouchMs = nowMs;
    }
  }

  /** A call's open scanners plus a tombstone marking that a reap has begun for the call. */
  private static final class CallScanners {
    final ConcurrentHashMap<Long, ScannerHolder> scanners = new ConcurrentHashMap<>();
    volatile boolean closing;
  }

  private final ConcurrentHashMap<Long, CallScanners> byCall = new ConcurrentHashMap<>();
  private final AtomicLong nextScannerId = new AtomicLong();
  private final int maxScannersPerCall;
  private final long idleLeaseMs;
  private final LongSupplier clock;

  /** Unbounded, never-evicting registry (tests/diagnostics). */
  public ScannerRegistry() {
    this(Integer.MAX_VALUE, Long.MAX_VALUE, System::currentTimeMillis);
  }

  /**
   * @param maxScannersPerCall max concurrent open scanners per call (TE42 max-scanners-per-call)
   * @param idleLeaseMs reap a scanner untouched for longer than this (TE42 scanner-idle-lease)
   * @param clock millisecond clock, injectable for tests
   */
  public ScannerRegistry(int maxScannersPerCall, long idleLeaseMs, LongSupplier clock) {
    if (maxScannersPerCall <= 0) {
      throw new IllegalArgumentException(
          "maxScannersPerCall must be > 0, got " + maxScannersPerCall);
    }
    if (idleLeaseMs <= 0) {
      throw new IllegalArgumentException("idleLeaseMs must be > 0, got " + idleLeaseMs);
    }
    this.maxScannersPerCall = maxScannersPerCall;
    this.idleLeaseMs = idleLeaseMs;
    this.clock = clock;
  }

  /**
   * Register a freshly opened scanner under {@code callId}; returns its assigned scanner id, or
   * {@link #REJECTED} (call being reaped) / {@link #AT_CAPACITY} (per-call cap reached). For both
   * negative returns {@code scanner} has been closed here.
   */
  public long register(long callId, RegionScanner scanner) {
    long id = nextScannerId.incrementAndGet();
    long now = clock.getAsLong();
    long[] result = {id};
    byCall.compute(
        callId,
        (k, call) -> {
          if (call != null && call.closing) {
            result[0] = REJECTED; // a reap is in progress for this call; keep the tombstone
            return call;
          }
          if (call == null) {
            call = new CallScanners();
          }
          if (call.scanners.size() >= maxScannersPerCall) {
            result[0] = AT_CAPACITY;
            return call;
          }
          call.scanners.put(id, new ScannerHolder(scanner, now));
          return call;
        });
    if (result[0] < 0) {
      // The just-opened scanner is not tracked (reaped call, or over cap) — close it now so it
      // never outlives its owner pinning a read point.
      closeQuietly(scanner);
    }
    return result[0];
  }

  /**
   * The scanner for {@code (callId, scannerId)}, or {@code null} if unknown/closed/reaping. A hit
   * refreshes the idle lease (this is the scanner's "touch").
   */
  public RegionScanner lookup(long callId, long scannerId) {
    CallScanners call = byCall.get(callId);
    if (call == null || call.closing) {
      return null;
    }
    ScannerHolder h = call.scanners.get(scannerId);
    if (h == null) {
      return null;
    }
    h.lastTouchMs = clock.getAsLong();
    return h.scanner;
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
          ScannerHolder h = call.scanners.remove(scannerId);
          out[0] = h == null ? null : h.scanner;
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

  /**
   * Close scanners untouched for longer than the idle lease and prune drained tombstones (TE42).
   * Runs on the runtime's scheduler tick; returns how many scanners were reaped.
   */
  public int evictIdle() {
    long now = clock.getAsLong();
    int[] reaped = {0};
    for (Long callId : byCall.keySet()) {
      byCall.compute(
          callId,
          (k, call) -> {
            if (call == null) {
              return null;
            }
            call.scanners
                .values()
                .removeIf(
                    h -> {
                      if (now - h.lastTouchMs > idleLeaseMs) {
                        closeQuietly(h.scanner);
                        reaped[0]++;
                        return true;
                      }
                      return false;
                    });
            // Prune an emptied entry (a drained tombstone, or a live call whose scanners all idled
            // out); a later register for a reaped call falls back to the idle lease as the
            // backstop.
            return call.scanners.isEmpty() ? null : call;
          });
    }
    return reaped[0];
  }

  /** Number of currently-open scanners. Intended for tests and diagnostics. */
  public int size() {
    int n = 0;
    for (CallScanners call : byCall.values()) {
      n += call.scanners.size();
    }
    return n;
  }

  private static int drain(Map<Long, ScannerHolder> scanners) {
    int n = 0;
    for (ScannerHolder h : scanners.values()) {
      closeQuietly(h.scanner);
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
