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

public final class ScannerRegistry {

  public static final long REJECTED = -1L;

  public static final long AT_CAPACITY = -2L;

  private static final Logger LOG = System.getLogger(ScannerRegistry.class.getName());

  private static final class ScannerHolder {
    final RegionScanner scanner;
    volatile long lastTouchMs;

    ScannerHolder(RegionScanner scanner, long nowMs) {
      this.scanner = scanner;
      this.lastTouchMs = nowMs;
    }
  }

  private static final class CallScanners {
    final ConcurrentHashMap<Long, ScannerHolder> scanners = new ConcurrentHashMap<>();
    volatile boolean closing;
  }

  private final ConcurrentHashMap<Long, CallScanners> byCall = new ConcurrentHashMap<>();
  private final AtomicLong nextScannerId = new AtomicLong();
  private final int maxScannersPerCall;
  private final long idleLeaseMs;
  private final LongSupplier clock;

  public ScannerRegistry() {
    this(Integer.MAX_VALUE, Long.MAX_VALUE, System::currentTimeMillis);
  }

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
      closeQuietly(scanner);
    }
    return result[0];
  }

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
          return call.scanners.isEmpty() && !call.closing ? null : call;
        });
    return out[0];
  }

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
          return null;
        });
    return n[0];
  }

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
          return call;
        });
    return n[0];
  }

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
            return call.scanners.isEmpty() ? null : call;
          });
    }
    return reaped[0];
  }

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
