// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Synchronous request/response gateway from the Java RegionObserver layer down to the Go runtime.
 * The adapter ({@link RegionObserverAdapter}) calls {@link #dispatchHook} on every observer
 * invocation; the implementation owns req_id allocation, wire framing (length-prefixed chunk +
 * {@code wirepb.Request} envelope), the Java↔Go shmem rings and req_id↔response matching.
 *
 * <p>T23 declares the contract only; the production implementation lands in T24 (mux v0). Tests
 * mock this interface to drive the adapter directly.
 */
public interface HookDispatcher {

  /**
   * Send a Request frame carrying {@code hookCtxBytes} for {@code hookId} on the route identified
   * by {@code regionId} and synchronously wait for the matching peer response. Returns the
   * serialized {@code hookpb.HookResponse} bytes (i.e. the {@code wirepb.Response.hook_resp}
   * payload) the adapter then parses into a {@code HookResponse} proto.
   *
   * <p>{@code regionId} is the wire-level routing key for T61 multi-region multiplexing. Pass the
   * id allocated by {@link com.virogg.hbasecop.multiplex.RegionIdAllocator} for the RegionObserver
   * instance owning this call, or {@code 0} for observer surfaces without region scope (Master,
   * RegionServer, WAL, BulkLoad).
   *
   * @throws IOException on transport-level failure
   * @throws InterruptedException if the caller thread is interrupted while waiting
   * @throws TimeoutException if no response arrives within {@code timeout}
   */
  byte[] dispatchHook(int regionId, byte hookId, byte[] hookCtxBytes, Duration timeout)
      throws IOException, InterruptedException, TimeoutException;
}
