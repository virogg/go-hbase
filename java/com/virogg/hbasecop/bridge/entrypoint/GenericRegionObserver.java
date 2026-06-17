// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.entrypoint;

import com.google.protobuf.Service;
import com.virogg.hbasecop.bridge.SharedRuntime;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

/**
 * Stock region-side entrypoint: name it in {@code setCoprocessor} to deploy a Go RegionObserver
 * with no hand-written Java. Ring sizes and timeouts come from the host Configuration (see {@link
 * GenericCoprocessor}); the shared Go process is keyed on the coproc-jar's coproc-id.
 */
public final class GenericRegionObserver implements RegionCoprocessor {

  private static final String FALLBACK_KEY = GenericRegionObserver.class.getName();

  private SharedRuntime.Handle handle;

  // TE31: the encoded name of this instance's region, captured at start so
  // getServices() can resolve the wire region_id to stamp onto endpoint invokes.
  private volatile String encodedRegionName;

  public GenericRegionObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    handle = GenericCoprocessor.acquire(GenericCoprocessor.sharedKey(FALLBACK_KEY), env);
    if (env instanceof RegionCoprocessorEnvironment) {
      encodedRegionName =
          ((RegionCoprocessorEnvironment) env).getRegion().getRegionInfo().getEncodedName();
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (handle != null) {
      handle.release();
      handle = null;
    }
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return handle == null ? Optional.empty() : Optional.ofNullable(handle.getRegionObserver());
  }

  /** Exposes the generic Go endpoint Service (Tier 2) alongside the region observer. */
  @Override
  public Iterable<Service> getServices() {
    return GenericCoprocessor.endpointServices(
        () -> handle,
        () -> {
          SharedRuntime.Handle h = handle;
          String name = encodedRegionName;
          return (h == null || name == null) ? 0 : h.regionId(name);
        });
  }
}
