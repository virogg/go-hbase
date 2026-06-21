// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.entrypoint;

import com.virogg.hbasecop.bridge.SharedRuntime;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;

public final class GenericRegionServerObserver implements RegionServerCoprocessor {

  private static final String FALLBACK_KEY = GenericRegionServerObserver.class.getName();

  private SharedRuntime.Handle handle;

  public GenericRegionServerObserver() {}

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    handle = GenericCoprocessor.acquire(GenericCoprocessor.sharedKey(FALLBACK_KEY), env);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
    if (handle != null) {
      handle.release();
      handle = null;
    }
  }

  @Override
  public Optional<RegionServerObserver> getRegionServerObserver() {
    return handle == null
        ? Optional.empty()
        : Optional.ofNullable(handle.getRegionServerObserver());
  }
}
