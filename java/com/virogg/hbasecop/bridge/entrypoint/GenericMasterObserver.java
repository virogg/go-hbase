// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.entrypoint;

import com.google.protobuf.Service;
import com.virogg.hbasecop.bridge.SharedRuntime;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;

public final class GenericMasterObserver implements MasterCoprocessor {

  private static final String FALLBACK_KEY = GenericMasterObserver.class.getName();

  private SharedRuntime.Handle handle;

  public GenericMasterObserver() {}

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
  public Optional<MasterObserver> getMasterObserver() {
    return handle == null ? Optional.empty() : Optional.ofNullable(handle.getMasterObserver());
  }

  @Override
  public Iterable<Service> getServices() {
    return GenericCoprocessor.endpointServices(() -> handle);
  }
}
