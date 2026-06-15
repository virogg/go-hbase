// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.entrypoint;

import com.virogg.hbasecop.bridge.SharedRuntime;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessor;
import org.apache.hadoop.hbase.coprocessor.WALObserver;

/**
 * Stock WAL entrypoint: name it in {@code setCoprocessor} to deploy a Go WALObserver with no
 * hand-written Java. See {@link GenericCoprocessor} for config and keying.
 */
public final class GenericWALObserver implements WALCoprocessor {

  private static final String FALLBACK_KEY = GenericWALObserver.class.getName();

  private SharedRuntime.Handle handle;

  public GenericWALObserver() {}

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
  public Optional<WALObserver> getWALObserver() {
    return handle == null ? Optional.empty() : Optional.ofNullable(handle.getWALObserver());
  }
}
