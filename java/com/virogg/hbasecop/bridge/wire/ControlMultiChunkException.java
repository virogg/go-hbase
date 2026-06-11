// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/** A control-type frame (Heartbeat/Shutdown/Log) arrived with chunk_total &gt; 1. */
public final class ControlMultiChunkException extends WireException {
  private static final long serialVersionUID = 1L;

  public ControlMultiChunkException(String message) {
    super(message);
  }
}
