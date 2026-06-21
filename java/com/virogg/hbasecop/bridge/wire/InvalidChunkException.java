// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

public final class InvalidChunkException extends WireException {
  private static final long serialVersionUID = 1L;

  public InvalidChunkException(String message) {
    super(message);
  }
}
