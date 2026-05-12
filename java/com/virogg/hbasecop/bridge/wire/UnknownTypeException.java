// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/** The on-wire type byte is 0 or above {@link FrameType#LOG}. */
public final class UnknownTypeException extends WireException {
  private static final long serialVersionUID = 1L;

  public UnknownTypeException(String message) {
    super(message);
  }
}
