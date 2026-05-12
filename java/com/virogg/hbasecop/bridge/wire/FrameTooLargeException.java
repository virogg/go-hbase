// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/**
 * Declared frame length falls outside the {@code [HEADER_SIZE, MAX_FRAME_SIZE - 4]} window.
 * Returned for both oversized and shorter-than-header frames.
 */
public final class FrameTooLargeException extends WireException {
  private static final long serialVersionUID = 1L;

  public FrameTooLargeException(String message) {
    super(message);
  }
}
