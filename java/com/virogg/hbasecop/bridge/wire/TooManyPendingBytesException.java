// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/**
 * Storing this chunk would push the total payload bytes retained across all in-progress
 * reassemblies over {@link WireFormat#MAX_PENDING_BYTES}. Caps heap from abandoned near-complete
 * reassemblies, which the entry-count cap alone does not bound. Mirrors {@code
 * wire.ErrTooManyPendingBytes}.
 */
public final class TooManyPendingBytesException extends WireException {
  private static final long serialVersionUID = 1L;

  public TooManyPendingBytesException(String message) {
    super(message);
  }
}
