// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/**
 * The decoder already holds {@link WireFormat#MAX_PENDING_REASSEMBLIES} in-progress multi-chunk
 * messages and a frame opened yet another req_id; abandoned reassemblies (final chunk never sent)
 * must not grow the pending map without bound. Mirrors {@code wire.ErrTooManyPending}.
 */
public final class TooManyPendingException extends WireException {
  private static final long serialVersionUID = 1L;

  public TooManyPendingException(String message) {
    super(message);
  }
}
