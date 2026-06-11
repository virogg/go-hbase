// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/**
 * A frame declared {@code chunk_total} above {@link WireFormat#MAX_CHUNKS}. Rejected before the
 * decoder allocates the reassembly chunk array, so a hostile 27-byte frame cannot request a
 * multi-GiB allocation. Mirrors {@code wire.ErrTooManyChunks}.
 */
public final class TooManyChunksException extends WireException {
  private static final long serialVersionUID = 1L;

  public TooManyChunksException(String message) {
    super(message);
  }
}
