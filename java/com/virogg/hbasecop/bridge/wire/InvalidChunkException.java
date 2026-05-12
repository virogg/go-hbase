// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/**
 * Chunk_total/chunk_idx are inconsistent, the same chunk_idx was seen twice, the per-(req_id)
 * header drifted between chunks, or a multi-chunk message arrived with req_id=0.
 */
public final class InvalidChunkException extends WireException {
  private static final long serialVersionUID = 1L;

  public InvalidChunkException(String message) {
    super(message);
  }
}
