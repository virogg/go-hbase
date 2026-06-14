// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/**
 * The payload would split into more than {@link WireFormat#MAX_CHUNKS} chunks. Thrown by {@link
 * Encoder#encode} at the producer instead of emitting a frame stream carrying {@code chunk_total >
 * MAX_CHUNKS} that the matching decoder is required to reject with {@link TooManyChunksException}.
 * Failing here turns self-undecodable output into a clear, early producer-side error. Mirrors
 * {@code wire.ErrMessageTooLarge}.
 */
public final class MessageTooLargeException extends WireException {
  private static final long serialVersionUID = 1L;

  public MessageTooLargeException(String message) {
    super(message);
  }
}
