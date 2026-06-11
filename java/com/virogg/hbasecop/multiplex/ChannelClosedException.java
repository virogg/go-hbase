// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

/**
 * Raised on every outstanding and subsequent {@link Multiplexer#call} when the underlying channel
 * is shut down. Carried as the cause of the {@link java.util.concurrent.CompletableFuture}'s
 * failure.
 */
public final class ChannelClosedException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public ChannelClosedException() {
    super("multiplex: channel closed");
  }

  public ChannelClosedException(String message) {
    super(message);
  }
}
