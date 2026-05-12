// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

/**
 * Thrown by {@link Channel#send(byte[])} when the caller's frame would not fit in one ring slot
 * (i.e. {@code frame.length > maxObjectSize - 4}). Mirrors Go's {@code shmem.ErrFrameTooLarge}.
 *
 * <p>Distinct from {@code com.virogg.hbasecop.bridge.wire.FrameTooLargeException}, which guards the
 * on-wire 64 KiB chunk limit — different layer, different invariant.
 */
public class FrameTooLargeException extends ShmemException {

  private static final long serialVersionUID = 1L;

  public FrameTooLargeException(int frameSize, int maxPayloadSize) {
    super("shmem: frame exceeds max payload size: " + frameSize + " > " + maxPayloadSize);
  }
}
