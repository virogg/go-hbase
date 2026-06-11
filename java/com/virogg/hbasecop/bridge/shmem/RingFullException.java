// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

/**
 * Thrown by {@link Channel#send(byte[])} when the producer cannot allocate a slot because the
 * consumer side has not yet caught up. Mirrors Go's {@code shmem.ErrRingFull}.
 */
public class RingFullException extends ShmemException {

  private static final long serialVersionUID = 1L;

  public RingFullException() {
    super("shmem: ring full");
  }
}
