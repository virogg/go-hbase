// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

/**
 * Base class for checked exceptions thrown by {@link Channel}. Runtime invariants (wrong-role
 * usage, builder misuse) surface as {@link IllegalStateException} / {@link
 * IllegalArgumentException} instead and are intentionally <em>not</em> part of this hierarchy.
 */
public class ShmemException extends Exception {

  private static final long serialVersionUID = 1L;

  public ShmemException(String message) {
    super(message);
  }

  public ShmemException(String message, Throwable cause) {
    super(message, cause);
  }
}
