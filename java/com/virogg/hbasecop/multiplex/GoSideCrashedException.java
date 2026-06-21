// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

public final class GoSideCrashedException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public GoSideCrashedException(String message) {
    super(message);
  }

  public GoSideCrashedException(String message, Throwable cause) {
    super(message, cause);
  }
}
