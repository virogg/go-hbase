// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

public class ShmemException extends Exception {

  private static final long serialVersionUID = 1L;

  public ShmemException(String message) {
    super(message);
  }

  public ShmemException(String message, Throwable cause) {
    super(message, cause);
  }
}
