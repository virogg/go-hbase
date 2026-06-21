// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

public class RingFullException extends ShmemException {

  private static final long serialVersionUID = 1L;

  public RingFullException() {
    super("shmem: ring full");
  }
}
