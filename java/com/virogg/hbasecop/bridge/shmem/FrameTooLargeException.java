// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

public class FrameTooLargeException extends ShmemException {

  private static final long serialVersionUID = 1L;

  public FrameTooLargeException(int frameSize, int maxPayloadSize) {
    super("shmem: frame exceeds max payload size: " + frameSize + " > " + maxPayloadSize);
  }
}
