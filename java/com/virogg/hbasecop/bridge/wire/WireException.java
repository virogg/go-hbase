// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import java.io.IOException;

public class WireException extends IOException {
  private static final long serialVersionUID = 1L;

  public WireException(String message) {
    super(message);
  }
}
