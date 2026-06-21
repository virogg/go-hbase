// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

public final class WireFormat {

  public static final int HEADER_SIZE = 23;

  public static final int MAX_FRAME_SIZE = 64 * 1024;

  public static final int MAX_PAYLOAD_BYTES = MAX_FRAME_SIZE - 4 - HEADER_SIZE;

  public static final int MAX_CHUNKS = 1024;

  public static final int MAX_PENDING_REASSEMBLIES = 4096;

  public static final int MAX_PENDING_BYTES = 96 << 20;

  private WireFormat() {}
}
