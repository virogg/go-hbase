// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

/**
 * On-wire payload-type discriminator (the {@code type} byte).
 *
 * <p>Values match {@code internal/wire/frame.go}; the wire numeric values are independent of the PB
 * oneof tag numbers in {@code wire.proto}.
 */
public enum FrameType {
  UNKNOWN((byte) 0),
  REQUEST((byte) 1),
  RESPONSE((byte) 2),
  HEARTBEAT((byte) 3),
  ERROR((byte) 4),
  SHUTDOWN((byte) 5),
  LOG((byte) 6);

  private final byte value;

  FrameType(byte value) {
    this.value = value;
  }

  public byte value() {
    return value;
  }

  public boolean valid() {
    return value >= 1 && value <= 6;
  }

  /** Heartbeat/Shutdown/Log are stateless and must be single-chunk on the wire. */
  public boolean isControl() {
    return this == HEARTBEAT || this == SHUTDOWN || this == LOG;
  }

  public static FrameType fromByte(byte b) {
    switch (b) {
      case 1:
        return REQUEST;
      case 2:
        return RESPONSE;
      case 3:
        return HEARTBEAT;
      case 4:
        return ERROR;
      case 5:
        return SHUTDOWN;
      case 6:
        return LOG;
      default:
        return UNKNOWN;
    }
  }
}
