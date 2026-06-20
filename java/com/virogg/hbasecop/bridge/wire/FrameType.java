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
  LOG((byte) 6),
  // Tier 2 (wire v2): endpoint invoke/result and the Go-initiated reverse-RPC
  // channel. Appended; v1 values are unchanged.
  ENDPOINT_INVOKE((byte) 7),
  ENDPOINT_RESULT((byte) 8),
  RPC_REQUEST((byte) 9),
  RPC_RESPONSE((byte) 10);

  private final byte value;

  FrameType(byte value) {
    this.value = value;
  }

  public byte value() {
    return value;
  }

  public boolean valid() {
    return value >= 1 && value <= 10;
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
      case 7:
        return ENDPOINT_INVOKE;
      case 8:
        return ENDPOINT_RESULT;
      case 9:
        return RPC_REQUEST;
      case 10:
        return RPC_RESPONSE;
      default:
        return UNKNOWN;
    }
  }
}
