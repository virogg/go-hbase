// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import java.util.Arrays;
import java.util.Objects;

public final class Message {
  private final FrameType type;
  private final long reqId;
  private final int regionId;
  private final byte hookId;
  private final byte[] payload;

  public Message(FrameType type, long reqId, int regionId, byte hookId, byte[] payload) {
    this.type = Objects.requireNonNull(type, "type");
    this.reqId = reqId;
    this.regionId = regionId;
    this.hookId = hookId;
    this.payload = payload == null ? new byte[0] : payload;
  }

  public FrameType type() {
    return type;
  }

  public long reqId() {
    return reqId;
  }

  public int regionId() {
    return regionId;
  }

  public byte hookId() {
    return hookId;
  }

  public byte[] payload() {
    return payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Message)) {
      return false;
    }
    Message other = (Message) o;
    return type == other.type
        && reqId == other.reqId
        && regionId == other.regionId
        && hookId == other.hookId
        && Arrays.equals(payload, other.payload);
  }

  @Override
  public int hashCode() {
    int h = Objects.hash(type, reqId, regionId, hookId);
    return 31 * h + Arrays.hashCode(payload);
  }

  @Override
  public String toString() {
    return "Message{type="
        + type
        + ", reqId="
        + reqId
        + ", regionId="
        + regionId
        + ", hookId="
        + hookId
        + ", payload.len="
        + payload.length
        + '}';
  }
}
