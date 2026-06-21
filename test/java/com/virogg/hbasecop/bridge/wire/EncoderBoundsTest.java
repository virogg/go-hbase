// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class EncoderBoundsTest {

  @Test
  void rejectsMessageExceedingMaxChunks() {
    Encoder enc = new Encoder(1);
    byte[] payload = new byte[WireFormat.MAX_CHUNKS + 1];
    Message m = new Message(FrameType.REQUEST, 1L, 0, (byte) 0, payload);

    assertThrows(MessageTooLargeException.class, () -> enc.encode(m));
  }

  @Test
  void allowsExactlyMaxChunks() throws WireException {
    Encoder enc = new Encoder(1);
    byte[] payload = new byte[WireFormat.MAX_CHUNKS];
    Message m = new Message(FrameType.REQUEST, 1L, 0, (byte) 0, payload);

    Message decoded = new Decoder().decode(enc.encode(m));
    assertEquals(WireFormat.MAX_CHUNKS, decoded.payload().length);
  }
}
