// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Pins the producer-side MAX_CHUNKS cap on the Java encoder, mirroring {@code
 * TestEncodeRejectsMessageExceedingMaxChunks} in {@code internal/wire/encoder_cap_test.go}: a
 * payload that would split into more than {@link WireFormat#MAX_CHUNKS} chunks is rejected with
 * {@link MessageTooLargeException} rather than emitting a frame stream the matching decoder must
 * reject.
 */
class EncoderBoundsTest {

  @Test
  void rejectsMessageExceedingMaxChunks() {
    Encoder enc = new Encoder(1); // 1 byte per chunk
    byte[] payload = new byte[WireFormat.MAX_CHUNKS + 1]; // needs MAX_CHUNKS+1 chunks
    Message m = new Message(FrameType.REQUEST, 1L, 0, (byte) 0, payload);

    assertThrows(MessageTooLargeException.class, () -> enc.encode(m));
  }

  @Test
  void allowsExactlyMaxChunks() throws WireException {
    Encoder enc = new Encoder(1);
    byte[] payload = new byte[WireFormat.MAX_CHUNKS];
    Message m = new Message(FrameType.REQUEST, 1L, 0, (byte) 0, payload);

    // Encodes, and round-trips back to the same payload length.
    Message decoded = new Decoder().decode(enc.encode(m));
    assertEquals(WireFormat.MAX_CHUNKS, decoded.payload().length);
  }
}
