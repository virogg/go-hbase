// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Serializes a {@link Message} into a single {@link ByteBuffer} containing one or more chunk frames
 * back-to-back. Mirrors {@code internal/wire/encoder.go}.
 *
 * <p>The Encoder is stateless; a single instance may be reused across threads only if callers
 * supply distinct destination buffers per call (the encoder itself does not retain state).
 */
public final class Encoder {

  private final int maxPayload;

  /** Default Encoder using {@link WireFormat#MAX_PAYLOAD_BYTES} per chunk. */
  public Encoder() {
    this(WireFormat.MAX_PAYLOAD_BYTES);
  }

  /**
   * Encoder that splits payloads into chunks of at most {@code chunkSize} bytes. Used by tests to
   * exercise the chunking path without 64 KiB allocations; production code should use the
   * no-argument constructor.
   */
  public Encoder(int chunkSize) {
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be > 0: " + chunkSize);
    }
    this.maxPayload = chunkSize;
  }

  /**
   * Encode {@code m} into a fresh ByteBuffer (already flipped, ready to read). For multi-chunk
   * messages the buffer contains every chunk in chunk_idx order.
   */
  public ByteBuffer encode(Message m) throws WireException {
    if (!m.type().valid()) {
      throw new UnknownTypeException("type=" + m.type());
    }

    byte[] payload = m.payload();
    int total = 1;
    if (payload.length > maxPayload) {
      if (m.type().isControl()) {
        throw new ControlMultiChunkException(
            "control payload " + payload.length + " > maxPayload " + maxPayload);
      }
      total = (payload.length + maxPayload - 1) / maxPayload;
      // Refuse to emit a frame stream the matching Decoder would reject
      // (chunk_total > MAX_CHUNKS). Fail at the producer with the payload
      // size in hand rather than producing self-undecodable output; this
      // also keeps the totalBytes computation below from overflowing int.
      if (total > WireFormat.MAX_CHUNKS) {
        throw new MessageTooLargeException(
            "payload "
                + payload.length
                + " would need "
                + total
                + " chunks > MAX_CHUNKS "
                + WireFormat.MAX_CHUNKS);
      }
    }

    int totalBytes = total * (4 + WireFormat.HEADER_SIZE) + payload.length;
    ByteBuffer buf = ByteBuffer.allocate(totalBytes).order(ByteOrder.BIG_ENDIAN);

    for (int i = 0; i < total; i++) {
      int start = i * maxPayload;
      int end = Math.min(start + maxPayload, payload.length);
      writeChunk(buf, m, i, total, payload, start, end);
    }

    buf.flip();
    return buf;
  }

  private static void writeChunk(
      ByteBuffer buf, Message m, int idx, int total, byte[] payload, int start, int end) {
    int payloadLen = end - start;
    buf.putInt(WireFormat.HEADER_SIZE + payloadLen);
    buf.put(m.type().value());
    buf.putLong(m.reqId());
    buf.putInt(m.regionId());
    buf.put(m.hookId());
    buf.put((byte) 0); // chunk_flags reserved
    buf.putInt(idx);
    buf.putInt(total);
    if (payloadLen > 0) {
      buf.put(payload, start, payloadLen);
    }
  }
}
