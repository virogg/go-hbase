// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public final class Decoder {

  private final Map<Long, Reassembly> pending = new HashMap<>();
  private final int maxFrame = WireFormat.MAX_FRAME_SIZE;

  private int pendingBytes;

  public Message decode(ByteBuffer src) throws WireException {
    while (true) {
      if (!src.hasRemaining()) {
        return null;
      }
      Message msg = readChunk(src);
      if (msg != null) {
        return msg;
      }
    }
  }

  private Message readChunk(ByteBuffer src) throws WireException {
    if (src.remaining() < 4) {
      throw new FrameTooLargeException("truncated length prefix");
    }
    int len = src.getInt();
    if (len < WireFormat.HEADER_SIZE || len > maxFrame - 4) {
      throw new FrameTooLargeException("frame length out of range: " + len);
    }
    if (src.remaining() < len) {
      throw new FrameTooLargeException(
          "truncated frame body: need " + len + " have " + src.remaining());
    }

    byte typeByte = src.get();
    FrameType type = FrameType.fromByte(typeByte);
    if (!type.valid()) {
      throw new UnknownTypeException("type=" + Byte.toUnsignedInt(typeByte));
    }
    long reqId = src.getLong();
    int regionId = src.getInt();
    byte hookId = src.get();
    src.get(); // chunk_flags reserved
    int chunkIdx = src.getInt();
    int chunkTotal = src.getInt();
    int payloadLen = len - WireFormat.HEADER_SIZE;
    byte[] payload = new byte[payloadLen];
    src.get(payload);

    if (chunkTotal <= 0 || chunkIdx < 0 || chunkIdx >= chunkTotal) {
      throw new InvalidChunkException("idx=" + chunkIdx + " total=" + chunkTotal);
    }
    if (chunkTotal > WireFormat.MAX_CHUNKS) {
      throw new TooManyChunksException(chunkTotal + " > " + WireFormat.MAX_CHUNKS);
    }

    if (chunkTotal == 1) {
      return new Message(type, reqId, regionId, hookId, payload);
    }

    if (type.isControl()) {
      throw new ControlMultiChunkException("type=" + type);
    }
    if (reqId == 0) {
      throw new InvalidChunkException("multi-chunk requires non-zero req_id");
    }

    Reassembly re = pending.get(reqId);
    if (re == null) {
      if (pending.size() >= WireFormat.MAX_PENDING_REASSEMBLIES) {
        throw new TooManyPendingException("pending reassemblies: " + pending.size());
      }
      re = new Reassembly(type, regionId, hookId, chunkTotal);
      pending.put(reqId, re);
    } else if (re.type != type
        || re.regionId != regionId
        || re.hookId != hookId
        || re.total != chunkTotal) {
      throw new InvalidChunkException("header drift on req_id " + reqId);
    }
    if (re.chunks[chunkIdx] != null) {
      throw new InvalidChunkException("duplicate chunk_idx " + chunkIdx + " for req_id " + reqId);
    }
    if (pendingBytes + payload.length > WireFormat.MAX_PENDING_BYTES) {
      throw new TooManyPendingBytesException(
          pendingBytes + " + " + payload.length + " > " + WireFormat.MAX_PENDING_BYTES);
    }
    re.chunks[chunkIdx] = payload;
    re.received++;
    re.size += payload.length;
    pendingBytes += payload.length;

    if (re.received < re.total) {
      return null;
    }

    byte[] out = new byte[re.size];
    int cursor = 0;
    for (byte[] c : re.chunks) {
      System.arraycopy(c, 0, out, cursor, c.length);
      cursor += c.length;
    }
    pending.remove(reqId);
    pendingBytes -= re.size;
    return new Message(re.type, reqId, re.regionId, re.hookId, out);
  }

  private static final class Reassembly {
    final FrameType type;
    final int regionId;
    final byte hookId;
    final int total;
    final byte[][] chunks;
    int received;
    int size;

    Reassembly(FrameType type, int regionId, byte hookId, int total) {
      this.type = type;
      this.regionId = regionId;
      this.hookId = hookId;
      this.total = total;
      this.chunks = new byte[total][];
    }
  }
}
