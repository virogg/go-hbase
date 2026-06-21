// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

class DecoderBoundsTest {

  @Test
  void rejectsHugeChunkTotalBeforeAllocating() {
    for (int total : new int[] {WireFormat.MAX_CHUNKS + 1, 1 << 20, Integer.MAX_VALUE}) {
      ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
      writeRaw(buf, (byte) 1, 1L, 0, (byte) 7, 0, total, new byte[] {'x'});
      buf.flip();
      Decoder d = new Decoder();
      TooManyChunksException e = assertThrows(TooManyChunksException.class, () -> d.decode(buf));
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void capsPendingReassemblies() throws WireException {
    Decoder d = new Decoder();
    for (long reqId = 1; reqId <= WireFormat.MAX_PENDING_REASSEMBLIES; reqId++) {
      ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
      writeRaw(buf, (byte) 1, reqId, 0, (byte) 7, 0, 2, new byte[] {'a'});
      buf.flip();
      assertNull(d.decode(buf), "incomplete reassembly must yield null at req " + reqId);
    }
    ByteBuffer over = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    writeRaw(
        over,
        (byte) 1,
        WireFormat.MAX_PENDING_REASSEMBLIES + 1L,
        0,
        (byte) 7,
        0,
        2,
        new byte[] {'a'});
    over.flip();
    assertThrows(TooManyPendingException.class, () -> d.decode(over));
  }

  @Test
  void capsPendingBytesBeforeEntryCap() throws WireException {
    int framesToCap = WireFormat.MAX_PENDING_BYTES / WireFormat.MAX_PAYLOAD_BYTES + 2;
    assertTrue(
        framesToCap < WireFormat.MAX_PENDING_REASSEMBLIES,
        "test premise broken: byte cap must be reachable below the entry cap");
    byte[] payload = new byte[WireFormat.MAX_PAYLOAD_BYTES];
    Decoder d = new Decoder();
    TooManyPendingBytesException thrown = null;
    for (long reqId = 1; reqId <= framesToCap; reqId++) {
      ByteBuffer buf = ByteBuffer.allocate(WireFormat.MAX_FRAME_SIZE).order(ByteOrder.BIG_ENDIAN);
      writeRaw(buf, (byte) 1, reqId, 0, (byte) 7, 0, 2, payload);
      buf.flip();
      try {
        assertNull(d.decode(buf));
      } catch (TooManyPendingBytesException e) {
        thrown = e;
        break;
      }
    }
    assertNotNull(
        thrown, "expected TooManyPendingBytesException before " + framesToCap + " frames");
  }

  @Test
  void pendingCapDoesNotBlockExistingReassembly() throws WireException {
    Decoder d = new Decoder();
    for (long reqId = 1; reqId <= WireFormat.MAX_PENDING_REASSEMBLIES; reqId++) {
      ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
      writeRaw(buf, (byte) 1, reqId, 0, (byte) 7, 0, 2, new byte[] {'a'});
      buf.flip();
      d.decode(buf);
    }
    ByteBuffer fin = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    writeRaw(fin, (byte) 1, 1L, 0, (byte) 7, 1, 2, new byte[] {'b'});
    fin.flip();
    Message msg = d.decode(fin);
    assertNotNull(msg, "final chunk of an open reassembly must complete despite the cap");
    assertEquals(1L, msg.reqId());
  }

  private static void writeRaw(
      ByteBuffer buf,
      byte type,
      long reqId,
      int regionId,
      byte hookId,
      int chunkIdx,
      int chunkTotal,
      byte[] payload) {
    int len = WireFormat.HEADER_SIZE + payload.length;
    buf.putInt(len);
    buf.put(type);
    buf.putLong(reqId);
    buf.putInt(regionId);
    buf.put(hookId);
    buf.put((byte) 0);
    buf.putInt(chunkIdx);
    buf.putInt(chunkTotal);
    buf.put(payload);
  }
}
