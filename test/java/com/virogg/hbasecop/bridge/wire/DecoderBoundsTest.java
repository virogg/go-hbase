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

/**
 * Pins the H2/H4 allocation bounds on the Java decoder, mirroring {@code
 * internal/wire/bounds_test.go}: a hostile peer must not be able to OOM the RegionServer JVM with a
 * tiny frame declaring a huge {@code chunk_total}, nor by abandoning unboundedly many multi-chunk
 * reassemblies.
 */
class DecoderBoundsTest {

  @Test
  void rejectsHugeChunkTotalBeforeAllocating() {
    // 0xFFFFFFFF reads as a negative int and is caught by the idx/total sanity
    // check; the values that matter here are the large POSITIVE totals that
    // would previously reach `new byte[total][]`.
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
    // Open MAX_PENDING_REASSEMBLIES distinct multi-chunk messages, each
    // sending only chunk 0 of 2 so none ever completes.
    for (long reqId = 1; reqId <= WireFormat.MAX_PENDING_REASSEMBLIES; reqId++) {
      ByteBuffer buf = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
      writeRaw(buf, (byte) 1, reqId, 0, (byte) 7, 0, 2, new byte[] {'a'});
      buf.flip();
      assertNull(d.decode(buf), "incomplete reassembly must yield null at req " + reqId);
    }
    // One more distinct req_id must be rejected rather than grow the map.
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
    // Each frame opens a distinct 2-chunk reassembly holding one max-size
    // chunk that never completes. The retained-bytes cap must fire long
    // before MAX_PENDING_REASSEMBLIES entries exist — the entry cap alone
    // would permit ~256 GiB of near-complete reassemblies.
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
    // Completing an ALREADY-OPEN reassembly at the cap must still succeed.
    ByteBuffer fin = ByteBuffer.allocate(64).order(ByteOrder.BIG_ENDIAN);
    writeRaw(fin, (byte) 1, 1L, 0, (byte) 7, 1, 2, new byte[] {'b'});
    fin.flip();
    Message msg = d.decode(fin);
    assertNotNull(msg, "final chunk of an open reassembly must complete despite the cap");
    assertEquals(1L, msg.reqId());
  }

  // Hand-crafts a chunk frame (length-prefixed) with caller-chosen header
  // fields, mirroring WireRoundTripTest.writeRaw / Go's craftHeader.
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
    buf.put((byte) 0); // chunk_flags reserved
    buf.putInt(chunkIdx);
    buf.putInt(chunkTotal);
    buf.put(payload);
  }
}
