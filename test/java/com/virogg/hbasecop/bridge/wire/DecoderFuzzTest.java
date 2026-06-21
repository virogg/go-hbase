// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import com.code_intelligence.jazzer.junit.FuzzTest;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class DecoderFuzzTest {

  @FuzzTest(maxDuration = "10m")
  void decodeNeverThrowsUnchecked(byte[] data) {
    ByteBuffer src = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    Decoder d = new Decoder();
    for (int i = 0; i < 64; i++) {
      Message msg;
      try {
        msg = d.decode(src);
      } catch (WireException e) {
        return;
      }
      if (msg == null) {
        return;
      }
    }
  }
}
