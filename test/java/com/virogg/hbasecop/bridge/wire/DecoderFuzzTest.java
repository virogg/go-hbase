// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import com.code_intelligence.jazzer.junit.FuzzTest;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Feeds arbitrary bytes through the framing decoder, mirroring Go's {@code FuzzDecode} (T83). The
 * decoder reads length-prefixed, chunked frames off an untrusted shmem ring, so it is the project's
 * primary adversarial-input surface. The invariant: {@code decode} must never throw anything but
 * {@link WireException} and never make an unbounded allocation - every malformed input must surface
 * as a checked decode error.
 *
 * <p>Seed corpus: the golden wire fixtures from {@code test/golden/wire/v1}, mapped to the {@code
 * DecoderFuzzTestInputs} classpath resource by the pom's testResources block.
 *
 * <p>Without {@code JAZZER_FUZZ=1} in the environment this replays only the seeds (regression mode,
 * runs in every {@code mvn verify}); with it, jazzer fuzzes for {@code maxDuration} - see {@code
 * make java-fuzz}.
 */
class DecoderFuzzTest {

  @FuzzTest(maxDuration = "10m")
  void decodeNeverThrowsUnchecked(byte[] data) {
    ByteBuffer src = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
    Decoder d = new Decoder();
    for (int i = 0; i < 64; i++) { // bounded: a fuzz input must not loop forever
      Message msg;
      try {
        msg = d.decode(src);
      } catch (WireException e) {
        return; // any decode error is acceptable; the contract is "no panic, no OOM"
      }
      if (msg == null) {
        return;
      }
    }
  }
}
