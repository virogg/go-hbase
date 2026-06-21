// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.counter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import org.junit.jupiter.api.Test;

final class CounterCoprocJarTest {

  private static final String RESOURCE_PATH = "bin/linux-amd64/hbasecop-runtime";

  private static final byte[] ELF_MAGIC = new byte[] {0x7F, 'E', 'L', 'F'};

  @Test
  void embeddedGoBinaryIsLinuxElf() throws Exception {
    try (InputStream in =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE_PATH)) {
      assertNotNull(in, "embedded Go binary missing at classpath: " + RESOURCE_PATH);
      byte[] head = in.readNBytes(ELF_MAGIC.length);
      assertArrayEquals(ELF_MAGIC, head, "expected ELF magic at start of " + RESOURCE_PATH);
      assertTrue(in.available() >= 0, "binary must be non-empty");
    }
  }
}
