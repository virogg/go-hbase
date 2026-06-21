// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.shmem.Channel;
import com.virogg.hbasecop.bridge.shmem.Config;
import com.virogg.hbasecop.bridge.shmem.Role;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class GoProcessChecksumTest {

  private static final String SHA256_EMPTY =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  private static final String SHA256_ABC =
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
  private static final int CAPACITY = 16;
  private static final int MAX_OBJECT_SIZE = 4096;

  @Test
  void computeSha256_emptyFile(@TempDir Path dir) throws IOException {
    Path f = dir.resolve("empty");
    Files.write(f, new byte[0]);
    assertEquals(SHA256_EMPTY, GoProcess.computeSha256Hex(f));
  }

  @Test
  void computeSha256_abc(@TempDir Path dir) throws IOException {
    Path f = dir.resolve("abc");
    Files.write(f, "abc".getBytes(StandardCharsets.UTF_8));
    assertEquals(SHA256_ABC, GoProcess.computeSha256Hex(f));
  }

  @Test
  void verifyChecksum_match(@TempDir Path dir) throws IOException {
    Path f = dir.resolve("abc");
    Files.write(f, "abc".getBytes(StandardCharsets.UTF_8));
    assertDoesNotThrow(() -> GoProcess.verifyChecksum(f, SHA256_ABC));
    assertDoesNotThrow(() -> GoProcess.verifyChecksum(f, SHA256_ABC.toUpperCase()));
  }

  @Test
  void verifyChecksum_mismatch_namesBothDigestsInMessage(@TempDir Path dir) throws IOException {
    Path f = dir.resolve("abc");
    Files.write(f, "abc".getBytes(StandardCharsets.UTF_8));
    String wrong = "0".repeat(64);
    IOException ex = assertThrows(IOException.class, () -> GoProcess.verifyChecksum(f, wrong));
    String msg = ex.getMessage();
    assertTrue(msg.contains(wrong), "message must include expected digest");
    assertTrue(msg.contains(SHA256_ABC), "message must include actual digest");
  }

  @Test
  void start_wrongExpectedSha_throwsBeforeExec(@TempDir Path tmp) throws Exception {
    Path inFile = tmp.resolve("in.mmap");
    Path outFile = tmp.resolve("out.mmap");

    try (Channel javaToGo = openChannel(inFile, Role.PRODUCER);
        Channel goToJava = openChannel(outFile, Role.CONSUMER)) {

      String wrong = "f".repeat(64);
      GoProcessConfig cfg =
          GoProcessConfig.builder()
              .binaryResourcePath("bin/linux-amd64/hbasecop-runtime")
              .javaToGoFile(inFile)
              .goToJavaFile(outFile)
              .capacity(CAPACITY)
              .maxObjectSize(MAX_OBJECT_SIZE)
              .heartbeatPeriodMs(-1)
              .expectedBinarySha256(wrong)
              .build();

      try (GoProcess go = new GoProcess(cfg, javaToGo)) {
        IOException ex = assertThrows(IOException.class, go::start);
        assertTrue(
            ex.getMessage().contains(wrong) || ex.getMessage().contains("SHA-256"),
            "fail-fast IOException should reference checksum mismatch (got: "
                + ex.getMessage()
                + ")");
        assertFalse(go.isAlive(), "Go process must not have spawned on bad checksum");
      }
      assertTrue(goToJava != null);
    }
  }

  private Channel openChannel(Path file, Role role) {
    return Channel.open(
        Config.builder()
            .filename(file.toString())
            .capacity(CAPACITY)
            .maxObjectSize(MAX_OBJECT_SIZE)
            .role(role)
            .build());
  }
}
