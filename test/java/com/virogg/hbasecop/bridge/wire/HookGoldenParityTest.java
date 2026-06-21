// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.virogg.hbasecop.bridge.wire.pb.HookContext;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PrePutRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class HookGoldenParityTest {

  private static final Path DIR = Paths.get("test", "golden", "hooks", "v1");

  private static byte[] golden(String name) throws IOException {
    return Files.readAllBytes(DIR.resolve(name + ".bin"));
  }

  @Test
  void hookContextMatchesGoGolden() throws IOException {
    byte[] g = golden("hook_context");
    assertArrayEquals(
        g, HookContext.parseFrom(g).toByteArray(), "Java re-encode must equal Go golden bytes");
  }

  @Test
  void prePutRequestMatchesGoGolden() throws IOException {
    byte[] g = golden("pre_put_request");
    assertArrayEquals(
        g, PrePutRequest.parseFrom(g).toByteArray(), "Java re-encode must equal Go golden bytes");
  }

  @Test
  void hookResponseBypassMatchesGoGolden() throws IOException {
    byte[] g = golden("hook_response_bypass");
    assertArrayEquals(
        g, HookResponse.parseFrom(g).toByteArray(), "Java re-encode must equal Go golden bytes");
  }

  @Test
  void hookResponseErrorMatchesGoGolden() throws IOException {
    byte[] g = golden("hook_response_error");
    assertArrayEquals(
        g, HookResponse.parseFrom(g).toByteArray(), "Java re-encode must equal Go golden bytes");
  }
}
