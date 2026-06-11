// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit-level checks for the {@link GoProcessConfig} builder, focused on the {@code extraEnv}
 * channel that the T36 fault-injector uses to forward {@code HBASECOP_FAULT_MODE} to the spawned Go
 * process.
 */
final class GoProcessConfigTest {

  @Test
  void extraEnvDefaultsToEmpty() {
    GoProcessConfig cfg =
        GoProcessConfig.builder()
            .javaToGoFile(Path.of("/tmp/in"))
            .goToJavaFile(Path.of("/tmp/out"))
            .capacity(8)
            .maxObjectSize(1024)
            .heartbeatPeriodMs(-1)
            .build();
    assertTrue(cfg.extraEnv().isEmpty(), "default extraEnv must be empty");
  }

  @Test
  void extraEnvBuilderRoundTrip() {
    Map<String, String> env = new LinkedHashMap<>();
    env.put("HBASECOP_FAULT_MODE", "kill-9");
    env.put("HBASECOP_OBSERVER_TAG", "fault-1");

    GoProcessConfig cfg =
        GoProcessConfig.builder()
            .javaToGoFile(Path.of("/tmp/in"))
            .goToJavaFile(Path.of("/tmp/out"))
            .capacity(8)
            .maxObjectSize(1024)
            .heartbeatPeriodMs(-1)
            .extraEnv(env)
            .build();

    assertEquals(env, cfg.extraEnv(), "extraEnv must round-trip through the builder");
  }

  @Test
  void extraEnvIsDefensivelyCopied() {
    Map<String, String> env = new HashMap<>();
    env.put("HBASECOP_FAULT_MODE", "kill-9");

    GoProcessConfig cfg =
        GoProcessConfig.builder()
            .javaToGoFile(Path.of("/tmp/in"))
            .goToJavaFile(Path.of("/tmp/out"))
            .capacity(8)
            .maxObjectSize(1024)
            .heartbeatPeriodMs(-1)
            .extraEnv(env)
            .build();

    // Mutating the source after build() must not leak into the config.
    env.put("HBASECOP_FAULT_MODE", "exit-1");
    env.put("HBASECOP_OBSERVER_TAG", "after-build");

    assertEquals("kill-9", cfg.extraEnv().get("HBASECOP_FAULT_MODE"));
    assertEquals(1, cfg.extraEnv().size(), "post-build mutations must not leak in");
  }

  @Test
  void extraEnvViewIsUnmodifiable() {
    GoProcessConfig cfg =
        GoProcessConfig.builder()
            .javaToGoFile(Path.of("/tmp/in"))
            .goToJavaFile(Path.of("/tmp/out"))
            .capacity(8)
            .maxObjectSize(1024)
            .heartbeatPeriodMs(-1)
            .extraEnv(Map.of("HBASECOP_FAULT_MODE", "hang"))
            .build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> cfg.extraEnv().put("HBASECOP_OBSERVER_TAG", "x"),
        "extraEnv() must return an unmodifiable view");
  }

  @Test
  void nullExtraEnvResetsToEmpty() {
    GoProcessConfig.Builder b =
        GoProcessConfig.builder()
            .javaToGoFile(Path.of("/tmp/in"))
            .goToJavaFile(Path.of("/tmp/out"))
            .capacity(8)
            .maxObjectSize(1024)
            .heartbeatPeriodMs(-1)
            .extraEnv(Map.of("HBASECOP_FAULT_MODE", "hang"));

    b.extraEnv(null);
    GoProcessConfig cfg = b.build();
    assertTrue(cfg.extraEnv().isEmpty(), "extraEnv(null) must clear the builder map");
  }

  @Test
  void extraEnvRetainsLastValueOnDuplicateBuilderCalls() {
    GoProcessConfig cfg =
        GoProcessConfig.builder()
            .javaToGoFile(Path.of("/tmp/in"))
            .goToJavaFile(Path.of("/tmp/out"))
            .capacity(8)
            .maxObjectSize(1024)
            .heartbeatPeriodMs(-1)
            .extraEnv(Map.of("A", "1"))
            .extraEnv(Map.of("B", "2")) // replaces; not merged.
            .build();

    assertEquals(Map.of("B", "2"), cfg.extraEnv());
  }
}
