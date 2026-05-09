package com.virogg.hbasecop;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Sanity check that the Maven build pipeline (compile + Surefire + JaCoCo + Spotless) is wired up.
 * Real bridge tests land starting in T23.
 */
class BuildSmokeTest {

  @Test
  void buildPipelineAlive() {
    assertTrue(true, "build pipeline alive");
  }
}
