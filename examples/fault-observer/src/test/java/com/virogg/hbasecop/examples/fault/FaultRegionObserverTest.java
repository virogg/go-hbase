// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.fault;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Unit-level checks for the {@code hbasecop.fault.mode} → {@code HBASECOP_FAULT_MODE} mapping.
 * Driving a real {@link FaultRegionObserver} through its RegionCoprocessor lifecycle requires a
 * live RegionServer; here we exercise the pure {@code envFromConfig} helper that the lifecycle
 * code calls when populating {@code CoprocessorRuntime.Config.extraEnv}.
 */
final class FaultRegionObserverTest {

  @Test
  void emptyConfigYieldsEmptyEnv() {
    Map<String, String> env = FaultRegionObserver.envFromConfig(new Configuration(false));
    assertNotNull(env);
    assertTrue(env.isEmpty(), "missing fault-mode key must produce empty env map");
  }

  @Test
  void faultModeKeyMapsToEnvVar() {
    Configuration conf = new Configuration(false);
    conf.set(FaultRegionObserver.KEY_FAULT_MODE, "kill-9");
    Map<String, String> env = FaultRegionObserver.envFromConfig(conf);
    assertEquals(Map.of("HBASECOP_FAULT_MODE", "kill-9"), env);
  }

  @Test
  void everyDocumentedModeIsAccepted() {
    for (String mode :
        new String[] {"none", "kill-9", "hang", "exit-1", "protocol-error", "oom"}) {
      Configuration conf = new Configuration(false);
      conf.set(FaultRegionObserver.KEY_FAULT_MODE, mode);
      assertEquals(
          Map.of("HBASECOP_FAULT_MODE", mode),
          FaultRegionObserver.envFromConfig(conf),
          "mode=" + mode);
    }
  }

  @Test
  void blankModeTreatedAsUnset() {
    Configuration conf = new Configuration(false);
    conf.set(FaultRegionObserver.KEY_FAULT_MODE, "   ");
    assertTrue(
        FaultRegionObserver.envFromConfig(conf).isEmpty(),
        "blank fault-mode value must not propagate to env");
  }

  @Test
  void nullConfigurationRejected() {
    assertThrows(NullPointerException.class, () -> FaultRegionObserver.envFromConfig(null));
  }
}
