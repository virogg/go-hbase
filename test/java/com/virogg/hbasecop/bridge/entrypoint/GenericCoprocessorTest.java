// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.entrypoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.CoprocessorRuntime;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.endpoint.GoEndpointServiceImpl;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class GenericCoprocessorTest {

  @Test
  void buildConfigUsesDocumentedDefaults(@TempDir Path tmp) {
    CoprocessorRuntime.Config cfg = GenericCoprocessor.buildConfig(new Configuration(false), tmp);
    assertEquals(GenericCoprocessor.DEFAULT_RING_CAPACITY, cfg.ringCapacity());
    assertEquals(GenericCoprocessor.DEFAULT_RING_MAX_OBJECT_SIZE, cfg.ringMaxObjectSize());
    assertEquals(GenericCoprocessor.DEFAULT_HOOK_TIMEOUT, cfg.hookTimeout());
    assertEquals(GenericCoprocessor.DEFAULT_GRACEFUL_SHUTDOWN, cfg.gracefulShutdownTimeout());
  }

  @Test
  void buildConfigHonoursConfigurationOverrides(@TempDir Path tmp) {
    Configuration conf = new Configuration(false);
    conf.setInt(GenericCoprocessor.KEY_RING_CAPACITY, 32);
    conf.setInt(GenericCoprocessor.KEY_RING_MAX_OBJECT_SIZE, 1 << 21);
    conf.set(PolicyConfig.KEY_TIMEOUT_DEFAULT, "2s");
    conf.set(GenericCoprocessor.KEY_GRACEFUL_SHUTDOWN, "3s");

    CoprocessorRuntime.Config cfg = GenericCoprocessor.buildConfig(conf, tmp);
    assertEquals(32, cfg.ringCapacity());
    assertEquals(1 << 21, cfg.ringMaxObjectSize());
    assertEquals(Duration.ofSeconds(2), cfg.hookTimeout());
    assertEquals(Duration.ofSeconds(3), cfg.gracefulShutdownTimeout());
  }

  @Test
  void sharedKeyFallsBackToClassNameWithoutCoprocJar() {
    // The bridge test classpath carries no coproc-jar manifest, so the key is the fallback.
    assertEquals("fallback-key", GenericCoprocessor.sharedKey("fallback-key"));
  }

  @Test
  void endpointServicesExposesOneGoEndpointService() {
    var services = GenericCoprocessor.endpointServices(() -> null);
    var it = services.iterator();
    assertTrue(it.hasNext(), "one endpoint service expected");
    assertTrue(it.next() instanceof GoEndpointServiceImpl);
    assertFalse(it.hasNext(), "exactly one endpoint service");
  }

  @Test
  void regionAndMasterEntrypointsRegisterTheEndpointService() {
    // getServices() is independent of start(): the service registers without a running handle.
    assertTrue(
        new GenericRegionObserver().getServices().iterator().next()
            instanceof GoEndpointServiceImpl);
    assertTrue(
        new GenericMasterObserver().getServices().iterator().next()
            instanceof GoEndpointServiceImpl);
  }
}
