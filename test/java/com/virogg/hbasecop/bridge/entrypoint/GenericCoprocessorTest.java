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
    assertEquals(GenericCoprocessor.DEFAULT_ENDPOINT_TIMEOUT, cfg.endpointTimeout());
    assertEquals(GenericCoprocessor.DEFAULT_GRACEFUL_SHUTDOWN, cfg.gracefulShutdownTimeout());
    assertFalse(cfg.allowMutate(), "reverse MUTATE must be off by default (TE41)");
    assertEquals(GenericCoprocessor.DEFAULT_MAX_CONCURRENT_CALLS, cfg.maxConcurrentCalls());
    assertEquals(GenericCoprocessor.DEFAULT_MAX_SCANNERS_PER_CALL, cfg.maxScannersPerCall());
    assertEquals(GenericCoprocessor.DEFAULT_MAX_BYTES_PER_RESP, cfg.maxBytesPerResp());
    assertEquals(GenericCoprocessor.DEFAULT_MAX_ROWS_PER_NEXT, cfg.maxRowsPerNext());
    assertEquals(GenericCoprocessor.DEFAULT_SCANNER_IDLE_LEASE, cfg.scannerIdleLease());
  }

  @Test
  void buildConfigHonoursTe42LimitOverrides(@TempDir Path tmp) {
    Configuration conf = new Configuration(false);
    conf.setInt(GenericCoprocessor.KEY_MAX_CONCURRENT_CALLS, 32);
    conf.setInt(GenericCoprocessor.KEY_MAX_SCANNERS_PER_CALL, 4);
    conf.setInt(GenericCoprocessor.KEY_MAX_BYTES_PER_RESP, 1 << 18);
    conf.setInt(GenericCoprocessor.KEY_MAX_ROWS_PER_NEXT, 50);
    conf.set(GenericCoprocessor.KEY_SCANNER_IDLE_LEASE, "30s");

    CoprocessorRuntime.Config cfg = GenericCoprocessor.buildConfig(conf, tmp);
    assertEquals(32, cfg.maxConcurrentCalls());
    assertEquals(4, cfg.maxScannersPerCall());
    assertEquals(1 << 18, cfg.maxBytesPerResp());
    assertEquals(50, cfg.maxRowsPerNext());
    assertEquals(Duration.ofSeconds(30), cfg.scannerIdleLease());
  }

  @Test
  void buildConfigHonoursAllowMutateOverride(@TempDir Path tmp) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(GenericCoprocessor.KEY_ALLOW_MUTATE, true);

    CoprocessorRuntime.Config cfg = GenericCoprocessor.buildConfig(conf, tmp);
    assertTrue(cfg.allowMutate());
  }

  @Test
  void buildConfigHonoursEndpointTimeoutOverride(@TempDir Path tmp) {
    Configuration conf = new Configuration(false);
    conf.set(GenericCoprocessor.KEY_ENDPOINT_TIMEOUT, "12s");
    conf.set(PolicyConfig.KEY_TIMEOUT_DEFAULT, "2s");

    CoprocessorRuntime.Config cfg = GenericCoprocessor.buildConfig(conf, tmp);
    assertEquals(Duration.ofSeconds(12), cfg.endpointTimeout());
    assertEquals(Duration.ofSeconds(2), cfg.hookTimeout());
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
    assertTrue(
        new GenericRegionObserver().getServices().iterator().next()
            instanceof GoEndpointServiceImpl);
    assertTrue(
        new GenericMasterObserver().getServices().iterator().next()
            instanceof GoEndpointServiceImpl);
  }
}
