// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PreStopRegionServerRequest;
import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * T52 Wave B unit test for {@link RegionServerObserverAdapter}: verifies the adapter encodes the
 * RegionServer's {@link ServerName} into the right proto Request, drives the injected {@link
 * HookDispatcher}, and translates {@code bypass=true} / strict-mode error responses into the
 * matching {@code ObserverContext#bypass()} / {@code IOException} reactions — mirrors the master
 * adapter's coverage on the region-server surface.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RegionServerObserverAdapterTest {

  @Mock private HookDispatcher dispatcher;
  @Mock private ObserverContext<RegionServerCoprocessorEnvironment> ctx;
  @Mock private RegionServerCoprocessorEnvironment env;

  private RegionServerObserverAdapter adapter;

  @BeforeEach
  void setUp() {
    adapter =
        new RegionServerObserverAdapter(dispatcher, new PolicyConfig(new Configuration(false)));
    when(ctx.getEnvironment()).thenReturn(env);
    when(env.getServerName())
        .thenReturn(ServerName.valueOf("rs-7.example.com", 16020, 1700000000000L));
  }

  @Test
  void preStopRegionServer_encodesServerNameAndDispatches()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(
            anyInt(), eq(HookId.PRE_STOP_REGION_SERVER.value()), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.preStopRegionServer(ctx);

    ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
    verify(dispatcher, times(1))
        .dispatchHook(
            anyInt(),
            eq(HookId.PRE_STOP_REGION_SERVER.value()),
            payload.capture(),
            any(Duration.class));
    PreStopRegionServerRequest req = parseStop(payload.getValue());
    assertEquals("rs-7.example.com", req.getServer().getHost(), "host not round-tripped");
    assertEquals(16020, req.getServer().getPort(), "port not round-tripped");
    assertEquals(1700000000000L, req.getServer().getStartCode(), "start code not round-tripped");
    verify(ctx, times(0)).bypass();
  }

  @Test
  void preStopRegionServer_bypassPropagates()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().setBypass(true).build().toByteArray());

    adapter.preStopRegionServer(ctx);

    verify(ctx, times(1)).bypass();
  }

  @Test
  void preStopRegionServer_strictErrorResponseRaisesIOException()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.preStopRegionServer", "strict");
    adapter = new RegionServerObserverAdapter(dispatcher, new PolicyConfig(conf));

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any(Duration.class)))
        .thenReturn(
            HookResponse.newBuilder()
                .setError(HookError.newBuilder().setCode(7).setMessage("stop vetoed"))
                .build()
                .toByteArray());

    IOException thrown = assertThrows(IOException.class, () -> adapter.preStopRegionServer(ctx));
    assertTrue(
        thrown.getMessage().contains("stop vetoed"),
        "strict failure must surface the observer message; got: " + thrown.getMessage());
    verify(ctx, times(0)).bypass();
  }

  @Test
  void postExecuteProcedures_dispatches()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(
            anyInt(), eq(HookId.POST_EXECUTE_PROCEDURES.value()), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.postExecuteProcedures(ctx);

    verify(dispatcher, times(1))
        .dispatchHook(
            anyInt(), eq(HookId.POST_EXECUTE_PROCEDURES.value()), any(), any(Duration.class));
  }

  // --- helpers -------------------------------------------------------------

  private static PreStopRegionServerRequest parseStop(byte[] bytes) {
    try {
      return PreStopRegionServerRequest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError("parse PreStopRegionServerRequest", e);
    }
  }
}
