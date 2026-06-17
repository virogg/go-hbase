// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PrePrepareBulkLoadRequest;
import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * T54 Wave B unit test for {@link BulkLoadObserverAdapter}: verifies the adapter encodes the
 * bulk-load target table/region into HookContext, drives the injected {@link HookDispatcher}, and
 * translates {@code bypass=true} / strict-mode error responses into the matching {@code
 * ObserverContext#bypass()} / {@code IOException} reactions - mirrors the master adapter's coverage
 * on the bulk-load surface.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BulkLoadObserverAdapterTest {

  @Mock private HookDispatcher dispatcher;
  @Mock private ObserverContext<RegionCoprocessorEnvironment> ctx;
  @Mock private RegionCoprocessorEnvironment env;

  private BulkLoadObserverAdapter adapter;

  @BeforeEach
  void setUp() {
    adapter = new BulkLoadObserverAdapter(dispatcher, new PolicyConfig(new Configuration(false)));
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.valueOf("default", "ingest")).build();
    when(env.getRegionInfo()).thenReturn(ri);
  }

  @Test
  void prePrepareBulkLoad_encodesTableNameAndDispatches()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(
            anyInt(), eq(HookId.PRE_PREPARE_BULK_LOAD.value()), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.prePrepareBulkLoad(ctx);

    ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
    verify(dispatcher, times(1))
        .dispatchHook(
            anyInt(),
            eq(HookId.PRE_PREPARE_BULK_LOAD.value()),
            payload.capture(),
            any(Duration.class));
    PrePrepareBulkLoadRequest req = parsePrepare(payload.getValue());
    assertArrayEquals(
        "default".getBytes(),
        req.getCtx().getTableName().getNamespace().toByteArray(),
        "namespace not round-tripped");
    assertArrayEquals(
        "ingest".getBytes(),
        req.getCtx().getTableName().getQualifier().toByteArray(),
        "qualifier not round-tripped");
    verify(ctx, times(0)).bypass();
  }

  @Test
  void prePrepareBulkLoad_bypassPropagates()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().setBypass(true).build().toByteArray());

    adapter.prePrepareBulkLoad(ctx);

    verify(ctx, times(1)).bypass();
  }

  @Test
  void prePrepareBulkLoad_strictErrorResponseRaisesIOException()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.prePrepareBulkLoad", "strict");
    adapter = new BulkLoadObserverAdapter(dispatcher, new PolicyConfig(conf));

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any(Duration.class)))
        .thenReturn(
            HookResponse.newBuilder()
                .setError(HookError.newBuilder().setCode(7).setMessage("bulk load vetoed"))
                .build()
                .toByteArray());

    IOException thrown = assertThrows(IOException.class, () -> adapter.prePrepareBulkLoad(ctx));
    assertTrue(
        thrown.getMessage().contains("bulk load vetoed"),
        "strict failure must surface the observer message; got: " + thrown.getMessage());
    verify(ctx, times(0)).bypass();
  }

  @Test
  void preCleanupBulkLoad_dispatches()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(
            anyInt(), eq(HookId.PRE_CLEANUP_BULK_LOAD.value()), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.preCleanupBulkLoad(ctx);

    verify(dispatcher, times(1))
        .dispatchHook(
            anyInt(), eq(HookId.PRE_CLEANUP_BULK_LOAD.value()), any(), any(Duration.class));
  }

  // --- helpers -------------------------------------------------------------

  private static PrePrepareBulkLoadRequest parsePrepare(byte[] bytes) {
    try {
      return PrePrepareBulkLoadRequest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError("parse PrePrepareBulkLoadRequest", e);
    }
  }
}
