// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PrePutRequest;
import com.virogg.hbasecop.hbase.v1.ClientProtos.MutationProto;
import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * T23 + T32 acceptance — verifies that the Java {@code RegionObserverAdapter} converts an HBase
 * {@code Put} into a {@code PrePutRequest} protobuf and dispatches it through the injected {@link
 * HookDispatcher}, that {@code bypass=true} and observer-error response branches map to {@code
 * ObserverContext#bypass()} and {@code IOException} respectively when the resolved policy is
 * strict, and that best-effort policy swallows Go-side failures (error response, timeout, transport
 * error) as no-ops with a WARN log instead of propagating an IOException.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RegionObserverAdapterTest {

  @Mock private HookDispatcher dispatcher;
  @Mock private ObserverContext<RegionCoprocessorEnvironment> ctx;
  @Mock private RegionCoprocessorEnvironment env;
  @Mock private Region region;
  @Mock private RegionInfo regionInfo;
  @Mock private WALEdit walEdit;

  private RegionObserverAdapter adapter;

  @BeforeEach
  void setUp() {
    when(ctx.getEnvironment()).thenReturn(env);
    when(env.getRegion()).thenReturn(region);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    when(regionInfo.getTable()).thenReturn(TableName.valueOf("default", "users"));
    when(regionInfo.getEncodedNameAsBytes()).thenReturn("abc1234".getBytes());

    adapter = new RegionObserverAdapter(dispatcher, new PolicyConfig(new Configuration(false)));
  }

  private static Put samplePut() {
    Put p = new Put("row-7".getBytes());
    p.addColumn("cf".getBytes(), "q1".getBytes(), 1_700_000_000_000L, "hello".getBytes());
    return p;
  }

  @Test
  void prePutSerializesMutationAndDispatchesAsHook1() throws Exception {
    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    ArgumentCaptor<byte[]> bytesCap = ArgumentCaptor.forClass(byte[].class);
    verify(dispatcher)
        .dispatchHook(
            eq(RegionObserverAdapter.HOOK_PRE_PUT), bytesCap.capture(), eq(Duration.ofSeconds(5)));

    PrePutRequest decoded = PrePutRequest.parseFrom(bytesCap.getValue());
    assertEquals("row-7", decoded.getMutation().getRow().toStringUtf8());
    assertEquals(MutationProto.MutationType.PUT, decoded.getMutation().getMutateType());
    assertEquals(1, decoded.getMutation().getColumnValueCount());
    assertEquals("cf", decoded.getMutation().getColumnValue(0).getFamily().toStringUtf8());
    assertEquals(
        "q1",
        decoded.getMutation().getColumnValue(0).getQualifierValue(0).getQualifier().toStringUtf8());
    assertEquals(
        "hello",
        decoded.getMutation().getColumnValue(0).getQualifierValue(0).getValue().toStringUtf8());
    assertEquals(
        1_700_000_000_000L,
        decoded.getMutation().getColumnValue(0).getQualifierValue(0).getTimestamp());

    // HookContext carries the table+region scope.
    assertEquals("default", decoded.getCtx().getTableName().getNamespace().toStringUtf8());
    assertEquals("users", decoded.getCtx().getTableName().getQualifier().toStringUtf8());
    assertEquals("abc1234", decoded.getCtx().getRegionName().toStringUtf8());

    // No bypass / no exception on a clean response.
    verify(ctx, never()).bypass();
  }

  @Test
  void prePutBypassResponseTriggersObserverBypass() throws Exception {
    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().setBypass(true).build().toByteArray());

    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(ctx, times(1)).bypass();
  }

  @Test
  void prePutErrorResponseThrowsIOExceptionUnderStrictDefault() throws Exception {
    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenReturn(
            HookResponse.newBuilder()
                .setError(HookError.newBuilder().setCode(7).setMessage("policy rejected"))
                .build()
                .toByteArray());

    IOException ex =
        assertThrows(
            IOException.class,
            () -> adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT));
    assertTrue(ex.getMessage().contains("policy rejected"));
    verify(ctx, never()).bypass();
  }

  @Test
  void postPutDispatchesAsHook2() throws Exception {
    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.postPut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(dispatcher).dispatchHook(eq(RegionObserverAdapter.HOOK_POST_PUT), any(), any());
  }

  @Test
  void prePutTimeoutMapsToIOExceptionUnderStrictDefault() throws Exception {
    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenThrow(new java.util.concurrent.TimeoutException("ring stalled"));

    IOException ex =
        assertThrows(
            IOException.class,
            () -> adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT));
    assertTrue(ex.getMessage().contains("timeout"), () -> "msg=" + ex.getMessage());
    assertFalse(
        Thread.currentThread().isInterrupted(), "timeout must not leave thread interrupted");
  }

  @Test
  void prePutInterruptPropagatesAndPreservesInterruptFlag() throws Exception {
    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenThrow(new InterruptedException("supervisor stopped"));

    IOException ex =
        assertThrows(
            IOException.class,
            () -> adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT));
    assertTrue(ex.getMessage().toLowerCase().contains("interrupt"));
    assertTrue(Thread.currentThread().isInterrupted(), "interrupt flag must be re-set");
    // Reset so subsequent tests are not affected.
    Thread.interrupted();
  }

  // ---------- T32: best-effort policy swallows Go-side failures ----------

  @Test
  void postPutErrorResponseSwallowedUnderDefaultBestEffort() throws Exception {
    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenReturn(
            HookResponse.newBuilder()
                .setError(HookError.newBuilder().setCode(7).setMessage("go rejected"))
                .build()
                .toByteArray());

    // Default policy for post* is best-effort → no IOException, operation continues.
    adapter.postPut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(ctx, never()).bypass();
  }

  @Test
  void postPutTimeoutSwallowedUnderDefaultBestEffort() throws Exception {
    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenThrow(new java.util.concurrent.TimeoutException("ring stalled"));

    adapter.postPut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(ctx, never()).bypass();
    assertFalse(
        Thread.currentThread().isInterrupted(), "timeout must not leave thread interrupted");
  }

  @Test
  void postPutTransportIOExceptionSwallowedUnderDefaultBestEffort() throws Exception {
    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenThrow(new IOException("channel closed"));

    adapter.postPut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(ctx, never()).bypass();
  }

  @Test
  void prePutErrorResponseSwallowedWhenConfiguredBestEffort() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.prePut", "best-effort");
    adapter = new RegionObserverAdapter(dispatcher, new PolicyConfig(conf));

    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenReturn(
            HookResponse.newBuilder()
                .setError(HookError.newBuilder().setCode(1).setMessage("nope"))
                .build()
                .toByteArray());

    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(ctx, never()).bypass();
  }

  @Test
  void postPutErrorResponseThrowsWhenConfiguredStrict() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.postPut", "strict");
    adapter = new RegionObserverAdapter(dispatcher, new PolicyConfig(conf));

    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenReturn(
            HookResponse.newBuilder()
                .setError(HookError.newBuilder().setCode(2).setMessage("go rejected"))
                .build()
                .toByteArray());

    IOException ex =
        assertThrows(
            IOException.class,
            () -> adapter.postPut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT));
    assertTrue(ex.getMessage().contains("go rejected"));
  }

  @Test
  void perHookTimeoutPropagatesToDispatcher() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.timeout.prePut", "250ms");
    adapter = new RegionObserverAdapter(dispatcher, new PolicyConfig(conf));

    when(dispatcher.dispatchHook(anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(dispatcher)
        .dispatchHook(eq(RegionObserverAdapter.HOOK_PRE_PUT), any(), eq(Duration.ofMillis(250)));
  }
}
