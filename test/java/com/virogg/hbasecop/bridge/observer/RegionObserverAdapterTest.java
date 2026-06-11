// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PrePutRequest;
import com.virogg.hbasecop.hbase.v1.CellProtos;
import com.virogg.hbasecop.hbase.v1.ClientProtos.MutationProto;
import com.virogg.hbasecop.multiplex.RegionIdAllocator;
import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    ArgumentCaptor<byte[]> bytesCap = ArgumentCaptor.forClass(byte[].class);
    verify(dispatcher)
        .dispatchHook(
            anyInt(),
            eq(RegionObserverAdapter.HOOK_PRE_PUT),
            bytesCap.capture(),
            eq(Duration.ofSeconds(5)));

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
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().setBypass(true).build().toByteArray());

    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(ctx, times(1)).bypass();
  }

  @Test
  void preAppendBypassReturnsSubstituteResultFromHookResponseCells() throws Exception {
    // H12 full fix: a value-returning bypass on preAppend carries the
    // observer's substitute cells in HookResponse.result; the adapter must
    // return them to the client as a Result (which bypasses the Append).
    CellProtos.Cell cell =
        CellProtos.Cell.newBuilder()
            .setRow(ByteString.copyFromUtf8("row-9"))
            .setFamily(ByteString.copyFromUtf8("cf"))
            .setQualifier(ByteString.copyFromUtf8("q"))
            .setTimestamp(123L)
            .setCellType(CellProtos.CellType.PUT)
            .setValue(ByteString.copyFromUtf8("appended"))
            .build();
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(
            HookResponse.newBuilder().setBypass(true).addResult(cell).build().toByteArray());

    Append append =
        new Append("row-9".getBytes()).addColumn("cf".getBytes(), "q".getBytes(), "x".getBytes());
    Result r = adapter.preAppend(ctx, append);

    assertNotNull(r, "bypass must return a substitute Result, not null");
    assertEquals(1, r.rawCells().length);
    Cell got = r.rawCells()[0];
    assertEquals("row-9", new String(CellUtil.cloneRow(got)));
    assertEquals("q", new String(CellUtil.cloneQualifier(got)));
    assertEquals("appended", new String(CellUtil.cloneValue(got)));
  }

  @Test
  void preAppendNoBypassReturnsNullSoOperationProceeds() throws Exception {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    Append append =
        new Append("row-9".getBytes()).addColumn("cf".getBytes(), "q".getBytes(), "x".getBytes());
    assertNull(adapter.preAppend(ctx, append), "no bypass → null → HBase runs the Append");
  }

  @Test
  void prePutErrorResponseThrowsIOExceptionUnderStrictDefault() throws Exception {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
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
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.postPut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(dispatcher)
        .dispatchHook(anyInt(), eq(RegionObserverAdapter.HOOK_POST_PUT), any(), any());
  }

  @Test
  void prePutTimeoutMapsToIOExceptionUnderStrictDefault() throws Exception {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
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
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
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
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
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
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenThrow(new java.util.concurrent.TimeoutException("ring stalled"));

    adapter.postPut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(ctx, never()).bypass();
    assertFalse(
        Thread.currentThread().isInterrupted(), "timeout must not leave thread interrupted");
  }

  @Test
  void postPutTransportIOExceptionSwallowedUnderDefaultBestEffort() throws Exception {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenThrow(new IOException("channel closed"));

    adapter.postPut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(ctx, never()).bypass();
  }

  @Test
  void prePutErrorResponseSwallowedWhenConfiguredBestEffort() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.prePut", "best-effort");
    adapter = new RegionObserverAdapter(dispatcher, new PolicyConfig(conf));

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
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

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
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

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    verify(dispatcher)
        .dispatchHook(
            anyInt(), eq(RegionObserverAdapter.HOOK_PRE_PUT), any(), eq(Duration.ofMillis(250)));
  }

  // ---------- T61: region_id routing through dispatch ----------

  @Test
  void prePutDispatchesAllocatedRegionIdFromEnv() throws Exception {
    RegionIdAllocator allocator = new RegionIdAllocator();
    adapter =
        new RegionObserverAdapter(
            dispatcher, new PolicyConfig(new Configuration(false)), allocator);

    // Region "abc1234" — already stubbed in @BeforeEach.
    when(regionInfo.getEncodedName()).thenReturn("abc1234");
    adapter.start(env);

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    int expected = allocator.idFor("abc1234");
    assertTrue(expected > 0, "start(env) must allocate a non-zero region id");
    verify(dispatcher)
        .dispatchHook(eq(expected), eq(RegionObserverAdapter.HOOK_PRE_PUT), any(), any());
  }

  @Test
  void distinctRegionsDispatchUnderDistinctRegionIds() throws Exception {
    RegionIdAllocator allocator = new RegionIdAllocator();
    adapter =
        new RegionObserverAdapter(
            dispatcher, new PolicyConfig(new Configuration(false)), allocator);

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    // First region.
    when(regionInfo.getEncodedName()).thenReturn("region-a");
    adapter.start(env);
    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    // Second region — same adapter instance, different env scope. The
    // T63 design holds one adapter per RegionObserver registration, so
    // distinct regions go through distinct adapter instances in practice;
    // exercising one adapter across two regions here keeps the test focused
    // on the allocator contract.
    RegionInfo regionInfo2 = org.mockito.Mockito.mock(RegionInfo.class);
    Region region2 = org.mockito.Mockito.mock(Region.class);
    RegionCoprocessorEnvironment env2 =
        org.mockito.Mockito.mock(RegionCoprocessorEnvironment.class);
    @SuppressWarnings("unchecked")
    ObserverContext<RegionCoprocessorEnvironment> ctx2 =
        org.mockito.Mockito.mock(ObserverContext.class);
    when(ctx2.getEnvironment()).thenReturn(env2);
    when(env2.getRegion()).thenReturn(region2);
    when(region2.getRegionInfo()).thenReturn(regionInfo2);
    when(regionInfo2.getTable()).thenReturn(TableName.valueOf("default", "users"));
    when(regionInfo2.getEncodedNameAsBytes()).thenReturn("region-b".getBytes());
    when(regionInfo2.getEncodedName()).thenReturn("region-b");
    adapter.start(env2);
    adapter.prePut(ctx2, samplePut(), walEdit, Durability.USE_DEFAULT);

    int idA = allocator.idFor("region-a");
    int idB = allocator.idFor("region-b");
    assertTrue(idA > 0 && idB > 0 && idA != idB, "regions must receive distinct non-zero ids");

    verify(dispatcher).dispatchHook(eq(idA), eq(RegionObserverAdapter.HOOK_PRE_PUT), any(), any());
    verify(dispatcher).dispatchHook(eq(idB), eq(RegionObserverAdapter.HOOK_PRE_PUT), any(), any());
  }

  @Test
  void stopReleasesRegionIdMapping() throws Exception {
    RegionIdAllocator allocator = new RegionIdAllocator();
    adapter =
        new RegionObserverAdapter(
            dispatcher, new PolicyConfig(new Configuration(false)), allocator);

    when(regionInfo.getEncodedName()).thenReturn("abc1234");
    adapter.start(env);
    assertTrue(allocator.idFor("abc1234") > 0);

    adapter.stop(env);
    assertEquals(0, allocator.idFor("abc1234"));
  }

  @Test
  void prePutWithoutStartFallsBackToZeroRegionId() throws Exception {
    RegionIdAllocator allocator = new RegionIdAllocator();
    adapter =
        new RegionObserverAdapter(
            dispatcher, new PolicyConfig(new Configuration(false)), allocator);

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    when(regionInfo.getEncodedName()).thenReturn("never-started");
    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);

    // No start(env) ⇒ allocator empty ⇒ idFor=0 ⇒ wire region_id=0. This
    // matches the Phase-2 wire shape so hooks issued before lifecycle
    // wiring kicks in still reach the Go side, just without region scope.
    verify(dispatcher).dispatchHook(eq(0), eq(RegionObserverAdapter.HOOK_PRE_PUT), any(), any());
  }

  // ---------- bypass() safety on non-bypassable hooks ----------

  @Test
  void bypassRequestOnNonBypassableHookDoesNotPropagate() throws Exception {
    // HBase 2.5 makes ObserverContext#bypass() throw UnsupportedOperationException
    // on hooks that are not bypassable; uncaught, that aborts the whole
    // RegionServer. An over-eager observer must never be able to do that.
    doThrow(new UnsupportedOperationException("This method does not support 'bypass'."))
        .when(ctx)
        .bypass();
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().setBypass(true).build().toByteArray());

    // Must complete normally — the rejected bypass is downgraded to a WARN.
    adapter.prePut(ctx, samplePut(), walEdit, Durability.USE_DEFAULT);
  }

  // ---------- preScannerOpen bypass → empty Scan (HBase 2.5) ----------

  @Test
  void preScannerOpenBypassConstrainsScanToEmptyRange() throws Exception {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().setBypass(true).build().toByteArray());

    Scan scan = new Scan();
    adapter.preScannerOpen(ctx, scan);

    // preScannerOpen is not bypassable in HBase 2.5, so the adapter must NOT
    // call ObserverContext#bypass(); it neuters the Scan to an empty
    // half-open row interval [sentinel, sentinel) instead.
    verify(ctx, never()).bypass();
    assertArrayEquals(new byte[] {0}, scan.getStartRow());
    assertArrayEquals(new byte[] {0}, scan.getStopRow());
    assertTrue(scan.includeStartRow(), "start row must be inclusive");
    assertFalse(scan.includeStopRow(), "stop row must be exclusive → empty interval");
  }

  @Test
  void preScannerOpenWithoutBypassLeavesScanUntouched() throws Exception {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    Scan scan = new Scan().withStartRow("ok-".getBytes()).withStopRow("ok-~".getBytes());
    adapter.preScannerOpen(ctx, scan);

    verify(ctx, never()).bypass();
    assertArrayEquals("ok-".getBytes(), scan.getStartRow());
    assertArrayEquals("ok-~".getBytes(), scan.getStopRow());
  }
}
