// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RegionObserverAdapterPayloadHooksTest {

  private static final byte[] ROW = "row-1".getBytes();
  private static final byte[] CF = "cf".getBytes();
  private static final byte[] Q = "q".getBytes();
  private static final byte[] V = "v".getBytes();

  @Mock private HookDispatcher dispatcher;
  @Mock private ObserverContext<RegionCoprocessorEnvironment> ctx;
  @Mock private RegionCoprocessorEnvironment env;
  @Mock private Region region;
  @Mock private RegionInfo regionInfo;

  private RegionObserverAdapter adapter;

  @BeforeEach
  void setUp() throws Exception {
    when(ctx.getEnvironment()).thenReturn(env);
    when(env.getRegion()).thenReturn(region);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    when(regionInfo.getTable()).thenReturn(TableName.valueOf("default", "t"));
    when(regionInfo.getEncodedNameAsBytes()).thenReturn("enc".getBytes());
    when(regionInfo.getEncodedName()).thenReturn("enc");
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());
    adapter = new RegionObserverAdapter(dispatcher, new PolicyConfig(new Configuration(false)));
  }

  private void verifyDispatched() throws Exception {
    verify(dispatcher, atLeastOnce()).dispatchHook(anyInt(), anyByte(), any(), any());
  }

  private static Put samplePut() {
    return new Put(ROW).addColumn(CF, Q, V);
  }

  private static Delete sampleDelete() {
    return new Delete(ROW).addColumns(CF, Q);
  }

  private static Get sampleGet() {
    return new Get(ROW).addColumn(CF, Q);
  }

  @Test
  void readPathHooksDispatch() throws Exception {
    List<Cell> result = new ArrayList<>();
    result.add(new KeyValue(ROW, CF, Q, 1L, V));
    adapter.preGetOp(ctx, sampleGet(), new ArrayList<>());
    adapter.postGetOp(ctx, sampleGet(), result);
    assertEquals(false, adapter.preExists(ctx, sampleGet(), false));
    adapter.postExists(ctx, sampleGet(), true);
    verifyDispatched();
  }

  @Test
  void deleteHooksDispatch() throws Exception {
    adapter.preDelete(ctx, sampleDelete(), new org.apache.hadoop.hbase.wal.WALEdit(), null);
    adapter.postDelete(ctx, sampleDelete(), new org.apache.hadoop.hbase.wal.WALEdit(), null);
    Cell cell = new KeyValue(ROW, CF, Q, 1L, V);
    adapter.prePrepareTimeStampForDeleteVersion(
        ctx, samplePut(), cell, "1".getBytes(), sampleGet());
    verifyDispatched();
  }

  @Test
  void checkAndPutHooksDispatch() throws Exception {
    BinaryComparator cmp = new BinaryComparator(V);
    adapter.preCheckAndPut(ctx, ROW, CF, Q, CompareOperator.EQUAL, cmp, samplePut(), false);
    adapter.postCheckAndPut(ctx, ROW, CF, Q, CompareOperator.EQUAL, cmp, samplePut(), true);
    adapter.preCheckAndPutAfterRowLock(
        ctx, ROW, CF, Q, CompareOperator.EQUAL, cmp, samplePut(), false);
    verifyDispatched();
  }

  @Test
  void checkAndDeleteHooksDispatch() throws Exception {
    BinaryComparator cmp = new BinaryComparator(V);
    adapter.preCheckAndDelete(ctx, ROW, CF, Q, CompareOperator.EQUAL, cmp, sampleDelete(), false);
    adapter.postCheckAndDelete(ctx, ROW, CF, Q, CompareOperator.EQUAL, cmp, sampleDelete(), true);
    adapter.preCheckAndDeleteAfterRowLock(
        ctx, ROW, CF, Q, CompareOperator.EQUAL, cmp, sampleDelete(), false);
    verifyDispatched();
  }

  @Test
  void checkAndMutateHooksDispatch() throws Exception {
    CheckAndMutate cam = CheckAndMutate.newBuilder(ROW).ifEquals(CF, Q, V).build(samplePut());
    CheckAndMutateResult res = new CheckAndMutateResult(true, null);
    adapter.preCheckAndMutate(ctx, cam, res);
    adapter.postCheckAndMutate(ctx, cam, res);
    adapter.preCheckAndMutateAfterRowLock(ctx, cam, res);
    verifyDispatched();
  }

  @Test
  void incrementAndAppendHooksDispatch() throws Exception {
    Increment inc = new Increment(ROW).addColumn(CF, Q, 1L);
    Append app = new Append(ROW).addColumn(CF, Q, V);
    Result result = Result.create(new Cell[] {new KeyValue(ROW, CF, Q, 1L, V)});
    adapter.preIncrement(ctx, inc);
    adapter.postIncrement(ctx, inc, result);
    adapter.postAppend(ctx, app, result);
    verifyDispatched();
  }

  @Test
  void beforeWalHooksArePassthrough() throws Exception {
    Cell oldCell = new KeyValue(ROW, CF, Q, 1L, V);
    Cell newCell = new KeyValue(ROW, CF, Q, 2L, V);
    adapter.postMutationBeforeWAL(
        ctx, RegionObserver.MutationType.APPEND, samplePut(), oldCell, newCell);

    List<Pair<Cell, Cell>> pairs = new ArrayList<>();
    pairs.add(new Pair<>(oldCell, newCell));
    List<Pair<Cell, Cell>> incOut = adapter.postIncrementBeforeWAL(ctx, new Increment(ROW), pairs);
    List<Pair<Cell, Cell>> appOut = adapter.postAppendBeforeWAL(ctx, new Append(ROW), pairs);
    assertEquals(pairs, incOut);
    assertEquals(pairs, appOut);
    verifyDispatched();
  }
}
