// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * T44 acceptance - verifies that {@code preBatchMutate} forwards {@code
 * HookResponse.blocked_indices} back into the {@link MiniBatchOperationInProgress} as per-mutation
 * {@link OperationStatus} failures, so the matching individual mutations are skipped while the rest
 * of the batch proceeds (partial-block semantics).
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RegionObserverAdapterBatchTest {

  @Mock private HookDispatcher dispatcher;
  @Mock private ObserverContext<RegionCoprocessorEnvironment> ctx;
  @Mock private RegionCoprocessorEnvironment env;
  @Mock private Region region;
  @Mock private RegionInfo regionInfo;

  @SuppressWarnings("unchecked")
  @Mock
  private MiniBatchOperationInProgress<Mutation> miniBatch;

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

  private static Mutation putRow(String row) {
    Put p = new Put(row.getBytes());
    p.addColumn("cf".getBytes(), "q".getBytes(), 1L, "v".getBytes());
    return p;
  }

  private void primeMiniBatch(int size) {
    when(miniBatch.size()).thenReturn(size);
    for (int i = 0; i < size; i++) {
      when(miniBatch.getOperation(i)).thenReturn(putRow("row-" + i));
    }
  }

  @Test
  void preBatchMutateAppliesPerIndexFailureForBlockedIndices() throws Exception {
    primeMiniBatch(5);
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(
            HookResponse.newBuilder()
                .addBlockedIndices(1)
                .addBlockedIndices(3)
                .build()
                .toByteArray());

    adapter.preBatchMutate(ctx, miniBatch);

    ArgumentCaptor<Integer> idxCap = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<OperationStatus> statusCap = ArgumentCaptor.forClass(OperationStatus.class);
    verify(miniBatch, org.mockito.Mockito.times(2))
        .setOperationStatus(idxCap.capture(), statusCap.capture());

    assertEquals(java.util.List.of(1, 3), idxCap.getAllValues());
    for (OperationStatus s : statusCap.getAllValues()) {
      assertEquals(HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, s.getOperationStatusCode());
    }
  }

  @Test
  void preBatchMutateLeavesMiniBatchAloneWhenNoBlockedIndices() throws Exception {
    primeMiniBatch(3);
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.preBatchMutate(ctx, miniBatch);

    verify(miniBatch, never()).setOperationStatus(anyInt(), any(OperationStatus.class));
  }

  @Test
  void preBatchMutateIgnoresOutOfRangeBlockedIndices() throws Exception {
    primeMiniBatch(3);
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(
            HookResponse.newBuilder()
                .addBlockedIndices(0)
                .addBlockedIndices(99)
                .build()
                .toByteArray());

    adapter.preBatchMutate(ctx, miniBatch);

    verify(miniBatch).setOperationStatus(eq(0), any(OperationStatus.class));
    verify(miniBatch, never()).setOperationStatus(eq(99), any(OperationStatus.class));
  }
}
