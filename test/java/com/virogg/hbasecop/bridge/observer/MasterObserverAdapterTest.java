// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import com.virogg.hbasecop.bridge.wire.pb.PreCreateTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreDeleteTableRequest;
import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * T51 Wave B unit test for {@link MasterObserverAdapter}: verifies the adapter encodes the HBase
 * descriptor / table-name arguments into the right proto Request, drives the injected {@link
 * HookDispatcher}, and translates {@code bypass=true} / strict-mode error responses into the
 * matching {@code ObserverContext#bypass()} / {@code IOException} reactions — mirrors the Region
 * adapter's coverage on a representative slice of the master surface.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MasterObserverAdapterTest {

  @Mock private HookDispatcher dispatcher;
  @Mock private ObserverContext<MasterCoprocessorEnvironment> ctx;

  private MasterObserverAdapter adapter;

  @BeforeEach
  void setUp() {
    adapter = new MasterObserverAdapter(dispatcher, new PolicyConfig(new Configuration(false)));
  }

  private static TableDescriptor sampleDescriptor(TableName tn) {
    return TableDescriptorBuilder.newBuilder(tn)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf"))
        .build();
  }

  @Test
  void preCreateTable_encodesTableNameAndDispatches()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    TableName tn = TableName.valueOf("default", "make-me");
    TableDescriptor desc = sampleDescriptor(tn);

    when(dispatcher.dispatchHook(
            anyInt(), eq(HookId.PRE_CREATE_TABLE.value()), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.preCreateTable(ctx, desc, new org.apache.hadoop.hbase.client.RegionInfo[0]);

    ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
    verify(dispatcher, times(1))
        .dispatchHook(
            anyInt(), eq(HookId.PRE_CREATE_TABLE.value()), payload.capture(), any(Duration.class));
    PreCreateTableRequest req = parseCreate(payload.getValue());
    assertArrayEquals(
        "default".getBytes(),
        req.getTableName().getNamespace().toByteArray(),
        "namespace not round-tripped");
    assertArrayEquals(
        "make-me".getBytes(),
        req.getTableName().getQualifier().toByteArray(),
        "qualifier not round-tripped");
    verify(ctx, times(0)).bypass();
  }

  @Test
  void preCreateTable_bypassPropagates()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    TableName tn = TableName.valueOf("default", "bypass-me");
    TableDescriptor desc = sampleDescriptor(tn);

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().setBypass(true).build().toByteArray());

    adapter.preCreateTable(ctx, desc, new org.apache.hadoop.hbase.client.RegionInfo[0]);

    verify(ctx, times(1)).bypass();
  }

  @Test
  void preCreateTable_strictErrorResponseRaisesIOException()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    Configuration conf = new Configuration(false);
    conf.set("hbasecop.policy.preCreateTable", "strict");
    adapter = new MasterObserverAdapter(dispatcher, new PolicyConfig(conf));

    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any(Duration.class)))
        .thenReturn(
            HookResponse.newBuilder()
                .setError(HookError.newBuilder().setCode(7).setMessage("nope"))
                .build()
                .toByteArray());

    TableName tn = TableName.valueOf("default", "bad");
    TableDescriptor desc = sampleDescriptor(tn);

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                adapter.preCreateTable(
                    ctx, desc, new org.apache.hadoop.hbase.client.RegionInfo[0]));
    assertEquals(
        true,
        thrown.getMessage().contains("nope"),
        "strict failure must surface the observer message; got: " + thrown.getMessage());
    verify(ctx, times(0)).bypass();
  }

  @Test
  void preDeleteTable_encodesTableName()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(
            anyInt(), eq(HookId.PRE_DELETE_TABLE.value()), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    adapter.preDeleteTable(ctx, TableName.valueOf("ns1", "drop-me"));

    ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
    verify(dispatcher, times(1))
        .dispatchHook(
            anyInt(), eq(HookId.PRE_DELETE_TABLE.value()), payload.capture(), any(Duration.class));
    PreDeleteTableRequest req = parseDelete(payload.getValue());
    assertArrayEquals("ns1".getBytes(), req.getTableName().getNamespace().toByteArray());
    assertArrayEquals("drop-me".getBytes(), req.getTableName().getQualifier().toByteArray());
  }

  @Test
  void preModifyTable_passesNewDescriptorThrough()
      throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any(Duration.class)))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());

    TableName tn = TableName.valueOf("default", "modify-me");
    TableDescriptor current = sampleDescriptor(tn);
    TableDescriptor next =
        TableDescriptorBuilder.newBuilder(current)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf2"))
            .build();

    TableDescriptor result = adapter.preModifyTable(ctx, tn, current, next);
    assertEquals(
        next, result, "preModifyTable must pass the candidate descriptor through unchanged");
    assertFalse(result == current, "preModifyTable must not substitute the current descriptor");
  }

  // --- helpers -----------------------------------------------------------

  private static PreCreateTableRequest parseCreate(byte[] bytes) {
    try {
      return PreCreateTableRequest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError("parse PreCreateTableRequest", e);
    }
  }

  private static PreDeleteTableRequest parseDelete(byte[] bytes) {
    try {
      return PreDeleteTableRequest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError("parse PreDeleteTableRequest", e);
    }
  }
}
