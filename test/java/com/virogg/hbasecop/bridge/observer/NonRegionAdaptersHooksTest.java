// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NonRegionAdaptersHooksTest {

  @Mock private HookDispatcher dispatcher;

  private final TableName tn = TableName.valueOf("default", "t");
  private final ServerName server = ServerName.valueOf("host", 16020, 1L);
  private RegionInfo regionInfo;
  private TableDescriptor desc;

  @BeforeEach
  void setUp() throws Exception {
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());
    regionInfo = RegionInfoBuilder.newBuilder(tn).build();
    desc = TableDescriptorBuilder.newBuilder(tn).build();
  }

  private PolicyConfig policy() {
    return new PolicyConfig(new Configuration(false));
  }

  private void verifyDispatched() throws Exception {
    verify(dispatcher, atLeastOnce()).dispatchHook(anyInt(), anyByte(), any(), any());
  }

  @Test
  void masterTableLifecycleHooksDispatch() throws Exception {
    MasterObserverAdapter a = new MasterObserverAdapter(dispatcher, policy());
    @SuppressWarnings("unchecked")
    ObserverContext<MasterCoprocessorEnvironment> c = mock(ObserverContext.class);
    RegionInfo[] regions = {regionInfo};

    a.preCreateTable(c, desc, regions);
    a.postCreateTable(c, desc, regions);
    a.preDeleteTable(c, tn);
    a.postDeleteTable(c, tn);
    assertSame(desc, a.preModifyTable(c, tn, desc, desc));
    a.postModifyTable(c, tn, desc, desc);
    a.preTruncateTable(c, tn);
    a.postTruncateTable(c, tn);
    a.preEnableTable(c, tn);
    a.postEnableTable(c, tn);
    a.preDisableTable(c, tn);
    a.postDisableTable(c, tn);
    verifyDispatched();
  }

  @Test
  void masterRegionAndBalanceHooksDispatch() throws Exception {
    MasterObserverAdapter a = new MasterObserverAdapter(dispatcher, policy());
    @SuppressWarnings("unchecked")
    ObserverContext<MasterCoprocessorEnvironment> c = mock(ObserverContext.class);

    a.preMove(c, regionInfo, server, server);
    a.postMove(c, regionInfo, server, server);
    a.preAssign(c, regionInfo);
    a.postAssign(c, regionInfo);
    a.preUnassign(c, regionInfo);
    a.postUnassign(c, regionInfo);
    BalanceRequest req = BalanceRequest.newBuilder().build();
    a.preBalance(c, req);
    a.postBalance(c, req, List.of());
    verifyDispatched();
  }

  @Test
  void regionServerHooksDispatch() throws Exception {
    RegionServerObserverAdapter a = new RegionServerObserverAdapter(dispatcher, policy());
    @SuppressWarnings("unchecked")
    ObserverContext<RegionServerCoprocessorEnvironment> c = mock(ObserverContext.class);

    a.preStopRegionServer(c);
    a.preRollWALWriterRequest(c);
    a.postRollWALWriterRequest(c);
    a.preReplicateLogEntries(c);
    a.postReplicateLogEntries(c);
    a.preClearCompactionQueues(c);
    a.postClearCompactionQueues(c);
    a.preExecuteProcedures(c);
    a.postExecuteProcedures(c);
    verifyDispatched();
  }

  @Test
  void walHooksDispatch() throws Exception {
    WALObserverAdapter a = new WALObserverAdapter(dispatcher, policy());
    @SuppressWarnings("unchecked")
    ObserverContext<WALCoprocessorEnvironment> c = mock(ObserverContext.class);

    a.preWALWrite(c, regionInfo, null, null);
    a.postWALWrite(c, regionInfo, null, null);
    a.preWALRoll(c, new Path("/wal/old"), new Path("/wal/new"));
    a.postWALRoll(c, new Path("/wal/old"), new Path("/wal/new"));
    verifyDispatched();
  }

  @Test
  void bulkLoadHooksDispatch() throws Exception {
    BulkLoadObserverAdapter a = new BulkLoadObserverAdapter(dispatcher, policy());
    @SuppressWarnings("unchecked")
    ObserverContext<RegionCoprocessorEnvironment> c = mock(ObserverContext.class);

    a.prePrepareBulkLoad(c);
    a.preCleanupBulkLoad(c);
    verifyDispatched();
  }
}
