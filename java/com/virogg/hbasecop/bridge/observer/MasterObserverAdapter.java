// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.bridge.config.HookPolicy;
import com.virogg.hbasecop.bridge.config.Policy;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookContext;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PostAssignRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostBalanceRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCreateTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostDeleteTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostDisableTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostEnableTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostModifyTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostMoveRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostTruncateTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostUnassignRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreAssignRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreBalanceRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCreateTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreDeleteTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreDisableTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreEnableTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreModifyTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreMoveRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreTruncateTableRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreUnassignRequest;
import com.virogg.hbasecop.bridge.wire.pb.ServerNameProto;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * T51 MasterObserver bridge. Routes every MasterObserver hook the SDK exposes (T51 Wave A - 20
 * master hooks across table lifecycle, enable/disable, region placement and balance) through the
 * shared {@link HookDispatcher} mux. Mirrors {@link RegionObserverAdapter}'s policy / dispatch /
 * bypass plumbing exactly: the same {@link PolicyConfig} resolves per-hook strict-vs-best-effort
 * behaviour by method name (e.g. {@code hbasecop.policy.preCreateTable}), so failure semantics stay
 * symmetric across the region and master surfaces.
 */
public final class MasterObserverAdapter implements MasterObserver {

  private static final Logger LOG = System.getLogger(MasterObserverAdapter.class.getName());

  private final HookDispatcher dispatcher;
  private final PolicyConfig policyConfig;

  public MasterObserverAdapter(HookDispatcher dispatcher, PolicyConfig policyConfig) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.policyConfig = Objects.requireNonNull(policyConfig, "policyConfig");
  }

  // --- Table lifecycle -----------------------------------------------------

  @Override
  public void preCreateTable(
      ObserverContext<MasterCoprocessorEnvironment> c, TableDescriptor desc, RegionInfo[] regions)
      throws IOException {
    PreCreateTableRequest.Builder b =
        PreCreateTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(desc.getTableName()))
            .setTableDescriptor(serialize(desc));
    addSplitKeys(b::addSplitKey, regions);
    HookResponse resp = dispatch(HookId.PRE_CREATE_TABLE.value(), b.build().toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postCreateTable(
      ObserverContext<MasterCoprocessorEnvironment> c, TableDescriptor desc, RegionInfo[] regions)
      throws IOException {
    PostCreateTableRequest.Builder b =
        PostCreateTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(desc.getTableName()))
            .setTableDescriptor(serialize(desc));
    addSplitKeys(b::addSplitKey, regions);
    HookResponse resp = dispatch(HookId.POST_CREATE_TABLE.value(), b.build().toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tn)
      throws IOException {
    PreDeleteTableRequest req =
        PreDeleteTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .build();
    HookResponse resp = dispatch(HookId.PRE_DELETE_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tn)
      throws IOException {
    PostDeleteTableRequest req =
        PostDeleteTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .build();
    HookResponse resp = dispatch(HookId.POST_DELETE_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public TableDescriptor preModifyTable(
      ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tn,
      TableDescriptor currentDescriptor,
      TableDescriptor newDescriptor)
      throws IOException {
    PreModifyTableRequest req =
        PreModifyTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .setNewTableDescriptor(serialize(newDescriptor))
            .build();
    HookResponse resp = dispatch(HookId.PRE_MODIFY_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
    // The 4-arg preModifyTable hook lets observers replace the candidate
    // descriptor before the modify commits; we always pass it through
    // unchanged (modification policy is not exposed on the wire today).
    return newDescriptor;
  }

  @Override
  public void postModifyTable(
      ObserverContext<MasterCoprocessorEnvironment> c,
      TableName tn,
      TableDescriptor oldDescriptor,
      TableDescriptor currentDescriptor)
      throws IOException {
    PostModifyTableRequest req =
        PostModifyTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .setNewTableDescriptor(serialize(currentDescriptor))
            .setOldTableDescriptor(serialize(oldDescriptor))
            .build();
    HookResponse resp = dispatch(HookId.POST_MODIFY_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tn)
      throws IOException {
    PreTruncateTableRequest req =
        PreTruncateTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .build();
    HookResponse resp = dispatch(HookId.PRE_TRUNCATE_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tn)
      throws IOException {
    PostTruncateTableRequest req =
        PostTruncateTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .build();
    HookResponse resp = dispatch(HookId.POST_TRUNCATE_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- Enable / disable ----------------------------------------------------

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tn)
      throws IOException {
    PreEnableTableRequest req =
        PreEnableTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .build();
    HookResponse resp = dispatch(HookId.PRE_ENABLE_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tn)
      throws IOException {
    PostEnableTableRequest req =
        PostEnableTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .build();
    HookResponse resp = dispatch(HookId.POST_ENABLE_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tn)
      throws IOException {
    PreDisableTableRequest req =
        PreDisableTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .build();
    HookResponse resp = dispatch(HookId.PRE_DISABLE_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tn)
      throws IOException {
    PostDisableTableRequest req =
        PostDisableTableRequest.newBuilder()
            .setCtx(emptyCtx())
            .setTableName(toProtoTableName(tn))
            .build();
    HookResponse resp = dispatch(HookId.POST_DISABLE_TABLE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- Region placement ----------------------------------------------------

  @Override
  public void preMove(
      ObserverContext<MasterCoprocessorEnvironment> c,
      RegionInfo region,
      ServerName srcServer,
      ServerName destServer)
      throws IOException {
    PreMoveRequest req =
        PreMoveRequest.newBuilder()
            .setCtx(emptyCtx())
            .setRegionInfo(serialize(region))
            .setSource(toProtoServerName(srcServer))
            .setDestination(toProtoServerName(destServer))
            .build();
    HookResponse resp = dispatch(HookId.PRE_MOVE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postMove(
      ObserverContext<MasterCoprocessorEnvironment> c,
      RegionInfo region,
      ServerName srcServer,
      ServerName destServer)
      throws IOException {
    PostMoveRequest req =
        PostMoveRequest.newBuilder()
            .setCtx(emptyCtx())
            .setRegionInfo(serialize(region))
            .setSource(toProtoServerName(srcServer))
            .setDestination(toProtoServerName(destServer))
            .build();
    HookResponse resp = dispatch(HookId.POST_MOVE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo region)
      throws IOException {
    PreAssignRequest req =
        PreAssignRequest.newBuilder().setCtx(emptyCtx()).setRegionInfo(serialize(region)).build();
    HookResponse resp = dispatch(HookId.PRE_ASSIGN.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo region)
      throws IOException {
    PostAssignRequest req =
        PostAssignRequest.newBuilder().setCtx(emptyCtx()).setRegionInfo(serialize(region)).build();
    HookResponse resp = dispatch(HookId.POST_ASSIGN.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo region)
      throws IOException {
    PreUnassignRequest req =
        PreUnassignRequest.newBuilder().setCtx(emptyCtx()).setRegionInfo(serialize(region)).build();
    HookResponse resp = dispatch(HookId.PRE_UNASSIGN.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> c, RegionInfo region)
      throws IOException {
    PostUnassignRequest req =
        PostUnassignRequest.newBuilder()
            .setCtx(emptyCtx())
            .setRegionInfo(serialize(region))
            .build();
    HookResponse resp = dispatch(HookId.POST_UNASSIGN.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- Cluster balance -----------------------------------------------------

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> c, BalanceRequest request)
      throws IOException {
    PreBalanceRequest req =
        PreBalanceRequest.newBuilder()
            .setCtx(emptyCtx())
            .setBalanceMode(balanceMode(request))
            .build();
    HookResponse resp = dispatch(HookId.PRE_BALANCE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postBalance(
      ObserverContext<MasterCoprocessorEnvironment> c,
      BalanceRequest request,
      List<RegionPlan> plans)
      throws IOException {
    PostBalanceRequest req =
        PostBalanceRequest.newBuilder()
            .setCtx(emptyCtx())
            .setBalanceMode(balanceMode(request))
            .setRan(plans != null && !plans.isEmpty())
            .build();
    HookResponse resp = dispatch(HookId.POST_BALANCE.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- Helpers -------------------------------------------------------------

  /**
   * HookContext on the master surface carries no table/region context (the per-hook payload does),
   * so we send an empty envelope. RequestId is filled in by the mux.
   */
  private static HookContext emptyCtx() {
    return HookContext.getDefaultInstance();
  }

  private static HBaseProtos.TableName toProtoTableName(TableName tn) {
    if (tn == null) {
      return HBaseProtos.TableName.getDefaultInstance();
    }
    return HBaseProtos.TableName.newBuilder()
        .setNamespace(ByteString.copyFrom(tn.getNamespace()))
        .setQualifier(ByteString.copyFrom(tn.getQualifier()))
        .build();
  }

  private static ServerNameProto toProtoServerName(ServerName sn) {
    if (sn == null) {
      return ServerNameProto.getDefaultInstance();
    }
    return ServerNameProto.newBuilder()
        .setHost(sn.getHostname() == null ? "" : sn.getHostname())
        .setPort(sn.getPort())
        .setStartCode(sn.getStartcode())
        .build();
  }

  private static ByteString serialize(TableDescriptor desc) {
    if (desc == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(
        org.apache.hadoop.hbase.client.TableDescriptorBuilder.toByteArray(desc));
  }

  private static ByteString serialize(RegionInfo ri) {
    if (ri == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(RegionInfo.toByteArray(ri));
  }

  /** Pulls each region's endKey (skipping the last, which always ends at +inf) as a split key. */
  private static void addSplitKeys(
      java.util.function.Consumer<ByteString> add, RegionInfo[] regions) {
    if (regions == null || regions.length <= 1) {
      return;
    }
    for (int i = 0; i < regions.length - 1; i++) {
      byte[] end = regions[i].getEndKey();
      if (end == null || end.length == 0) {
        continue;
      }
      add.accept(ByteString.copyFrom(end));
    }
  }

  /**
   * Packs the two boolean knobs on HBase 2.5's {@link BalanceRequest} into the {@code balance_mode}
   * int32 the proto carries (bit 0 = dryRun, bit 1 = ignoreRegionsInTransition). HBase 2.5's
   * BalanceRequest doesn't expose a single "mode" enum - earlier versions of this adapter assumed
   * it did and failed to compile.
   */
  private static int balanceMode(BalanceRequest request) {
    if (request == null) {
      return 0;
    }
    int mode = 0;
    if (request.isDryRun()) {
      mode |= 1;
    }
    if (request.isIgnoreRegionsInTransition()) {
      mode |= 2;
    }
    return mode;
  }

  /**
   * Drive one hook call. Mirrors {@link RegionObserverAdapter#dispatch} exactly so policy
   * resolution and STRICT-vs-best-effort behaviour stay symmetric across the two adapters.
   */
  private HookResponse dispatch(byte hookId, byte[] reqBytes) throws IOException {
    HookPolicy pol = policyConfig.forHook(hookId);
    final byte[] respBytes;
    try {
      respBytes = dispatcher.dispatchHook(0, hookId, reqBytes, pol.timeout());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("hbasecop: hook " + hookId + " dispatch interrupted", e);
    } catch (TimeoutException e) {
      return handleFailure(hookId, pol, "timeout after " + pol.timeout(), e);
    } catch (IOException e) {
      return handleFailure(hookId, pol, "transport failure: " + e.getMessage(), e);
    }
    final HookResponse resp;
    try {
      resp = HookResponse.parseFrom(respBytes);
    } catch (InvalidProtocolBufferException e) {
      return handleFailure(hookId, pol, "malformed HookResponse", e);
    }
    if (resp.hasError()) {
      HookError err = resp.getError();
      return handleFailure(
          hookId, pol, "rejected (code=" + err.getCode() + "): " + err.getMessage(), null);
    }
    return resp;
  }

  private static HookResponse handleFailure(
      byte hookId, HookPolicy pol, String message, Throwable cause) throws IOException {
    String detail = "hbasecop: master hook " + hookId + " " + message;
    if (pol.policy() == Policy.STRICT) {
      throw cause == null ? new IOException(detail) : new IOException(detail, cause);
    }
    LOG.log(Level.WARNING, "{0} - best-effort, treated as no-op", detail);
    return null;
  }

  private static void applyBypass(
      ObserverContext<MasterCoprocessorEnvironment> c, HookResponse resp) {
    if (resp == null) {
      return;
    }
    if (resp.getBypass()) {
      ObserverBypass.tryBypass(c);
    }
  }
}
