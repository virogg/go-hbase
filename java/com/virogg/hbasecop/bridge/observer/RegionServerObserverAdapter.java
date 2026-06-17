// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.bridge.config.HookPolicy;
import com.virogg.hbasecop.bridge.config.Policy;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookContext;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PostClearCompactionQueuesRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostExecuteProceduresRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostReplicateLogEntriesRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostRollWalWriterRequestRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreClearCompactionQueuesRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreExecuteProceduresRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreReplicateLogEntriesRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreRollWalWriterRequestRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreStopRegionServerRequest;
import com.virogg.hbasecop.bridge.wire.pb.ServerNameProto;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * T52 RegionServerObserver bridge. Routes every RegionServerObserver hook the SDK exposes (T52 Wave
 * A - 9 server-scoped hooks: stop, WAL-writer roll, replication, compaction-queue clearing and
 * procedure execution) through the shared {@link HookDispatcher} mux. Mirrors {@link
 * MasterObserverAdapter}'s policy / dispatch / bypass plumbing exactly: the same {@link
 * PolicyConfig} resolves per-hook strict-vs-best-effort behaviour by method name (e.g. {@code
 * hbasecop.policy.preStopRegionServer}), so failure semantics stay symmetric across the region,
 * master and region-server surfaces.
 *
 * <p>RegionServerObserver hooks carry no table/region context - the affected RegionServer is
 * identified by the {@code server} field, taken from {@code env.getServerName()}.
 */
public final class RegionServerObserverAdapter implements RegionServerObserver {

  private static final Logger LOG = System.getLogger(RegionServerObserverAdapter.class.getName());

  private final HookDispatcher dispatcher;
  private final PolicyConfig policyConfig;

  public RegionServerObserverAdapter(HookDispatcher dispatcher, PolicyConfig policyConfig) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.policyConfig = Objects.requireNonNull(policyConfig, "policyConfig");
  }

  // --- Server lifecycle ----------------------------------------------------

  @Override
  public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> c)
      throws IOException {
    PreStopRegionServerRequest req =
        PreStopRegionServerRequest.newBuilder().setCtx(emptyCtx()).setServer(serverOf(c)).build();
    HookResponse resp = dispatch(HookId.PRE_STOP_REGION_SERVER.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- WAL writer roll -----------------------------------------------------

  @Override
  public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> c)
      throws IOException {
    PreRollWalWriterRequestRequest req =
        PreRollWalWriterRequestRequest.newBuilder()
            .setCtx(emptyCtx())
            .setServer(serverOf(c))
            .build();
    HookResponse resp = dispatch(HookId.PRE_ROLL_WAL_WRITER_REQUEST.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> c)
      throws IOException {
    PostRollWalWriterRequestRequest req =
        PostRollWalWriterRequestRequest.newBuilder()
            .setCtx(emptyCtx())
            .setServer(serverOf(c))
            .build();
    HookResponse resp = dispatch(HookId.POST_ROLL_WAL_WRITER_REQUEST.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- Replication log entries ---------------------------------------------

  @Override
  public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> c)
      throws IOException {
    PreReplicateLogEntriesRequest req =
        PreReplicateLogEntriesRequest.newBuilder()
            .setCtx(emptyCtx())
            .setServer(serverOf(c))
            .build();
    HookResponse resp = dispatch(HookId.PRE_REPLICATE_LOG_ENTRIES.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> c)
      throws IOException {
    PostReplicateLogEntriesRequest req =
        PostReplicateLogEntriesRequest.newBuilder()
            .setCtx(emptyCtx())
            .setServer(serverOf(c))
            .build();
    HookResponse resp = dispatch(HookId.POST_REPLICATE_LOG_ENTRIES.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- Compaction queue clearing -------------------------------------------

  @Override
  public void preClearCompactionQueues(ObserverContext<RegionServerCoprocessorEnvironment> c)
      throws IOException {
    PreClearCompactionQueuesRequest req =
        PreClearCompactionQueuesRequest.newBuilder()
            .setCtx(emptyCtx())
            .setServer(serverOf(c))
            .build();
    HookResponse resp = dispatch(HookId.PRE_CLEAR_COMPACTION_QUEUES.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postClearCompactionQueues(ObserverContext<RegionServerCoprocessorEnvironment> c)
      throws IOException {
    PostClearCompactionQueuesRequest req =
        PostClearCompactionQueuesRequest.newBuilder()
            .setCtx(emptyCtx())
            .setServer(serverOf(c))
            .build();
    HookResponse resp = dispatch(HookId.POST_CLEAR_COMPACTION_QUEUES.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- Procedure execution -------------------------------------------------

  @Override
  public void preExecuteProcedures(ObserverContext<RegionServerCoprocessorEnvironment> c)
      throws IOException {
    PreExecuteProceduresRequest req =
        PreExecuteProceduresRequest.newBuilder().setCtx(emptyCtx()).setServer(serverOf(c)).build();
    HookResponse resp = dispatch(HookId.PRE_EXECUTE_PROCEDURES.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postExecuteProcedures(ObserverContext<RegionServerCoprocessorEnvironment> c)
      throws IOException {
    PostExecuteProceduresRequest req =
        PostExecuteProceduresRequest.newBuilder().setCtx(emptyCtx()).setServer(serverOf(c)).build();
    HookResponse resp = dispatch(HookId.POST_EXECUTE_PROCEDURES.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- Helpers -------------------------------------------------------------

  /**
   * HookContext on the region-server surface carries no table/region context (the {@code server}
   * field does), so we send an empty envelope. RequestId is filled in by the mux.
   */
  private static HookContext emptyCtx() {
    return HookContext.getDefaultInstance();
  }

  private static ServerNameProto serverOf(ObserverContext<RegionServerCoprocessorEnvironment> c) {
    if (c == null || c.getEnvironment() == null) {
      return ServerNameProto.getDefaultInstance();
    }
    return toProtoServerName(c.getEnvironment().getServerName());
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

  /**
   * Drive one hook call. Mirrors {@link MasterObserverAdapter#dispatch} exactly so policy
   * resolution and STRICT-vs-best-effort behaviour stay symmetric across the three adapters.
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
    String detail = "hbasecop: region-server hook " + hookId + " " + message;
    if (pol.policy() == Policy.STRICT) {
      throw cause == null ? new IOException(detail) : new IOException(detail, cause);
    }
    LOG.log(Level.WARNING, "{0} - best-effort, treated as no-op", detail);
    return null;
  }

  private static void applyBypass(
      ObserverContext<RegionServerCoprocessorEnvironment> c, HookResponse resp) {
    if (resp == null) {
      return;
    }
    if (resp.getBypass()) {
      ObserverBypass.tryBypass(c);
    }
  }
}
