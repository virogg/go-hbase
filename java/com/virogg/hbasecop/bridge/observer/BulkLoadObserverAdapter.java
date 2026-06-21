// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.bridge.config.HookPolicy;
import com.virogg.hbasecop.bridge.config.Policy;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookContext;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PreCleanupBulkLoadRequest;
import com.virogg.hbasecop.bridge.wire.pb.PrePrepareBulkLoadRequest;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

public final class BulkLoadObserverAdapter implements BulkLoadObserver {

  private static final Logger LOG = System.getLogger(BulkLoadObserverAdapter.class.getName());

  private final HookDispatcher dispatcher;
  private final PolicyConfig policyConfig;

  public BulkLoadObserverAdapter(HookDispatcher dispatcher, PolicyConfig policyConfig) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.policyConfig = Objects.requireNonNull(policyConfig, "policyConfig");
  }

  @Override
  public void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> c)
      throws IOException {
    PrePrepareBulkLoadRequest req =
        PrePrepareBulkLoadRequest.newBuilder().setCtx(hookCtx(c)).build();
    HookResponse resp = dispatch(HookId.PRE_PREPARE_BULK_LOAD.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> c)
      throws IOException {
    PreCleanupBulkLoadRequest req =
        PreCleanupBulkLoadRequest.newBuilder().setCtx(hookCtx(c)).build();
    HookResponse resp = dispatch(HookId.PRE_CLEANUP_BULK_LOAD.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  private static HookContext hookCtx(ObserverContext<RegionCoprocessorEnvironment> c) {
    HookContext.Builder b = HookContext.newBuilder();
    if (c == null) {
      return b.build();
    }
    RegionCoprocessorEnvironment env = c.getEnvironment();
    if (env == null) {
      return b.build();
    }
    RegionInfo ri = env.getRegionInfo();
    if (ri != null) {
      TableName tn = ri.getTable();
      if (tn != null) {
        b.setTableName(
            HBaseProtos.TableName.newBuilder()
                .setNamespace(ByteString.copyFrom(tn.getNamespace()))
                .setQualifier(ByteString.copyFrom(tn.getQualifier())));
      }
      byte[] encodedRegion = ri.getEncodedNameAsBytes();
      if (encodedRegion != null) {
        b.setRegionName(ByteString.copyFrom(encodedRegion));
      }
    }
    return b.build();
  }

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
    String detail = "hbasecop: bulk-load hook " + hookId + " " + message;
    if (pol.policy() == Policy.STRICT) {
      throw cause == null ? new IOException(detail) : new IOException(detail, cause);
    }
    LOG.log(Level.WARNING, "{0} - best-effort, treated as no-op", detail);
    return null;
  }

  private static void applyBypass(
      ObserverContext<RegionCoprocessorEnvironment> c, HookResponse resp) {
    if (resp == null) {
      return;
    }
    if (resp.getBypass()) {
      ObserverBypass.tryBypass(c);
    }
  }
}
