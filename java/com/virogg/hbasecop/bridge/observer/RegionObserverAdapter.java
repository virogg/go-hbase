// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.virogg.hbasecop.bridge.config.HookPolicy;
import com.virogg.hbasecop.bridge.config.Policy;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookContext;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PostPutRequest;
import com.virogg.hbasecop.bridge.wire.pb.PrePutRequest;
import com.virogg.hbasecop.hbase.v1.ClientProtos.MutationProto;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;

/**
 * RegionServer-side bridge that intercepts {@code prePut}/{@code postPut} and relays them as
 * protobuf hook invocations to the long-running Go runtime via a {@link HookDispatcher}.
 *
 * <p>Each hook resolves its {@link HookPolicy} (policy + timeout) from the supplied {@link
 * PolicyConfig}. Under {@link Policy#STRICT}, Go-side errors (error response, timeout, transport
 * IOException, malformed payload) propagate to the HBase client as {@code IOException} so the
 * mutation aborts. Under {@link Policy#BEST_EFFORT} the same failures are logged at {@code WARN}
 * and the hook is treated as a no-op so the operation continues. Caller interruption is always
 * surfaced as {@code IOException} regardless of policy and re-sets the interrupt flag.
 *
 * <p>{@code bypass=true} in the Go-side response triggers {@link ObserverContext#bypass()} so HBase
 * skips its own implementation; it is only honoured when the hook actually returned a clean
 * response.
 */
public final class RegionObserverAdapter implements RegionObserver {

  private static final Logger LOG = System.getLogger(RegionObserverAdapter.class.getName());

  /** Hook IDs. Mirror {@code pkg/hbasecop/hooks.go} on the Go side. */
  public static final byte HOOK_PRE_PUT = 1;

  public static final byte HOOK_POST_PUT = 2;

  private final HookDispatcher dispatcher;
  private final PolicyConfig policyConfig;

  public RegionObserverAdapter(HookDispatcher dispatcher, PolicyConfig policyConfig) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.policyConfig = Objects.requireNonNull(policyConfig, "policyConfig");
  }

  @Override
  public void prePut(
      ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    MutationProto mutation = PutConverter.toProto(put);
    byte[] reqBytes =
        PrePutRequest.newBuilder().setCtx(hookCtx).setMutation(mutation).build().toByteArray();
    HookResponse resp = dispatch(HOOK_PRE_PUT, reqBytes);
    applyHookResponse(c, resp);
  }

  @Override
  public void postPut(
      ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    MutationProto mutation = PutConverter.toProto(put);
    byte[] reqBytes =
        PostPutRequest.newBuilder().setCtx(hookCtx).setMutation(mutation).build().toByteArray();
    HookResponse resp = dispatch(HOOK_POST_PUT, reqBytes);
    applyHookResponse(c, resp);
  }

  /**
   * Drive one hook call. Returns the Go-side {@link HookResponse} on success, or {@code null} if
   * the call failed and the hook's policy is best-effort (caller must treat as no-op). Strict
   * failures throw {@link IOException}.
   */
  private HookResponse dispatch(byte hookId, byte[] reqBytes) throws IOException {
    HookPolicy pol = policyConfig.forHook(hookId);
    final byte[] respBytes;
    try {
      respBytes = dispatcher.dispatchHook(hookId, reqBytes, pol.timeout());
    } catch (InterruptedException e) {
      // Caller-driven cancellation: bypass the policy and propagate.
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
    String detail = "hbasecop: hook " + hookId + " " + message;
    if (pol.policy() == Policy.STRICT) {
      throw cause == null ? new IOException(detail) : new IOException(detail, cause);
    }
    LOG.log(Level.WARNING, "{0} — best-effort, treated as no-op", detail);
    return null;
  }

  private static void applyHookResponse(
      ObserverContext<RegionCoprocessorEnvironment> c, HookResponse resp) {
    if (resp == null) {
      return;
    }
    if (resp.getBypass()) {
      c.bypass();
    }
  }

  private static HookContext buildHookContext(ObserverContext<RegionCoprocessorEnvironment> c) {
    RegionInfo ri = c.getEnvironment().getRegion().getRegionInfo();
    TableName tn = ri.getTable();
    return HookContext.newBuilder()
        .setTableName(
            HBaseProtos.TableName.newBuilder()
                .setNamespace(ByteString.copyFrom(tn.getNamespace()))
                .setQualifier(ByteString.copyFrom(tn.getQualifier())))
        .setRegionName(ByteString.copyFrom(ri.getEncodedNameAsBytes()))
        .build();
  }
}
