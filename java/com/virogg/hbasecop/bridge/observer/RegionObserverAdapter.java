// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.virogg.hbasecop.bridge.wire.pb.HookContext;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PostPutRequest;
import com.virogg.hbasecop.bridge.wire.pb.PrePutRequest;
import com.virogg.hbasecop.hbase.v1.ClientProtos.MutationProto;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.io.IOException;
import java.time.Duration;
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
 * <p>Phase 2 surface is intentionally tiny: only the Put hooks are wired. {@code bypass=true} in
 * the Go-side response triggers {@link ObserverContext#bypass()} so HBase skips its own
 * implementation; a populated {@code HookError} is surfaced as {@code IOException} (strict policy,
 * the P2 default — best-effort lands in T31/T32).
 *
 * <p>The adapter is stateless apart from the injected dispatcher and timeout; one instance per
 * region is fine.
 */
public final class RegionObserverAdapter implements RegionObserver {

  /** Hook IDs. Mirror {@code pkg/hbasecop/hooks.go} on the Go side. */
  public static final byte HOOK_PRE_PUT = 1;

  public static final byte HOOK_POST_PUT = 2;

  private final HookDispatcher dispatcher;
  private final Duration timeout;

  public RegionObserverAdapter(HookDispatcher dispatcher, Duration timeout) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
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
    applyHookResponse(c, HOOK_PRE_PUT, resp);
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
    applyHookResponse(c, HOOK_POST_PUT, resp);
  }

  private HookResponse dispatch(byte hookId, byte[] reqBytes) throws IOException {
    final byte[] respBytes;
    try {
      respBytes = dispatcher.dispatchHook(hookId, reqBytes, timeout);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("hbasecop: hook " + hookId + " dispatch interrupted", e);
    } catch (TimeoutException e) {
      throw new IOException("hbasecop: hook " + hookId + " timeout after " + timeout, e);
    }
    try {
      return HookResponse.parseFrom(respBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IOException("hbasecop: malformed HookResponse for hook " + hookId, e);
    }
  }

  private static void applyHookResponse(
      ObserverContext<RegionCoprocessorEnvironment> c, byte hookId, HookResponse resp)
      throws IOException {
    if (resp.hasError()) {
      throw new IOException(
          "hbasecop: hook "
              + hookId
              + " rejected (code="
              + resp.getError().getCode()
              + "): "
              + resp.getError().getMessage());
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
