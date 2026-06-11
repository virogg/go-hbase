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
import com.virogg.hbasecop.bridge.wire.pb.PostWALRollRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostWALWriteRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreWALRollRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreWALWriteRequest;
import com.virogg.hbasecop.bridge.wire.pb.WalEditProto;
import com.virogg.hbasecop.bridge.wire.pb.WalKeyProto;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * T53 WALObserver bridge. Routes every WALObserver hook the SDK exposes (T53 Wave A — 4 hooks:
 * preWALWrite / postWALWrite on the latency-critical WAL append path and preWALRoll / postWALRoll
 * on file rotation) through the shared {@link HookDispatcher} mux. Mirrors {@link
 * MasterObserverAdapter}'s policy / dispatch / bypass plumbing exactly: the same {@link
 * PolicyConfig} resolves per-hook strict-vs-best-effort behaviour by method name (e.g. {@code
 * hbasecop.policy.preWALWrite}), so failure semantics stay symmetric across every observer surface.
 *
 * <p>preWALWrite / postWALWrite sit on the WAL hot path; the {@code WALKey} / {@code WALEdit} slim
 * envelopes carry only the shippable scalars + the cell array, keeping per-call serialization cost
 * bounded.
 */
public final class WALObserverAdapter implements WALObserver {

  private static final Logger LOG = System.getLogger(WALObserverAdapter.class.getName());

  private final HookDispatcher dispatcher;
  private final PolicyConfig policyConfig;

  public WALObserverAdapter(HookDispatcher dispatcher, PolicyConfig policyConfig) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.policyConfig = Objects.requireNonNull(policyConfig, "policyConfig");
  }

  // --- WAL write — latency-critical hot path -------------------------------

  @Override
  public void preWALWrite(
      ObserverContext<? extends WALCoprocessorEnvironment> c,
      RegionInfo info,
      WALKey logKey,
      WALEdit logEdit)
      throws IOException {
    PreWALWriteRequest.Builder b = PreWALWriteRequest.newBuilder().setCtx(emptyCtx());
    if (info != null) {
      b.setRegionInfo(regionInfoProto(info));
    }
    if (logKey != null) {
      b.setLogKey(walKey(logKey));
    }
    if (logEdit != null) {
      b.setLogEdit(walEdit(logEdit));
    }
    HookResponse resp = dispatch(HookId.PRE_WAL_WRITE.value(), b.build().toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postWALWrite(
      ObserverContext<? extends WALCoprocessorEnvironment> c,
      RegionInfo info,
      WALKey logKey,
      WALEdit logEdit)
      throws IOException {
    PostWALWriteRequest.Builder b = PostWALWriteRequest.newBuilder().setCtx(emptyCtx());
    if (info != null) {
      b.setRegionInfo(regionInfoProto(info));
    }
    if (logKey != null) {
      b.setLogKey(walKey(logKey));
    }
    if (logEdit != null) {
      b.setLogEdit(walEdit(logEdit));
    }
    HookResponse resp = dispatch(HookId.POST_WAL_WRITE.value(), b.build().toByteArray());
    applyBypass(c, resp);
  }

  // --- WAL roll ------------------------------------------------------------

  @Override
  public void preWALRoll(
      ObserverContext<? extends WALCoprocessorEnvironment> c, Path oldPath, Path newPath)
      throws IOException {
    PreWALRollRequest req =
        PreWALRollRequest.newBuilder()
            .setCtx(emptyCtx())
            .setOldPath(pathString(oldPath))
            .setNewPath(pathString(newPath))
            .build();
    HookResponse resp = dispatch(HookId.PRE_WAL_ROLL.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  @Override
  public void postWALRoll(
      ObserverContext<? extends WALCoprocessorEnvironment> c, Path oldPath, Path newPath)
      throws IOException {
    PostWALRollRequest req =
        PostWALRollRequest.newBuilder()
            .setCtx(emptyCtx())
            .setOldPath(pathString(oldPath))
            .setNewPath(pathString(newPath))
            .build();
    HookResponse resp = dispatch(HookId.POST_WAL_ROLL.value(), req.toByteArray());
    applyBypass(c, resp);
  }

  // --- Helpers -------------------------------------------------------------

  /**
   * HookContext on the WAL surface carries no table/region context (the per-hook payload's {@code
   * region_info} / {@code log_key} does), so we send an empty envelope. RequestId is filled in by
   * the mux.
   */
  private static HookContext emptyCtx() {
    return HookContext.getDefaultInstance();
  }

  private static String pathString(Path p) {
    return p == null ? "" : p.toString();
  }

  private static HBaseProtos.RegionInfo regionInfoProto(RegionInfo info) {
    HBaseProtos.RegionInfo.Builder b =
        HBaseProtos.RegionInfo.newBuilder()
            .setRegionId(info.getRegionId())
            .setTableName(
                HBaseProtos.TableName.newBuilder()
                    .setNamespace(ByteString.copyFrom(info.getTable().getNamespace()))
                    .setQualifier(ByteString.copyFrom(info.getTable().getQualifier())))
            .setOffline(info.isOffline())
            .setSplit(info.isSplit())
            .setReplicaId(info.getReplicaId());
    if (info.getStartKey() != null) {
      b.setStartKey(ByteString.copyFrom(info.getStartKey()));
    }
    if (info.getEndKey() != null) {
      b.setEndKey(ByteString.copyFrom(info.getEndKey()));
    }
    return b.build();
  }

  private static WalKeyProto walKey(WALKey logKey) {
    WalKeyProto.Builder b =
        WalKeyProto.newBuilder()
            .setLogSeqNum(logKey.getSequenceId())
            .setWriteTime(logKey.getWriteTime())
            .setOriginSeqNum(logKey.getOrigLogSeqNum());
    if (logKey.getEncodedRegionName() != null) {
      b.setEncodedRegionName(ByteString.copyFrom(logKey.getEncodedRegionName()));
    }
    if (logKey.getTableName() != null) {
      b.setTableName(ByteString.copyFrom(logKey.getTableName().getName()));
    }
    return b.build();
  }

  private static WalEditProto walEdit(WALEdit logEdit) {
    WalEditProto.Builder b = WalEditProto.newBuilder().setHasReplayMeta(logEdit.isReplay());
    if (logEdit.getCells() != null) {
      for (Cell cell : logEdit.getCells()) {
        b.addCell(CellConverter.toProto(cell));
      }
    }
    return b.build();
  }

  /**
   * Drive one hook call. Mirrors {@link MasterObserverAdapter#dispatch} exactly so policy
   * resolution and STRICT-vs-best-effort behaviour stay symmetric across every adapter.
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
    String detail = "hbasecop: WAL hook " + hookId + " " + message;
    if (pol.policy() == Policy.STRICT) {
      throw cause == null ? new IOException(detail) : new IOException(detail, cause);
    }
    LOG.log(Level.WARNING, "{0} — best-effort, treated as no-op", detail);
    return null;
  }

  private static void applyBypass(
      ObserverContext<? extends WALCoprocessorEnvironment> c, HookResponse resp) {
    if (resp == null) {
      return;
    }
    if (resp.getBypass()) {
      ObserverBypass.tryBypass(c);
    }
  }
}
