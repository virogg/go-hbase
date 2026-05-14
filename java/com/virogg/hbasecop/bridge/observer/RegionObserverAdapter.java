// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.virogg.hbasecop.bridge.config.HookPolicy;
import com.virogg.hbasecop.bridge.config.Policy;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.wire.pb.HookContext;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PostAppendBeforeWALRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostAppendRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostBatchMutateIndispensablyRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostBatchMutateRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostBulkLoadHFileRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCheckAndDeleteRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCheckAndMutateRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCheckAndPutRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCloseRegionOperationRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCloseRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCommitStoreFileRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCompactRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCompactSelectionRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostDeleteRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostExistsRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostFlushRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostGetOpRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostIncrementBeforeWALRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostIncrementRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostInstantiateDeleteTrackerRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostMemStoreCompactionRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostMutationBeforeWALRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostPutRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostReplayWALsRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostScannerCloseRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostScannerFilterRowRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostScannerNextRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostScannerOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostStartRegionOperationRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostStoreFileReaderOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostWALRestoreRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreAppendAfterRowLockRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreAppendRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreBatchMutateRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreBulkLoadHFileRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCheckAndDeleteAfterRowLockRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCheckAndDeleteRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCheckAndMutateAfterRowLockRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCheckAndMutateRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCheckAndPutAfterRowLockRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCheckAndPutRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCloseRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCommitStoreFileRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCompactRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCompactScannerOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCompactSelectionRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreDeleteRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreExistsRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreFlushRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreFlushScannerOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreGetOpRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreIncrementAfterRowLockRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreIncrementRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreMemStoreCompactionCompactRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreMemStoreCompactionCompactScannerOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreMemStoreCompactionRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PrePrepareTimeStampForDeleteVersionRequest;
import com.virogg.hbasecop.bridge.wire.pb.PrePutRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreReplayWALsRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreScannerCloseRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreScannerNextRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreScannerOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreStoreFileReaderOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreStoreScannerOpenRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreWALAppendRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreWALRestoreRequest;
import com.virogg.hbasecop.hbase.v1.ClientProtos.MutationProto;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanOptions;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * RegionServer-side bridge that intercepts every {@link RegionObserver} method and relays the call
 * as a protobuf hook invocation to the long-running Go runtime via a {@link HookDispatcher}.
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
 *
 * <p>T41 surface: this class overrides every {@link RegionObserver} method declared in HBase 2.5.
 * For methods returning a value, the noop default is to return the input value unchanged so the
 * adapter behaves like a passthrough until the user's Go observer wires up a real handler. T42
 * grows the per-hook proto Request bodies + return-value plumbing.
 */
public final class RegionObserverAdapter implements RegionObserver {

  private static final Logger LOG = System.getLogger(RegionObserverAdapter.class.getName());

  /**
   * Hook IDs. Mirror {@code pkg/hbasecop/hooks.go} on the Go side. Kept as public byte constants
   * for backward compatibility with existing Phase-2 tests; new code should use {@link HookId}.
   */
  public static final byte HOOK_PRE_PUT = HookId.PRE_PUT.value();

  public static final byte HOOK_POST_PUT = HookId.POST_PUT.value();

  private final HookDispatcher dispatcher;
  private final PolicyConfig policyConfig;

  public RegionObserverAdapter(HookDispatcher dispatcher, PolicyConfig policyConfig) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.policyConfig = Objects.requireNonNull(policyConfig, "policyConfig");
  }

  // --- Lifecycle ---------------------------------------------------------

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
    dispatchStub(c, HookId.PRE_OPEN, PreOpenRequest.newBuilder());
  }

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    dispatchBestEffort(c, HookId.POST_OPEN, PostOpenRequest.newBuilder());
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
      throws IOException {
    dispatchStub(c, HookId.PRE_CLOSE, PreCloseRequest.newBuilder());
  }

  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
    dispatchBestEffort(c, HookId.POST_CLOSE, PostCloseRequest.newBuilder());
  }

  // --- Flush -------------------------------------------------------------

  @Override
  public void preFlush(
      ObserverContext<RegionCoprocessorEnvironment> c, FlushLifeCycleTracker tracker)
      throws IOException {
    dispatchStub(c, HookId.PRE_FLUSH, PreFlushRequest.newBuilder());
  }

  @Override
  public void preFlushScannerOpen(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Store store,
      ScanOptions options,
      FlushLifeCycleTracker tracker)
      throws IOException {
    dispatchStub(c, HookId.PRE_FLUSH_SCANNER_OPEN, PreFlushScannerOpenRequest.newBuilder());
  }

  @Override
  public void postFlush(
      ObserverContext<RegionCoprocessorEnvironment> c, FlushLifeCycleTracker tracker)
      throws IOException {
    dispatchStub(c, HookId.POST_FLUSH, PostFlushRequest.newBuilder());
  }

  // --- MemStore compaction ----------------------------------------------

  @Override
  public void preMemStoreCompaction(ObserverContext<RegionCoprocessorEnvironment> c, Store store)
      throws IOException {
    dispatchStub(c, HookId.PRE_MEM_STORE_COMPACTION, PreMemStoreCompactionRequest.newBuilder());
  }

  @Override
  public void preMemStoreCompactionCompactScannerOpen(
      ObserverContext<RegionCoprocessorEnvironment> c, Store store, ScanOptions options)
      throws IOException {
    dispatchStub(
        c,
        HookId.PRE_MEM_STORE_COMPACTION_COMPACT_SCANNER_OPEN,
        PreMemStoreCompactionCompactScannerOpenRequest.newBuilder());
  }

  @Override
  public InternalScanner preMemStoreCompactionCompact(
      ObserverContext<RegionCoprocessorEnvironment> c, Store store, InternalScanner scanner)
      throws IOException {
    dispatchStub(
        c,
        HookId.PRE_MEM_STORE_COMPACTION_COMPACT,
        PreMemStoreCompactionCompactRequest.newBuilder());
    return scanner;
  }

  @Override
  public void postMemStoreCompaction(ObserverContext<RegionCoprocessorEnvironment> c, Store store)
      throws IOException {
    dispatchStub(c, HookId.POST_MEM_STORE_COMPACTION, PostMemStoreCompactionRequest.newBuilder());
  }

  // --- Compaction --------------------------------------------------------

  @Override
  public void preCompactSelection(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Store store,
      List<? extends StoreFile> candidates,
      CompactionLifeCycleTracker tracker)
      throws IOException {
    dispatchStub(c, HookId.PRE_COMPACT_SELECTION, PreCompactSelectionRequest.newBuilder());
  }

  @Override
  public void postCompactSelection(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Store store,
      List<? extends StoreFile> selected,
      CompactionLifeCycleTracker tracker,
      CompactionRequest request) {
    dispatchBestEffort(c, HookId.POST_COMPACT_SELECTION, PostCompactSelectionRequest.newBuilder());
  }

  @Override
  public void preCompactScannerOpen(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Store store,
      ScanType scanType,
      ScanOptions options,
      CompactionLifeCycleTracker tracker,
      CompactionRequest request)
      throws IOException {
    dispatchStub(c, HookId.PRE_COMPACT_SCANNER_OPEN, PreCompactScannerOpenRequest.newBuilder());
  }

  @Override
  public InternalScanner preCompact(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Store store,
      InternalScanner scanner,
      ScanType scanType,
      CompactionLifeCycleTracker tracker,
      CompactionRequest request)
      throws IOException {
    dispatchStub(c, HookId.PRE_COMPACT, PreCompactRequest.newBuilder());
    return scanner;
  }

  @Override
  public void postCompact(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Store store,
      StoreFile resultFile,
      CompactionLifeCycleTracker tracker,
      CompactionRequest request)
      throws IOException {
    dispatchStub(c, HookId.POST_COMPACT, PostCompactRequest.newBuilder());
  }

  // --- Read path (T42 Wave 1: bodies populated) -------------------------

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreGetOpRequest.newBuilder()
            .setCtx(hookCtx)
            .setGet(GetConverter.toProto(get))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_GET_OP.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  @Override
  public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<Cell> result)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostGetOpRequest.Builder b =
        PostGetOpRequest.newBuilder().setCtx(hookCtx).setGet(GetConverter.toProto(get));
    if (result != null) {
      for (Cell cell : result) {
        b.addResult(CellConverter.toProto(cell));
      }
    }
    HookResponse resp = dispatch(HookId.POST_GET_OP.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreExistsRequest.newBuilder()
            .setCtx(hookCtx)
            .setGet(GetConverter.toProto(get))
            .setExists(exists)
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_EXISTS.value(), reqBytes);
    applyHookResponse(c, resp);
    return exists;
  }

  @Override
  public boolean postExists(
      ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists) throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostExistsRequest.newBuilder()
            .setCtx(hookCtx)
            .setGet(GetConverter.toProto(get))
            .setExists(exists)
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.POST_EXISTS.value(), reqBytes);
    applyHookResponse(c, resp);
    return exists;
  }

  // --- Write path — Put (Phase-2 frozen contract) -----------------------

  @Override
  public void prePut(
      ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    MutationProto mutation = MutationConverter.toProto(put);
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
    MutationProto mutation = MutationConverter.toProto(put);
    byte[] reqBytes =
        PostPutRequest.newBuilder().setCtx(hookCtx).setMutation(mutation).build().toByteArray();
    HookResponse resp = dispatch(HOOK_POST_PUT, reqBytes);
    applyHookResponse(c, resp);
  }

  // --- Write path — Delete + version timestamp --------------------------

  @Override
  public void preDelete(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Delete delete,
      WALEdit edit,
      Durability durability)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreDeleteRequest.newBuilder()
            .setCtx(hookCtx)
            .setMutation(MutationConverter.toProto(delete))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_DELETE.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  @Override
  public void postDelete(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Delete delete,
      WALEdit edit,
      Durability durability)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostDeleteRequest.newBuilder()
            .setCtx(hookCtx)
            .setMutation(MutationConverter.toProto(delete))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.POST_DELETE.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  @Override
  public void prePrepareTimeStampForDeleteVersion(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Mutation mutation,
      Cell cell,
      byte[] byteNow,
      Get get)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PrePrepareTimeStampForDeleteVersionRequest.Builder b =
        PrePrepareTimeStampForDeleteVersionRequest.newBuilder()
            .setCtx(hookCtx)
            .setMutation(MutationConverter.toProto(mutation))
            .setCell(CellConverter.toProto(cell));
    if (byteNow != null) {
      b.setByteNow(ByteString.copyFrom(byteNow));
    }
    if (get != null) {
      b.setGet(GetConverter.toProto(get));
    }
    HookResponse resp =
        dispatch(HookId.PRE_PREPARE_TIME_STAMP_FOR_DELETE_VERSION.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  // --- Batch mutate + region operation envelope -------------------------

  @Override
  public void preBatchMutate(
      ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatch)
      throws IOException {
    dispatchStub(c, HookId.PRE_BATCH_MUTATE, PreBatchMutateRequest.newBuilder());
  }

  @Override
  public void postBatchMutate(
      ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatch)
      throws IOException {
    dispatchStub(c, HookId.POST_BATCH_MUTATE, PostBatchMutateRequest.newBuilder());
  }

  @Override
  public void postBatchMutateIndispensably(
      ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatch,
      boolean success)
      throws IOException {
    dispatchStub(
        c,
        HookId.POST_BATCH_MUTATE_INDISPENSABLY,
        PostBatchMutateIndispensablyRequest.newBuilder());
  }

  @Override
  public void postStartRegionOperation(
      ObserverContext<RegionCoprocessorEnvironment> c, Region.Operation op) throws IOException {
    dispatchStub(
        c, HookId.POST_START_REGION_OPERATION, PostStartRegionOperationRequest.newBuilder());
  }

  @Override
  public void postCloseRegionOperation(
      ObserverContext<RegionCoprocessorEnvironment> c, Region.Operation op) throws IOException {
    dispatchStub(
        c, HookId.POST_CLOSE_REGION_OPERATION, PostCloseRegionOperationRequest.newBuilder());
  }

  // --- Check-and-Put -----------------------------------------------------

  @Override
  public boolean preCheckAndPut(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOperator op,
      ByteArrayComparable comparator,
      Put put,
      boolean result)
      throws IOException {
    dispatchStub(c, HookId.PRE_CHECK_AND_PUT, PreCheckAndPutRequest.newBuilder());
    return result;
  }

  @Override
  public boolean postCheckAndPut(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOperator op,
      ByteArrayComparable comparator,
      Put put,
      boolean result)
      throws IOException {
    dispatchStub(c, HookId.POST_CHECK_AND_PUT, PostCheckAndPutRequest.newBuilder());
    return result;
  }

  @Override
  public boolean preCheckAndPutAfterRowLock(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOperator op,
      ByteArrayComparable comparator,
      Put put,
      boolean result)
      throws IOException {
    dispatchStub(
        c, HookId.PRE_CHECK_AND_PUT_AFTER_ROW_LOCK, PreCheckAndPutAfterRowLockRequest.newBuilder());
    return result;
  }

  // --- Check-and-Delete --------------------------------------------------

  @Override
  public boolean preCheckAndDelete(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOperator op,
      ByteArrayComparable comparator,
      Delete delete,
      boolean result)
      throws IOException {
    dispatchStub(c, HookId.PRE_CHECK_AND_DELETE, PreCheckAndDeleteRequest.newBuilder());
    return result;
  }

  @Override
  public boolean postCheckAndDelete(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOperator op,
      ByteArrayComparable comparator,
      Delete delete,
      boolean result)
      throws IOException {
    dispatchStub(c, HookId.POST_CHECK_AND_DELETE, PostCheckAndDeleteRequest.newBuilder());
    return result;
  }

  @Override
  public boolean preCheckAndDeleteAfterRowLock(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOperator op,
      ByteArrayComparable comparator,
      Delete delete,
      boolean result)
      throws IOException {
    dispatchStub(
        c,
        HookId.PRE_CHECK_AND_DELETE_AFTER_ROW_LOCK,
        PreCheckAndDeleteAfterRowLockRequest.newBuilder());
    return result;
  }

  // --- Check-and-Mutate --------------------------------------------------

  @Override
  public CheckAndMutateResult preCheckAndMutate(
      ObserverContext<RegionCoprocessorEnvironment> c,
      CheckAndMutate checkAndMutate,
      CheckAndMutateResult result)
      throws IOException {
    dispatchStub(c, HookId.PRE_CHECK_AND_MUTATE, PreCheckAndMutateRequest.newBuilder());
    return result;
  }

  @Override
  public CheckAndMutateResult postCheckAndMutate(
      ObserverContext<RegionCoprocessorEnvironment> c,
      CheckAndMutate checkAndMutate,
      CheckAndMutateResult result)
      throws IOException {
    dispatchStub(c, HookId.POST_CHECK_AND_MUTATE, PostCheckAndMutateRequest.newBuilder());
    return result;
  }

  @Override
  public CheckAndMutateResult preCheckAndMutateAfterRowLock(
      ObserverContext<RegionCoprocessorEnvironment> c,
      CheckAndMutate checkAndMutate,
      CheckAndMutateResult result)
      throws IOException {
    dispatchStub(
        c,
        HookId.PRE_CHECK_AND_MUTATE_AFTER_ROW_LOCK,
        PreCheckAndMutateAfterRowLockRequest.newBuilder());
    return result;
  }

  // --- Append (T42 Wave 2: bodies populated) ----------------------------

  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreAppendRequest.newBuilder()
            .setCtx(hookCtx)
            .setAppend(MutationConverter.toProto(append))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_APPEND.value(), reqBytes);
    applyHookResponse(c, resp);
    return null;
  }

  @Override
  public Result postAppend(
      ObserverContext<RegionCoprocessorEnvironment> c, Append append, Result result)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostAppendRequest.Builder b =
        PostAppendRequest.newBuilder().setCtx(hookCtx).setAppend(MutationConverter.toProto(append));
    if (result != null) {
      b.setResult(ResultConverter.toProto(result));
    }
    HookResponse resp = dispatch(HookId.POST_APPEND.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
    return result;
  }

  @Override
  public Result preAppendAfterRowLock(
      ObserverContext<RegionCoprocessorEnvironment> c, Append append) throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreAppendAfterRowLockRequest.newBuilder()
            .setCtx(hookCtx)
            .setAppend(MutationConverter.toProto(append))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_APPEND_AFTER_ROW_LOCK.value(), reqBytes);
    applyHookResponse(c, resp);
    return null;
  }

  // --- Increment (T42 Wave 2: bodies populated) -------------------------

  @Override
  public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreIncrementRequest.newBuilder()
            .setCtx(hookCtx)
            .setIncrement(MutationConverter.toProto(increment))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_INCREMENT.value(), reqBytes);
    applyHookResponse(c, resp);
    return null;
  }

  @Override
  public Result postIncrement(
      ObserverContext<RegionCoprocessorEnvironment> c, Increment increment, Result result)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostIncrementRequest.Builder b =
        PostIncrementRequest.newBuilder()
            .setCtx(hookCtx)
            .setIncrement(MutationConverter.toProto(increment));
    if (result != null) {
      b.setResult(ResultConverter.toProto(result));
    }
    HookResponse resp = dispatch(HookId.POST_INCREMENT.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
    return result;
  }

  @Override
  public Result preIncrementAfterRowLock(
      ObserverContext<RegionCoprocessorEnvironment> c, Increment increment) throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreIncrementAfterRowLockRequest.newBuilder()
            .setCtx(hookCtx)
            .setIncrement(MutationConverter.toProto(increment))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_INCREMENT_AFTER_ROW_LOCK.value(), reqBytes);
    applyHookResponse(c, resp);
    return null;
  }

  // --- Scanner (T42 Wave 1: bodies populated) ---------------------------

  @Override
  public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreScannerOpenRequest.newBuilder()
            .setCtx(hookCtx)
            .setScan(ScanConverter.toProto(scan))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_SCANNER_OPEN.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  @Override
  public RegionScanner postScannerOpen(
      ObserverContext<RegionCoprocessorEnvironment> c, Scan scan, RegionScanner scanner)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostScannerOpenRequest.newBuilder()
            .setCtx(hookCtx)
            .setScan(ScanConverter.toProto(scan))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.POST_SCANNER_OPEN.value(), reqBytes);
    applyHookResponse(c, resp);
    return scanner;
  }

  @Override
  public boolean preScannerNext(
      ObserverContext<RegionCoprocessorEnvironment> c,
      InternalScanner scanner,
      List<Result> result,
      int limit,
      boolean hasMore)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreScannerNextRequest.newBuilder()
            .setCtx(hookCtx)
            .setLimit(limit)
            .setHasMore(hasMore)
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_SCANNER_NEXT.value(), reqBytes);
    applyHookResponse(c, resp);
    return hasMore;
  }

  @Override
  public boolean postScannerNext(
      ObserverContext<RegionCoprocessorEnvironment> c,
      InternalScanner scanner,
      List<Result> result,
      int limit,
      boolean hasMore)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostScannerNextRequest.Builder b =
        PostScannerNextRequest.newBuilder().setCtx(hookCtx).setLimit(limit).setHasMore(hasMore);
    if (result != null) {
      for (Result r : result) {
        b.addResult(ResultConverter.toProto(r));
      }
    }
    HookResponse resp = dispatch(HookId.POST_SCANNER_NEXT.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
    return hasMore;
  }

  @Override
  public boolean postScannerFilterRow(
      ObserverContext<RegionCoprocessorEnvironment> c,
      InternalScanner scanner,
      Cell currentRow,
      boolean hasMore)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostScannerFilterRowRequest.newBuilder()
            .setCtx(hookCtx)
            .setCurrentRow(CellConverter.toProto(currentRow))
            .setHasMore(hasMore)
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.POST_SCANNER_FILTER_ROW.value(), reqBytes);
    applyHookResponse(c, resp);
    return hasMore;
  }

  @Override
  public void preScannerClose(
      ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner scanner) throws IOException {
    dispatchStub(c, HookId.PRE_SCANNER_CLOSE, PreScannerCloseRequest.newBuilder());
  }

  @Override
  public void postScannerClose(
      ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner scanner) throws IOException {
    dispatchStub(c, HookId.POST_SCANNER_CLOSE, PostScannerCloseRequest.newBuilder());
  }

  @Override
  public void preStoreScannerOpen(
      ObserverContext<RegionCoprocessorEnvironment> c, Store store, ScanOptions options)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreStoreScannerOpenRequest.newBuilder()
            .setCtx(hookCtx)
            .setColumnFamily(
                store == null
                    ? com.google.protobuf.ByteString.EMPTY
                    : com.google.protobuf.ByteString.copyFrom(
                        store.getColumnFamilyDescriptor().getName()))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(HookId.PRE_STORE_SCANNER_OPEN.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  // --- WAL replay/restore -----------------------------------------------

  @Override
  public void preReplayWALs(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      RegionInfo info,
      org.apache.hadoop.fs.Path edits)
      throws IOException {
    dispatchStub(c, HookId.PRE_REPLAY_WA_LS, PreReplayWALsRequest.newBuilder());
  }

  @Override
  public void postReplayWALs(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      RegionInfo info,
      org.apache.hadoop.fs.Path edits)
      throws IOException {
    dispatchStub(c, HookId.POST_REPLAY_WA_LS, PostReplayWALsRequest.newBuilder());
  }

  @Override
  public void preWALRestore(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      RegionInfo info,
      WALKey logKey,
      WALEdit logEdit)
      throws IOException {
    dispatchStub(c, HookId.PRE_WAL_RESTORE, PreWALRestoreRequest.newBuilder());
  }

  @Override
  public void postWALRestore(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      RegionInfo info,
      WALKey logKey,
      WALEdit logEdit)
      throws IOException {
    dispatchStub(c, HookId.POST_WAL_RESTORE, PostWALRestoreRequest.newBuilder());
  }

  // --- Bulk load + store-file commit ------------------------------------

  @Override
  public void preBulkLoadHFile(
      ObserverContext<RegionCoprocessorEnvironment> c, List<Pair<byte[], String>> familyPaths)
      throws IOException {
    dispatchStub(c, HookId.PRE_BULK_LOAD_H_FILE, PreBulkLoadHFileRequest.newBuilder());
  }

  @Override
  public void postBulkLoadHFile(
      ObserverContext<RegionCoprocessorEnvironment> c,
      List<Pair<byte[], String>> stagingFamilyPaths,
      Map<byte[], List<org.apache.hadoop.fs.Path>> finalPaths)
      throws IOException {
    dispatchStub(c, HookId.POST_BULK_LOAD_H_FILE, PostBulkLoadHFileRequest.newBuilder());
  }

  @Override
  public void preCommitStoreFile(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] family,
      List<Pair<org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path>> pairs)
      throws IOException {
    dispatchStub(c, HookId.PRE_COMMIT_STORE_FILE, PreCommitStoreFileRequest.newBuilder());
  }

  @Override
  public void postCommitStoreFile(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] family,
      org.apache.hadoop.fs.Path srcPath,
      org.apache.hadoop.fs.Path dstPath)
      throws IOException {
    dispatchStub(c, HookId.POST_COMMIT_STORE_FILE, PostCommitStoreFileRequest.newBuilder());
  }

  // --- Store-file reader -------------------------------------------------

  @Override
  public StoreFileReader preStoreFileReaderOpen(
      ObserverContext<RegionCoprocessorEnvironment> c,
      FileSystem fs,
      org.apache.hadoop.fs.Path p,
      FSDataInputStreamWrapper in,
      long size,
      CacheConfig cacheConf,
      Reference r,
      StoreFileReader reader)
      throws IOException {
    dispatchStub(c, HookId.PRE_STORE_FILE_READER_OPEN, PreStoreFileReaderOpenRequest.newBuilder());
    return reader;
  }

  @Override
  public StoreFileReader postStoreFileReaderOpen(
      ObserverContext<RegionCoprocessorEnvironment> c,
      FileSystem fs,
      org.apache.hadoop.fs.Path p,
      FSDataInputStreamWrapper in,
      long size,
      CacheConfig cacheConf,
      Reference r,
      StoreFileReader reader)
      throws IOException {
    dispatchStub(
        c, HookId.POST_STORE_FILE_READER_OPEN, PostStoreFileReaderOpenRequest.newBuilder());
    return reader;
  }

  // --- Before-WAL hooks (T42 Wave 2: bodies populated) ------------------

  @Override
  public Cell postMutationBeforeWAL(
      ObserverContext<RegionCoprocessorEnvironment> c,
      RegionObserver.MutationType opType,
      Mutation mutation,
      Cell oldCell,
      Cell newCell)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostMutationBeforeWALRequest.Builder b =
        PostMutationBeforeWALRequest.newBuilder()
            .setCtx(hookCtx)
            .setMutationType(opType == null ? 0 : opType.ordinal())
            .setMutation(MutationConverter.toProto(mutation));
    if (oldCell != null) {
      b.setOldCell(CellConverter.toProto(oldCell));
    }
    if (newCell != null) {
      b.setNewCell(CellConverter.toProto(newCell));
    }
    HookResponse resp = dispatch(HookId.POST_MUTATION_BEFORE_WAL.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
    return newCell;
  }

  @Override
  public List<Pair<Cell, Cell>> postIncrementBeforeWAL(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Mutation mutation,
      List<Pair<Cell, Cell>> cellPairs)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostIncrementBeforeWALRequest.Builder b =
        PostIncrementBeforeWALRequest.newBuilder()
            .setCtx(hookCtx)
            .setMutation(MutationConverter.toProto(mutation));
    if (cellPairs != null) {
      for (Pair<Cell, Cell> p : cellPairs) {
        b.addCellPair(buildCellPair(p));
      }
    }
    HookResponse resp = dispatch(HookId.POST_INCREMENT_BEFORE_WAL.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
    return cellPairs;
  }

  @Override
  public List<Pair<Cell, Cell>> postAppendBeforeWAL(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Mutation mutation,
      List<Pair<Cell, Cell>> cellPairs)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostAppendBeforeWALRequest.Builder b =
        PostAppendBeforeWALRequest.newBuilder()
            .setCtx(hookCtx)
            .setMutation(MutationConverter.toProto(mutation));
    if (cellPairs != null) {
      for (Pair<Cell, Cell> p : cellPairs) {
        b.addCellPair(buildCellPair(p));
      }
    }
    HookResponse resp = dispatch(HookId.POST_APPEND_BEFORE_WAL.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
    return cellPairs;
  }

  private static com.virogg.hbasecop.bridge.wire.pb.CellPair buildCellPair(Pair<Cell, Cell> p) {
    com.virogg.hbasecop.bridge.wire.pb.CellPair.Builder b =
        com.virogg.hbasecop.bridge.wire.pb.CellPair.newBuilder();
    if (p.getFirst() != null) {
      b.setFirst(CellConverter.toProto(p.getFirst()));
    }
    if (p.getSecond() != null) {
      b.setSecond(CellConverter.toProto(p.getSecond()));
    }
    return b.build();
  }

  // --- Delete tracker, WAL append ---------------------------------------

  @Override
  public DeleteTracker postInstantiateDeleteTracker(
      ObserverContext<RegionCoprocessorEnvironment> c, DeleteTracker tracker) throws IOException {
    dispatchStub(
        c,
        HookId.POST_INSTANTIATE_DELETE_TRACKER,
        PostInstantiateDeleteTrackerRequest.newBuilder());
    return tracker;
  }

  @Override
  public void preWALAppend(
      ObserverContext<RegionCoprocessorEnvironment> c, WALKey key, WALEdit edit)
      throws IOException {
    dispatchStub(c, HookId.PRE_WAL_APPEND, PreWALAppendRequest.newBuilder());
  }

  // === Internals =========================================================

  /**
   * Stub dispatch helper for hooks whose Request body is just {@link HookContext} — every T41 stub
   * Request message in {@code proto/hooks.proto} embeds {@link HookContext} at field 1, so we set
   * it via the field descriptor on the generic {@link Message.Builder}. T42 will widen each Request
   * with hook-specific payload fields; the call sites with specialised builders (currently {@link
   * #prePut} / {@link #postPut}) demonstrate the pattern.
   */
  private void dispatchStub(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      HookId hookId,
      Message.Builder builder)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    Descriptors.FieldDescriptor ctxField = builder.getDescriptorForType().findFieldByNumber(1);
    builder.setField(ctxField, hookCtx);
    HookResponse resp = dispatch(hookId.value(), builder.build().toByteArray());
    applyHookResponse(c, resp);
  }

  /**
   * Non-throwing variant for post-hooks declared in HBase as {@code void postX(...)} without {@code
   * throws IOException}. A strict-mode failure is logged at WARN and swallowed so the adapter's
   * signature stays compatible with the interface; observers needing hard failure on these hooks
   * should use a Pre-* variant.
   */
  private void dispatchBestEffort(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      HookId hookId,
      Message.Builder builder) {
    try {
      dispatchStub(c, hookId, builder);
    } catch (IOException e) {
      LOG.log(
          Level.WARNING,
          "hbasecop: post-open/close hook {0} threw IOException, swallowed (best-effort)",
          hookId.methodName(),
          e);
    }
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
      ObserverContext<? extends RegionCoprocessorEnvironment> c, HookResponse resp) {
    if (resp == null) {
      return;
    }
    if (resp.getBypass()) {
      c.bypass();
    }
  }

  private static HookContext buildHookContext(
      ObserverContext<? extends RegionCoprocessorEnvironment> c) {
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
