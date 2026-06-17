// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

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
import com.virogg.hbasecop.hbase.v1.CellProtos;
import com.virogg.hbasecop.hbase.v1.ClientProtos.MutationProto;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
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
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

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
 * skips its own implementation; only honoured when the hook returned a clean response.
 *
 * <p>T41 surface: overrides every {@link RegionObserver} method declared in HBase 2.5. Value-
 * returning methods default to passthrough (return the input unchanged) until the user's Go
 * observer wires up a real handler. T42 grows the per-hook proto Request bodies and return-value
 * plumbing.
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
  private final com.virogg.hbasecop.multiplex.RegionIdAllocator regionIdAllocator;

  public RegionObserverAdapter(HookDispatcher dispatcher, PolicyConfig policyConfig) {
    this(dispatcher, policyConfig, new com.virogg.hbasecop.multiplex.RegionIdAllocator());
  }

  /**
   * T61 ctor: inject a shared {@link com.virogg.hbasecop.multiplex.RegionIdAllocator} so multiple
   * adapter instances on one RegionServer share the same region_id space (T63 lifecycle refcount).
   */
  public RegionObserverAdapter(
      HookDispatcher dispatcher,
      PolicyConfig policyConfig,
      com.virogg.hbasecop.multiplex.RegionIdAllocator regionIdAllocator) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    this.policyConfig = Objects.requireNonNull(policyConfig, "policyConfig");
    this.regionIdAllocator = Objects.requireNonNull(regionIdAllocator, "regionIdAllocator");
  }

  // --- Region lifecycle: region_id allocation (T61) ---------------------
  //
  // RegionObserver fires preOpen/postClose per region open/close: the right
  // granularity for the per-region wire routing key. Mint the id in preOpen
  // (so every subsequent hook on this region carries a non-zero region_id),
  // release in postClose (reopen gets a fresh monotonic id, never recycled).
  // Coprocessor.start(env) on the supertype fires once per RegionServer load,
  // so it can't scope per-region.

  /**
   * Public hook for the supervisor/tests to register a region without going through {@link
   * #preOpen(ObserverContext)} (e.g. T61 unit tests skip the preOpen dispatch round-trip and just
   * want the allocator wired). Idempotent.
   */
  public void start(RegionCoprocessorEnvironment env) {
    Objects.requireNonNull(env, "env");
    regionIdAllocator.allocate(env.getRegion().getRegionInfo().getEncodedName());
  }

  /** Counterpart of {@link #start(RegionCoprocessorEnvironment)}. */
  public void stop(RegionCoprocessorEnvironment env) {
    Objects.requireNonNull(env, "env");
    regionIdAllocator.release(env.getRegion().getRegionInfo().getEncodedName());
  }

  // --- Lifecycle ---------------------------------------------------------

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
    // T61: allocate the wire region_id before dispatching so the preOpen
    // frame and every subsequent hook on this region share the same id.
    regionIdAllocator.allocate(c.getEnvironment().getRegion().getRegionInfo().getEncodedName());
    dispatchStub(c, HookId.PRE_OPEN, PreOpenRequest.newBuilder());
  }

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    dispatchBestEffort(c, HookId.POST_OPEN, PostOpenRequest.newBuilder());
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreCloseRequest.newBuilder()
            .setCtx(hookCtx)
            .setAbortRequested(abortRequested)
            .build()
            .toByteArray();
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_CLOSE.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
    HookContext hookCtx = buildHookContext(c);
    try {
      PostCloseRequest req =
          PostCloseRequest.newBuilder().setCtx(hookCtx).setAbortRequested(abortRequested).build();
      HookResponse resp = dispatch(regionIdFor(c), HookId.POST_CLOSE.value(), req.toByteArray());
      applyHookResponse(c, resp);
    } catch (IOException e) {
      LOG.log(Level.WARNING, "hbasecop: postClose threw IOException, swallowed (best-effort)", e);
    } finally {
      // T61: release the region_id mapping last, so postClose's own frame
      // is dispatched under the same id as the rest of the region's lifecycle.
      regionIdAllocator.release(c.getEnvironment().getRegion().getRegionInfo().getEncodedName());
    }
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

  // --- Compaction (T42 Wave 4: bodies populated) -----------------------

  @Override
  public void preCompactSelection(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Store store,
      List<? extends StoreFile> candidates,
      CompactionLifeCycleTracker tracker)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PreCompactSelectionRequest.Builder b =
        PreCompactSelectionRequest.newBuilder().setCtx(hookCtx).setColumnFamily(familyOf(store));
    addStoreFiles(b::addCandidate, candidates);
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_COMPACT_SELECTION.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public void postCompactSelection(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Store store,
      List<? extends StoreFile> selected,
      CompactionLifeCycleTracker tracker,
      CompactionRequest request) {
    HookContext hookCtx = buildHookContext(c);
    try {
      PostCompactSelectionRequest.Builder b =
          PostCompactSelectionRequest.newBuilder().setCtx(hookCtx).setColumnFamily(familyOf(store));
      addStoreFiles(b::addSelected, selected);
      b.setRequest(compactionSummary(request));
      HookResponse resp =
          dispatch(regionIdFor(c), HookId.POST_COMPACT_SELECTION.value(), b.build().toByteArray());
      applyHookResponse(c, resp);
    } catch (IOException e) {
      LOG.log(
          Level.WARNING,
          "hbasecop: postCompactSelection threw IOException, swallowed (best-effort)",
          e);
    }
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
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreCompactScannerOpenRequest.newBuilder()
            .setCtx(hookCtx)
            .setColumnFamily(familyOf(store))
            .setScanType(scanType == null ? -1 : scanType.ordinal())
            .setRequest(compactionSummary(request))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_COMPACT_SCANNER_OPEN.value(), reqBytes);
    applyHookResponse(c, resp);
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
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreCompactRequest.newBuilder()
            .setCtx(hookCtx)
            .setColumnFamily(familyOf(store))
            .setScanType(scanType == null ? -1 : scanType.ordinal())
            .setRequest(compactionSummary(request))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_COMPACT.value(), reqBytes);
    applyHookResponse(c, resp);
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
    HookContext hookCtx = buildHookContext(c);
    PostCompactRequest.Builder b =
        PostCompactRequest.newBuilder()
            .setCtx(hookCtx)
            .setColumnFamily(familyOf(store))
            .setRequest(compactionSummary(request));
    if (resultFile != null) {
      b.setResultFile(storeFilePath(resultFile));
    }
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_COMPACT.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  private static ByteString familyOf(Store store) {
    if (store == null || store.getColumnFamilyDescriptor() == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(store.getColumnFamilyDescriptor().getName());
  }

  private static com.virogg.hbasecop.bridge.wire.pb.StoreFilePathProto storeFilePath(StoreFile f) {
    com.virogg.hbasecop.bridge.wire.pb.StoreFilePathProto.Builder b =
        com.virogg.hbasecop.bridge.wire.pb.StoreFilePathProto.newBuilder();
    if (f.getPath() != null) {
      b.setPath(f.getPath().toString());
    }
    // StoreFile (2.5 interface) doesn't expose file-byte size; size_bytes stays 0.
    // T46 can switch to HFileInfo lookup when needed.
    return b.build();
  }

  private static void addStoreFiles(
      java.util.function.Consumer<com.virogg.hbasecop.bridge.wire.pb.StoreFilePathProto> add,
      List<? extends StoreFile> files) {
    if (files == null) {
      return;
    }
    for (StoreFile f : files) {
      if (f == null) {
        continue;
      }
      add.accept(storeFilePath(f));
    }
  }

  private static com.virogg.hbasecop.bridge.wire.pb.CompactionRequestSummary compactionSummary(
      CompactionRequest request) {
    com.virogg.hbasecop.bridge.wire.pb.CompactionRequestSummary.Builder b =
        com.virogg.hbasecop.bridge.wire.pb.CompactionRequestSummary.newBuilder();
    if (request == null) {
      return b.build();
    }
    return b.setIsMajor(request.isMajor())
        .setIsAllFiles(request.isAllFiles())
        .setSize(request.getSize())
        .setSelectionTime(request.getSelectionTime())
        .build();
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_GET_OP.value(), reqBytes);
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
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_GET_OP.value(), b.build().toByteArray());
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_EXISTS.value(), reqBytes);
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.POST_EXISTS.value(), reqBytes);
    applyHookResponse(c, resp);
    return exists;
  }

  // --- Write path: Put (Phase-2 frozen contract) ------------------------

  @Override
  public void prePut(
      ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    MutationProto mutation = MutationConverter.toProto(put);
    byte[] reqBytes =
        PrePutRequest.newBuilder().setCtx(hookCtx).setMutation(mutation).build().toByteArray();
    HookResponse resp = dispatch(regionIdFor(c), HOOK_PRE_PUT, reqBytes);
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
    HookResponse resp = dispatch(regionIdFor(c), HOOK_POST_PUT, reqBytes);
    applyHookResponse(c, resp);
  }

  // --- Write path: Delete + version timestamp ---------------------------

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
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_DELETE.value(), reqBytes);
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.POST_DELETE.value(), reqBytes);
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
        dispatch(
            regionIdFor(c),
            HookId.PRE_PREPARE_TIME_STAMP_FOR_DELETE_VERSION.value(),
            b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  // --- Batch mutate + region operation envelope (T42 Wave 3) -----------

  @Override
  public void preBatchMutate(
      ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatch)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PreBatchMutateRequest.Builder b = PreBatchMutateRequest.newBuilder().setCtx(hookCtx);
    addMiniBatchOperations(b::addOperation, miniBatch);
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_BATCH_MUTATE.value(), b.build().toByteArray());
    applyBatchHookResponse(c, miniBatch, resp);
  }

  @Override
  public void postBatchMutate(
      ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatch)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostBatchMutateRequest.Builder b = PostBatchMutateRequest.newBuilder().setCtx(hookCtx);
    addMiniBatchOperations(b::addOperation, miniBatch);
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_BATCH_MUTATE.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public void postBatchMutateIndispensably(
      ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatch,
      boolean success)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostBatchMutateIndispensablyRequest.Builder b =
        PostBatchMutateIndispensablyRequest.newBuilder().setCtx(hookCtx).setSuccess(success);
    addMiniBatchOperations(b::addOperation, miniBatch);
    HookResponse resp =
        dispatch(
            regionIdFor(c),
            HookId.POST_BATCH_MUTATE_INDISPENSABLY.value(),
            b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public void postStartRegionOperation(
      ObserverContext<RegionCoprocessorEnvironment> c, Region.Operation op) throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostStartRegionOperationRequest.newBuilder()
            .setCtx(hookCtx)
            .setOperation(op == null ? -1 : op.ordinal())
            .build()
            .toByteArray();
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_START_REGION_OPERATION.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  @Override
  public void postCloseRegionOperation(
      ObserverContext<RegionCoprocessorEnvironment> c, Region.Operation op) throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostCloseRegionOperationRequest.newBuilder()
            .setCtx(hookCtx)
            .setOperation(op == null ? -1 : op.ordinal())
            .build()
            .toByteArray();
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_CLOSE_REGION_OPERATION.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  // --- Check-and-Put (T42 Wave 3) --------------------------------------

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
    HookContext hookCtx = buildHookContext(c);
    PreCheckAndPutRequest.Builder b =
        PreCheckAndPutRequest.newBuilder()
            .setCtx(hookCtx)
            .setRow(ByteString.copyFrom(row))
            .setFamily(ByteString.copyFrom(family))
            .setQualifier(ByteString.copyFrom(qualifier))
            .setCompareOp(compareOpOrdinal(op))
            .setComparator(ComparatorConverter.toProto(comparator))
            .setPut(MutationConverter.toProto(put))
            .setInputResult(result);
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_CHECK_AND_PUT.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
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
    HookContext hookCtx = buildHookContext(c);
    PostCheckAndPutRequest.Builder b =
        PostCheckAndPutRequest.newBuilder()
            .setCtx(hookCtx)
            .setRow(ByteString.copyFrom(row))
            .setFamily(ByteString.copyFrom(family))
            .setQualifier(ByteString.copyFrom(qualifier))
            .setCompareOp(compareOpOrdinal(op))
            .setComparator(ComparatorConverter.toProto(comparator))
            .setPut(MutationConverter.toProto(put))
            .setInputResult(result);
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_CHECK_AND_PUT.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
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
    HookContext hookCtx = buildHookContext(c);
    PreCheckAndPutAfterRowLockRequest.Builder b =
        PreCheckAndPutAfterRowLockRequest.newBuilder()
            .setCtx(hookCtx)
            .setRow(ByteString.copyFrom(row))
            .setFamily(ByteString.copyFrom(family))
            .setQualifier(ByteString.copyFrom(qualifier))
            .setCompareOp(compareOpOrdinal(op))
            .setComparator(ComparatorConverter.toProto(comparator))
            .setPut(MutationConverter.toProto(put))
            .setInputResult(result);
    HookResponse resp =
        dispatch(
            regionIdFor(c),
            HookId.PRE_CHECK_AND_PUT_AFTER_ROW_LOCK.value(),
            b.build().toByteArray());
    applyHookResponse(c, resp);
    return result;
  }

  // --- Check-and-Delete (T42 Wave 3) -----------------------------------

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
    HookContext hookCtx = buildHookContext(c);
    PreCheckAndDeleteRequest.Builder b =
        PreCheckAndDeleteRequest.newBuilder()
            .setCtx(hookCtx)
            .setRow(ByteString.copyFrom(row))
            .setFamily(ByteString.copyFrom(family))
            .setQualifier(ByteString.copyFrom(qualifier))
            .setCompareOp(compareOpOrdinal(op))
            .setComparator(ComparatorConverter.toProto(comparator))
            .setDelete(MutationConverter.toProto(delete))
            .setInputResult(result);
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_CHECK_AND_DELETE.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
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
    HookContext hookCtx = buildHookContext(c);
    PostCheckAndDeleteRequest.Builder b =
        PostCheckAndDeleteRequest.newBuilder()
            .setCtx(hookCtx)
            .setRow(ByteString.copyFrom(row))
            .setFamily(ByteString.copyFrom(family))
            .setQualifier(ByteString.copyFrom(qualifier))
            .setCompareOp(compareOpOrdinal(op))
            .setComparator(ComparatorConverter.toProto(comparator))
            .setDelete(MutationConverter.toProto(delete))
            .setInputResult(result);
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_CHECK_AND_DELETE.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
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
    HookContext hookCtx = buildHookContext(c);
    PreCheckAndDeleteAfterRowLockRequest.Builder b =
        PreCheckAndDeleteAfterRowLockRequest.newBuilder()
            .setCtx(hookCtx)
            .setRow(ByteString.copyFrom(row))
            .setFamily(ByteString.copyFrom(family))
            .setQualifier(ByteString.copyFrom(qualifier))
            .setCompareOp(compareOpOrdinal(op))
            .setComparator(ComparatorConverter.toProto(comparator))
            .setDelete(MutationConverter.toProto(delete))
            .setInputResult(result);
    HookResponse resp =
        dispatch(
            regionIdFor(c),
            HookId.PRE_CHECK_AND_DELETE_AFTER_ROW_LOCK.value(),
            b.build().toByteArray());
    applyHookResponse(c, resp);
    return result;
  }

  // --- Check-and-Mutate (T42 Wave 3) -----------------------------------

  @Override
  public CheckAndMutateResult preCheckAndMutate(
      ObserverContext<RegionCoprocessorEnvironment> c,
      CheckAndMutate checkAndMutate,
      CheckAndMutateResult result)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreCheckAndMutateRequest.newBuilder()
            .setCtx(hookCtx)
            .setAction(buildCheckAndMutateAction(checkAndMutate))
            .setInputResult(buildCheckAndMutateResult(result))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_CHECK_AND_MUTATE.value(), reqBytes);
    applyHookResponse(c, resp);
    return result;
  }

  @Override
  public CheckAndMutateResult postCheckAndMutate(
      ObserverContext<RegionCoprocessorEnvironment> c,
      CheckAndMutate checkAndMutate,
      CheckAndMutateResult result)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostCheckAndMutateRequest.newBuilder()
            .setCtx(hookCtx)
            .setAction(buildCheckAndMutateAction(checkAndMutate))
            .setInputResult(buildCheckAndMutateResult(result))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(regionIdFor(c), HookId.POST_CHECK_AND_MUTATE.value(), reqBytes);
    applyHookResponse(c, resp);
    return result;
  }

  @Override
  public CheckAndMutateResult preCheckAndMutateAfterRowLock(
      ObserverContext<RegionCoprocessorEnvironment> c,
      CheckAndMutate checkAndMutate,
      CheckAndMutateResult result)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreCheckAndMutateAfterRowLockRequest.newBuilder()
            .setCtx(hookCtx)
            .setAction(buildCheckAndMutateAction(checkAndMutate))
            .setInputResult(buildCheckAndMutateResult(result))
            .build()
            .toByteArray();
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_CHECK_AND_MUTATE_AFTER_ROW_LOCK.value(), reqBytes);
    applyHookResponse(c, resp);
    return result;
  }

  private static int compareOpOrdinal(CompareOperator op) {
    return op == null ? -1 : op.ordinal();
  }

  private static com.virogg.hbasecop.bridge.wire.pb.CheckAndMutateAction buildCheckAndMutateAction(
      CheckAndMutate cam) throws IOException {
    com.virogg.hbasecop.bridge.wire.pb.CheckAndMutateAction.Builder b =
        com.virogg.hbasecop.bridge.wire.pb.CheckAndMutateAction.newBuilder();
    if (cam.getRow() != null) {
      b.setRow(ByteString.copyFrom(cam.getRow()));
    }
    if (cam.hasFilter()) {
      b.setFilter(FilterConverter.toProto(cam.getFilter()));
    } else {
      if (cam.getFamily() != null) {
        b.setFamily(ByteString.copyFrom(cam.getFamily()));
      }
      if (cam.getQualifier() != null) {
        b.setQualifier(ByteString.copyFrom(cam.getQualifier()));
      }
      b.setCompareOp(compareOpOrdinal(cam.getCompareOp()));
      if (cam.getValue() != null) {
        b.setValue(ByteString.copyFrom(cam.getValue()));
      }
    }
    if (cam.getAction() instanceof Mutation) {
      b.setAction(MutationConverter.toProto((Mutation) cam.getAction()));
    }
    return b.build();
  }

  private static com.virogg.hbasecop.bridge.wire.pb.CheckAndMutateResultProto
      buildCheckAndMutateResult(CheckAndMutateResult result) {
    com.virogg.hbasecop.bridge.wire.pb.CheckAndMutateResultProto.Builder b =
        com.virogg.hbasecop.bridge.wire.pb.CheckAndMutateResultProto.newBuilder();
    if (result == null) {
      return b.build();
    }
    b.setSuccess(result.isSuccess());
    if (result.getResult() != null) {
      b.setResult(ResultConverter.toProto(result.getResult()));
    }
    return b.build();
  }

  private static void addMiniBatchOperations(
      java.util.function.Consumer<com.virogg.hbasecop.bridge.wire.pb.MutationOperation> add,
      MiniBatchOperationInProgress<Mutation> miniBatch)
      throws IOException {
    if (miniBatch == null) {
      return;
    }
    int size = miniBatch.size();
    for (int i = 0; i < size; i++) {
      Mutation m = miniBatch.getOperation(i);
      com.virogg.hbasecop.bridge.wire.pb.MutationOperation.Builder ob =
          com.virogg.hbasecop.bridge.wire.pb.MutationOperation.newBuilder()
              .setMutation(MutationConverter.toProto(m));
      org.apache.hadoop.hbase.regionserver.OperationStatus status = miniBatch.getOperationStatus(i);
      if (status != null) {
        ob.setOperationStatusCode(status.getOperationStatusCode().ordinal());
      }
      add.accept(ob.build());
    }
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_APPEND.value(), reqBytes);
    return valueReturningResult(resp);
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
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_APPEND.value(), b.build().toByteArray());
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
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_APPEND_AFTER_ROW_LOCK.value(), reqBytes);
    return valueReturningResult(resp);
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_INCREMENT.value(), reqBytes);
    return valueReturningResult(resp);
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
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_INCREMENT.value(), b.build().toByteArray());
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
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_INCREMENT_AFTER_ROW_LOCK.value(), reqBytes);
    return valueReturningResult(resp);
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_SCANNER_OPEN.value(), reqBytes);
    applyScannerOpenHookResponse(scan, resp);
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.POST_SCANNER_OPEN.value(), reqBytes);
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_SCANNER_NEXT.value(), reqBytes);
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
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_SCANNER_NEXT.value(), b.build().toByteArray());
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
    HookResponse resp = dispatch(regionIdFor(c), HookId.POST_SCANNER_FILTER_ROW.value(), reqBytes);
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
                    ? org.apache.hbase.thirdparty.com.google.protobuf.ByteString.EMPTY
                    : org.apache.hbase.thirdparty.com.google.protobuf.ByteString.copyFrom(
                        store.getColumnFamilyDescriptor().getName()))
            .build()
            .toByteArray();
    HookResponse resp = dispatch(regionIdFor(c), HookId.PRE_STORE_SCANNER_OPEN.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  // --- WAL replay/restore (T42 Wave 4: bodies populated) ---------------

  @Override
  public void preReplayWALs(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      RegionInfo info,
      org.apache.hadoop.fs.Path edits)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PreReplayWALsRequest.Builder b = PreReplayWALsRequest.newBuilder().setCtx(hookCtx);
    if (info != null) {
      b.setRegionInfo(regionInfoProto(info));
    }
    if (edits != null) {
      b.setEditsPath(edits.toString());
    }
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_REPLAY_WA_LS.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public void postReplayWALs(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      RegionInfo info,
      org.apache.hadoop.fs.Path edits)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostReplayWALsRequest.Builder b = PostReplayWALsRequest.newBuilder().setCtx(hookCtx);
    if (info != null) {
      b.setRegionInfo(regionInfoProto(info));
    }
    if (edits != null) {
      b.setEditsPath(edits.toString());
    }
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_REPLAY_WA_LS.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public void preWALRestore(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      RegionInfo info,
      WALKey logKey,
      WALEdit logEdit)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PreWALRestoreRequest.Builder b = PreWALRestoreRequest.newBuilder().setCtx(hookCtx);
    if (info != null) {
      b.setRegionInfo(regionInfoProto(info));
    }
    if (logKey != null) {
      b.setLogKey(walKey(logKey));
    }
    if (logEdit != null) {
      b.setLogEdit(walEdit(logEdit));
    }
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_WAL_RESTORE.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public void postWALRestore(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      RegionInfo info,
      WALKey logKey,
      WALEdit logEdit)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostWALRestoreRequest.Builder b = PostWALRestoreRequest.newBuilder().setCtx(hookCtx);
    if (info != null) {
      b.setRegionInfo(regionInfoProto(info));
    }
    if (logKey != null) {
      b.setLogKey(walKey(logKey));
    }
    if (logEdit != null) {
      b.setLogEdit(walEdit(logEdit));
    }
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_WAL_RESTORE.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  // --- Bulk load + store-file commit (T42 Wave 4: bodies populated) -----

  @Override
  public void preBulkLoadHFile(
      ObserverContext<RegionCoprocessorEnvironment> c, List<Pair<byte[], String>> familyPaths)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PreBulkLoadHFileRequest.Builder b = PreBulkLoadHFileRequest.newBuilder().setCtx(hookCtx);
    if (familyPaths != null) {
      for (Pair<byte[], String> p : familyPaths) {
        b.addFamilyPath(
            com.virogg.hbasecop.bridge.wire.pb.FamilyPath.newBuilder()
                .setFamily(ByteString.copyFrom(p.getFirst()))
                .setPath(p.getSecond() == null ? "" : p.getSecond())
                .build());
      }
    }
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_BULK_LOAD_H_FILE.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public void postBulkLoadHFile(
      ObserverContext<RegionCoprocessorEnvironment> c,
      List<Pair<byte[], String>> stagingFamilyPaths,
      Map<byte[], List<org.apache.hadoop.fs.Path>> finalPaths)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PostBulkLoadHFileRequest.Builder b = PostBulkLoadHFileRequest.newBuilder().setCtx(hookCtx);
    if (stagingFamilyPaths != null) {
      for (Pair<byte[], String> p : stagingFamilyPaths) {
        b.addStagingFamilyPath(
            com.virogg.hbasecop.bridge.wire.pb.FamilyPath.newBuilder()
                .setFamily(ByteString.copyFrom(p.getFirst()))
                .setPath(p.getSecond() == null ? "" : p.getSecond())
                .build());
      }
    }
    if (finalPaths != null) {
      for (Map.Entry<byte[], List<org.apache.hadoop.fs.Path>> e : finalPaths.entrySet()) {
        com.virogg.hbasecop.bridge.wire.pb.BulkLoadFamilyPaths.Builder fb =
            com.virogg.hbasecop.bridge.wire.pb.BulkLoadFamilyPaths.newBuilder()
                .setFamily(ByteString.copyFrom(e.getKey()));
        if (e.getValue() != null) {
          for (org.apache.hadoop.fs.Path p : e.getValue()) {
            if (p != null) {
              fb.addPath(p.toString());
            }
          }
        }
        b.addFinalPath(fb);
      }
    }
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_BULK_LOAD_H_FILE.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public void preCommitStoreFile(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] family,
      List<Pair<org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path>> pairs)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PreCommitStoreFileRequest.Builder b =
        PreCommitStoreFileRequest.newBuilder()
            .setCtx(hookCtx)
            .setFamily(family == null ? ByteString.EMPTY : ByteString.copyFrom(family));
    if (pairs != null) {
      for (Pair<org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path> p : pairs) {
        b.addPair(
            com.virogg.hbasecop.bridge.wire.pb.PathPair.newBuilder()
                .setSource(p.getFirst() == null ? "" : p.getFirst().toString())
                .setDestination(p.getSecond() == null ? "" : p.getSecond().toString())
                .build());
      }
    }
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_COMMIT_STORE_FILE.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  @Override
  public void postCommitStoreFile(
      ObserverContext<RegionCoprocessorEnvironment> c,
      byte[] family,
      org.apache.hadoop.fs.Path srcPath,
      org.apache.hadoop.fs.Path dstPath)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostCommitStoreFileRequest.newBuilder()
            .setCtx(hookCtx)
            .setFamily(family == null ? ByteString.EMPTY : ByteString.copyFrom(family))
            .setSourcePath(srcPath == null ? "" : srcPath.toString())
            .setDestinationPath(dstPath == null ? "" : dstPath.toString())
            .build()
            .toByteArray();
    HookResponse resp = dispatch(regionIdFor(c), HookId.POST_COMMIT_STORE_FILE.value(), reqBytes);
    applyHookResponse(c, resp);
  }

  // --- Store-file reader (T42 Wave 4: bodies populated) ----------------

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
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PreStoreFileReaderOpenRequest.newBuilder()
            .setCtx(hookCtx)
            .setPath(p == null ? "" : p.toString())
            .setSizeBytes(size)
            .build()
            .toByteArray();
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_STORE_FILE_READER_OPEN.value(), reqBytes);
    applyHookResponse(c, resp);
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
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostStoreFileReaderOpenRequest.newBuilder()
            .setCtx(hookCtx)
            .setPath(p == null ? "" : p.toString())
            .setSizeBytes(size)
            .build()
            .toByteArray();
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_STORE_FILE_READER_OPEN.value(), reqBytes);
    applyHookResponse(c, resp);
    return reader;
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

  private static com.virogg.hbasecop.bridge.wire.pb.WalKeyProto walKey(WALKey logKey) {
    com.virogg.hbasecop.bridge.wire.pb.WalKeyProto.Builder b =
        com.virogg.hbasecop.bridge.wire.pb.WalKeyProto.newBuilder()
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

  private static com.virogg.hbasecop.bridge.wire.pb.WalEditProto walEdit(WALEdit logEdit) {
    com.virogg.hbasecop.bridge.wire.pb.WalEditProto.Builder b =
        com.virogg.hbasecop.bridge.wire.pb.WalEditProto.newBuilder()
            .setHasReplayMeta(logEdit.isReplay());
    if (logEdit.getCells() != null) {
      for (Cell cell : logEdit.getCells()) {
        b.addCell(CellConverter.toProto(cell));
      }
    }
    return b.build();
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
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_MUTATION_BEFORE_WAL.value(), b.build().toByteArray());
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
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_INCREMENT_BEFORE_WAL.value(), b.build().toByteArray());
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
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_APPEND_BEFORE_WAL.value(), b.build().toByteArray());
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

  // --- Delete tracker, WAL append (T42 Wave 4: bodies populated) -------

  @Override
  public DeleteTracker postInstantiateDeleteTracker(
      ObserverContext<RegionCoprocessorEnvironment> c, DeleteTracker tracker) throws IOException {
    HookContext hookCtx = buildHookContext(c);
    byte[] reqBytes =
        PostInstantiateDeleteTrackerRequest.newBuilder()
            .setCtx(hookCtx)
            .setTrackerClass(tracker == null ? "" : tracker.getClass().getName())
            .build()
            .toByteArray();
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.POST_INSTANTIATE_DELETE_TRACKER.value(), reqBytes);
    applyHookResponse(c, resp);
    return tracker;
  }

  @Override
  public void preWALAppend(
      ObserverContext<RegionCoprocessorEnvironment> c, WALKey key, WALEdit edit)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    PreWALAppendRequest.Builder b = PreWALAppendRequest.newBuilder().setCtx(hookCtx);
    if (key != null) {
      b.setLogKey(walKey(key));
    }
    if (edit != null) {
      b.setLogEdit(walEdit(edit));
    }
    HookResponse resp =
        dispatch(regionIdFor(c), HookId.PRE_WAL_APPEND.value(), b.build().toByteArray());
    applyHookResponse(c, resp);
  }

  // === Internals =========================================================

  /**
   * Stub dispatch helper for hooks whose Request body is just {@link HookContext}. Every T41 stub
   * Request message in {@code proto/hooks.proto} embeds {@link HookContext} at field 1, set here
   * via the field descriptor on the generic {@link Message.Builder}. T42 widens each Request with
   * hook-specific payload fields; the specialised-builder call sites (currently {@link #prePut} /
   * {@link #postPut}) show the pattern.
   */
  private void dispatchStub(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      HookId hookId,
      Message.Builder builder)
      throws IOException {
    HookContext hookCtx = buildHookContext(c);
    Descriptors.FieldDescriptor ctxField = builder.getDescriptorForType().findFieldByNumber(1);
    builder.setField(ctxField, hookCtx);
    HookResponse resp = dispatch(regionIdFor(c), hookId.value(), builder.build().toByteArray());
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
  private HookResponse dispatch(int regionId, byte hookId, byte[] reqBytes) throws IOException {
    HookPolicy pol = policyConfig.forHook(hookId);
    final byte[] respBytes;
    try {
      respBytes = dispatcher.dispatchHook(regionId, hookId, reqBytes, pol.timeout());
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
    LOG.log(Level.WARNING, "{0} - best-effort, treated as no-op", detail);
    return null;
  }

  private static void applyHookResponse(
      ObserverContext<? extends RegionCoprocessorEnvironment> c, HookResponse resp) {
    if (resp == null) {
      return;
    }
    if (resp.getBypass()) {
      requestBypass(c);
    }
  }

  /**
   * Sentinel row used to neuter a scan an observer asked to bypass. HBase 2.5's {@code
   * preScannerOpen} {@link ObserverContext} is <em>not</em> bypassable, so "bypass this scan" is
   * realized instead by constraining the {@link Scan} to the empty half-open interval {@code
   * [SENTINEL, SENTINEL)}: the opened scanner yields no rows, the observable equivalent of a
   * bypassed scan.
   */
  private static final byte[] SCAN_BYPASS_SENTINEL = {0};

  /**
   * Invoke {@link ObserverContext#bypass()} defensively. HBase 2.5 only makes the ObserverContext
   * bypassable for a subset of hooks; calling {@code bypass()} on a non-bypassable hook throws
   * {@link UnsupportedOperationException}, which (uncaught from a coprocessor) aborts the entire
   * RegionServer. An over-eager observer must never be able to do that, so a rejected bypass is
   * downgraded to a WARN and the host operation proceeds unbypassed.
   */
  private static void requestBypass(ObserverContext<? extends RegionCoprocessorEnvironment> c) {
    try {
      c.bypass();
    } catch (UnsupportedOperationException e) {
      LOG.log(
          Level.WARNING,
          "hbasecop: observer requested bypass on a hook that does not support it - ignored",
          e);
    }
  }

  /**
   * Build the substitute {@link Result} for a value-returning pre-hook: {@code preAppend} / {@code
   * preIncrement} and their after-row-lock variants. In HBase 2.5 the bypass mechanism for these
   * hooks is "return a non-null {@link Result} to substitute for the operation", <em>not</em>
   * {@link ObserverContext#bypass()}.
   *
   * <p>Returns {@code null} when the observer did not bypass (HBase then runs the operation
   * normally). On bypass, returns a Result assembled from the cells the observer supplied in {@code
   * HookResponse.result} (an empty list yields an empty Result), which HBase returns to the client
   * in place of the operation.
   */
  private static Result valueReturningResult(HookResponse resp) {
    if (resp == null || !resp.getBypass()) {
      return null;
    }
    List<Cell> cells = new ArrayList<>(resp.getResultCount());
    for (CellProtos.Cell c : resp.getResultList()) {
      cells.add(CellConverter.fromProto(c));
    }
    return Result.create(cells);
  }

  /**
   * Apply a {@code preScannerOpen} HookResponse. Unlike {@link #applyHookResponse}, a bypass here
   * cannot go through {@link ObserverContext#bypass()}: HBase 2.5 does not make {@code
   * preScannerOpen} bypassable. Instead the {@link Scan} itself is constrained to an empty row
   * range so the scanner about to be opened yields no rows.
   */
  private static void applyScannerOpenHookResponse(Scan scan, HookResponse resp) {
    if (resp == null || !resp.getBypass()) {
      return;
    }
    scan.withStartRow(SCAN_BYPASS_SENTINEL, true).withStopRow(SCAN_BYPASS_SENTINEL, false);
  }

  /**
   * Batch-shaped variant of {@link #applyHookResponse}: besides honoring {@code bypass}, walks
   * {@code resp.getBlockedIndicesList()} and stamps {@code OperationStatus(SANITY_CHECK_FAILURE)}
   * on every in-range index of the supplied {@link MiniBatchOperationInProgress}. Out-of-range
   * indices are silently ignored: observer-supplied indices are a hint, not an authority, so a
   * buggy observer can never crash the RegionServer's batch path.
   */
  private static void applyBatchHookResponse(
      ObserverContext<? extends RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatch,
      HookResponse resp) {
    if (resp == null) {
      return;
    }
    if (resp.getBypass()) {
      requestBypass(c);
    }
    if (miniBatch == null || resp.getBlockedIndicesCount() == 0) {
      return;
    }
    int size = miniBatch.size();
    for (int i = 0; i < resp.getBlockedIndicesCount(); i++) {
      int idx = resp.getBlockedIndices(i);
      if (idx < 0 || idx >= size) {
        continue;
      }
      miniBatch.setOperationStatus(
          idx,
          new org.apache.hadoop.hbase.regionserver.OperationStatus(
              org.apache.hadoop.hbase.HConstants.OperationStatusCode.SANITY_CHECK_FAILURE,
              "hbasecop: mutation blocked by observer"));
    }
  }

  /**
   * Resolve the wire-level region_id for {@code c}. Returns {@code 0} if {@link
   * #start(org.apache.hadoop.hbase.CoprocessorEnvironment)} has not yet registered the region,
   * preserving the Phase-2 wire shape so an unwired adapter still works against the Go runtime.
   */
  private int regionIdFor(ObserverContext<? extends RegionCoprocessorEnvironment> c) {
    return regionIdAllocator.idFor(c.getEnvironment().getRegion().getRegionInfo().getEncodedName());
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
