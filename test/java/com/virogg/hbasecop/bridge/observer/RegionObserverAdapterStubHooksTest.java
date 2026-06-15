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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
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
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Coverage for the RegionObserverAdapter hooks that route context-only (no per-arg serialization):
 * lifecycle, flush, compaction, scanner, storage-file, WAL-replay/restore, bulk-load and before-WAL
 * passthrough hooks. Each invocation must reach {@code dispatcher.dispatchHook}; the
 * value-returning passthrough hooks must hand back their input argument unchanged on an empty
 * response. The arg-serializing hooks (Get/Put/Delete/CheckAndMutate/...) are covered separately in
 * {@link RegionObserverAdapterPayloadHooksTest}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RegionObserverAdapterStubHooksTest {

  @Mock private HookDispatcher dispatcher;
  @Mock private ObserverContext<RegionCoprocessorEnvironment> ctx;
  @Mock private RegionCoprocessorEnvironment env;
  @Mock private Region region;
  @Mock private RegionInfo regionInfo;

  private RegionObserverAdapter adapter;

  @BeforeEach
  void setUp() throws Exception {
    when(ctx.getEnvironment()).thenReturn(env);
    when(env.getRegion()).thenReturn(region);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    when(regionInfo.getTable()).thenReturn(TableName.valueOf("default", "t"));
    when(regionInfo.getEncodedNameAsBytes()).thenReturn("enc".getBytes());
    when(regionInfo.getEncodedName()).thenReturn("enc"); // preOpen allocates a region id from this
    when(dispatcher.dispatchHook(anyInt(), anyByte(), any(), any()))
        .thenReturn(HookResponse.newBuilder().build().toByteArray());
    adapter = new RegionObserverAdapter(dispatcher, new PolicyConfig(new Configuration(false)));
  }

  private void verifyDispatched() throws Exception {
    verify(dispatcher, atLeastOnce()).dispatchHook(anyInt(), anyByte(), any(), any());
  }

  @Test
  void lifecycleHooksDispatch() throws Exception {
    adapter.preOpen(ctx);
    adapter.postOpen(ctx);
    adapter.preClose(ctx, true);
    adapter.postClose(ctx, false);
    verifyDispatched();
  }

  @Test
  void flushHooksDispatch() throws Exception {
    FlushLifeCycleTracker tracker = mock(FlushLifeCycleTracker.class);
    Store store = mock(Store.class);
    ScanOptions opts = mock(ScanOptions.class);
    adapter.preFlush(ctx, tracker);
    adapter.preFlushScannerOpen(ctx, store, opts, tracker);
    adapter.postFlush(ctx, tracker);
    verifyDispatched();
  }

  @Test
  void memStoreCompactionHooksDispatch() throws Exception {
    Store store = mock(Store.class);
    ScanOptions opts = mock(ScanOptions.class);
    InternalScanner scanner = mock(InternalScanner.class);
    adapter.preMemStoreCompaction(ctx, store);
    adapter.preMemStoreCompactionCompactScannerOpen(ctx, store, opts);
    assertSame(scanner, adapter.preMemStoreCompactionCompact(ctx, store, scanner));
    adapter.postMemStoreCompaction(ctx, store);
    verifyDispatched();
  }

  @Test
  void compactionHooksDispatch() throws Exception {
    Store store = mock(Store.class);
    ScanOptions opts = mock(ScanOptions.class);
    InternalScanner scanner = mock(InternalScanner.class);
    StoreFile resultFile = mock(StoreFile.class);
    CompactionLifeCycleTracker tracker = mock(CompactionLifeCycleTracker.class);
    CompactionRequest request = mock(CompactionRequest.class);
    List<StoreFile> candidates = List.of(mock(StoreFile.class));

    adapter.preCompactSelection(ctx, store, candidates, tracker);
    adapter.postCompactSelection(ctx, store, candidates, tracker, request);
    adapter.preCompactScannerOpen(
        ctx, store, ScanType.COMPACT_RETAIN_DELETES, opts, tracker, request);
    assertSame(
        scanner,
        adapter.preCompact(ctx, store, scanner, ScanType.COMPACT_RETAIN_DELETES, tracker, request));
    adapter.postCompact(ctx, store, resultFile, tracker, request);
    verifyDispatched();
  }

  @Test
  void scannerHooksDispatch() throws Exception {
    Scan scan = new Scan();
    RegionScanner rs = mock(RegionScanner.class);
    InternalScanner is = mock(InternalScanner.class);
    Store store = mock(Store.class);
    when(store.getColumnFamilyDescriptor())
        .thenReturn(org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.of("cf"));
    ScanOptions opts = mock(ScanOptions.class);

    assertSame(rs, adapter.postScannerOpen(ctx, scan, rs));
    adapter.preScannerNext(ctx, is, new java.util.ArrayList<>(), 10, true);
    adapter.postScannerNext(ctx, is, new java.util.ArrayList<>(), 10, true);
    Cell currentRow =
        new KeyValue("r".getBytes(), "cf".getBytes(), "q".getBytes(), 1L, "v".getBytes());
    adapter.postScannerFilterRow(ctx, is, currentRow, true);
    adapter.preScannerClose(ctx, is);
    adapter.postScannerClose(ctx, is);
    adapter.preStoreScannerOpen(ctx, store, opts);
    verifyDispatched();
  }

  @Test
  void regionOperationAndBatchPostHooksDispatch() throws Exception {
    @SuppressWarnings("unchecked")
    MiniBatchOperationInProgress<org.apache.hadoop.hbase.client.Mutation> mb =
        mock(MiniBatchOperationInProgress.class);
    adapter.postBatchMutate(ctx, mb);
    adapter.postBatchMutateIndispensably(ctx, mb, true);
    adapter.postStartRegionOperation(ctx, Region.Operation.SCAN);
    adapter.postCloseRegionOperation(ctx, Region.Operation.SCAN);
    verifyDispatched();
  }

  @Test
  void walReplayAndRestoreHooksDispatch() throws Exception {
    // preReplayWALs/preWALRestore serialize the RegionInfo argument, so use a
    // real one (a bare mock returns null from every getter).
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf("default", "t")).build();
    Path edits = new Path("/wal/edits");
    WALKey key = mock(WALKey.class);
    WALEdit edit = mock(WALEdit.class);
    adapter.preReplayWALs(ctx, info, edits);
    adapter.postReplayWALs(ctx, info, edits);
    adapter.preWALRestore(ctx, info, key, edit);
    adapter.postWALRestore(ctx, info, key, edit);
    adapter.preWALAppend(ctx, key, edit);
    verifyDispatched();
  }

  @Test
  void bulkLoadAndStoreFileHooksDispatch() throws Exception {
    List<org.apache.hadoop.hbase.util.Pair<byte[], String>> familyPaths =
        List.of(new org.apache.hadoop.hbase.util.Pair<>("cf".getBytes(), "/staging/f"));
    adapter.preBulkLoadHFile(ctx, familyPaths);
    adapter.postBulkLoadHFile(ctx, familyPaths, new java.util.HashMap<>());

    byte[] family = "cf".getBytes();
    List<org.apache.hadoop.hbase.util.Pair<Path, Path>> pairs =
        List.of(new org.apache.hadoop.hbase.util.Pair<>(new Path("/a"), new Path("/b")));
    adapter.preCommitStoreFile(ctx, family, pairs);
    adapter.postCommitStoreFile(ctx, family, new Path("/a"), new Path("/b"));
    verifyDispatched();
  }

  @Test
  void storeFileReaderOpenHooksArePassthrough() throws Exception {
    FileSystem fs = mock(FileSystem.class);
    Path p = new Path("/f");
    FSDataInputStreamWrapper in = mock(FSDataInputStreamWrapper.class);
    CacheConfig cacheConf = mock(CacheConfig.class);
    Reference ref = mock(Reference.class);
    StoreFileReader reader = mock(StoreFileReader.class);

    assertSame(reader, adapter.preStoreFileReaderOpen(ctx, fs, p, in, 1L, cacheConf, ref, reader));
    assertSame(reader, adapter.postStoreFileReaderOpen(ctx, fs, p, in, 1L, cacheConf, ref, reader));
    verifyDispatched();
  }

  @Test
  void deleteTrackerHookIsPassthrough() throws Exception {
    DeleteTracker tracker = mock(DeleteTracker.class);
    assertSame(tracker, adapter.postInstantiateDeleteTracker(ctx, tracker));
    verifyDispatched();
  }
}
