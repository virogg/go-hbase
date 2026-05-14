// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

/**
 * Canonical mapping between the wire-level hook byte, the HBase {@link
 * org.apache.hadoop.hbase.coprocessor.RegionObserver} method name and the Go-side HookID constant.
 *
 * <p>This enum is the single source of truth on the Java adapter side. Each entry pairs:
 *
 * <ul>
 *   <li>A wire byte ({@link #value()}) — same as the Go {@code HookID} constant and the {@code
 *       com.virogg.hbasecop.bridge.wire.pb.HookId} proto-generated value.
 *   <li>A method name ({@link #methodName()}) — matches the corresponding HBase API method (e.g.
 *       {@code "prePut"}), so {@link com.virogg.hbasecop.bridge.config.PolicyConfig} can resolve
 *       policy keys (e.g. {@code hbasecop.policy.prePut}) directly from a HookId.
 * </ul>
 *
 * <p>{@link com.virogg.hbasecop.bridge.observer.RegionObserverAdapterFullCoverageTest} pins the
 * three-way parity (this enum ↔ HBase API ↔ proto enum).
 */
public enum HookId {
  // Lifecycle.
  PRE_OPEN((byte) 1, "preOpen"),
  POST_OPEN((byte) 2, "postOpen"),
  PRE_CLOSE((byte) 3, "preClose"),
  POST_CLOSE((byte) 4, "postClose"),

  // Flush.
  PRE_FLUSH((byte) 5, "preFlush"),
  PRE_FLUSH_SCANNER_OPEN((byte) 6, "preFlushScannerOpen"),
  POST_FLUSH((byte) 7, "postFlush"),

  // MemStore compaction.
  PRE_MEM_STORE_COMPACTION((byte) 8, "preMemStoreCompaction"),
  PRE_MEM_STORE_COMPACTION_COMPACT_SCANNER_OPEN(
      (byte) 9, "preMemStoreCompactionCompactScannerOpen"),
  PRE_MEM_STORE_COMPACTION_COMPACT((byte) 10, "preMemStoreCompactionCompact"),
  POST_MEM_STORE_COMPACTION((byte) 11, "postMemStoreCompaction"),

  // Compaction.
  PRE_COMPACT_SELECTION((byte) 12, "preCompactSelection"),
  POST_COMPACT_SELECTION((byte) 13, "postCompactSelection"),
  PRE_COMPACT_SCANNER_OPEN((byte) 14, "preCompactScannerOpen"),
  PRE_COMPACT((byte) 15, "preCompact"),
  POST_COMPACT((byte) 16, "postCompact"),

  // Read path.
  PRE_GET_OP((byte) 17, "preGetOp"),
  POST_GET_OP((byte) 18, "postGetOp"),
  PRE_EXISTS((byte) 19, "preExists"),
  POST_EXISTS((byte) 20, "postExists"),

  // Write path — Put.
  PRE_PUT((byte) 21, "prePut"),
  POST_PUT((byte) 22, "postPut"),

  // Write path — Delete + version timestamp.
  PRE_DELETE((byte) 23, "preDelete"),
  POST_DELETE((byte) 24, "postDelete"),
  PRE_PREPARE_TIME_STAMP_FOR_DELETE_VERSION((byte) 25, "prePrepareTimeStampForDeleteVersion"),

  // Batch mutate + region operation envelope.
  PRE_BATCH_MUTATE((byte) 26, "preBatchMutate"),
  POST_BATCH_MUTATE((byte) 27, "postBatchMutate"),
  POST_BATCH_MUTATE_INDISPENSABLY((byte) 28, "postBatchMutateIndispensably"),
  POST_START_REGION_OPERATION((byte) 29, "postStartRegionOperation"),
  POST_CLOSE_REGION_OPERATION((byte) 30, "postCloseRegionOperation"),

  // Check-and-Put.
  PRE_CHECK_AND_PUT((byte) 31, "preCheckAndPut"),
  POST_CHECK_AND_PUT((byte) 32, "postCheckAndPut"),
  PRE_CHECK_AND_PUT_AFTER_ROW_LOCK((byte) 33, "preCheckAndPutAfterRowLock"),

  // Check-and-Delete.
  PRE_CHECK_AND_DELETE((byte) 34, "preCheckAndDelete"),
  POST_CHECK_AND_DELETE((byte) 35, "postCheckAndDelete"),
  PRE_CHECK_AND_DELETE_AFTER_ROW_LOCK((byte) 36, "preCheckAndDeleteAfterRowLock"),

  // Check-and-Mutate.
  PRE_CHECK_AND_MUTATE((byte) 37, "preCheckAndMutate"),
  POST_CHECK_AND_MUTATE((byte) 38, "postCheckAndMutate"),
  PRE_CHECK_AND_MUTATE_AFTER_ROW_LOCK((byte) 39, "preCheckAndMutateAfterRowLock"),

  // Append.
  PRE_APPEND((byte) 40, "preAppend"),
  POST_APPEND((byte) 41, "postAppend"),
  PRE_APPEND_AFTER_ROW_LOCK((byte) 42, "preAppendAfterRowLock"),

  // Increment.
  PRE_INCREMENT((byte) 43, "preIncrement"),
  POST_INCREMENT((byte) 44, "postIncrement"),
  PRE_INCREMENT_AFTER_ROW_LOCK((byte) 45, "preIncrementAfterRowLock"),

  // Scanner.
  PRE_SCANNER_OPEN((byte) 46, "preScannerOpen"),
  POST_SCANNER_OPEN((byte) 47, "postScannerOpen"),
  PRE_SCANNER_NEXT((byte) 48, "preScannerNext"),
  POST_SCANNER_NEXT((byte) 49, "postScannerNext"),
  POST_SCANNER_FILTER_ROW((byte) 50, "postScannerFilterRow"),
  PRE_SCANNER_CLOSE((byte) 51, "preScannerClose"),
  POST_SCANNER_CLOSE((byte) 52, "postScannerClose"),
  PRE_STORE_SCANNER_OPEN((byte) 53, "preStoreScannerOpen"),

  // WAL replay/restore.
  PRE_REPLAY_WA_LS((byte) 54, "preReplayWALs"),
  POST_REPLAY_WA_LS((byte) 55, "postReplayWALs"),
  PRE_WAL_RESTORE((byte) 56, "preWALRestore"),
  POST_WAL_RESTORE((byte) 57, "postWALRestore"),

  // Bulk load + store-file commit.
  PRE_BULK_LOAD_H_FILE((byte) 58, "preBulkLoadHFile"),
  POST_BULK_LOAD_H_FILE((byte) 59, "postBulkLoadHFile"),
  PRE_COMMIT_STORE_FILE((byte) 60, "preCommitStoreFile"),
  POST_COMMIT_STORE_FILE((byte) 61, "postCommitStoreFile"),

  // Store-file reader.
  PRE_STORE_FILE_READER_OPEN((byte) 62, "preStoreFileReaderOpen"),
  POST_STORE_FILE_READER_OPEN((byte) 63, "postStoreFileReaderOpen"),

  // Before-WAL hooks.
  POST_MUTATION_BEFORE_WAL((byte) 64, "postMutationBeforeWAL"),
  POST_INCREMENT_BEFORE_WAL((byte) 65, "postIncrementBeforeWAL"),
  POST_APPEND_BEFORE_WAL((byte) 66, "postAppendBeforeWAL"),

  // Delete tracker, WAL append.
  POST_INSTANTIATE_DELETE_TRACKER((byte) 67, "postInstantiateDeleteTracker"),
  PRE_WAL_APPEND((byte) 68, "preWALAppend");

  private final byte value;
  private final String methodName;

  HookId(byte value, String methodName) {
    this.value = value;
    this.methodName = methodName;
  }

  /** The on-wire byte placed in {@code FrameHeader.hook_id}. */
  public byte value() {
    return value;
  }

  /** The HBase RegionObserver method this HookId routes to (e.g. {@code "prePut"}). */
  public String methodName() {
    return methodName;
  }

  /** Resolve a HookId by its wire byte, or null if no entry matches. */
  public static HookId byValue(byte value) {
    for (HookId id : values()) {
      if (id.value == value) {
        return id;
      }
    }
    return null;
  }

  /** Resolve a HookId by its HBase method name (e.g. {@code "prePut"}), or null if absent. */
  public static HookId byMethodName(String name) {
    for (HookId id : values()) {
      if (id.methodName.equals(name)) {
        return id;
      }
    }
    return null;
  }
}
