<!--
Copyright 2026 The go-hbase Authors
SPDX-License-Identifier: Apache-2.0
-->

# RegionObserver coverage matrix (T46)

Single source of truth for "which HBase 2.5 `RegionObserver` method is
wired to which hook id, which test exercises it, and what its coverage
status is." This document is parsed by
`pkg/hbasecop/coverage_test.go::TestCoverageMatrixDocCoversAllHooks`,
which fails the build if any hook from the canonical dispatch table is
missing a row, missing `covered_by`, or marked anything other than
`covered`.

Adding a new hook to `pkg/hbasecop/hooktable.go` therefore requires
appending a row to the table below; the CI gate will catch the
omission.

## Test anchors

Short names referenced in the `covered_by` column.

| anchor | scope | location |
|---|---|---|
| **dispatch** | T41 reflection parity (every hook in `hookTable` ↔ `RegionObserver` method ↔ `HookId` enum) | `pkg/hbasecop/hooktable_test.go::TestHookTableIsCanonical`, `TestRegionObserverInterfaceCoversAllHooks` |
| **roundtrip** | T42 wire round-trip: each `proto.Request` encodes / decodes losslessly | `internal/wire/hookpb/*` (corpus-driven) and Java `wire.pb.*RoundTripTest` |
| **prePutIT** | T27 live IT (Put → Go observer counter) | `test/java/.../PrePutCounterIT.java`, `examples/counter-observer/` |
| **readIT** | T43 live IT (Get / Scan bypass) | `test/java/.../ReadPathFilterIT.java`, `examples/filter-observer/` |
| **batchIT** | T44 live IT (`Table.batch` partial block) | `test/java/.../BatchPartialBlockIT.java` |
| **storageIT** | T45 live IT (`admin.flush` + `admin.majorCompact`) | `test/java/.../StorageHooksIT.java` |
| **faultIT** | T36 fault-injection matrix | `test/java/.../FaultMatrixIT.java` |
| **adapter** | Java unit test on `RegionObserverAdapter` | `test/java/.../RegionObserverAdapter*Test.java` |

## Coverage matrix

68 hooks (frozen against `proto/hooks.proto` HookId enum).

| hook_id | name | proto Request | covered_by | status |
|---|---|---|---|---|
| 1 | PreOpen | PreOpenRequest | dispatch + roundtrip | covered |
| 2 | PostOpen | PostOpenRequest | dispatch + roundtrip | covered |
| 3 | PreClose | PreCloseRequest | dispatch + roundtrip | covered |
| 4 | PostClose | PostCloseRequest | dispatch + roundtrip | covered |
| 5 | PreFlush | PreFlushRequest | storageIT + roundtrip | covered |
| 6 | PreFlushScannerOpen | PreFlushScannerOpenRequest | dispatch + roundtrip | covered |
| 7 | PostFlush | PostFlushRequest | storageIT + roundtrip | covered |
| 8 | PreMemStoreCompaction | PreMemStoreCompactionRequest | dispatch + roundtrip | covered |
| 9 | PreMemStoreCompactionCompactScannerOpen | PreMemStoreCompactionCompactScannerOpenRequest | dispatch + roundtrip | covered |
| 10 | PreMemStoreCompactionCompact | PreMemStoreCompactionCompactRequest | dispatch + roundtrip | covered |
| 11 | PostMemStoreCompaction | PostMemStoreCompactionRequest | dispatch + roundtrip | covered |
| 12 | PreCompactSelection | PreCompactSelectionRequest | storageIT + roundtrip | covered |
| 13 | PostCompactSelection | PostCompactSelectionRequest | dispatch + roundtrip | covered |
| 14 | PreCompactScannerOpen | PreCompactScannerOpenRequest | dispatch + roundtrip | covered |
| 15 | PreCompact | PreCompactRequest | storageIT + roundtrip | covered |
| 16 | PostCompact | PostCompactRequest | storageIT + roundtrip | covered |
| 17 | PreGetOp | PreGetOpRequest | readIT + roundtrip | covered |
| 18 | PostGetOp | PostGetOpRequest | dispatch + roundtrip | covered |
| 19 | PreExists | PreExistsRequest | dispatch + roundtrip | covered |
| 20 | PostExists | PostExistsRequest | dispatch + roundtrip | covered |
| 21 | PrePut | PrePutRequest | prePutIT + faultIT + adapter + roundtrip | covered |
| 22 | PostPut | PostPutRequest | adapter + roundtrip | covered |
| 23 | PreDelete | PreDeleteRequest | dispatch + roundtrip | covered |
| 24 | PostDelete | PostDeleteRequest | dispatch + roundtrip | covered |
| 25 | PrePrepareTimeStampForDeleteVersion | PrePrepareTimeStampForDeleteVersionRequest | dispatch + roundtrip | covered |
| 26 | PreBatchMutate | PreBatchMutateRequest | batchIT + adapter + roundtrip | covered |
| 27 | PostBatchMutate | PostBatchMutateRequest | dispatch + roundtrip | covered |
| 28 | PostBatchMutateIndispensably | PostBatchMutateIndispensablyRequest | dispatch + roundtrip | covered |
| 29 | PostStartRegionOperation | PostStartRegionOperationRequest | dispatch + roundtrip | covered |
| 30 | PostCloseRegionOperation | PostCloseRegionOperationRequest | dispatch + roundtrip | covered |
| 31 | PreCheckAndPut | PreCheckAndPutRequest | dispatch + roundtrip | covered |
| 32 | PostCheckAndPut | PostCheckAndPutRequest | dispatch + roundtrip | covered |
| 33 | PreCheckAndPutAfterRowLock | PreCheckAndPutAfterRowLockRequest | dispatch + roundtrip | covered |
| 34 | PreCheckAndDelete | PreCheckAndDeleteRequest | dispatch + roundtrip | covered |
| 35 | PostCheckAndDelete | PostCheckAndDeleteRequest | dispatch + roundtrip | covered |
| 36 | PreCheckAndDeleteAfterRowLock | PreCheckAndDeleteAfterRowLockRequest | dispatch + roundtrip | covered |
| 37 | PreCheckAndMutate | PreCheckAndMutateRequest | dispatch + roundtrip | covered |
| 38 | PostCheckAndMutate | PostCheckAndMutateRequest | dispatch + roundtrip | covered |
| 39 | PreCheckAndMutateAfterRowLock | PreCheckAndMutateAfterRowLockRequest | dispatch + roundtrip | covered |
| 40 | PreAppend | PreAppendRequest | dispatch + roundtrip | covered |
| 41 | PostAppend | PostAppendRequest | dispatch + roundtrip | covered |
| 42 | PreAppendAfterRowLock | PreAppendAfterRowLockRequest | dispatch + roundtrip | covered |
| 43 | PreIncrement | PreIncrementRequest | dispatch + roundtrip | covered |
| 44 | PostIncrement | PostIncrementRequest | dispatch + roundtrip | covered |
| 45 | PreIncrementAfterRowLock | PreIncrementAfterRowLockRequest | dispatch + roundtrip | covered |
| 46 | PreScannerOpen | PreScannerOpenRequest | readIT + roundtrip | covered |
| 47 | PostScannerOpen | PostScannerOpenRequest | dispatch + roundtrip | covered |
| 48 | PreScannerNext | PreScannerNextRequest | readIT + roundtrip | covered |
| 49 | PostScannerNext | PostScannerNextRequest | dispatch + roundtrip | covered |
| 50 | PostScannerFilterRow | PostScannerFilterRowRequest | dispatch + roundtrip | covered |
| 51 | PreScannerClose | PreScannerCloseRequest | dispatch + roundtrip | covered |
| 52 | PostScannerClose | PostScannerCloseRequest | dispatch + roundtrip | covered |
| 53 | PreStoreScannerOpen | PreStoreScannerOpenRequest | dispatch + roundtrip | covered |
| 54 | PreReplayWALs | PreReplayWALsRequest | dispatch + roundtrip | covered |
| 55 | PostReplayWALs | PostReplayWALsRequest | dispatch + roundtrip | covered |
| 56 | PreWALRestore | PreWALRestoreRequest | dispatch + roundtrip | covered |
| 57 | PostWALRestore | PostWALRestoreRequest | dispatch + roundtrip | covered |
| 58 | PreBulkLoadHFile | PreBulkLoadHFileRequest | dispatch + roundtrip | covered |
| 59 | PostBulkLoadHFile | PostBulkLoadHFileRequest | dispatch + roundtrip | covered |
| 60 | PreCommitStoreFile | PreCommitStoreFileRequest | dispatch + roundtrip | covered |
| 61 | PostCommitStoreFile | PostCommitStoreFileRequest | dispatch + roundtrip | covered |
| 62 | PreStoreFileReaderOpen | PreStoreFileReaderOpenRequest | dispatch + roundtrip | covered |
| 63 | PostStoreFileReaderOpen | PostStoreFileReaderOpenRequest | dispatch + roundtrip | covered |
| 64 | PostMutationBeforeWAL | PostMutationBeforeWALRequest | dispatch + roundtrip | covered |
| 65 | PostIncrementBeforeWAL | PostIncrementBeforeWALRequest | dispatch + roundtrip | covered |
| 66 | PostAppendBeforeWAL | PostAppendBeforeWALRequest | dispatch + roundtrip | covered |
| 67 | PostInstantiateDeleteTracker | PostInstantiateDeleteTrackerRequest | dispatch + roundtrip | covered |
| 68 | PreWALAppend | PreWALAppendRequest | dispatch + roundtrip | covered |

## Coverage tiers

The `covered_by` column lists the strongest anchor first.

- **Live IT** (`*IT`): the hook runs inside a real HBase RegionServer
  against the bridge; proves the wire path, the Java adapter override
  and the Go SDK callback all work end-to-end. Strongest tier.
- **adapter**: Java unit test runs the bridge's `RegionObserverAdapter`
  hook with a stub `ObserverContext`, exercising the per-hook payload
  mapper.
- **roundtrip**: proto Request serialises identically on both sides
  against the committed golden corpus, so cross-language wire
  agreement is pinned even when no live IT exists yet.
- **dispatch**: T41 reflection asserts the hook's row in `hookTable`
  has the right id, name, decoder and invoke closure, and that the
  matching `RegionObserver` interface method exists. Every hook is
  covered by `dispatch` at minimum.

Hooks that depend on rarely-triggered server paths (BulkLoad,
WALRestore, MemStoreCompaction internals) are deliberately not yet
covered by a live IT; Phase 5 will add the dedicated observer-type
adapters that exercise those code paths.
