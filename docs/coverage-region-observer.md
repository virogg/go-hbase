<!--
Copyright 2026 The go-hbase Authors
SPDX-License-Identifier: Apache-2.0
-->

# Матрица покрытия RegionObserver (T46)

Единый источник истины по вопросу «какой метод HBase 2.5 `RegionObserver`
привязан к какому hook id, какой тест его прогоняет и каков его статус
покрытия». Этот документ разбирается
`pkg/hbasecop/coverage_test.go::TestCoverageMatrixDocCoversAllHooks`,
который роняет сборку, если для какого-либо hook из канонической dispatch-таблицы
отсутствует строка, отсутствует `covered_by` или он помечен чем-либо иным,
кроме `covered`.

Поэтому добавление нового hook в `pkg/hbasecop/hooktable.go` требует
дописать строку в таблицу ниже; CI-gate поймает
пропуск.

## Якоря тестов

Короткие имена, на которые ссылается колонка `covered_by`.

| anchor | scope | location |
|---|---|---|
| **dispatch** | T41 паритет через рефлексию (каждый hook в `hookTable` ↔ метод `RegionObserver` ↔ enum `HookId`) | `pkg/hbasecop/hooktable_test.go::TestHookTableIsCanonical`, `TestRegionObserverInterfaceCoversAllHooks` |
| **roundtrip** | T42 wire round-trip: каждый `proto.Request` кодируется / декодируется без потерь | `internal/wire/hookpb/*` (на основе corpus) и Java `wire.pb.*RoundTripTest` |
| **prePutIT** | T27 живой IT (Put → счётчик Go observer) | `test/java/.../PrePutCounterIT.java`, `examples/counter-observer/` |
| **readIT** | T43 живой IT (Get / Scan bypass) | `test/java/.../ReadPathFilterIT.java`, `examples/filter-observer/` |
| **batchIT** | T44 живой IT (частичный блок `Table.batch`) | `test/java/.../BatchPartialBlockIT.java` |
| **storageIT** | T45 живой IT (`admin.flush` + `admin.majorCompact`) | `test/java/.../StorageHooksIT.java` |
| **faultIT** | T36 матрица fault-injection | `test/java/.../FaultMatrixIT.java` |
| **adapter** | Java unit-тест на `RegionObserverAdapter` | `test/java/.../RegionObserverAdapter*Test.java` |

## Матрица покрытия

68 hooks (заморожены относительно enum HookId в `proto/hooks.proto`).

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

## Уровни покрытия

Колонка `covered_by` перечисляет самый сильный якорь первым.

- **Live IT** (`*IT`): hook выполняется внутри настоящего HBase RegionServer
  поверх bridge; доказывает, что wire-путь, override Java-адаптера
  и callback Go SDK работают end-to-end. Самый сильный уровень.
- **adapter**: Java unit-тест прогоняет hook `RegionObserverAdapter` из bridge
  со stub `ObserverContext`, прогоняя per-hook
  payload-маппер.
- **roundtrip**: proto Request сериализуется идентично с обеих сторон
  относительно закоммиченного golden corpus, так что межъязыковое
  wire-согласие зафиксировано даже там, где живого IT
  ещё нет.
- **dispatch**: T41 через рефлексию утверждает, что строка hook в `hookTable`
  имеет правильные id, name, decoder и invoke-замыкание, и что
  соответствующий метод интерфейса `RegionObserver` существует. Каждый hook
  покрыт как минимум `dispatch`.

Hooks, которые зависят от редко срабатывающих серверных путей (BulkLoad,
WALRestore, внутренности MemStoreCompaction), намеренно пока не
покрыты живым IT; Phase 5 добавит выделенные адаптеры observer-типов,
которые прогоняют эти участки кода.
