// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

type MutationProto = hbasepb.MutationProto

type HookResult struct {
	Bypass         bool
	BlockedIndices []uint32

	ResultCells []*hbasepb.Cell
}

type RegionObserver interface {
	// Lifecycle.
	PreOpen(ctx context.Context, env ObserverEnv, req *hookpb.PreOpenRequest) (HookResult, error)
	PostOpen(ctx context.Context, env ObserverEnv, req *hookpb.PostOpenRequest) error
	PreClose(ctx context.Context, env ObserverEnv, req *hookpb.PreCloseRequest) (HookResult, error)
	PostClose(ctx context.Context, env ObserverEnv, req *hookpb.PostCloseRequest) error

	// Flush.
	PreFlush(ctx context.Context, env ObserverEnv, req *hookpb.PreFlushRequest) (HookResult, error)
	PreFlushScannerOpen(ctx context.Context, env ObserverEnv, req *hookpb.PreFlushScannerOpenRequest) (HookResult, error)
	PostFlush(ctx context.Context, env ObserverEnv, req *hookpb.PostFlushRequest) error

	// MemStore compaction.
	PreMemStoreCompaction(ctx context.Context, env ObserverEnv, req *hookpb.PreMemStoreCompactionRequest) (HookResult, error)
	PreMemStoreCompactionCompactScannerOpen(ctx context.Context, env ObserverEnv, req *hookpb.PreMemStoreCompactionCompactScannerOpenRequest) (HookResult, error)
	PreMemStoreCompactionCompact(ctx context.Context, env ObserverEnv, req *hookpb.PreMemStoreCompactionCompactRequest) (HookResult, error)
	PostMemStoreCompaction(ctx context.Context, env ObserverEnv, req *hookpb.PostMemStoreCompactionRequest) error

	// Compaction.
	PreCompactSelection(ctx context.Context, env ObserverEnv, req *hookpb.PreCompactSelectionRequest) (HookResult, error)
	PostCompactSelection(ctx context.Context, env ObserverEnv, req *hookpb.PostCompactSelectionRequest) error
	PreCompactScannerOpen(ctx context.Context, env ObserverEnv, req *hookpb.PreCompactScannerOpenRequest) (HookResult, error)
	PreCompact(ctx context.Context, env ObserverEnv, req *hookpb.PreCompactRequest) (HookResult, error)
	PostCompact(ctx context.Context, env ObserverEnv, req *hookpb.PostCompactRequest) error

	// Read path.
	PreGetOp(ctx context.Context, env ObserverEnv, req *hookpb.PreGetOpRequest) (HookResult, error)
	PostGetOp(ctx context.Context, env ObserverEnv, req *hookpb.PostGetOpRequest) error
	PreExists(ctx context.Context, env ObserverEnv, req *hookpb.PreExistsRequest) (HookResult, error)
	PostExists(ctx context.Context, env ObserverEnv, req *hookpb.PostExistsRequest) error

	// Write path - Put (frozen signatures, Phase-2 contract).
	PrePut(ctx context.Context, env ObserverEnv, mutation *MutationProto) (HookResult, error)
	PostPut(ctx context.Context, env ObserverEnv, mutation *MutationProto) error

	// Write path - Delete + version timestamp.
	PreDelete(ctx context.Context, env ObserverEnv, req *hookpb.PreDeleteRequest) (HookResult, error)
	PostDelete(ctx context.Context, env ObserverEnv, req *hookpb.PostDeleteRequest) error
	PrePrepareTimeStampForDeleteVersion(ctx context.Context, env ObserverEnv, req *hookpb.PrePrepareTimeStampForDeleteVersionRequest) (HookResult, error)

	// Batch mutate + region operation envelope.
	PreBatchMutate(ctx context.Context, env ObserverEnv, req *hookpb.PreBatchMutateRequest) (HookResult, error)
	PostBatchMutate(ctx context.Context, env ObserverEnv, req *hookpb.PostBatchMutateRequest) error
	PostBatchMutateIndispensably(ctx context.Context, env ObserverEnv, req *hookpb.PostBatchMutateIndispensablyRequest) error
	PostStartRegionOperation(ctx context.Context, env ObserverEnv, req *hookpb.PostStartRegionOperationRequest) error
	PostCloseRegionOperation(ctx context.Context, env ObserverEnv, req *hookpb.PostCloseRegionOperationRequest) error

	// Check-and-Put.
	PreCheckAndPut(ctx context.Context, env ObserverEnv, req *hookpb.PreCheckAndPutRequest) (HookResult, error)
	PostCheckAndPut(ctx context.Context, env ObserverEnv, req *hookpb.PostCheckAndPutRequest) error
	PreCheckAndPutAfterRowLock(ctx context.Context, env ObserverEnv, req *hookpb.PreCheckAndPutAfterRowLockRequest) (HookResult, error)

	// Check-and-Delete.
	PreCheckAndDelete(ctx context.Context, env ObserverEnv, req *hookpb.PreCheckAndDeleteRequest) (HookResult, error)
	PostCheckAndDelete(ctx context.Context, env ObserverEnv, req *hookpb.PostCheckAndDeleteRequest) error
	PreCheckAndDeleteAfterRowLock(ctx context.Context, env ObserverEnv, req *hookpb.PreCheckAndDeleteAfterRowLockRequest) (HookResult, error)

	// Check-and-Mutate.
	PreCheckAndMutate(ctx context.Context, env ObserverEnv, req *hookpb.PreCheckAndMutateRequest) (HookResult, error)
	PostCheckAndMutate(ctx context.Context, env ObserverEnv, req *hookpb.PostCheckAndMutateRequest) error
	PreCheckAndMutateAfterRowLock(ctx context.Context, env ObserverEnv, req *hookpb.PreCheckAndMutateAfterRowLockRequest) (HookResult, error)

	// Append.
	PreAppend(ctx context.Context, env ObserverEnv, req *hookpb.PreAppendRequest) (HookResult, error)
	PostAppend(ctx context.Context, env ObserverEnv, req *hookpb.PostAppendRequest) error
	PreAppendAfterRowLock(ctx context.Context, env ObserverEnv, req *hookpb.PreAppendAfterRowLockRequest) (HookResult, error)

	// Increment.
	PreIncrement(ctx context.Context, env ObserverEnv, req *hookpb.PreIncrementRequest) (HookResult, error)
	PostIncrement(ctx context.Context, env ObserverEnv, req *hookpb.PostIncrementRequest) error
	PreIncrementAfterRowLock(ctx context.Context, env ObserverEnv, req *hookpb.PreIncrementAfterRowLockRequest) (HookResult, error)

	// Scanner.
	PreScannerOpen(ctx context.Context, env ObserverEnv, req *hookpb.PreScannerOpenRequest) (HookResult, error)
	PostScannerOpen(ctx context.Context, env ObserverEnv, req *hookpb.PostScannerOpenRequest) error
	PreScannerNext(ctx context.Context, env ObserverEnv, req *hookpb.PreScannerNextRequest) (HookResult, error)
	PostScannerNext(ctx context.Context, env ObserverEnv, req *hookpb.PostScannerNextRequest) error
	PostScannerFilterRow(ctx context.Context, env ObserverEnv, req *hookpb.PostScannerFilterRowRequest) error
	PreScannerClose(ctx context.Context, env ObserverEnv, req *hookpb.PreScannerCloseRequest) (HookResult, error)
	PostScannerClose(ctx context.Context, env ObserverEnv, req *hookpb.PostScannerCloseRequest) error
	PreStoreScannerOpen(ctx context.Context, env ObserverEnv, req *hookpb.PreStoreScannerOpenRequest) (HookResult, error)

	// WAL replay/restore.
	PreReplayWALs(ctx context.Context, env ObserverEnv, req *hookpb.PreReplayWALsRequest) (HookResult, error)
	PostReplayWALs(ctx context.Context, env ObserverEnv, req *hookpb.PostReplayWALsRequest) error
	PreWALRestore(ctx context.Context, env ObserverEnv, req *hookpb.PreWALRestoreRequest) (HookResult, error)
	PostWALRestore(ctx context.Context, env ObserverEnv, req *hookpb.PostWALRestoreRequest) error

	// Bulk load + store-file commit.
	PreBulkLoadHFile(ctx context.Context, env ObserverEnv, req *hookpb.PreBulkLoadHFileRequest) (HookResult, error)
	PostBulkLoadHFile(ctx context.Context, env ObserverEnv, req *hookpb.PostBulkLoadHFileRequest) error
	PreCommitStoreFile(ctx context.Context, env ObserverEnv, req *hookpb.PreCommitStoreFileRequest) (HookResult, error)
	PostCommitStoreFile(ctx context.Context, env ObserverEnv, req *hookpb.PostCommitStoreFileRequest) error

	// Store-file reader.
	PreStoreFileReaderOpen(ctx context.Context, env ObserverEnv, req *hookpb.PreStoreFileReaderOpenRequest) (HookResult, error)
	PostStoreFileReaderOpen(ctx context.Context, env ObserverEnv, req *hookpb.PostStoreFileReaderOpenRequest) error

	// Before-WAL hooks.
	PostMutationBeforeWAL(ctx context.Context, env ObserverEnv, req *hookpb.PostMutationBeforeWALRequest) error
	PostIncrementBeforeWAL(ctx context.Context, env ObserverEnv, req *hookpb.PostIncrementBeforeWALRequest) error
	PostAppendBeforeWAL(ctx context.Context, env ObserverEnv, req *hookpb.PostAppendBeforeWALRequest) error

	// Delete tracker, WAL append.
	PostInstantiateDeleteTracker(ctx context.Context, env ObserverEnv, req *hookpb.PostInstantiateDeleteTrackerRequest) error
	PreWALAppend(ctx context.Context, env ObserverEnv, req *hookpb.PreWALAppendRequest) (HookResult, error)
}
