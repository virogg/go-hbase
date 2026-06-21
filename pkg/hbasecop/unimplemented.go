// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

type UnimplementedRegionObserver struct{}

// The 68 methods below are intentionally undocumented one-liners: they
// all do the same thing (return the zero value, no error). The type
// doc-comment above is the single source of truth for the contract.
//revive:disable:exported

// Lifecycle.

func (UnimplementedRegionObserver) PreOpen(context.Context, ObserverEnv, *hookpb.PreOpenRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostOpen(context.Context, ObserverEnv, *hookpb.PostOpenRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreClose(context.Context, ObserverEnv, *hookpb.PreCloseRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostClose(context.Context, ObserverEnv, *hookpb.PostCloseRequest) error {
	return nil
}

// Flush.

func (UnimplementedRegionObserver) PreFlush(context.Context, ObserverEnv, *hookpb.PreFlushRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PreFlushScannerOpen(context.Context, ObserverEnv, *hookpb.PreFlushScannerOpenRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostFlush(context.Context, ObserverEnv, *hookpb.PostFlushRequest) error {
	return nil
}

// MemStore compaction.

func (UnimplementedRegionObserver) PreMemStoreCompaction(context.Context, ObserverEnv, *hookpb.PreMemStoreCompactionRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PreMemStoreCompactionCompactScannerOpen(context.Context, ObserverEnv, *hookpb.PreMemStoreCompactionCompactScannerOpenRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PreMemStoreCompactionCompact(context.Context, ObserverEnv, *hookpb.PreMemStoreCompactionCompactRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostMemStoreCompaction(context.Context, ObserverEnv, *hookpb.PostMemStoreCompactionRequest) error {
	return nil
}

// Compaction.

func (UnimplementedRegionObserver) PreCompactSelection(context.Context, ObserverEnv, *hookpb.PreCompactSelectionRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostCompactSelection(context.Context, ObserverEnv, *hookpb.PostCompactSelectionRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreCompactScannerOpen(context.Context, ObserverEnv, *hookpb.PreCompactScannerOpenRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PreCompact(context.Context, ObserverEnv, *hookpb.PreCompactRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostCompact(context.Context, ObserverEnv, *hookpb.PostCompactRequest) error {
	return nil
}

// Read path.

func (UnimplementedRegionObserver) PreGetOp(context.Context, ObserverEnv, *hookpb.PreGetOpRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostGetOp(context.Context, ObserverEnv, *hookpb.PostGetOpRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreExists(context.Context, ObserverEnv, *hookpb.PreExistsRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostExists(context.Context, ObserverEnv, *hookpb.PostExistsRequest) error {
	return nil
}

// Write path, Put (frozen Phase-2 signatures).

func (UnimplementedRegionObserver) PrePut(context.Context, ObserverEnv, *MutationProto) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostPut(context.Context, ObserverEnv, *MutationProto) error {
	return nil
}

// Write path, Delete + version timestamp.

func (UnimplementedRegionObserver) PreDelete(context.Context, ObserverEnv, *hookpb.PreDeleteRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostDelete(context.Context, ObserverEnv, *hookpb.PostDeleteRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PrePrepareTimeStampForDeleteVersion(context.Context, ObserverEnv, *hookpb.PrePrepareTimeStampForDeleteVersionRequest) (HookResult, error) {
	return HookResult{}, nil
}

// Batch mutate + region operation envelope.

func (UnimplementedRegionObserver) PreBatchMutate(context.Context, ObserverEnv, *hookpb.PreBatchMutateRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostBatchMutate(context.Context, ObserverEnv, *hookpb.PostBatchMutateRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PostBatchMutateIndispensably(context.Context, ObserverEnv, *hookpb.PostBatchMutateIndispensablyRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PostStartRegionOperation(context.Context, ObserverEnv, *hookpb.PostStartRegionOperationRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PostCloseRegionOperation(context.Context, ObserverEnv, *hookpb.PostCloseRegionOperationRequest) error {
	return nil
}

// Check-and-Put.

func (UnimplementedRegionObserver) PreCheckAndPut(context.Context, ObserverEnv, *hookpb.PreCheckAndPutRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostCheckAndPut(context.Context, ObserverEnv, *hookpb.PostCheckAndPutRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreCheckAndPutAfterRowLock(context.Context, ObserverEnv, *hookpb.PreCheckAndPutAfterRowLockRequest) (HookResult, error) {
	return HookResult{}, nil
}

// Check-and-Delete.

func (UnimplementedRegionObserver) PreCheckAndDelete(context.Context, ObserverEnv, *hookpb.PreCheckAndDeleteRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostCheckAndDelete(context.Context, ObserverEnv, *hookpb.PostCheckAndDeleteRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreCheckAndDeleteAfterRowLock(context.Context, ObserverEnv, *hookpb.PreCheckAndDeleteAfterRowLockRequest) (HookResult, error) {
	return HookResult{}, nil
}

// Check-and-Mutate.

func (UnimplementedRegionObserver) PreCheckAndMutate(context.Context, ObserverEnv, *hookpb.PreCheckAndMutateRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostCheckAndMutate(context.Context, ObserverEnv, *hookpb.PostCheckAndMutateRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreCheckAndMutateAfterRowLock(context.Context, ObserverEnv, *hookpb.PreCheckAndMutateAfterRowLockRequest) (HookResult, error) {
	return HookResult{}, nil
}

// Append.

func (UnimplementedRegionObserver) PreAppend(context.Context, ObserverEnv, *hookpb.PreAppendRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostAppend(context.Context, ObserverEnv, *hookpb.PostAppendRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreAppendAfterRowLock(context.Context, ObserverEnv, *hookpb.PreAppendAfterRowLockRequest) (HookResult, error) {
	return HookResult{}, nil
}

// Increment.

func (UnimplementedRegionObserver) PreIncrement(context.Context, ObserverEnv, *hookpb.PreIncrementRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostIncrement(context.Context, ObserverEnv, *hookpb.PostIncrementRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreIncrementAfterRowLock(context.Context, ObserverEnv, *hookpb.PreIncrementAfterRowLockRequest) (HookResult, error) {
	return HookResult{}, nil
}

// Scanner.

func (UnimplementedRegionObserver) PreScannerOpen(context.Context, ObserverEnv, *hookpb.PreScannerOpenRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostScannerOpen(context.Context, ObserverEnv, *hookpb.PostScannerOpenRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreScannerNext(context.Context, ObserverEnv, *hookpb.PreScannerNextRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostScannerNext(context.Context, ObserverEnv, *hookpb.PostScannerNextRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PostScannerFilterRow(context.Context, ObserverEnv, *hookpb.PostScannerFilterRowRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreScannerClose(context.Context, ObserverEnv, *hookpb.PreScannerCloseRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostScannerClose(context.Context, ObserverEnv, *hookpb.PostScannerCloseRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreStoreScannerOpen(context.Context, ObserverEnv, *hookpb.PreStoreScannerOpenRequest) (HookResult, error) {
	return HookResult{}, nil
}

// WAL replay/restore.

func (UnimplementedRegionObserver) PreReplayWALs(context.Context, ObserverEnv, *hookpb.PreReplayWALsRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostReplayWALs(context.Context, ObserverEnv, *hookpb.PostReplayWALsRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreWALRestore(context.Context, ObserverEnv, *hookpb.PreWALRestoreRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostWALRestore(context.Context, ObserverEnv, *hookpb.PostWALRestoreRequest) error {
	return nil
}

// Bulk load + store-file commit.

func (UnimplementedRegionObserver) PreBulkLoadHFile(context.Context, ObserverEnv, *hookpb.PreBulkLoadHFileRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostBulkLoadHFile(context.Context, ObserverEnv, *hookpb.PostBulkLoadHFileRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreCommitStoreFile(context.Context, ObserverEnv, *hookpb.PreCommitStoreFileRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostCommitStoreFile(context.Context, ObserverEnv, *hookpb.PostCommitStoreFileRequest) error {
	return nil
}

// Store-file reader.

func (UnimplementedRegionObserver) PreStoreFileReaderOpen(context.Context, ObserverEnv, *hookpb.PreStoreFileReaderOpenRequest) (HookResult, error) {
	return HookResult{}, nil
}

func (UnimplementedRegionObserver) PostStoreFileReaderOpen(context.Context, ObserverEnv, *hookpb.PostStoreFileReaderOpenRequest) error {
	return nil
}

// Before-WAL hooks.

func (UnimplementedRegionObserver) PostMutationBeforeWAL(context.Context, ObserverEnv, *hookpb.PostMutationBeforeWALRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PostIncrementBeforeWAL(context.Context, ObserverEnv, *hookpb.PostIncrementBeforeWALRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PostAppendBeforeWAL(context.Context, ObserverEnv, *hookpb.PostAppendBeforeWALRequest) error {
	return nil
}

// Delete tracker, WAL append.

func (UnimplementedRegionObserver) PostInstantiateDeleteTracker(context.Context, ObserverEnv, *hookpb.PostInstantiateDeleteTrackerRequest) error {
	return nil
}

func (UnimplementedRegionObserver) PreWALAppend(context.Context, ObserverEnv, *hookpb.PreWALAppendRequest) (HookResult, error) {
	return HookResult{}, nil
}
