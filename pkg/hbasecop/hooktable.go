// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

type hookEntry struct {
	id     HookID
	name   string
	decode func() proto.Message
	invoke hookInvoker
}

type hookInvoker func(observer RegionObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error)

func newReq[T any, PT interface {
	*T
	proto.Message
}]() proto.Message {
	return PT(new(T))
}

func preHook[Req proto.Message](method func(RegionObserver, context.Context, ObserverEnv, Req) (HookResult, error)) hookInvoker {
	return func(o RegionObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
		return method(o, ctx, env, req.(Req))
	}
}

func postHook[Req proto.Message](method func(RegionObserver, context.Context, ObserverEnv, Req) error) hookInvoker {
	return func(o RegionObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
		return HookResult{}, method(o, ctx, env, req.(Req))
	}
}

var hookTable = []hookEntry{
	// Lifecycle
	{HookIDPreOpen, "PreOpen", newReq[hookpb.PreOpenRequest], preHook(RegionObserver.PreOpen)},
	{HookIDPostOpen, "PostOpen", newReq[hookpb.PostOpenRequest], postHook(RegionObserver.PostOpen)},
	{HookIDPreClose, "PreClose", newReq[hookpb.PreCloseRequest], preHook(RegionObserver.PreClose)},
	{HookIDPostClose, "PostClose", newReq[hookpb.PostCloseRequest], postHook(RegionObserver.PostClose)},

	// Flush
	{HookIDPreFlush, "PreFlush", newReq[hookpb.PreFlushRequest], preHook(RegionObserver.PreFlush)},
	{HookIDPreFlushScannerOpen, "PreFlushScannerOpen", newReq[hookpb.PreFlushScannerOpenRequest], preHook(RegionObserver.PreFlushScannerOpen)},
	{HookIDPostFlush, "PostFlush", newReq[hookpb.PostFlushRequest], postHook(RegionObserver.PostFlush)},

	// MemStore compaction
	{HookIDPreMemStoreCompaction, "PreMemStoreCompaction", newReq[hookpb.PreMemStoreCompactionRequest], preHook(RegionObserver.PreMemStoreCompaction)},
	{HookIDPreMemStoreCompactionCompactScannerOpen, "PreMemStoreCompactionCompactScannerOpen", newReq[hookpb.PreMemStoreCompactionCompactScannerOpenRequest], preHook(RegionObserver.PreMemStoreCompactionCompactScannerOpen)},
	{HookIDPreMemStoreCompactionCompact, "PreMemStoreCompactionCompact", newReq[hookpb.PreMemStoreCompactionCompactRequest], preHook(RegionObserver.PreMemStoreCompactionCompact)},
	{HookIDPostMemStoreCompaction, "PostMemStoreCompaction", newReq[hookpb.PostMemStoreCompactionRequest], postHook(RegionObserver.PostMemStoreCompaction)},

	// Compaction
	{HookIDPreCompactSelection, "PreCompactSelection", newReq[hookpb.PreCompactSelectionRequest], preHook(RegionObserver.PreCompactSelection)},
	{HookIDPostCompactSelection, "PostCompactSelection", newReq[hookpb.PostCompactSelectionRequest], postHook(RegionObserver.PostCompactSelection)},
	{HookIDPreCompactScannerOpen, "PreCompactScannerOpen", newReq[hookpb.PreCompactScannerOpenRequest], preHook(RegionObserver.PreCompactScannerOpen)},
	{HookIDPreCompact, "PreCompact", newReq[hookpb.PreCompactRequest], preHook(RegionObserver.PreCompact)},
	{HookIDPostCompact, "PostCompact", newReq[hookpb.PostCompactRequest], postHook(RegionObserver.PostCompact)},

	// Read path
	{HookIDPreGetOp, "PreGetOp", newReq[hookpb.PreGetOpRequest], preHook(RegionObserver.PreGetOp)},
	{HookIDPostGetOp, "PostGetOp", newReq[hookpb.PostGetOpRequest], postHook(RegionObserver.PostGetOp)},
	{HookIDPreExists, "PreExists", newReq[hookpb.PreExistsRequest], preHook(RegionObserver.PreExists)},
	{HookIDPostExists, "PostExists", newReq[hookpb.PostExistsRequest], postHook(RegionObserver.PostExists)},

	// Write path - Put
	{
		HookIDPrePut, "PrePut",
		newReq[hookpb.PrePutRequest],
		func(o RegionObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
			return o.PrePut(ctx, env, req.(*hookpb.PrePutRequest).GetMutation())
		},
	},
	{
		HookIDPostPut, "PostPut",
		newReq[hookpb.PostPutRequest],
		func(o RegionObserver, ctx context.Context, env ObserverEnv, req proto.Message) (HookResult, error) {
			return HookResult{}, o.PostPut(ctx, env, req.(*hookpb.PostPutRequest).GetMutation())
		},
	},

	// Write path - Delete + version timestamp.
	{HookIDPreDelete, "PreDelete", newReq[hookpb.PreDeleteRequest], preHook(RegionObserver.PreDelete)},
	{HookIDPostDelete, "PostDelete", newReq[hookpb.PostDeleteRequest], postHook(RegionObserver.PostDelete)},
	{HookIDPrePrepareTimeStampForDeleteVersion, "PrePrepareTimeStampForDeleteVersion", newReq[hookpb.PrePrepareTimeStampForDeleteVersionRequest], preHook(RegionObserver.PrePrepareTimeStampForDeleteVersion)},

	// Batch mutate + region operation envelope.
	{HookIDPreBatchMutate, "PreBatchMutate", newReq[hookpb.PreBatchMutateRequest], preHook(RegionObserver.PreBatchMutate)},
	{HookIDPostBatchMutate, "PostBatchMutate", newReq[hookpb.PostBatchMutateRequest], postHook(RegionObserver.PostBatchMutate)},
	{HookIDPostBatchMutateIndispensably, "PostBatchMutateIndispensably", newReq[hookpb.PostBatchMutateIndispensablyRequest], postHook(RegionObserver.PostBatchMutateIndispensably)},
	{HookIDPostStartRegionOperation, "PostStartRegionOperation", newReq[hookpb.PostStartRegionOperationRequest], postHook(RegionObserver.PostStartRegionOperation)},
	{HookIDPostCloseRegionOperation, "PostCloseRegionOperation", newReq[hookpb.PostCloseRegionOperationRequest], postHook(RegionObserver.PostCloseRegionOperation)},

	// Check-and-Put.
	{HookIDPreCheckAndPut, "PreCheckAndPut", newReq[hookpb.PreCheckAndPutRequest], preHook(RegionObserver.PreCheckAndPut)},
	{HookIDPostCheckAndPut, "PostCheckAndPut", newReq[hookpb.PostCheckAndPutRequest], postHook(RegionObserver.PostCheckAndPut)},
	{HookIDPreCheckAndPutAfterRowLock, "PreCheckAndPutAfterRowLock", newReq[hookpb.PreCheckAndPutAfterRowLockRequest], preHook(RegionObserver.PreCheckAndPutAfterRowLock)},

	// Check-and-Delete.
	{HookIDPreCheckAndDelete, "PreCheckAndDelete", newReq[hookpb.PreCheckAndDeleteRequest], preHook(RegionObserver.PreCheckAndDelete)},
	{HookIDPostCheckAndDelete, "PostCheckAndDelete", newReq[hookpb.PostCheckAndDeleteRequest], postHook(RegionObserver.PostCheckAndDelete)},
	{HookIDPreCheckAndDeleteAfterRowLock, "PreCheckAndDeleteAfterRowLock", newReq[hookpb.PreCheckAndDeleteAfterRowLockRequest], preHook(RegionObserver.PreCheckAndDeleteAfterRowLock)},

	// Check-and-Mutate.
	{HookIDPreCheckAndMutate, "PreCheckAndMutate", newReq[hookpb.PreCheckAndMutateRequest], preHook(RegionObserver.PreCheckAndMutate)},
	{HookIDPostCheckAndMutate, "PostCheckAndMutate", newReq[hookpb.PostCheckAndMutateRequest], postHook(RegionObserver.PostCheckAndMutate)},
	{HookIDPreCheckAndMutateAfterRowLock, "PreCheckAndMutateAfterRowLock", newReq[hookpb.PreCheckAndMutateAfterRowLockRequest], preHook(RegionObserver.PreCheckAndMutateAfterRowLock)},

	// Append.
	{HookIDPreAppend, "PreAppend", newReq[hookpb.PreAppendRequest], preHook(RegionObserver.PreAppend)},
	{HookIDPostAppend, "PostAppend", newReq[hookpb.PostAppendRequest], postHook(RegionObserver.PostAppend)},
	{HookIDPreAppendAfterRowLock, "PreAppendAfterRowLock", newReq[hookpb.PreAppendAfterRowLockRequest], preHook(RegionObserver.PreAppendAfterRowLock)},

	// Increment.
	{HookIDPreIncrement, "PreIncrement", newReq[hookpb.PreIncrementRequest], preHook(RegionObserver.PreIncrement)},
	{HookIDPostIncrement, "PostIncrement", newReq[hookpb.PostIncrementRequest], postHook(RegionObserver.PostIncrement)},
	{HookIDPreIncrementAfterRowLock, "PreIncrementAfterRowLock", newReq[hookpb.PreIncrementAfterRowLockRequest], preHook(RegionObserver.PreIncrementAfterRowLock)},

	// Scanner.
	{HookIDPreScannerOpen, "PreScannerOpen", newReq[hookpb.PreScannerOpenRequest], preHook(RegionObserver.PreScannerOpen)},
	{HookIDPostScannerOpen, "PostScannerOpen", newReq[hookpb.PostScannerOpenRequest], postHook(RegionObserver.PostScannerOpen)},
	{HookIDPreScannerNext, "PreScannerNext", newReq[hookpb.PreScannerNextRequest], preHook(RegionObserver.PreScannerNext)},
	{HookIDPostScannerNext, "PostScannerNext", newReq[hookpb.PostScannerNextRequest], postHook(RegionObserver.PostScannerNext)},
	{HookIDPostScannerFilterRow, "PostScannerFilterRow", newReq[hookpb.PostScannerFilterRowRequest], postHook(RegionObserver.PostScannerFilterRow)},
	{HookIDPreScannerClose, "PreScannerClose", newReq[hookpb.PreScannerCloseRequest], preHook(RegionObserver.PreScannerClose)},
	{HookIDPostScannerClose, "PostScannerClose", newReq[hookpb.PostScannerCloseRequest], postHook(RegionObserver.PostScannerClose)},
	{HookIDPreStoreScannerOpen, "PreStoreScannerOpen", newReq[hookpb.PreStoreScannerOpenRequest], preHook(RegionObserver.PreStoreScannerOpen)},

	// WAL replay/restore.
	{HookIDPreReplayWALs, "PreReplayWALs", newReq[hookpb.PreReplayWALsRequest], preHook(RegionObserver.PreReplayWALs)},
	{HookIDPostReplayWALs, "PostReplayWALs", newReq[hookpb.PostReplayWALsRequest], postHook(RegionObserver.PostReplayWALs)},
	{HookIDPreWALRestore, "PreWALRestore", newReq[hookpb.PreWALRestoreRequest], preHook(RegionObserver.PreWALRestore)},
	{HookIDPostWALRestore, "PostWALRestore", newReq[hookpb.PostWALRestoreRequest], postHook(RegionObserver.PostWALRestore)},

	// Bulk load + store-file commit.
	{HookIDPreBulkLoadHFile, "PreBulkLoadHFile", newReq[hookpb.PreBulkLoadHFileRequest], preHook(RegionObserver.PreBulkLoadHFile)},
	{HookIDPostBulkLoadHFile, "PostBulkLoadHFile", newReq[hookpb.PostBulkLoadHFileRequest], postHook(RegionObserver.PostBulkLoadHFile)},
	{HookIDPreCommitStoreFile, "PreCommitStoreFile", newReq[hookpb.PreCommitStoreFileRequest], preHook(RegionObserver.PreCommitStoreFile)},
	{HookIDPostCommitStoreFile, "PostCommitStoreFile", newReq[hookpb.PostCommitStoreFileRequest], postHook(RegionObserver.PostCommitStoreFile)},

	// Store-file reader.
	{HookIDPreStoreFileReaderOpen, "PreStoreFileReaderOpen", newReq[hookpb.PreStoreFileReaderOpenRequest], preHook(RegionObserver.PreStoreFileReaderOpen)},
	{HookIDPostStoreFileReaderOpen, "PostStoreFileReaderOpen", newReq[hookpb.PostStoreFileReaderOpenRequest], postHook(RegionObserver.PostStoreFileReaderOpen)},

	// Before-WAL hooks.
	{HookIDPostMutationBeforeWAL, "PostMutationBeforeWAL", newReq[hookpb.PostMutationBeforeWALRequest], postHook(RegionObserver.PostMutationBeforeWAL)},
	{HookIDPostIncrementBeforeWAL, "PostIncrementBeforeWAL", newReq[hookpb.PostIncrementBeforeWALRequest], postHook(RegionObserver.PostIncrementBeforeWAL)},
	{HookIDPostAppendBeforeWAL, "PostAppendBeforeWAL", newReq[hookpb.PostAppendBeforeWALRequest], postHook(RegionObserver.PostAppendBeforeWAL)},

	// Delete tracker, WAL append.
	{HookIDPostInstantiateDeleteTracker, "PostInstantiateDeleteTracker", newReq[hookpb.PostInstantiateDeleteTrackerRequest], postHook(RegionObserver.PostInstantiateDeleteTracker)},
	{HookIDPreWALAppend, "PreWALAppend", newReq[hookpb.PreWALAppendRequest], preHook(RegionObserver.PreWALAppend)},
}

var hooksByID = func() map[HookID]hookEntry {
	m := make(map[HookID]hookEntry, len(hookTable))
	for _, h := range hookTable {
		m[h.id] = h
	}
	return m
}()

func HookNames() []string {
	names := make([]string, 0,
		len(hookTable)+len(masterHookTable)+len(regionServerHookTable)+len(walHookTable)+len(bulkLoadHookTable))
	for _, h := range hookTable {
		names = append(names, h.name)
	}
	for _, h := range masterHookTable {
		names = append(names, h.name)
	}
	for _, h := range regionServerHookTable {
		names = append(names, h.name)
	}
	for _, h := range walHookTable {
		names = append(names, h.name)
	}
	for _, h := range bulkLoadHookTable {
		names = append(names, h.name)
	}
	return names
}
