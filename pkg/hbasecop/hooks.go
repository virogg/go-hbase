// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

type HookID uint8

const (
	HookIDUnknown HookID = 0

	HookIDPreOpen   HookID = 1
	HookIDPostOpen  HookID = 2
	HookIDPreClose  HookID = 3
	HookIDPostClose HookID = 4

	HookIDPreFlush            HookID = 5
	HookIDPreFlushScannerOpen HookID = 6
	HookIDPostFlush           HookID = 7

	HookIDPreMemStoreCompaction                   HookID = 8
	HookIDPreMemStoreCompactionCompactScannerOpen HookID = 9
	HookIDPreMemStoreCompactionCompact            HookID = 10
	HookIDPostMemStoreCompaction                  HookID = 11

	HookIDPreCompactSelection   HookID = 12
	HookIDPostCompactSelection  HookID = 13
	HookIDPreCompactScannerOpen HookID = 14
	HookIDPreCompact            HookID = 15
	HookIDPostCompact           HookID = 16

	HookIDPreGetOp   HookID = 17
	HookIDPostGetOp  HookID = 18
	HookIDPreExists  HookID = 19
	HookIDPostExists HookID = 20

	HookIDPrePut                              HookID = 21
	HookIDPostPut                             HookID = 22
	HookIDPreDelete                           HookID = 23
	HookIDPostDelete                          HookID = 24
	HookIDPrePrepareTimeStampForDeleteVersion HookID = 25

	HookIDPreBatchMutate               HookID = 26
	HookIDPostBatchMutate              HookID = 27
	HookIDPostBatchMutateIndispensably HookID = 28
	HookIDPostStartRegionOperation     HookID = 29
	HookIDPostCloseRegionOperation     HookID = 30

	HookIDPreCheckAndPut             HookID = 31
	HookIDPostCheckAndPut            HookID = 32
	HookIDPreCheckAndPutAfterRowLock HookID = 33

	HookIDPreCheckAndDelete             HookID = 34
	HookIDPostCheckAndDelete            HookID = 35
	HookIDPreCheckAndDeleteAfterRowLock HookID = 36

	HookIDPreCheckAndMutate             HookID = 37
	HookIDPostCheckAndMutate            HookID = 38
	HookIDPreCheckAndMutateAfterRowLock HookID = 39

	HookIDPreAppend             HookID = 40
	HookIDPostAppend            HookID = 41
	HookIDPreAppendAfterRowLock HookID = 42

	HookIDPreIncrement             HookID = 43
	HookIDPostIncrement            HookID = 44
	HookIDPreIncrementAfterRowLock HookID = 45

	HookIDPreScannerOpen       HookID = 46
	HookIDPostScannerOpen      HookID = 47
	HookIDPreScannerNext       HookID = 48
	HookIDPostScannerNext      HookID = 49
	HookIDPostScannerFilterRow HookID = 50
	HookIDPreScannerClose      HookID = 51
	HookIDPostScannerClose     HookID = 52
	HookIDPreStoreScannerOpen  HookID = 53

	HookIDPreReplayWALs  HookID = 54
	HookIDPostReplayWALs HookID = 55
	HookIDPreWALRestore  HookID = 56
	HookIDPostWALRestore HookID = 57

	HookIDPreBulkLoadHFile    HookID = 58
	HookIDPostBulkLoadHFile   HookID = 59
	HookIDPreCommitStoreFile  HookID = 60
	HookIDPostCommitStoreFile HookID = 61

	HookIDPreStoreFileReaderOpen  HookID = 62
	HookIDPostStoreFileReaderOpen HookID = 63

	HookIDPostMutationBeforeWAL  HookID = 64
	HookIDPostIncrementBeforeWAL HookID = 65
	HookIDPostAppendBeforeWAL    HookID = 66

	HookIDPostInstantiateDeleteTracker HookID = 67
	HookIDPreWALAppend                 HookID = 68

	// MasterObserver
	HookIDPreCreateTable    HookID = 100
	HookIDPostCreateTable   HookID = 101
	HookIDPreDeleteTable    HookID = 102
	HookIDPostDeleteTable   HookID = 103
	HookIDPreModifyTable    HookID = 104
	HookIDPostModifyTable   HookID = 105
	HookIDPreTruncateTable  HookID = 106
	HookIDPostTruncateTable HookID = 107
	HookIDPreEnableTable    HookID = 108
	HookIDPostEnableTable   HookID = 109
	HookIDPreDisableTable   HookID = 110
	HookIDPostDisableTable  HookID = 111
	HookIDPreMove           HookID = 112
	HookIDPostMove          HookID = 113
	HookIDPreAssign         HookID = 114
	HookIDPostAssign        HookID = 115
	HookIDPreUnassign       HookID = 116
	HookIDPostUnassign      HookID = 117
	HookIDPreBalance        HookID = 118
	HookIDPostBalance       HookID = 119

	// RegionServerObserver
	HookIDPreStopRegionServer       HookID = 200
	HookIDPreRollWALWriterRequest   HookID = 201
	HookIDPostRollWALWriterRequest  HookID = 202
	HookIDPreReplicateLogEntries    HookID = 203
	HookIDPostReplicateLogEntries   HookID = 204
	HookIDPreClearCompactionQueues  HookID = 205
	HookIDPostClearCompactionQueues HookID = 206
	HookIDPreExecuteProcedures      HookID = 207
	HookIDPostExecuteProcedures     HookID = 208

	// WALObserver
	HookIDPreWALWrite  HookID = 220
	HookIDPostWALWrite HookID = 221
	HookIDPreWALRoll   HookID = 222
	HookIDPostWALRoll  HookID = 223

	// BulkLoadObserver
	HookIDPrePrepareBulkLoad HookID = 224
	HookIDPreCleanupBulkLoad HookID = 225
)
