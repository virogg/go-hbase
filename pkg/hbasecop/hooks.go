// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

// HookID is the on-wire discriminator that selects a RegionObserver
// method on the Go side. Values are stable across versions; the
// authoritative hook table is generated from the .proto registry in
// T41.
//
// Phase 2 only assigns PrePut/PostPut; the 0xFF slot is reserved for
// the in-process ping/pong probe used by cpruntime tests.
type HookID uint8

// HookID values. The Phase 2 surface is intentionally tiny; T41
// regenerates this block from the .proto registry once every
// RegionObserver hook is in scope.
const (
	HookIDUnknown HookID = 0
	HookIDPrePut  HookID = 1
	HookIDPostPut HookID = 2
)
