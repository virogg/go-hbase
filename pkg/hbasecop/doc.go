// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package hbasecop is the public Go SDK for writing HBase coprocessors.
//
// Users implement one of the Observer interfaces (RegionObserver,
// MasterObserver, RegionServerObserver, WALObserver, BulkLoadObserver)
// and call Run to start the runtime that receives hook invocations
// from the Java RegionServer-side adapter over a shared-memory ring
// buffer. For RegionObserver, NewRegion plus the On* setters assemble an
// observer from per-hook closures instead of implementing the interface.
//
// Phase 2 only delivers RegionObserver{PrePut, PostPut}; the rest of
// the Observer surface lands in T41+. See SPEC.md for the architecture
// and tasks/plan.md for delivery state.
package hbasecop

//go:generate go run ../../tools/gen-wiretypes -hookpb ../../internal/wire/hookpb -hbasepb ../../internal/wire/hbasepb -out wiretypes.go
//go:generate go run ../../tools/gen-builder -src observer.go -iface RegionObserver -out region_builder.go
