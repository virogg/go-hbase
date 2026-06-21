// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

//go:generate go run ../../tools/gen-wiretypes -hookpb ../../internal/wire/hookpb -hbasepb ../../internal/wire/hbasepb -out wiretypes.go
//go:generate go run ../../tools/gen-builder -src observer.go -iface RegionObserver -out region_builder.go
