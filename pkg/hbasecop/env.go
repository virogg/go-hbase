// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

// ObserverEnv identifies the table/region scope of a single hook
// invocation. It mirrors the relevant subset of
// org.apache.hadoop.hbase.coprocessor.ObserverContext for the Go SDK.
//
// TableName is rendered as "namespace:qualifier"; the namespace is
// omitted when empty so callers can do plain string equality against
// the table identifiers they registered against. RegionName is the
// HBase encoded region name (RegionInfo.getEncodedName), opaque to the
// SDK but stable across the lifetime of a region.
//
// RegionID is the wire-level routing key allocated by the Java
// supervisor on RegionObserver.start(env) and freed on stop. It is
// monotonic per Go-process lifetime and stable across the lifetime of
// the region in this process - restarts of the Go process or the
// region itself will produce a fresh id. RegionID is 0 for Observer
// surfaces that don't carry region scope (Master, RegionServer); WAL
// hooks may set it when the entry references a specific region.
type ObserverEnv struct {
	TableName  string
	RegionName string
	RegionID   uint32
}
