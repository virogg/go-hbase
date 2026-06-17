// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import "log/slog"

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

	// logger is the runtime's base logger (inherits HBASECOP_LOG_LEVEL); the
	// hook name and req id are kept aside so Logger can tag lazily. Both are
	// zero on a hand-constructed env, in which case Logger falls back to the
	// global slog default.
	logger *slog.Logger
	hook   string
	reqID  uint64
}

// Logger returns a logger pre-tagged with this hook's name, request id, table
// and region, so an observer's log lines correlate with the framework's and
// inherit HBASECOP_LOG_LEVEL. Prefer it over the global slog.
//
// The tagged logger is built on demand, so a hook that never logs pays nothing
// on the dispatch hot path. It is always safe to call: on an ObserverEnv built
// outside the runtime (e.g. a direct unit-test call) it returns slog.Default().
func (e ObserverEnv) Logger() *slog.Logger {
	if e.logger == nil {
		return slog.Default()
	}
	return e.logger.With("hook", e.hook, "req_id", e.reqID, "table", e.TableName, "region", e.RegionName)
}
