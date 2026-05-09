// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package cpruntime is the Go-side event loop for HBase coprocessors:
// it owns the shmem channel, dispatches inbound requests to user Observer
// handlers and emits heartbeats.
//
// Named cpruntime (coproc runtime) to avoid colliding with the stdlib
// `runtime` package.
package cpruntime
