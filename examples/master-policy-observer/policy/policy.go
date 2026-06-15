// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package policy implements the MasterObserver used by the T51
// integration test. The Observer enforces a naming policy on the master
// table-lifecycle hooks: any table whose qualifier carries the
// configured blocked prefix is rejected. PreCreateTable returns an
// error so the (strict-by-default) Java MasterObserverAdapter surfaces
// it as an IOException back to the HBase admin client - the failure
// path required by T51's AC.
package policy

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

// Observer is a MasterObserver that rejects table-lifecycle operations
// on tables whose qualifier begins with the configured blocked prefix.
// All other MasterObserver methods inherit the no-op behaviour of
// UnimplementedMasterObserver.
type Observer struct {
	hbasecop.UnimplementedMasterObserver

	blocked []byte

	preCreateTables atomic.Uint64
	preDeleteTables atomic.Uint64
	rejectedCreates atomic.Uint64
	rejectedDeletes atomic.Uint64
}

// New constructs an Observer that rejects tables whose qualifier begins
// with blockedPrefix. A nil or empty prefix disables rejection entirely
// (every table is allowed).
func New(blockedPrefix []byte) *Observer {
	cp := append([]byte(nil), blockedPrefix...)
	return &Observer{blocked: cp}
}

// PreCreateTable rejects the create when the target table's qualifier
// matches the blocked prefix. Returning an error drives the strict-mode
// MasterObserverAdapter to throw IOException at the admin client.
func (o *Observer) PreCreateTable(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	req *hookpb.PreCreateTableRequest,
) (hbasecop.HookResult, error) {
	o.preCreateTables.Add(1)
	qualifier := req.GetTableName().GetQualifier()
	if o.matchesBlocked(qualifier) {
		o.rejectedCreates.Add(1)
		return hbasecop.HookResult{}, fmt.Errorf(
			"master-policy-observer: table %q rejected by naming policy (blocked prefix %q)",
			qualifier, o.blocked,
		)
	}
	return hbasecop.HookResult{}, nil
}

// PreDeleteTable rejects the delete when the target table's qualifier
// matches the blocked prefix, protecting policy-named tables from
// removal.
func (o *Observer) PreDeleteTable(
	_ context.Context,
	_ hbasecop.ObserverEnv,
	req *hookpb.PreDeleteTableRequest,
) (hbasecop.HookResult, error) {
	o.preDeleteTables.Add(1)
	qualifier := req.GetTableName().GetQualifier()
	if o.matchesBlocked(qualifier) {
		o.rejectedDeletes.Add(1)
		return hbasecop.HookResult{}, fmt.Errorf(
			"master-policy-observer: table %q delete rejected by naming policy (blocked prefix %q)",
			qualifier, o.blocked,
		)
	}
	return hbasecop.HookResult{}, nil
}

// PreCreateTableCount returns the cumulative count of PreCreateTable invocations.
func (o *Observer) PreCreateTableCount() uint64 { return o.preCreateTables.Load() }

// PreDeleteTableCount returns the cumulative count of PreDeleteTable invocations.
func (o *Observer) PreDeleteTableCount() uint64 { return o.preDeleteTables.Load() }

// RejectedCreateCount returns how many create-table calls were rejected.
func (o *Observer) RejectedCreateCount() uint64 { return o.rejectedCreates.Load() }

// RejectedDeleteCount returns how many delete-table calls were rejected.
func (o *Observer) RejectedDeleteCount() uint64 { return o.rejectedDeletes.Load() }

func (o *Observer) matchesBlocked(qualifier []byte) bool {
	return len(o.blocked) > 0 && bytes.HasPrefix(qualifier, o.blocked)
}
