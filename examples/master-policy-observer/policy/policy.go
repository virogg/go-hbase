// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type Observer struct {
	hbasecop.UnimplementedMasterObserver

	blocked []byte

	preCreateTables atomic.Uint64
	preDeleteTables atomic.Uint64
	rejectedCreates atomic.Uint64
	rejectedDeletes atomic.Uint64
}

func New(blockedPrefix []byte) *Observer {
	cp := append([]byte(nil), blockedPrefix...)
	return &Observer{blocked: cp}
}

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

func (o *Observer) PreCreateTableCount() uint64 { return o.preCreateTables.Load() }

func (o *Observer) PreDeleteTableCount() uint64 { return o.preDeleteTables.Load() }

func (o *Observer) RejectedCreateCount() uint64 { return o.rejectedCreates.Load() }

func (o *Observer) RejectedDeleteCount() uint64 { return o.rejectedDeletes.Load() }

func (o *Observer) matchesBlocked(qualifier []byte) bool {
	return len(o.blocked) > 0 && bytes.HasPrefix(qualifier, o.blocked)
}
