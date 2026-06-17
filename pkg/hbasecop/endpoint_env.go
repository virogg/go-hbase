// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
)

// EndpointEnv is the server-side execution context handed to an [Endpoint.Call]
// (Tier 2): it reads region-local data from inside the call, against the region
// the client invoked the endpoint on. Reverse reads compose — a handler may read
// row A and then, using a value from A, read row B (data-dependent access) —
// because each [EndpointEnv.Get] is an independent correlated round-trip.
//
// Get lands in TE32; OpenScanner in TE34. When the reverse path is disabled
// (the supervisor did not provision the bulk ring), Get returns an error.
type EndpointEnv struct {
	rc       *cpruntime.ReverseClient
	regionID uint32
}

// Get reads the cells of row from the invoking region and blocks until the
// bridge replies or ctx is done. A missing row yields a non-nil [Result] with no
// cells (len(r.GetCell()) == 0), not an error.
func (e *EndpointEnv) Get(ctx context.Context, row []byte) (*Result, error) {
	if e == nil || e.rc == nil {
		return nil, errors.New("hbasecop: reverse reads unavailable (reverse path disabled)")
	}
	getProto, err := proto.Marshal(&hbasepb.Get{Row: row})
	if err != nil {
		return nil, fmt.Errorf("hbasecop: marshal Get: %w", err)
	}
	resp, err := e.rc.Get(ctx, e.regionID, getProto)
	if err != nil {
		return nil, err
	}
	var result Result // = hbasepb.Result
	if err := proto.Unmarshal(resp.GetPayload(), &result); err != nil {
		return nil, fmt.Errorf("hbasecop: unmarshal Result: %w", err)
	}
	return &result, nil
}

// CellValue returns the value of the first cell in r matching family:qualifier
// and whether such a cell was present. Convenience for data-dependent reverse
// reads: read row A, pull a pointer value out of it, then read row B.
func CellValue(r *Result, family, qualifier []byte) ([]byte, bool) {
	if r == nil {
		return nil, false
	}
	for _, c := range r.GetCell() {
		if bytes.Equal(c.GetFamily(), family) && bytes.Equal(c.GetQualifier(), qualifier) {
			return c.GetValue(), true
		}
	}
	return nil, false
}
