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
// Get lands in TE32, OpenScanner in TE33 (ergonomic iteration + the canonical
// aggregation in TE34). When the reverse path is disabled (the supervisor did
// not provision the bulk ring), the reverse methods return an error.
type EndpointEnv struct {
	rc       *cpruntime.ReverseClient
	regionID uint32
	callID   uint64 // groups this call's scanners for lifecycle/reaping
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

// Put writes the cells of m to the invoking region from inside the endpoint call
// (Tier 2, TE41); it sets m's MutateType to PUT. Delete is the tombstone
// counterpart. Build m with the re-exported [MutationProto] (and its
// [MutationProto_ColumnValue] cells).
//
// The write goes through the region's observer pipeline — Pre/Post-Put/Delete
// hooks fire, exactly as HBase's own MultiRowMutationEndpoint — but bypasses the
// client RPC stack (no RSRpcServices/ACL/quota). It is gated off server-side
// unless hbasecop.endpoint.allow-mutate=true; when disabled, or when the reverse
// path is unavailable, the call returns an error. Writing the region the
// endpoint was invoked on is safe (no self-deadlock); just avoid an observer
// that re-mutates in a loop (the bridge has no recursion guard, as in HBase).
func (e *EndpointEnv) Put(ctx context.Context, m *MutationProto) error {
	m.MutateType = hbasepb.MutationProto_PUT.Enum()
	return e.mutate(ctx, m)
}

// Delete applies m as a Delete to the invoking region; it sets m's MutateType to
// DELETE. Same gating and observer-pipeline semantics as [EndpointEnv.Put].
func (e *EndpointEnv) Delete(ctx context.Context, m *MutationProto) error {
	m.MutateType = hbasepb.MutationProto_DELETE.Enum()
	return e.mutate(ctx, m)
}

func (e *EndpointEnv) mutate(ctx context.Context, m *MutationProto) error {
	if e == nil || e.rc == nil {
		return errors.New("hbasecop: reverse writes unavailable (reverse path disabled)")
	}
	mutProto, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("hbasecop: marshal MutationProto: %w", err)
	}
	if _, err := e.rc.Mutate(ctx, e.regionID, mutProto); err != nil {
		return err
	}
	return nil
}

// Scanner is a server-side pull scanner over region-local data (Tier 2, TE33),
// opened by [EndpointEnv.OpenScanner]. Drive it by calling Next until hasMore is
// false, then Close. Always Close (defer) so the bridge releases the underlying
// RegionScanner promptly; a Go-process crash also reaps it bridge-side.
type Scanner struct {
	env       *EndpointEnv
	scannerID uint64
	closed    bool
}

// OpenScanner opens a scanner for scan against the invoking region. scan is a
// vendored HBase Scan (build it with the re-exported [Scan] type).
func (e *EndpointEnv) OpenScanner(ctx context.Context, scan *Scan) (*Scanner, error) {
	if e == nil || e.rc == nil {
		return nil, errors.New("hbasecop: reverse reads unavailable (reverse path disabled)")
	}
	scanProto, err := proto.Marshal(scan)
	if err != nil {
		return nil, fmt.Errorf("hbasecop: marshal Scan: %w", err)
	}
	id, err := e.rc.OpenScanner(ctx, e.regionID, e.callID, scanProto)
	if err != nil {
		return nil, err
	}
	return &Scanner{env: e, scannerID: id}, nil
}

// Next pulls the next batch of cells (flat across rows; each cell carries its
// row) and reports whether more rows remain. When hasMore is false the batch is
// the last one (it may still hold cells); a subsequent Next is not required.
func (s *Scanner) Next(ctx context.Context) (cells []*Cell, hasMore bool, err error) {
	if s.closed {
		return nil, false, errors.New("hbasecop: Next on a closed scanner")
	}
	resp, err := s.env.rc.ScanNext(ctx, s.env.regionID, s.env.callID, s.scannerID)
	if err != nil {
		return nil, false, err
	}
	var batch Result
	if err := proto.Unmarshal(resp.GetPayload(), &batch); err != nil {
		return nil, false, fmt.Errorf("hbasecop: unmarshal scan batch: %w", err)
	}
	return batch.GetCell(), resp.GetHasMore(), nil
}

// Close releases the scanner. Idempotent; safe to defer.
func (s *Scanner) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.env.rc.ScanClose(ctx, s.env.regionID, s.env.callID, s.scannerID)
}
