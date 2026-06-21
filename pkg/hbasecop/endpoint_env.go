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

type EndpointEnv struct {
	rc       *cpruntime.ReverseClient
	regionID uint32
	callID   uint64 // groups this call's scanners for lifecycle/reaping
}

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

func (e *EndpointEnv) Put(ctx context.Context, m *MutationProto) error {
	return e.mutate(ctx, m, hbasepb.MutationProto_PUT)
}

func (e *EndpointEnv) Delete(ctx context.Context, m *MutationProto) error {
	return e.mutate(ctx, m, hbasepb.MutationProto_DELETE)
}

func (e *EndpointEnv) mutate(ctx context.Context, m *MutationProto, t hbasepb.MutationProto_MutationType) error {
	if e == nil || e.rc == nil {
		return errors.New("hbasecop: reverse writes unavailable (reverse path disabled)")
	}
	if m == nil {
		return errors.New("hbasecop: nil mutation")
	}
	m.MutateType = t.Enum()
	mutProto, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("hbasecop: marshal MutationProto: %w", err)
	}
	return e.rc.Mutate(ctx, e.regionID, mutProto)
}

type Scanner struct {
	env       *EndpointEnv
	scannerID uint64
	closed    bool
}

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

func (s *Scanner) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.env.rc.ScanClose(ctx, s.env.regionID, s.env.callID, s.scannerID)
}
