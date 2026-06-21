// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasepb_test

import (
	"bytes"
	"testing"

	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"google.golang.org/protobuf/proto"
)

func TestHBaseMessageRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		msg  proto.Message
	}{
		{
			"Cell_put",
			&hbasepb.Cell{
				Row:       []byte("row-7"),
				Family:    []byte("cf"),
				Qualifier: []byte("q1"),
				Timestamp: proto.Uint64(1_700_000_000_000),
				CellType:  hbasepb.CellType_PUT.Enum(),
				Value:     []byte("hello"),
			},
		},
		{
			"TableName_default_ns",
			&hbasepb.TableName{
				Namespace: []byte("default"),
				Qualifier: []byte("users"),
			},
		},
		{
			"TimeRange_bounded",
			&hbasepb.TimeRange{
				From: proto.Uint64(1_700_000_000_000),
				To:   proto.Uint64(1_800_000_000_000),
			},
		},
		{
			"NameBytesPair_with_value",
			&hbasepb.NameBytesPair{
				Name:  proto.String("audit-tag"),
				Value: []byte{0x01, 0x02, 0x03},
			},
		},
		{
			"MutationProto_put_two_cols",
			newPutMutation(),
		},
		{
			"PrePutRequest_wrapped",
			&hookpb.PrePutRequest{
				Ctx: &hookpb.HookContext{
					TableName: &hbasepb.TableName{
						Namespace: []byte("default"),
						Qualifier: []byte("users"),
					},
					RegionName: []byte("users,,1700000000000.abcd."),
					RequestId:  4242,
				},
				Mutation: newPutMutation(),
			},
		},
		{
			"HookResponse_bypass_with_error",
			&hookpb.HookResponse{
				Bypass: true,
				Error: &hookpb.HookError{
					Code:    7,
					Message: "policy rejected: missing TTL marker",
				},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := proto.Marshal(tc.msg)
			if err != nil {
				t.Fatalf("Marshal(%T): %v", tc.msg, err)
			}
			if len(encoded) == 0 && tc.name != "TimeRange_empty" {
				t.Fatalf("Marshal(%T): empty payload for non-empty message", tc.msg)
			}

			decoded := proto.Clone(tc.msg)
			proto.Reset(decoded)
			if err := proto.Unmarshal(encoded, decoded); err != nil {
				t.Fatalf("Unmarshal(%T): %v", tc.msg, err)
			}
			if !proto.Equal(tc.msg, decoded) {
				t.Fatalf("round-trip diverged:\n  want: %v\n  got:  %v", tc.msg, decoded)
			}

			reEncoded, err := proto.Marshal(decoded)
			if err != nil {
				t.Fatalf("re-Marshal(%T): %v", tc.msg, err)
			}
			if !bytes.Equal(encoded, reEncoded) {
				t.Fatalf("re-encode mismatch for %s:\n  first:  %x\n  second: %x",
					tc.name, encoded, reEncoded)
			}
		})
	}
}

func newPutMutation() *hbasepb.MutationProto {
	return &hbasepb.MutationProto{
		Row:        []byte("row-7"),
		MutateType: hbasepb.MutationProto_PUT.Enum(),
		Timestamp:  proto.Uint64(1_700_000_000_000),
		Durability: hbasepb.MutationProto_USE_DEFAULT.Enum(),
		ColumnValue: []*hbasepb.MutationProto_ColumnValue{
			{
				Family: []byte("cf"),
				QualifierValue: []*hbasepb.MutationProto_ColumnValue_QualifierValue{
					{
						Qualifier: []byte("q1"),
						Value:     []byte("v1"),
						Timestamp: proto.Uint64(1_700_000_000_000),
					},
					{
						Qualifier: []byte("q2"),
						Value:     []byte("v2"),
						Timestamp: proto.Uint64(1_700_000_000_001),
					},
				},
			},
			{
				Family: []byte("meta"),
				QualifierValue: []*hbasepb.MutationProto_ColumnValue_QualifierValue{
					{
						Qualifier: []byte("source"),
						Value:     []byte("audit"),
						Timestamp: proto.Uint64(1_700_000_000_002),
					},
				},
			},
		},
		Attribute: []*hbasepb.NameBytesPair{
			{Name: proto.String("trace_id"), Value: []byte("abc-123")},
		},
	}
}
