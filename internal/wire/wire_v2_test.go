// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package wire_test

import (
	"bytes"
	"testing"

	"github.com/virogg/go-hbase/internal/wire"
)

func TestRoundTripEndpointFrameTypes(t *testing.T) {
	cases := []struct {
		name string
		msg  wire.Message
	}{
		{
			name: "endpoint_invoke",
			msg: wire.Message{
				Type: wire.TypeEndpointInvoke, ReqID: 11, RegionID: 4,
				Payload: []byte("invoke"),
			},
		},
		{
			name: "endpoint_result",
			msg:  wire.Message{Type: wire.TypeEndpointResult, ReqID: 11, Payload: []byte("result")},
		},
		{
			name: "rpc_request",
			msg: wire.Message{
				Type: wire.TypeRPCRequest, ReqID: 12, RegionID: 4, Payload: []byte("scan-open"),
			},
		},
		{
			name: "rpc_response",
			msg:  wire.Message{Type: wire.TypeRPCResponse, ReqID: 12, Payload: []byte{0x01, 0x02, 0x03}},
		},
		{
			name: "rpc_response_multichunk",
			msg: wire.Message{
				Type: wire.TypeRPCResponse, ReqID: 13,
				Payload: makePayload(wire.MaxPayloadBytes*2 + 7),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := wire.NewEncoder(&buf).Encode(&tc.msg); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := wire.NewDecoder(&buf).Decode()
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			assertMessageEqual(t, got, &tc.msg)
		})
	}
}

func TestNewFrameTypesValidAndNotControl(t *testing.T) {
	for _, typ := range []wire.Type{
		wire.TypeEndpointInvoke, wire.TypeEndpointResult,
		wire.TypeRPCRequest, wire.TypeRPCResponse,
	} {
		if !typ.Valid() {
			t.Errorf("type %d: Valid()=false, want true", typ)
		}
	}
}
