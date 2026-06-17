// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package wiregolden parses test/golden/wire/v1/fixtures.tsv into
// concrete wire.Message values plus the path to each pre-encoded .bin
// fixture. Used by both the cross-language test in internal/wire and
// the cmd/wire-golden generator that produces the .bin files.
package wiregolden

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/virogg/go-hbase/internal/wire"
)

// Fixture describes one row of fixtures.tsv: the logical Message plus
// the per-fixture chunk size override.
type Fixture struct {
	Name      string
	Message   wire.Message
	ChunkSize int // 0 = wire.MaxPayloadBytes
}

// Parse reads fixtures.tsv lines from r and returns the fixture list.
// Lines starting with '#' or blank lines are ignored. The format is
// documented at the top of test/golden/wire/v1/fixtures.tsv.
func Parse(r io.Reader) ([]Fixture, error) {
	var out []Fixture
	scanner := bufio.NewScanner(r)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "|")
		if len(fields) != 8 {
			return nil, fmt.Errorf("line %d: want 8 fields, got %d", lineNo, len(fields))
		}
		fx, err := parseFixture(fields)
		if err != nil {
			return nil, fmt.Errorf("line %d (%s): %w", lineNo, fields[0], err)
		}
		out = append(out, fx)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func parseFixture(f []string) (Fixture, error) {
	name := f[0]
	typ, err := parseType(f[1])
	if err != nil {
		return Fixture{}, err
	}
	reqID, err := strconv.ParseUint(f[2], 10, 64)
	if err != nil {
		return Fixture{}, fmt.Errorf("req_id: %w", err)
	}
	regionID, err := strconv.ParseUint(f[3], 10, 32)
	if err != nil {
		return Fixture{}, fmt.Errorf("region_id: %w", err)
	}
	hookID, err := strconv.ParseUint(f[4], 10, 8)
	if err != nil {
		return Fixture{}, fmt.Errorf("hook_id: %w", err)
	}
	payload, err := parsePayload(f[5], f[6])
	if err != nil {
		return Fixture{}, fmt.Errorf("payload: %w", err)
	}
	chunkSize, err := strconv.Atoi(f[7])
	if err != nil {
		return Fixture{}, fmt.Errorf("chunk_size: %w", err)
	}
	return Fixture{
		Name: name,
		Message: wire.Message{
			Type:     typ,
			ReqID:    reqID,
			RegionID: uint32(regionID),
			HookID:   uint8(hookID),
			Payload:  payload,
		},
		ChunkSize: chunkSize,
	}, nil
}

func parseType(s string) (wire.Type, error) {
	switch s {
	case "REQUEST":
		return wire.TypeRequest, nil
	case "RESPONSE":
		return wire.TypeResponse, nil
	case "HEARTBEAT":
		return wire.TypeHeartbeat, nil
	case "ERROR":
		return wire.TypeError, nil
	case "SHUTDOWN":
		return wire.TypeShutdown, nil
	case "LOG":
		return wire.TypeLog, nil
	case "ENDPOINT_INVOKE":
		return wire.TypeEndpointInvoke, nil
	case "ENDPOINT_RESULT":
		return wire.TypeEndpointResult, nil
	case "RPC_REQUEST":
		return wire.TypeRpcRequest, nil
	case "RPC_RESPONSE":
		return wire.TypeRpcResponse, nil
	default:
		return wire.TypeUnknown, fmt.Errorf("unknown type %q", s)
	}
}

func parsePayload(kind, value string) ([]byte, error) {
	switch kind {
	case "HEX":
		if value == "" {
			return nil, nil
		}
		return hex.DecodeString(value)
	case "ASCENDING":
		n, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		out := make([]byte, n)
		for i := range out {
			out[i] = byte(i % 251)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown payload_kind %q", kind)
	}
}
