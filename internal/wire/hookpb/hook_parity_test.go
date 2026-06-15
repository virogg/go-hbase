// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hookpb_test

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire/hookpb"
)

// updateHookGolden regenerates the committed .bin corpus. Run:
//
//	go test ./internal/wire/hookpb/ -run TestHookPayloadGoldenParity -update-hookgolden
var updateHookGolden = flag.Bool("update-hookgolden", false, "regenerate hook payload golden .bin files")

// hookGoldenDir is relative to this package dir (internal/wire/hookpb).
const hookGoldenDir = "../../../test/golden/hooks/v1"

type hookFixture struct {
	name string
	msg  proto.Message
}

// hookFixtures is the representative set of hook payload messages whose
// canonical bytes form the cross-language contract. The matching Java
// test (HookGoldenParityTest) loads the SAME .bin files, parses them with
// the generated Java classes and re-serializes, asserting byte-equality -
// which is what proves Go↔Java wire parity for the payload layer (each
// side previously only round-tripped within its own runtime). The set
// spans the shared HookContext, a request that nests it, a response with
// a repeated scalar, and a response with a nested message + string.
func hookFixtures() []hookFixture {
	hc := &hookpb.HookContext{RegionName: []byte("region-7"), RequestId: 42}
	return []hookFixture{
		{"hook_context", hc},
		{"pre_put_request", &hookpb.PrePutRequest{Ctx: hc}},
		{"hook_response_bypass", &hookpb.HookResponse{Bypass: true, BlockedIndices: []uint32{1, 3, 5}}},
		{"hook_response_error", &hookpb.HookResponse{Error: &hookpb.HookError{Code: 7, Message: "boom"}}},
	}
}

func TestHookPayloadGoldenParity(t *testing.T) {
	mo := proto.MarshalOptions{Deterministic: true}
	for _, fx := range hookFixtures() {
		got, err := mo.Marshal(fx.msg)
		if err != nil {
			t.Fatalf("%s: marshal: %v", fx.name, err)
		}
		path := filepath.Join(hookGoldenDir, fx.name+".bin")

		if *updateHookGolden {
			if err := os.MkdirAll(hookGoldenDir, 0o755); err != nil {
				t.Fatalf("%s: mkdir golden dir: %v", fx.name, err)
			}
			if err := os.WriteFile(path, got, 0o644); err != nil {
				t.Fatalf("%s: write golden: %v", fx.name, err)
			}
			continue
		}

		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: read golden (regenerate with -update-hookgolden): %v", fx.name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: marshaled bytes differ from committed golden - proto change without "+
				"golden update, or non-deterministic encoding", fx.name)
		}

		// Parse the committed bytes back and re-marshal: must be byte-stable,
		// i.e. the committed bytes are canonical for the Go encoder. The Java
		// side asserts the same on the same files; together that is parity.
		clone := fx.msg.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(want, clone); err != nil {
			t.Fatalf("%s: unmarshal golden: %v", fx.name, err)
		}
		re, err := mo.Marshal(clone)
		if err != nil {
			t.Fatalf("%s: re-marshal: %v", fx.name, err)
		}
		if !bytes.Equal(re, want) {
			t.Errorf("%s: parse→re-marshal is not byte-stable (non-canonical)", fx.name)
		}
	}
}
