// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

// InvokeRegion runs obs through the production dispatcher for one region hook
// and returns the result as the Java side would observe it - env decode, panic
// recovery and result mapping all run. req is the per-hook *XxxRequest envelope
// with Ctx set. It is the seam for in-process testing without a cluster; see
// pkg/hbasecop/hbasecoptest for an ergonomic wrapper.
func InvokeRegion(obs RegionObserver, hookID HookID, req proto.Message) (HookResult, error) {
	inner, err := proto.Marshal(req)
	if err != nil {
		return HookResult{}, fmt.Errorf("marshal request: %w", err)
	}
	payload, err := proto.Marshal(&wirepb.Request{HookCtx: inner})
	if err != nil {
		return HookResult{}, fmt.Errorf("marshal wire request: %w", err)
	}
	frame := &wire.Message{Type: wire.TypeRequest, ReqID: 1, HookID: uint8(hookID), Payload: payload}
	return decodeResponseFrame(newDispatcher(obs, nil).dispatch(context.Background(), frame))
}

func decodeResponseFrame(frame *wire.Message) (HookResult, error) {
	if frame == nil {
		return HookResult{}, errors.New("hbasecop: nil response frame")
	}
	if frame.Type == wire.TypeError {
		var e wirepb.Error
		if err := proto.Unmarshal(frame.Payload, &e); err != nil {
			return HookResult{}, fmt.Errorf("unmarshal error frame: %w", err)
		}
		return HookResult{}, fmt.Errorf("hbasecop: %s (code %d)", e.GetMessage(), e.GetCode())
	}
	var resp wirepb.Response
	if err := proto.Unmarshal(frame.Payload, &resp); err != nil {
		return HookResult{}, fmt.Errorf("unmarshal response: %w", err)
	}
	var hr hookpb.HookResponse
	if err := proto.Unmarshal(resp.GetHookResp(), &hr); err != nil {
		return HookResult{}, fmt.Errorf("unmarshal hook response: %w", err)
	}
	res := HookResult{Bypass: hr.GetBypass(), BlockedIndices: hr.GetBlockedIndices(), ResultCells: hr.GetResult()}
	if e := hr.GetError(); e != nil {
		return res, errors.New(e.GetMessage())
	}
	return res, nil
}
