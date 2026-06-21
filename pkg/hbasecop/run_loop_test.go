// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/cpruntime"
	"github.com/virogg/go-hbase/internal/shmem"
	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/hbasepb"
	"github.com/virogg/go-hbase/internal/wire/hookpb"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

const (
	testRingCapacity      = 16
	testRingMaxObjectSize = 4096
)

type loopHarness struct {
	loopIn  *shmem.Channel
	loopOut *shmem.Channel
	mockOut *shmem.Channel
	mockIn  *shmem.Channel
}

func openLoopHarness(t *testing.T) *loopHarness {
	t.Helper()
	dir := t.TempDir()
	inFile := filepath.Join(dir, "in.mmap")
	outFile := filepath.Join(dir, "out.mmap")
	mkChan := func(file string, role shmem.Role) *shmem.Channel {
		ch, err := shmem.Open(shmem.Config{
			Filename:      file,
			Capacity:      testRingCapacity,
			MaxObjectSize: testRingMaxObjectSize,
			Role:          role,
		})
		if err != nil {
			t.Fatalf("shmem.Open: %v", err)
		}
		t.Cleanup(func() { _ = ch.Close() })
		return ch
	}
	return &loopHarness{
		mockOut: mkChan(inFile, shmem.RoleProducer),
		loopIn:  mkChan(inFile, shmem.RoleConsumer),
		loopOut: mkChan(outFile, shmem.RoleProducer),
		mockIn:  mkChan(outFile, shmem.RoleConsumer),
	}
}

func TestRunDispatchesPrePutThroughLoop(t *testing.T) {
	h := openLoopHarness(t)
	obs := &capturingObserver{prePutResult: HookResult{Bypass: true}}
	d := newDispatcher(obs, nil)

	loop, err := cpruntime.New(cpruntime.Config{
		InCh:            h.loopIn,
		OutCh:           h.loopOut,
		HeartbeatPeriod: -1,
		Handler:         d.dispatch,
	})
	if err != nil {
		t.Fatalf("cpruntime.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = loop.Run(ctx)
	}()

	mut := &hbasepb.MutationProto{Row: []byte("user-99")}
	hctx := &hookpb.HookContext{
		TableName: &hbasepb.TableName{
			Namespace: []byte("default"),
			Qualifier: []byte("users"),
		},
		RegionName: []byte("users,,1.encoded."),
		RequestId:  101,
	}
	innerBytes, err := proto.Marshal(&hookpb.PrePutRequest{Ctx: hctx, Mutation: mut})
	if err != nil {
		t.Fatalf("marshal PrePutRequest: %v", err)
	}
	outerBytes, err := proto.Marshal(&wirepb.Request{HookCtx: innerBytes})
	if err != nil {
		t.Fatalf("marshal wirepb.Request: %v", err)
	}
	reqMsg := &wire.Message{
		Type:    wire.TypeRequest,
		ReqID:   101,
		HookID:  uint8(HookIDPrePut),
		Payload: outerBytes,
	}
	var encoded bytes.Buffer
	if err := wire.NewEncoder(&encoded).Encode(reqMsg); err != nil {
		t.Fatalf("wire encode: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := h.mockOut.Send(encoded.Bytes())
		if err == nil {
			break
		}
		if !errors.Is(err, shmem.ErrRingFull) {
			t.Fatalf("send: %v", err)
		}
		runtime.Gosched()
	}

	var respFrame []byte
	for time.Now().Before(deadline) {
		data, err := h.mockIn.Recv()
		if err == nil {
			respFrame = data
			break
		}
		if !errors.Is(err, shmem.ErrNoData) {
			t.Fatalf("recv: %v", err)
		}
		runtime.Gosched()
	}
	if respFrame == nil {
		t.Fatal("no response within 2s")
	}

	respMsg, err := wire.NewDecoder(bytes.NewReader(respFrame)).Decode()
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if respMsg.Type != wire.TypeResponse {
		t.Fatalf("respMsg.Type = %v, want Response", respMsg.Type)
	}
	if respMsg.ReqID != reqMsg.ReqID {
		t.Fatalf("respMsg.ReqID = %d, want %d", respMsg.ReqID, reqMsg.ReqID)
	}

	hookResp := decodeHookResponse(t, respMsg)
	if !hookResp.GetBypass() {
		t.Fatalf("HookResponse.Bypass = false, want true (observer asked to bypass)")
	}
	if hookResp.GetError() != nil {
		t.Fatalf("HookResponse.Error = %v, want nil", hookResp.GetError())
	}

	preCalls, _, env, lastMut := obs.snapshot()
	if preCalls != 1 {
		t.Fatalf("PrePut calls = %d, want 1", preCalls)
	}
	if !bytes.Equal(lastMut.GetRow(), mut.Row) {
		t.Fatalf("observer got Row = %q, want %q", lastMut.GetRow(), mut.Row)
	}
	if env.TableName != "default:users" {
		t.Fatalf("env.TableName = %q, want %q", env.TableName, "default:users")
	}

	cancel()
	wg.Wait()
}

func TestRunRequiresObserver(t *testing.T) {
	if err := Run(); err == nil {
		t.Fatal("Run() without observers: expected error")
	}
}
