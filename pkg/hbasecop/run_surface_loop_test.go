// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"bytes"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/shmem"
	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

// runSurfaceHarness wires a mock peer onto the same mmap ring files a Run*
// entrypoint opens from the environment: producer on the Java→Go file,
// consumer on the Go→Java file. The Run* call itself opens the matching
// consumer/producer ends.
type runSurfaceHarness struct {
	mockOut *shmem.Channel // producer on in-file (we send REQUEST/SHUTDOWN)
	mockIn  *shmem.Channel // consumer on out-file (we read RESPONSE)
}

func openRunSurfaceHarness(t *testing.T) *runSurfaceHarness {
	t.Helper()
	dir := t.TempDir()
	inFile := filepath.Join(dir, "in.mmap")
	outFile := filepath.Join(dir, "out.mmap")

	// The mock producer opens (and sizes) the in-file before Run* opens its
	// consumer end; symmetrically the mock consumer opens the out-file.
	mockOut, err := shmem.Open(shmem.Config{
		Filename: inFile, Capacity: testRingCapacity,
		MaxObjectSize: testRingMaxObjectSize, Role: shmem.RoleProducer,
	})
	if err != nil {
		t.Fatalf("open mock producer: %v", err)
	}
	t.Cleanup(func() { _ = mockOut.Close() })
	mockIn, err := shmem.Open(shmem.Config{
		Filename: outFile, Capacity: testRingCapacity,
		MaxObjectSize: testRingMaxObjectSize, Role: shmem.RoleConsumer,
	})
	if err != nil {
		t.Fatalf("open mock consumer: %v", err)
	}
	t.Cleanup(func() { _ = mockIn.Close() })

	// Run* reads these; heartbeats disabled to keep the out-ring clean.
	t.Setenv("HBASECOP_SHMEM_IN_PATH", inFile)
	t.Setenv("HBASECOP_SHMEM_OUT_PATH", outFile)
	t.Setenv("HBASECOP_RING_CAPACITY", "16")
	t.Setenv("HBASECOP_RING_MAX_OBJECT_SIZE", "4096")
	t.Setenv("HBASECOP_HEARTBEAT_MS", "-1")
	return &runSurfaceHarness{mockOut: mockOut, mockIn: mockIn}
}

func (h *runSurfaceHarness) sendFrame(t *testing.T, m *wire.Message) {
	t.Helper()
	var buf bytes.Buffer
	if err := wire.NewEncoder(&buf).Encode(m); err != nil {
		t.Fatalf("encode: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := h.mockOut.Send(buf.Bytes())
		if err == nil {
			return
		}
		if !errors.Is(err, shmem.ErrRingFull) {
			t.Fatalf("send: %v", err)
		}
		runtime.Gosched()
	}
	t.Fatal("send timed out")
}

func (h *runSurfaceHarness) recvResponse(t *testing.T) *wire.Message {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, err := h.mockIn.Recv()
		if err == nil {
			msg, derr := wire.NewDecoder(bytes.NewReader(data)).Decode()
			if derr != nil {
				t.Fatalf("decode response: %v", derr)
			}
			return msg
		}
		if !errors.Is(err, shmem.ErrNoData) {
			t.Fatalf("recv: %v", err)
		}
		runtime.Gosched()
	}
	t.Fatal("no response within 3s")
	return nil
}

func requestFrame(t *testing.T, hookID HookID, reqID uint64) *wire.Message {
	t.Helper()
	payload, err := proto.Marshal(&wirepb.Request{HookCtx: nil})
	if err != nil {
		t.Fatalf("marshal wirepb.Request: %v", err)
	}
	return &wire.Message{Type: wire.TypeRequest, ReqID: reqID, HookID: uint8(hookID), Payload: payload}
}

// TestRunSurfacesEndToEnd drives each non-Region Run* entrypoint through a
// real shmem ring with an Unimplemented observer: send one valid REQUEST for
// that surface, confirm a RESPONSE returns (proving the surface dispatcher +
// event loop are wired), then send SHUTDOWN and confirm the entrypoint exits
// cleanly with nil. This covers the Run*/loadShmemConfigFromEnv bodies.
func TestRunSurfacesEndToEnd(t *testing.T) {
	surfaces := []struct {
		name   string
		run    func() error
		hookID HookID
	}{
		{"RunMaster", func() error { return RunMaster(UnimplementedMasterObserver{}) }, HookIDPreCreateTable},
		{"RunRegionServer", func() error { return RunRegionServer(UnimplementedRegionServerObserver{}) }, HookIDPreStopRegionServer},
		{"RunWAL", func() error { return RunWAL(UnimplementedWALObserver{}) }, HookIDPreWALWrite},
		{"RunBulkLoad", func() error { return RunBulkLoad(UnimplementedBulkLoadObserver{}) }, HookIDPrePrepareBulkLoad},
	}
	for _, s := range surfaces {
		t.Run(s.name, func(t *testing.T) {
			h := openRunSurfaceHarness(t)

			done := make(chan error, 1)
			go func() { done <- s.run() }()

			// One valid hook for this surface → expect a RESPONSE.
			h.sendFrame(t, requestFrame(t, s.hookID, 1))
			resp := h.recvResponse(t)
			if resp.Type != wire.TypeResponse {
				t.Fatalf("%s: resp.Type = %v, want Response", s.name, resp.Type)
			}
			if resp.ReqID != 1 {
				t.Fatalf("%s: resp.ReqID = %d, want 1", s.name, resp.ReqID)
			}
			hookResp := decodeHookResponse(t, resp)
			if hookResp.GetError() != nil {
				t.Fatalf("%s: unexpected hook error: %v", s.name, hookResp.GetError())
			}

			// SHUTDOWN frame → the loop cancels and the entrypoint returns nil.
			h.sendFrame(t, &wire.Message{Type: wire.TypeShutdown})
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("%s returned %v, want nil on clean shutdown", s.name, err)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("%s did not exit within 3s of SHUTDOWN", s.name)
			}
		})
	}
}
