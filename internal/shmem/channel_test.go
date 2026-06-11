// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package shmem_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/virogg/go-hbase/internal/shmem"
)

const (
	testCapacity      = 8
	testMaxObjectSize = 256 // → max payload = 252 bytes
)

func openPair(t *testing.T) (*shmem.Channel, *shmem.Channel) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "channel.mmap")

	prod, err := shmem.Open(shmem.Config{
		Filename:      file,
		Capacity:      testCapacity,
		MaxObjectSize: testMaxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err != nil {
		t.Fatalf("open producer: %v", err)
	}
	t.Cleanup(func() { _ = prod.Close() })

	cons, err := shmem.Open(shmem.Config{
		Filename:      file,
		Capacity:      testCapacity,
		MaxObjectSize: testMaxObjectSize,
		Role:          shmem.RoleConsumer,
	})
	if err != nil {
		t.Fatalf("open consumer: %v", err)
	}
	t.Cleanup(func() { _ = cons.Close() })

	return prod, cons
}

// TestChannelRoundtrip1000 is the T15 acceptance test: 1000 frames of
// varying payload sizes exchanged in a single goroutine, each Send
// immediately followed by a Recv. Forces ring wrap-around (capacity=8)
// and exercises the empty-payload edge case.
func TestChannelRoundtrip1000(t *testing.T) {
	t.Parallel()
	prod, cons := openPair(t)

	maxPayload := testMaxObjectSize - 4
	for i := range 1000 {
		size := i % (maxPayload + 1)
		in := make([]byte, size)
		for k := range in {
			in[k] = byte(i + k)
		}

		if err := prod.Send(in); err != nil {
			t.Fatalf("send %d (size=%d): %v", i, size, err)
		}
		out, err := cons.Recv()
		if err != nil {
			t.Fatalf("recv %d (size=%d): %v", i, size, err)
		}
		if !bytes.Equal(in, out) {
			t.Fatalf("frame %d mismatch: want %d bytes, got %d bytes", i, len(in), len(out))
		}
	}
}

func TestChannelRecvEmptyReturnsNoData(t *testing.T) {
	t.Parallel()
	_, cons := openPair(t)

	out, err := cons.Recv()
	if !errors.Is(err, shmem.ErrNoData) {
		t.Fatalf("Recv on empty ring: want ErrNoData, got err=%v out=%v", err, out)
	}
	if out != nil {
		t.Fatalf("Recv on empty ring should return nil frame, got %v", out)
	}
}

func TestChannelFrameTooLarge(t *testing.T) {
	t.Parallel()
	prod, _ := openPair(t)

	maxPayload := testMaxObjectSize - 4
	tooBig := make([]byte, maxPayload+1)
	if err := prod.Send(tooBig); !errors.Is(err, shmem.ErrFrameTooLarge) {
		t.Fatalf("Send oversized: want ErrFrameTooLarge, got %v", err)
	}
}

func TestChannelWrongRole(t *testing.T) {
	t.Parallel()
	prod, cons := openPair(t)

	if err := cons.Send([]byte("x")); !errors.Is(err, shmem.ErrWrongRole) {
		t.Fatalf("Send on consumer: want ErrWrongRole, got %v", err)
	}
	if _, err := prod.Recv(); !errors.Is(err, shmem.ErrWrongRole) {
		t.Fatalf("Recv on producer: want ErrWrongRole, got %v", err)
	}
}

func TestChannelOpenValidation(t *testing.T) {
	t.Parallel()

	if _, err := shmem.Open(shmem.Config{
		Capacity:      8,
		MaxObjectSize: 64,
		Role:          shmem.RoleProducer,
	}); err == nil {
		t.Fatalf("Open with empty filename: expected error")
	}

	if _, err := shmem.Open(shmem.Config{
		Filename:      filepath.Join(t.TempDir(), "x.mmap"),
		Capacity:      0,
		MaxObjectSize: 64,
		Role:          shmem.RoleProducer,
	}); err == nil {
		t.Fatalf("Open with capacity=0: expected error")
	}

	if _, err := shmem.Open(shmem.Config{
		Filename:      filepath.Join(t.TempDir(), "x.mmap"),
		Capacity:      8,
		MaxObjectSize: 0,
		Role:          shmem.RoleProducer,
	}); err == nil {
		t.Fatalf("Open with maxObjectSize=0: expected error")
	}

	if _, err := shmem.Open(shmem.Config{
		Filename:      filepath.Join(t.TempDir(), "x.mmap"),
		Capacity:      8,
		MaxObjectSize: 64,
		Role:          shmem.Role(99),
	}); err == nil {
		t.Fatalf("Open with bogus role: expected error")
	}
}
