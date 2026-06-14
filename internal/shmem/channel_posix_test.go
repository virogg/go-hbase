// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

//go:build posixshm

package shmem_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/virogg/go-hbase/internal/shmem"
)

// TestPosixShmRoundtrip exercises the POSIX named shared-memory backend
// (shm_open/mmap) end-to-end. It is gated behind the `posixshm` build tag
// and cgo because the backend is cgo-only; run it via
// `make go-test-posixshm`. Before this test the SPEC-advertised "POSIX
// shm" backend was compiled by nobody and exercised by no test — every
// CI run and every production path used only the file-backed mmap
// backend. This pins that the cgo backend actually opens, shares and
// round-trips frames between a producer and a consumer on the same name.
func TestPosixShmRoundtrip(t *testing.T) {
	name := fmt.Sprintf("/hbasecop-posix-%d", os.Getpid())

	prod, err := shmem.Open(shmem.Config{
		Backend:       "posix_shm",
		ShmName:       name,
		Capacity:      testCapacity,
		MaxObjectSize: testMaxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err != nil {
		t.Fatalf("open posix_shm producer: %v", err)
	}
	t.Cleanup(func() { _ = prod.Close() })

	cons, err := shmem.Open(shmem.Config{
		Backend:       "posix_shm",
		ShmName:       name,
		Capacity:      testCapacity,
		MaxObjectSize: testMaxObjectSize,
		Role:          shmem.RoleConsumer,
	})
	if err != nil {
		t.Fatalf("open posix_shm consumer: %v", err)
	}
	t.Cleanup(func() { _ = cons.Close() })

	for i := range 100 {
		in := []byte(fmt.Sprintf("posix-frame-%d", i))
		if err := prod.Send(in); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		out, err := cons.Recv()
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if !bytes.Equal(in, out) {
			t.Fatalf("frame %d mismatch: want %q got %q", i, in, out)
		}
	}
}

// TestPosixShmRequiresName pins the validation that a posix_shm channel
// needs an ShmName (and that it must look like a POSIX shm name).
func TestPosixShmRequiresName(t *testing.T) {
	_, err := shmem.Open(shmem.Config{
		Backend:       "posix_shm",
		Capacity:      testCapacity,
		MaxObjectSize: testMaxObjectSize,
		Role:          shmem.RoleProducer,
	})
	if err == nil {
		t.Fatal("posix_shm without ShmName: want error, got nil")
	}
}
