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
