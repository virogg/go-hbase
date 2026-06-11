// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package shmem_test

import (
	"errors"
	"testing"

	"github.com/viroge/go-shmem/pkg/ring"
)

// TestShmemDependencyLinked is the T14 hello-world: it proves the
// java-go-shmem submodule is reachable through the go.mod replace
// directive. We touch a constant and a sentinel error so a future drop
// of either surface forces a deliberate update of this test.
//
// Real wrapper (Open/Send/Recv/Close over ring.WaitingProducer/Consumer)
// lands in T15; this file is intentionally a smoke test only.
func TestShmemDependencyLinked(t *testing.T) {
	if ring.HeaderSize != 128 {
		t.Fatalf("ring.HeaderSize: got %d, want 128 (2 × 64-byte cache lines)", ring.HeaderSize)
	}
	if !errors.Is(ring.ErrRingFull, ring.ErrRingFull) {
		t.Fatalf("ring.ErrRingFull does not satisfy errors.Is for itself")
	}
}
