// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package shmem_test

import (
	"errors"
	"testing"

	"github.com/viroge/go-shmem/pkg/ring"
)

func TestShmemDependencyLinked(t *testing.T) {
	if ring.HeaderSize != 128 {
		t.Fatalf("ring.HeaderSize: got %d, want 128 (2 × 64-byte cache lines)", ring.HeaderSize)
	}
	if !errors.Is(ring.ErrRingFull, ring.ErrRingFull) {
		t.Fatalf("ring.ErrRingFull does not satisfy errors.Is for itself")
	}
}
