// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

/**
 * Direction of a {@link Channel} endpoint. Each shmem ring is one-way: exactly one PRODUCER and one
 * CONSUMER per file/shmName.
 */
public enum Role {
  PRODUCER,
  CONSUMER
}
