// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.config;

/** Failure-handling policy applied per hook when the Go side errors, times out, or is down. */
public enum Policy {
  /** Surface Go-side failure as IOException to the HBase client; mutation aborts. */
  STRICT,
  /** Log a warning and continue; the hook is treated as a no-op. */
  BEST_EFFORT
}
