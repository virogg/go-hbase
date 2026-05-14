// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.multiplex;

/**
 * Raised on every pending and newly-deferred {@link Multiplexer#call} when the Go-side process has
 * crashed (SIGKILL, abnormal exit) and the runtime is pausing the mux while the restart controller
 * (T34) attempts to bring a fresh process back up.
 *
 * <p>Carried as the cause of the {@link java.util.concurrent.CompletableFuture}'s failure;
 * downstream {@code MuxHookDispatcher} surfaces it as an {@link java.io.IOException} so the adapter
 * applies the configured per-hook policy (strict → propagate to client, best-effort → log + no-op).
 */
public final class GoSideCrashedException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public GoSideCrashedException(String message) {
    super(message);
  }

  public GoSideCrashedException(String message, Throwable cause) {
    super(message, cause);
  }
}
