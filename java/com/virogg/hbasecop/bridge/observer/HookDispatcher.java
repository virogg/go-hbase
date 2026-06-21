// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

public interface HookDispatcher {
  byte[] dispatchHook(int regionId, byte hookId, byte[] hookCtxBytes, Duration timeout)
      throws IOException, InterruptedException, TimeoutException;
}
