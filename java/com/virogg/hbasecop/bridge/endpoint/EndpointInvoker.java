// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.endpoint;

import com.virogg.hbasecop.bridge.wire.pb.EndpointInvoke;
import java.io.IOException;

@FunctionalInterface
public interface EndpointInvoker {
  byte[] invoke(EndpointInvoke invoke) throws IOException;
}
