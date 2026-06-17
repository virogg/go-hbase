// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.endpoint;

import com.virogg.hbasecop.bridge.wire.pb.EndpointInvoke;
import java.io.IOException;

/**
 * Forwards a built {@link EndpointInvoke} to the Go side and returns the result payload bytes.
 *
 * <p>The seam between {@link GoEndpointServiceImpl} (which maps an HBase coprocessor-endpoint call
 * onto the wire frame) and the transport. TE21 wires a logging/echo stub; the real implementation
 * that round-trips the frame over the shmem ring lands in TE22.
 */
@FunctionalInterface
public interface EndpointInvoker {

  /**
   * Forwards {@code invoke} to the Go endpoint handler and returns its result payload. Throws
   * {@link IOException} on transport failure or a Go-side error; the caller surfaces it to the
   * client as an endpoint error rather than propagating.
   */
  byte[] invoke(EndpointInvoke invoke) throws IOException;
}
