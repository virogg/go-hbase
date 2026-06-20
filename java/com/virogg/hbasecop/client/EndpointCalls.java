// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointResponse;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

/**
 * Shared invocation plumbing for the generic {@code GoEndpointService}, used by both the region
 * helper ({@link EndpointClient}, per-region stub) and the master helper ({@link
 * AdminEndpointClient}, master channel stub).
 *
 * <p>The endpoint Service is generated against the UNSHADED {@code com.google.protobuf} 2.5.0 that
 * HBase 2.5's {@code Coprocessor.getServices()} contract binds, so invocation uses an unshaded
 * {@link ServerRpcController} and {@link RpcCallback} — not HBase's shaded {@code
 * BlockingRpcCallback}, which would not bind to the generated stub.
 */
final class EndpointCalls {

  private EndpointCalls() {}

  /** Builds a {@code GoEndpointRequest}; a null payload is sent as empty bytes. */
  static GoEndpointRequest request(String method, byte[] payload) {
    return GoEndpointRequest.newBuilder()
        .setMethod(method)
        .setPayload(payload == null ? ByteString.EMPTY : ByteString.copyFrom(payload))
        .build();
  }

  /**
   * Runs one endpoint Call through the unshaded controller/callback shape and unwraps the result,
   * turning a controller failure, a missing response, or a Go-side error into an {@link
   * IOException}. The supplied {@code stub} is either a region per-region service instance or the
   * master channel stub; both are {@link GoEndpointService} with a synchronous channel, so the
   * callback has run by the time {@code stub.call} returns.
   */
  static byte[] invoke(GoEndpointService stub, GoEndpointRequest request) throws IOException {
    ServerRpcController controller = new ServerRpcController();
    AtomicReference<GoEndpointResponse> out = new AtomicReference<>();
    RpcCallback<GoEndpointResponse> done = out::set;
    stub.call(controller, request, done);
    if (controller.failed()) {
      throw new IOException("endpoint controller failed: " + controller.errorText());
    }
    GoEndpointResponse resp = out.get();
    if (resp == null) {
      throw new IOException("endpoint returned no response");
    }
    if (!resp.getError().isEmpty()) {
      throw new IOException("endpoint error: " + resp.getError());
    }
    return resp.getPayload().toByteArray();
  }
}
