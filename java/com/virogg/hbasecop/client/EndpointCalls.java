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

final class EndpointCalls {

  private EndpointCalls() {}

  static GoEndpointRequest request(String method, byte[] payload) {
    return GoEndpointRequest.newBuilder()
        .setMethod(method)
        .setPayload(payload == null ? ByteString.EMPTY : ByteString.copyFrom(payload))
        .build();
  }

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
