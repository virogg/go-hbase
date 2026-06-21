// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.endpoint;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointResponse;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import com.virogg.hbasecop.bridge.wire.pb.EndpointInvoke;
import java.io.IOException;
import java.util.Objects;

public final class GoEndpointServiceImpl extends GoEndpointService {

  private final EndpointInvoker invoker;

  public GoEndpointServiceImpl(EndpointInvoker invoker) {
    this.invoker = Objects.requireNonNull(invoker, "invoker");
  }

  @Override
  public void call(
      RpcController controller, GoEndpointRequest request, RpcCallback<GoEndpointResponse> done) {
    EndpointInvoke invoke =
        EndpointInvoke.newBuilder()
            .setService(getDescriptorForType().getFullName())
            .setMethod(request.getMethod())
            .setPayload(
                org.apache.hbase.thirdparty.com.google.protobuf.ByteString.copyFrom(
                    request.getPayload().toByteArray()))
            .build();

    GoEndpointResponse.Builder resp = GoEndpointResponse.newBuilder();
    try {
      byte[] out = invoker.invoke(invoke);
      resp.setPayload(ByteString.copyFrom(out));
    } catch (IOException e) {
      String msg = e.getMessage() != null ? e.getMessage() : e.toString();
      resp.setError(msg);
      if (controller != null) {
        controller.setFailed(msg);
      }
    }
    done.run(resp.build());
  }
}
