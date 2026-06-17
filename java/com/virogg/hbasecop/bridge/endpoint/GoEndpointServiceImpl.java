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

/**
 * The single generic HBase coprocessor endpoint {@link com.google.protobuf.Service} the stock
 * {@code Generic*} entrypoints register via {@code getServices()}. It maps each client {@code Call}
 * onto a wire {@link EndpointInvoke} and delegates to an {@link EndpointInvoker}, so authors expose
 * a Go endpoint without writing per-method Java.
 *
 * <p>This class straddles two protobuf worlds: the endpoint Service boundary uses the UNSHADED
 * {@code com.google.protobuf} 2.5.0 the RegionServer binds {@code Coprocessor.getServices()} to,
 * while {@link EndpointInvoke} is an internal wire frame on HBase's shaded {@code
 * org.apache.hbase.thirdparty.com.google.protobuf}. Only {@code String} and {@code byte[]} cross
 * between them — never a {@code ByteString}/{@code Message}/descriptor — so the two type-identities
 * never meet.
 */
public final class GoEndpointServiceImpl extends GoEndpointService {

  private final EndpointInvoker invoker;

  public GoEndpointServiceImpl(EndpointInvoker invoker) {
    this.invoker = Objects.requireNonNull(invoker, "invoker");
  }

  @Override
  public void call(
      RpcController controller, GoEndpointRequest request, RpcCallback<GoEndpointResponse> done) {
    // Cross the protobuf-world boundary via byte[] only: request.getPayload() is a 2.5.0
    // com.google.protobuf.ByteString; EndpointInvoke.payload is a shaded thirdparty ByteString.
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
