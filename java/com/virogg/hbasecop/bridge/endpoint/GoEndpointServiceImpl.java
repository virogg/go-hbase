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
 * <p>Uses the UNSHADED {@code com.google.protobuf} types throughout, matching HBase's {@code
 * Coprocessor.getServices(): Iterable<com.google.protobuf.Service>} and {@code
 * Table.coprocessorService(Class<? extends com.google.protobuf.Service>, ...)}.
 */
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
            .setPayload(request.getPayload())
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
