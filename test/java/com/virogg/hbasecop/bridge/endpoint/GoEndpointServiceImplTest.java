// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.endpoint;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointResponse;
import com.virogg.hbasecop.bridge.wire.pb.EndpointInvoke;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.junit.jupiter.api.Test;

final class GoEndpointServiceImplTest {

  private static GoEndpointResponse invokeCall(
      GoEndpointServiceImpl service, GoEndpointRequest request) {
    AtomicReference<GoEndpointResponse> out = new AtomicReference<>();
    RpcCallback<GoEndpointResponse> done = out::set;
    service.call(new ServerRpcController(), request, done);
    return out.get();
  }

  @Test
  void callMapsToEndpointInvokeAndReturnsInvokerResult() {
    AtomicReference<EndpointInvoke> captured = new AtomicReference<>();
    EndpointInvoker invoker =
        invoke -> {
          captured.set(invoke);
          return "RESULT".getBytes();
        };
    GoEndpointServiceImpl service = new GoEndpointServiceImpl(invoker);

    GoEndpointRequest request =
        GoEndpointRequest.newBuilder()
            .setMethod("sum")
            .setPayload(ByteString.copyFromUtf8("body"))
            .build();
    GoEndpointResponse resp = invokeCall(service, request);

    EndpointInvoke invoke = captured.get();
    assertEquals("virogg.hbasecop.v1.GoEndpointService", invoke.getService());
    assertEquals("sum", invoke.getMethod());
    assertArrayEquals("body".getBytes(), invoke.getPayload().toByteArray());

    assertArrayEquals("RESULT".getBytes(), resp.getPayload().toByteArray());
    assertTrue(resp.getError().isEmpty(), "no error on success");
  }

  @Test
  void callSurfacesInvokerErrorWithoutThrowing() {
    EndpointInvoker failing =
        invoke -> {
          throw new IOException("go side unavailable");
        };
    GoEndpointResponse resp =
        invokeCall(
            new GoEndpointServiceImpl(failing),
            GoEndpointRequest.newBuilder().setMethod("x").build());

    assertTrue(resp.getError().contains("go side unavailable"), resp.getError());
    assertTrue(resp.getPayload().isEmpty(), "no payload on failure");
  }
}
