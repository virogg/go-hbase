// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.junit.jupiter.api.Test;

final class AdminEndpointClientTest {

  private static Admin adminWithChannel(CoprocessorRpcChannel channel) {
    Admin admin = mock(Admin.class);
    when(admin.coprocessorService()).thenReturn(channel);
    return admin;
  }

  private static CoprocessorRpcChannel channelReturning(
      GoEndpointResponse response, AtomicReference<Message> seenRequest) {
    CoprocessorRpcChannel channel = mock(CoprocessorRpcChannel.class);
    doAnswer(
            invocation -> {
              seenRequest.set(invocation.getArgument(2));
              RpcCallback<Message> done = invocation.getArgument(4);
              done.run(response);
              return null;
            })
        .when(channel)
        .callMethod(any(), any(), any(), any(), any());
    return channel;
  }

  @Test
  void callMasterMapsRequestAndReturnsPayload() throws Exception {
    AtomicReference<Message> seen = new AtomicReference<>();
    CoprocessorRpcChannel channel =
        channelReturning(
            GoEndpointResponse.newBuilder()
                .setPayload(ByteString.copyFromUtf8("MASTER-HELLO"))
                .build(),
            seen);

    byte[] result =
        AdminEndpointClient.callMaster(
            adminWithChannel(channel), "upper", "master-hello".getBytes(UTF_8));

    assertEquals("MASTER-HELLO", new String(result, UTF_8));
    GoEndpointRequest sent = (GoEndpointRequest) seen.get();
    assertEquals("upper", sent.getMethod());
    assertEquals("master-hello", sent.getPayload().toStringUtf8());
  }

  @Test
  void nullPayloadIsSentAsEmptyBytes() throws Exception {
    AtomicReference<Message> seen = new AtomicReference<>();
    CoprocessorRpcChannel channel = channelReturning(GoEndpointResponse.newBuilder().build(), seen);

    AdminEndpointClient.callMaster(adminWithChannel(channel), "ping", null);

    GoEndpointRequest sent = (GoEndpointRequest) seen.get();
    assertTrue(sent.hasPayload(), "payload field must be set even when empty");
    assertEquals(0, sent.getPayload().size());
  }

  @Test
  void goSideErrorSurfacesAsIoException() {
    CoprocessorRpcChannel channel =
        channelReturning(
            GoEndpointResponse.newBuilder().setError("master state unavailable").build(),
            new AtomicReference<>());

    IOException err =
        assertThrows(
            IOException.class,
            () -> AdminEndpointClient.callMaster(adminWithChannel(channel), "x", null));
    assertTrue(err.getMessage().contains("master state unavailable"), err.getMessage());
  }

  @Test
  void controllerFailureSurfacesAsIoException() {
    CoprocessorRpcChannel channel = mock(CoprocessorRpcChannel.class);
    doAnswer(
            invocation -> {
              RpcController controller = invocation.getArgument(1);
              controller.setFailed("no active master");
              return null;
            })
        .when(channel)
        .callMethod(any(), any(), any(), any(), any());

    IOException err =
        assertThrows(
            IOException.class,
            () -> AdminEndpointClient.callMaster(adminWithChannel(channel), "x", null));
    assertTrue(err.getMessage().contains("no active master"), err.getMessage());
  }

  @Test
  void controllerFailureWinsOverADeliveredResponse() {
    CoprocessorRpcChannel channel = mock(CoprocessorRpcChannel.class);
    doAnswer(
            invocation -> {
              RpcController controller = invocation.getArgument(1);
              controller.setFailed("master gone");
              RpcCallback<Message> done = invocation.getArgument(4);
              done.run(
                  GoEndpointResponse.newBuilder()
                      .setPayload(ByteString.copyFromUtf8("late"))
                      .build());
              return null;
            })
        .when(channel)
        .callMethod(any(), any(), any(), any(), any());

    IOException err =
        assertThrows(
            IOException.class,
            () -> AdminEndpointClient.callMaster(adminWithChannel(channel), "x", null));
    assertTrue(err.getMessage().contains("master gone"), err.getMessage());
  }

  @Test
  void missingResponseSurfacesAsIoException() {
    CoprocessorRpcChannel channel = mock(CoprocessorRpcChannel.class);

    IOException err =
        assertThrows(
            IOException.class,
            () -> AdminEndpointClient.callMaster(adminWithChannel(channel), "x", null));
    assertTrue(err.getMessage().contains("no response"), err.getMessage());
  }
}
