// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.client;

import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

public final class AdminEndpointClient {

  private AdminEndpointClient() {}

  public static byte[] callMaster(Admin admin, String method, byte[] payload) throws IOException {
    CoprocessorRpcChannel channel = admin.coprocessorService();
    GoEndpointService.Stub stub = GoEndpointService.newStub(channel);
    return EndpointCalls.invoke(stub, EndpointCalls.request(method, payload));
  }
}
