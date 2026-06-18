// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.client;

import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

/**
 * TE52 admin master-endpoint helper: a single call to the generic {@code GoEndpointService} exposed
 * by the stock {@code GenericMasterObserver} via {@code MasterCoprocessor.getServices()}, invoked
 * over the active master's {@code CoprocessorRpcChannel} from {@link Admin#coprocessorService()}.
 *
 * <p>A master endpoint has no region (it carries region_id 0), so it reads master/meta state only —
 * region-local reverse reads/writes are unavailable there. There is exactly one active master, so
 * this is a single call rather than a fan-out + reduce; the region (per-table) counterpart is
 * {@link EndpointClient}.
 */
public final class AdminEndpointClient {

  private AdminEndpointClient() {}

  /**
   * Invokes {@code method} with {@code payload} on the active master's endpoint and returns its raw
   * response payload. A controller failure, a missing response, or a Go-side error surfaces as an
   * {@link IOException}.
   *
   * <p>This declares the narrow {@code IOException} rather than {@link EndpointClient}'s {@code
   * Throwable}: the region helper is forced to {@code throws Throwable} by {@code
   * Table#coprocessorService}, whereas the master path has no such constraint, so it surfaces the
   * single exception type {@link EndpointCalls#invoke} actually produces.
   */
  public static byte[] callMaster(Admin admin, String method, byte[] payload) throws IOException {
    CoprocessorRpcChannel channel = admin.coprocessorService();
    GoEndpointService.Stub stub = GoEndpointService.newStub(channel);
    return EndpointCalls.invoke(stub, EndpointCalls.request(method, payload));
  }
}
