// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.client;

import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.hadoop.hbase.client.Table;

public final class EndpointClient {

  private EndpointClient() {}

  public static Map<byte[], byte[]> callAllRegions(Table table, String method, byte[] payload)
      throws Throwable {
    return callRegions(table, null, null, method, payload);
  }

  public static Map<byte[], byte[]> callRegions(
      Table table, byte[] startKey, byte[] endKey, String method, byte[] payload) throws Throwable {
    GoEndpointRequest request = EndpointCalls.request(method, payload);
    return table.coprocessorService(
        GoEndpointService.class,
        startKey,
        endKey,
        instance -> EndpointCalls.invoke(instance, request));
  }

  public static <R> R callAndReduce(
      Table table, String method, byte[] payload, R identity, BiFunction<R, byte[], R> reducer)
      throws Throwable {
    return callAndReduce(table, null, null, method, payload, identity, reducer);
  }

  public static <R> R callAndReduce(
      Table table,
      byte[] startKey,
      byte[] endKey,
      String method,
      byte[] payload,
      R identity,
      BiFunction<R, byte[], R> reducer)
      throws Throwable {
    R acc = identity;
    for (byte[] regionResult : callRegions(table, startKey, endKey, method, payload).values()) {
      acc = reducer.apply(acc, regionResult);
    }
    return acc;
  }
}
