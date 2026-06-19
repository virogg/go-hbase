// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointResponse;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

/**
 * TE51: {@link EndpointClient} fans the generic {@code GoEndpointService.Call(method, payload)} out
 * over every region of a table and reduces the per-region results into one value. These unit tests
 * drive the captured {@link Batch.Call} against mock {@link GoEndpointService} stubs, so they cover
 * both the per-region invocation shape (unshaded controller + callback, request mapping, error
 * surfacing) and the fan-out reduce, without a live cluster.
 */
final class EndpointClientTest {

  /**
   * Stubs {@code table.coprocessorService} to invoke the helper's per-region {@link Batch.Call}
   * against one mock service per supplied response, keyed by a synthetic region name. Returns the
   * resulting per-region map exactly as HBase would.
   */
  private static Table tableYielding(GoEndpointService... regionServices) throws Throwable {
    Table table = mock(Table.class);
    when(table.coprocessorService(eq(GoEndpointService.class), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Batch.Call<GoEndpointService, byte[]> call = invocation.getArgument(3);
              TreeMap<byte[], byte[]> perRegion = new TreeMap<>(Bytes.BYTES_COMPARATOR);
              for (int i = 0; i < regionServices.length; i++) {
                perRegion.put(Bytes.toBytes("region-" + i), call.call(regionServices[i]));
              }
              return perRegion;
            });
    return table;
  }

  /** A mock service whose Call returns {@code payload} (success), recording the request it saw. */
  private static GoEndpointService serviceReturning(
      String payload, AtomicReference<GoEndpointRequest> seen) {
    return mockService(
        seen, GoEndpointResponse.newBuilder().setPayload(ByteString.copyFromUtf8(payload)).build());
  }

  /** A mock service whose Call returns an error response. */
  private static GoEndpointService serviceFailing(String error) {
    return mockService(
        new AtomicReference<>(), GoEndpointResponse.newBuilder().setError(error).build());
  }

  private static GoEndpointService mockService(
      AtomicReference<GoEndpointRequest> seen, GoEndpointResponse response) {
    GoEndpointService service = mock(GoEndpointService.class);
    doAnswer(
            invocation -> {
              seen.set(invocation.getArgument(1));
              RpcCallback<GoEndpointResponse> done = invocation.getArgument(2);
              done.run(response);
              return null;
            })
        .when(service)
        .call(any(RpcController.class), any(GoEndpointRequest.class), any());
    return service;
  }

  @Test
  void callAndReduceFoldsPerRegionResults() throws Throwable {
    AtomicReference<GoEndpointRequest> seen = new AtomicReference<>();
    Table table = tableYielding(serviceReturning("3", seen), serviceReturning("4", seen));

    long total =
        EndpointClient.callAndReduce(
            table,
            "sum",
            "n".getBytes(UTF_8),
            0L,
            (acc, regionResult) -> acc + Long.parseLong(new String(regionResult, UTF_8)));

    assertEquals(7L, total, "reduce must fold the partial sums from every region");
    // Each region saw the same method and payload verbatim.
    assertEquals("sum", seen.get().getMethod());
    assertEquals("n", seen.get().getPayload().toStringUtf8());
  }

  @Test
  void callAllRegionsReturnsRawPerRegionPayloads() throws Throwable {
    AtomicReference<GoEndpointRequest> seen = new AtomicReference<>();
    Table table = tableYielding(serviceReturning("a", seen), serviceReturning("b", seen));

    Map<byte[], byte[]> perRegion =
        EndpointClient.callAllRegions(table, "upper", "x".getBytes(UTF_8));

    assertEquals(2, perRegion.size(), "every region must contribute one result");
    List<String> values =
        perRegion.values().stream()
            .map(v -> new String(v, UTF_8))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of("a", "b"), values);
  }

  @Test
  void emptyPayloadIsSentAsEmptyBytes() throws Throwable {
    AtomicReference<GoEndpointRequest> seen = new AtomicReference<>();
    Table table = tableYielding(serviceReturning("ok", seen));

    EndpointClient.callAllRegions(table, "scan", null);

    assertTrue(seen.get().hasPayload(), "payload field must be set even when empty");
    assertEquals(0, seen.get().getPayload().size());
  }

  @Test
  void regionErrorSurfacesAsIoException() throws Throwable {
    Table table = tableYielding(serviceFailing("go side unavailable"));

    IOException err =
        assertThrows(
            IOException.class,
            () -> EndpointClient.callAllRegions(table, "sum", "n".getBytes(UTF_8)));
    assertTrue(err.getMessage().contains("go side unavailable"), err.getMessage());
  }

  @Test
  void nullResponseSurfacesAsIoException() throws Throwable {
    // A service that never invokes the callback leaves the response null.
    GoEndpointService silent = mock(GoEndpointService.class);
    Table table = tableYielding(silent);

    IOException err =
        assertThrows(
            IOException.class,
            () -> EndpointClient.callAllRegions(table, "sum", "n".getBytes(UTF_8)));
    assertTrue(err.getMessage().contains("no response"), err.getMessage());
  }

  @Test
  void controllerFailureSurfacesAsIoException() throws Throwable {
    GoEndpointService service = mock(GoEndpointService.class);
    doAnswer(
            invocation -> {
              ServerRpcController controller = invocation.getArgument(0);
              controller.setFailed("region not online");
              return null;
            })
        .when(service)
        .call(any(RpcController.class), any(GoEndpointRequest.class), any());
    Table table = tableYielding(service);

    IOException err =
        assertThrows(
            IOException.class,
            () -> EndpointClient.callAllRegions(table, "sum", "n".getBytes(UTF_8)));
    assertTrue(err.getMessage().contains("region not online"), err.getMessage());
  }

  // E5-3: the range-scoped overload must forward startKey/endKey to coprocessorService verbatim
  // (a swapped or dropped bound would silently select the wrong regions).
  @Test
  void callRegionsForwardsRangeBoundsVerbatim() throws Throwable {
    byte[] start = Bytes.toBytes("aaa");
    byte[] end = Bytes.toBytes("mmm");
    AtomicReference<byte[]> seenStart = new AtomicReference<>();
    AtomicReference<byte[]> seenEnd = new AtomicReference<>();
    Table table = mock(Table.class);
    when(table.coprocessorService(eq(GoEndpointService.class), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              seenStart.set(invocation.getArgument(1));
              seenEnd.set(invocation.getArgument(2));
              return new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
            });

    EndpointClient.callRegions(table, start, end, "sum", "n".getBytes(UTF_8));

    assertArrayEquals(start, seenStart.get(), "startKey must reach coprocessorService verbatim");
    assertArrayEquals(end, seenEnd.get(), "endKey must reach coprocessorService verbatim");
  }
}
