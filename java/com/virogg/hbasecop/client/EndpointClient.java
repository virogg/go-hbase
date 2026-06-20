// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.client;

import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.hadoop.hbase.client.Table;

/**
 * TE51 region client helper: invokes the generic {@code GoEndpointService.Call(method, payload)} on
 * every region of a table (fan-out) and optionally reduces the per-region results into one value.
 *
 * <p>An endpoint runs once per region, so a table-wide aggregation (e.g. a SUM) yields one partial
 * result per region; this helper performs the fan-out and the client-side reduce so callers write a
 * single call instead of hand-rolling {@link Table#coprocessorService} each time. It is the
 * client-side counterpart to the server-side aggregation endpoints in {@code pkg/hbasecop}. For the
 * master (no-region) endpoint, see {@link AdminEndpointClient}.
 *
 * <p>The methods declare {@code throws Throwable} only because {@link Table#coprocessorService}
 * does; an endpoint's own outcome (controller failure, missing response, or Go-side error) always
 * surfaces as an {@link java.io.IOException}, so a caller can {@code catch (IOException)} first —
 * symmetric with {@link AdminEndpointClient#callMaster}.
 *
 * <p><b>All-or-nothing fan-out:</b> these helpers use the non-callback {@link
 * Table#coprocessorService} overload, so if any single region's invoke fails the whole call aborts
 * with that exception and the other regions' already-computed partial results are discarded — there
 * is no partial-result accessor. This is the intended default for an aggregation helper (a partial
 * SUM is a silently-wrong SUM, worse than an error). A caller needing per-region tolerance should
 * use {@code Table.coprocessorService(..., Batch.Callback)} directly.
 */
public final class EndpointClient {

  private EndpointClient() {}

  /**
   * Invokes {@code method} with {@code payload} on every region of {@code table}.
   *
   * @return per-region raw response payloads keyed by region name
   */
  public static Map<byte[], byte[]> callAllRegions(Table table, String method, byte[] payload)
      throws Throwable {
    return callRegions(table, null, null, method, payload);
  }

  /**
   * Invokes {@code method} with {@code payload} on every region of {@code table} that covers any
   * row in {@code [startKey, endKey)} — i.e. HBase selects regions overlapping the range, including
   * the region containing {@code startKey} even if its own start key is {@code < startKey}. Null
   * keys mean unbounded (all regions).
   *
   * @return per-region raw response payloads keyed by region name
   */
  public static Map<byte[], byte[]> callRegions(
      Table table, byte[] startKey, byte[] endKey, String method, byte[] payload) throws Throwable {
    GoEndpointRequest request = EndpointCalls.request(method, payload);
    return table.coprocessorService(
        GoEndpointService.class,
        startKey,
        endKey,
        instance -> EndpointCalls.invoke(instance, request));
  }

  /**
   * Fans {@code method}/{@code payload} out over every region of {@code table} and folds the
   * per-region results into one aggregate, starting from {@code identity}.
   *
   * <p>{@code reducer} receives the running accumulator and one region's raw response payload. Make
   * it associative and commutative: regions are folded in region-name order, which carries no
   * semantic meaning for an aggregation.
   */
  public static <R> R callAndReduce(
      Table table, String method, byte[] payload, R identity, BiFunction<R, byte[], R> reducer)
      throws Throwable {
    return callAndReduce(table, null, null, method, payload, identity, reducer);
  }

  /** Range-scoped {@link #callAndReduce(Table, String, byte[], Object, BiFunction)}. */
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
