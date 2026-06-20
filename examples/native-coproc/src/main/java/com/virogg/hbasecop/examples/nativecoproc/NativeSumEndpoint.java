// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.nativecoproc;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointResponse;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

/**
 * Native (pure-Java) twin of the Go {@code examples/endpoint-observer} aggregating endpoint, for
 * the native-vs-Go comparison harness. It reuses the SAME {@link GoEndpointService} surface the Go
 * arm exposes, so the client call path ({@code com.virogg.hbasecop.client.EndpointClient}) is byte
 * identical across both arms — only this server-side implementation differs: here the {@code
 * sum}/{@code get}/{@code scan} logic runs directly on the RegionServer JVM over {@link
 * RegionCoprocessorEnvironment#getRegion()}, with no shmem bridge and no Go process.
 *
 * <p>Semantics mirror {@code examples/endpoint-observer/main.go} 1:1:
 *
 * <ul>
 *   <li>{@code sum}: scan the region, sum every {@code cf:<payload>} cell parsed as a base-10
 *       int64, return the per-region partial as an ASCII-decimal string (the client reduces).
 *   <li>{@code get}: return the first cell value of the row named by the payload (empty if absent).
 *   <li>{@code scan}: count the cells across the whole region, return the count as ASCII decimal.
 *   <li>default: upper-case the payload.
 * </ul>
 */
public final class NativeSumEndpoint implements RegionCoprocessor {

  /** The column family the reverse-read methods read from — matches the Go arm's {@code cf}. */
  static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);

  private RegionCoprocessorEnvironment env;

  public NativeSumEndpoint() {}

  @Override
  public void start(CoprocessorEnvironment e) {
    if (e instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) e;
    }
  }

  @Override
  public Iterable<Service> getServices() {
    // Late-bind the env: HBase can call getServices() before start(), so read the field at
    // invoke time (not registration time) — same contract the Go bridge's endpointServices uses.
    return Collections.singletonList(new EndpointService(() -> env));
  }

  /** The native {@link GoEndpointService} implementation that runs aggregation on the region. */
  static final class EndpointService extends GoEndpointService {

    private final java.util.function.Supplier<RegionCoprocessorEnvironment> envSupplier;

    EndpointService(java.util.function.Supplier<RegionCoprocessorEnvironment> envSupplier) {
      this.envSupplier = envSupplier;
    }

    private RegionCoprocessorEnvironment env() throws IOException {
      RegionCoprocessorEnvironment e = envSupplier.get();
      if (e == null) {
        throw new IOException("native-sum: endpoint invoked before coprocessor start");
      }
      return e;
    }

    @Override
    public void call(
        RpcController controller, GoEndpointRequest request, RpcCallback<GoEndpointResponse> done) {
      GoEndpointResponse.Builder resp = GoEndpointResponse.newBuilder();
      try {
        byte[] out = dispatch(request.getMethod(), request.getPayload().toByteArray());
        resp.setPayload(ByteString.copyFrom(out == null ? new byte[0] : out));
      } catch (IOException e) {
        String msg = e.getMessage() != null ? e.getMessage() : e.toString();
        resp.setError(msg);
        if (controller != null) {
          controller.setFailed(msg);
        }
      }
      done.run(resp.build());
    }

    private byte[] dispatch(String method, byte[] payload) throws IOException {
      switch (method) {
        case "sum":
          return sum(payload);
        case "get":
          return get(payload);
        case "scan":
          return Long.toString(scanCount()).getBytes(StandardCharsets.UTF_8);
        default:
          return new String(payload, StandardCharsets.UTF_8)
              .toUpperCase(java.util.Locale.ROOT)
              .getBytes(StandardCharsets.UTF_8);
      }
    }

    /** Sums cf:&lt;qualifier&gt; cells in this region as int64; returns the partial ASCII-decimal. */
    private byte[] sum(byte[] qualifier) throws IOException {
      Region region = env().getRegion();
      long total = 0;
      List<Cell> cells = new ArrayList<>();
      try (RegionScanner scanner = region.getScanner(new Scan())) {
        boolean hasMore;
        do {
          cells.clear();
          hasMore = scanner.next(cells);
          for (Cell c : cells) {
            if (CellUtil.matchingFamily(c, CF) && CellUtil.matchingQualifier(c, qualifier)) {
              String v = new String(CellUtil.cloneValue(c), StandardCharsets.UTF_8);
              try {
                total += Long.parseLong(v);
              } catch (NumberFormatException nfe) {
                throw new IOException(
                    "native-sum: non-numeric cf:"
                        + new String(qualifier, StandardCharsets.UTF_8)
                        + " value");
              }
            }
          }
        } while (hasMore);
      }
      return Long.toString(total).getBytes(StandardCharsets.UTF_8);
    }

    /** Returns the first cell value of the row named by {@code row} (empty bytes if absent). */
    private byte[] get(byte[] row) throws IOException {
      Region region = env().getRegion();
      Result r = region.get(new Get(row));
      if (r == null || r.isEmpty()) {
        return new byte[0];
      }
      Cell first = r.rawCells()[0];
      return CellUtil.cloneValue(first);
    }

    /** Counts every cell in the region (the Go arm's {@code scan} method). */
    private long scanCount() throws IOException {
      Region region = env().getRegion();
      long count = 0;
      List<Cell> cells = new ArrayList<>();
      try (RegionScanner scanner = region.getScanner(new Scan())) {
        boolean hasMore;
        do {
          cells.clear();
          hasMore = scanner.next(cells);
          count += cells.size();
        } while (hasMore);
      }
      return count;
    }
  }

  // RegionCoprocessor#getRegionObserver defaults to Optional.empty(): this coprocessor exposes only
  // the endpoint Service, no observer hooks. Declared for documentation symmetry with the Go arm.
  @Override
  public Optional<org.apache.hadoop.hbase.coprocessor.RegionObserver> getRegionObserver() {
    return Optional.empty();
  }
}
