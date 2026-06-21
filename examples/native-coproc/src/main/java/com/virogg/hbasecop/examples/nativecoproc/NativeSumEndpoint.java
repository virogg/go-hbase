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

public final class NativeSumEndpoint implements RegionCoprocessor {

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
    return Collections.singletonList(new EndpointService(() -> env));
  }

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

    private byte[] get(byte[] row) throws IOException {
      Region region = env().getRegion();
      Result r = region.get(new Get(row));
      if (r == null || r.isEmpty()) {
        return new byte[0];
      }
      Cell first = r.rawCells()[0];
      return CellUtil.cloneValue(first);
    }

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

  @Override
  public Optional<org.apache.hadoop.hbase.coprocessor.RegionObserver> getRegionObserver() {
    return Optional.empty();
  }
}
