// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * F4 conversion seam (Tier 2, TE31): vendored-pb reverse-RPC wire bytes &lt;-&gt; native HBase
 * client objects, using ONLY HBase's <em>shaded</em> {@link ProtobufUtil} and {@link ClientProtos}.
 *
 * <p>The reverse-RPC {@code op_payload} is a marshalled vendored {@code hbase.Get} whose field
 * numbers are byte-identical to upstream (see {@code proto/hbase/UPSTREAM.md}), so we re-parse the
 * raw bytes directly with HBase's own shaded {@code ClientProtos.Get} and never let the vendored
 * Java type ({@code com.virogg.hbasecop.hbase.v1.ClientProtos}) meet {@code ProtobufUtil}. This is
 * the F4 trap: the legacy unshaded {@code org.apache.hadoop.hbase.protobuf.ProtobufUtil} must never
 * be used for this conversion, and no {@code com.google.protobuf} type may cross in. Both the
 * vendored and the shaded {@code ClientProtos} ride the same shaded protobuf runtime, so the bytes
 * are interchangeable.
 */
final class ReverseGetConverter {

  private ReverseGetConverter() {}

  /** Parse a vendored {@code hbase.Get} wire payload into a native {@link Get}. */
  static Get toNativeGet(byte[] opPayload) throws IOException {
    return ProtobufUtil.toGet(ClientProtos.Get.parseFrom(opPayload));
  }

  /** Parse a vendored {@code hbase.Scan} wire payload into a native {@link Scan} (TE33). */
  static Scan toNativeScan(byte[] opPayload) throws IOException {
    return ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(opPayload));
  }

  /**
   * Parse a vendored {@code hbase.MutationProto} wire payload into a native {@link Put} or {@link
   * Delete} (TE41). Cells ride inline in the proto's {@code column_value} (no {@code CellScanner}),
   * so the single-arg shaded converters apply. Only PUT and DELETE are supported; APPEND/INCREMENT
   * (which return a value, needing a reply payload) are out of scope for this slice.
   */
  static Mutation toNativeMutation(byte[] opPayload) throws IOException {
    ClientProtos.MutationProto proto = ClientProtos.MutationProto.parseFrom(opPayload);
    switch (proto.getMutateType()) {
      case PUT:
        return ProtobufUtil.toPut(proto);
      case DELETE:
        return ProtobufUtil.toDelete(proto);
      default:
        throw new IOException("unsupported reverse mutate type: " + proto.getMutateType());
    }
  }

  /**
   * Serialize a native {@link Result} as vendored-compatible {@code hbase.Result} bytes (cells
   * inlined), ready to ship back to Go in an {@code RpcResponse} payload.
   */
  static byte[] toResultBytes(Result result) {
    return ProtobufUtil.toResult(result).toByteArray();
  }
}
