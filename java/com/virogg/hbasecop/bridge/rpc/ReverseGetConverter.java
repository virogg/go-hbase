// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

final class ReverseGetConverter {

  private ReverseGetConverter() {}

  static Get toNativeGet(byte[] opPayload) throws IOException {
    return ProtobufUtil.toGet(ClientProtos.Get.parseFrom(opPayload));
  }

  static Scan toNativeScan(byte[] opPayload) throws IOException {
    return ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(opPayload));
  }

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

  static byte[] toResultBytes(Result result) {
    return ProtobufUtil.toResult(result).toByteArray();
  }
}
