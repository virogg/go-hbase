// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.hbase.v1.ClientProtos;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

final class ComparatorConverter {

  private ComparatorConverter() {}

  static ClientProtos.Comparator toProto(ByteArrayComparable comparator) {
    ClientProtos.Comparator.Builder b =
        ClientProtos.Comparator.newBuilder().setName(comparator.getClass().getName());
    byte[] serialized = comparator.toByteArray();
    if (serialized != null) {
      b.setSerializedComparator(ByteString.copyFrom(serialized));
    }
    return b.build();
  }
}
