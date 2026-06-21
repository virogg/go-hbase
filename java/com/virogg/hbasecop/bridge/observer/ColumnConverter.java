// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.hbase.v1.ClientProtos;
import java.util.Map;
import java.util.NavigableSet;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

final class ColumnConverter {

  private ColumnConverter() {}

  static void appendColumns(
      ClientProtos.Get.Builder b, Map<byte[], NavigableSet<byte[]>> familyMap) {
    for (Map.Entry<byte[], NavigableSet<byte[]>> e : familyMap.entrySet()) {
      b.addColumn(buildColumn(e.getKey(), e.getValue()));
    }
  }

  static void appendColumns(
      ClientProtos.Scan.Builder b, Map<byte[], NavigableSet<byte[]>> familyMap) {
    for (Map.Entry<byte[], NavigableSet<byte[]>> e : familyMap.entrySet()) {
      b.addColumn(buildColumn(e.getKey(), e.getValue()));
    }
  }

  private static ClientProtos.Column buildColumn(byte[] family, NavigableSet<byte[]> qualifiers) {
    ClientProtos.Column.Builder cb =
        ClientProtos.Column.newBuilder().setFamily(ByteString.copyFrom(family));
    if (qualifiers != null) {
      for (byte[] q : qualifiers) {
        cb.addQualifier(ByteString.copyFrom(q));
      }
    }
    return cb.build();
  }
}
