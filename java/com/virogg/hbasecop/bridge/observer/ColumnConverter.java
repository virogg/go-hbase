// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.google.protobuf.ByteString;
import com.virogg.hbasecop.hbase.v1.ClientProtos;
import java.util.Map;
import java.util.NavigableSet;

/**
 * Builds {@link ClientProtos.Column} entries from the family→qualifiers map exposed by HBase
 * Get/Scan via {@code getFamilyMap()}. A null qualifier set means "whole family"; the proto then
 * carries the family with an empty repeated qualifier list, matching upstream semantics.
 */
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
