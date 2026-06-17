// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.hbase.v1.ClientProtos;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

/**
 * Converts an HBase {@link Get} into its vendored proto twin. Field selection mirrors upstream
 * Get's wire format so the bytes are interchangeable with hbase-protocol-shaded.
 */
final class GetConverter {

  private GetConverter() {}

  static ClientProtos.Get toProto(Get get) throws IOException {
    ClientProtos.Get.Builder b =
        ClientProtos.Get.newBuilder()
            .setRow(ByteString.copyFrom(get.getRow()))
            .setMaxVersions(get.getMaxVersions())
            .setCacheBlocks(get.getCacheBlocks())
            .setExistenceOnly(get.isCheckExistenceOnly())
            .setConsistency(toProtoConsistency(get.getConsistency()));

    if (get.getMaxResultsPerColumnFamily() >= 0) {
      b.setStoreLimit(get.getMaxResultsPerColumnFamily());
    }
    if (get.getRowOffsetPerColumnFamily() > 0) {
      b.setStoreOffset(get.getRowOffsetPerColumnFamily());
    }
    if (get.getLoadColumnFamiliesOnDemandValue() != null) {
      b.setLoadColumnFamiliesOnDemand(get.getLoadColumnFamiliesOnDemandValue());
    }
    if (get.getFilter() != null) {
      b.setFilter(FilterConverter.toProto(get.getFilter()));
    }
    b.setTimeRange(
        HBaseProtos.TimeRange.newBuilder()
            .setFrom(get.getTimeRange().getMin())
            .setTo(get.getTimeRange().getMax()));
    ColumnConverter.appendColumns(b, get.getFamilyMap());
    return b.build();
  }

  private static ClientProtos.Consistency toProtoConsistency(Consistency c) {
    return c == Consistency.TIMELINE
        ? ClientProtos.Consistency.TIMELINE
        : ClientProtos.Consistency.STRONG;
  }
}
