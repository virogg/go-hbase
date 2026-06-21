// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.hbase.v1.ClientProtos;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

final class ScanConverter {

  private ScanConverter() {}

  static ClientProtos.Scan toProto(Scan scan) throws IOException {
    ClientProtos.Scan.Builder b =
        ClientProtos.Scan.newBuilder()
            .setStartRow(ByteString.copyFrom(scan.getStartRow()))
            .setStopRow(ByteString.copyFrom(scan.getStopRow()))
            .setMaxVersions(scan.getMaxVersions())
            .setCacheBlocks(scan.getCacheBlocks())
            .setReversed(scan.isReversed())
            .setAllowPartialResults(scan.getAllowPartialResults())
            .setIncludeStartRow(scan.includeStartRow())
            .setIncludeStopRow(scan.includeStopRow())
            .setNeedCursorResult(scan.isNeedCursorResult())
            .setConsistency(toProtoConsistency(scan.getConsistency()));

    if (scan.getCaching() > 0) {
      b.setCaching(scan.getCaching());
    }
    if (scan.getBatch() > 0) {
      b.setBatchSize(scan.getBatch());
    }
    if (scan.getMaxResultSize() > 0) {
      b.setMaxResultSize(scan.getMaxResultSize());
    }
    if (scan.getMaxResultsPerColumnFamily() >= 0) {
      b.setStoreLimit(scan.getMaxResultsPerColumnFamily());
    }
    if (scan.getRowOffsetPerColumnFamily() > 0) {
      b.setStoreOffset(scan.getRowOffsetPerColumnFamily());
    }
    if (scan.getLoadColumnFamiliesOnDemandValue() != null) {
      b.setLoadColumnFamiliesOnDemand(scan.getLoadColumnFamiliesOnDemandValue());
    }
    if (scan.getFilter() != null) {
      b.setFilter(FilterConverter.toProto(scan.getFilter()));
    }
    b.setTimeRange(
        HBaseProtos.TimeRange.newBuilder()
            .setFrom(scan.getTimeRange().getMin())
            .setTo(scan.getTimeRange().getMax()));
    ColumnConverter.appendColumns(b, scan.getFamilyMap());
    b.setReadType(toProtoReadType(scan.getReadType()));
    return b.build();
  }

  private static ClientProtos.Consistency toProtoConsistency(Consistency c) {
    return c == Consistency.TIMELINE
        ? ClientProtos.Consistency.TIMELINE
        : ClientProtos.Consistency.STRONG;
  }

  private static ClientProtos.Scan.ReadType toProtoReadType(Scan.ReadType r) {
    if (r == null) {
      return ClientProtos.Scan.ReadType.DEFAULT;
    }
    switch (r) {
      case STREAM:
        return ClientProtos.Scan.ReadType.STREAM;
      case PREAD:
        return ClientProtos.Scan.ReadType.PREAD;
      case DEFAULT:
      default:
        return ClientProtos.Scan.ReadType.DEFAULT;
    }
  }
}
