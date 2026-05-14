// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.google.protobuf.ByteString;
import com.virogg.hbasecop.hbase.v1.CellProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;

/**
 * Converts an HBase {@link Cell} to the vendored {@link CellProtos.Cell} envelope. Used by the
 * read-path converters ({@link ResultConverter}) and by the per-mutation converters in T42 Wave 2+.
 */
final class CellConverter {

  private CellConverter() {}

  static CellProtos.Cell toProto(Cell cell) {
    CellProtos.Cell.Builder b =
        CellProtos.Cell.newBuilder()
            .setRow(ByteString.copyFrom(CellUtil.cloneRow(cell)))
            .setFamily(ByteString.copyFrom(CellUtil.cloneFamily(cell)))
            .setQualifier(ByteString.copyFrom(CellUtil.cloneQualifier(cell)))
            .setTimestamp(cell.getTimestamp())
            .setCellType(toProtoType(cell.getType()))
            .setValue(ByteString.copyFrom(CellUtil.cloneValue(cell)));
    if (cell.getTagsLength() > 0) {
      byte[] tags = PrivateCellUtil.cloneTags(cell);
      if (tags != null && tags.length > 0) {
        b.setTags(ByteString.copyFrom(tags));
      }
    }
    return b.build();
  }

  private static CellProtos.CellType toProtoType(Cell.Type type) {
    switch (type) {
      case Put:
        return CellProtos.CellType.PUT;
      case Delete:
        return CellProtos.CellType.DELETE;
      case DeleteFamilyVersion:
        return CellProtos.CellType.DELETE_FAMILY_VERSION;
      case DeleteColumn:
        return CellProtos.CellType.DELETE_COLUMN;
      case DeleteFamily:
        return CellProtos.CellType.DELETE_FAMILY;
      default:
        // HBase Cell.Type covers Put/Delete/*; treat anything else (Maximum/Minimum used as
        // markers) as MINIMUM to keep the byte stable.
        return CellProtos.CellType.MINIMUM;
    }
  }
}
