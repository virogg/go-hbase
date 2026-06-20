// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.hbase.v1.CellProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

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

  /**
   * Reverse of {@link #toProto}: build an HBase {@link Cell} (a {@link KeyValue}) from the vendored
   * proto envelope. Used to materialize the substitute Result an observer supplies on a
   * value-returning bypass (preAppend / preIncrement). Cell-level tags are not reconstructed -
   * observer-authored substitute values do not carry server-side tags.
   */
  static Cell fromProto(CellProtos.Cell c) {
    return new KeyValue(
        c.getRow().toByteArray(),
        c.getFamily().toByteArray(),
        c.getQualifier().toByteArray(),
        c.getTimestamp(),
        fromProtoType(c.getCellType()),
        c.getValue().toByteArray());
  }

  private static KeyValue.Type fromProtoType(CellProtos.CellType type) {
    switch (type) {
      case DELETE:
        return KeyValue.Type.Delete;
      case DELETE_FAMILY_VERSION:
        return KeyValue.Type.DeleteFamilyVersion;
      case DELETE_COLUMN:
        return KeyValue.Type.DeleteColumn;
      case DELETE_FAMILY:
        return KeyValue.Type.DeleteFamily;
      case PUT:
      default:
        // Substitute Result cells are observer-authored values → Put.
        return KeyValue.Type.Put;
    }
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
