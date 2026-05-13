// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.google.protobuf.ByteString;
import com.virogg.hbasecop.hbase.v1.ClientProtos.MutationProto;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;

/**
 * Converts an HBase {@link Put} into the vendored {@link MutationProto} that travels over the wire.
 * Field numbering matches upstream {@code hbase-protocol-shaded} (see proto/hbase/UPSTREAM.md), so
 * the produced bytes are wire-compatible with HBase's own protobuf even though the Java types live
 * in our own package.
 */
final class PutConverter {

  private PutConverter() {}

  static MutationProto toProto(Put put) {
    MutationProto.Builder b =
        MutationProto.newBuilder()
            .setRow(ByteString.copyFrom(put.getRow()))
            .setMutateType(MutationProto.MutationType.PUT)
            .setDurability(toProtoDurability(put.getDurability()));

    if (put.getTimestamp() != Long.MAX_VALUE) {
      b.setTimestamp(put.getTimestamp());
    }

    for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
      MutationProto.ColumnValue.Builder cv =
          MutationProto.ColumnValue.newBuilder().setFamily(ByteString.copyFrom(entry.getKey()));
      for (Cell cell : entry.getValue()) {
        cv.addQualifierValue(
            MutationProto.ColumnValue.QualifierValue.newBuilder()
                .setQualifier(ByteString.copyFrom(CellUtil.cloneQualifier(cell)))
                .setValue(ByteString.copyFrom(CellUtil.cloneValue(cell)))
                .setTimestamp(cell.getTimestamp()));
      }
      b.addColumnValue(cv);
    }
    return b.build();
  }

  private static MutationProto.Durability toProtoDurability(Durability d) {
    switch (d) {
      case SKIP_WAL:
        return MutationProto.Durability.SKIP_WAL;
      case ASYNC_WAL:
        return MutationProto.Durability.ASYNC_WAL;
      case SYNC_WAL:
        return MutationProto.Durability.SYNC_WAL;
      case FSYNC_WAL:
        return MutationProto.Durability.FSYNC_WAL;
      case USE_DEFAULT:
      default:
        return MutationProto.Durability.USE_DEFAULT;
    }
  }
}
