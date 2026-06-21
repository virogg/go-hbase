// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.hbase.v1.ClientProtos.MutationProto;
import com.virogg.hbasecop.hbase.v1.HBaseProtos.NameBytesPair;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

final class MutationConverter {

  private MutationConverter() {}

  static MutationProto toProto(Mutation mutation) throws IOException {
    if (mutation instanceof Put) {
      return toProto((Put) mutation);
    }
    if (mutation instanceof Delete) {
      return toProto((Delete) mutation);
    }
    if (mutation instanceof Append) {
      return toProto((Append) mutation);
    }
    if (mutation instanceof Increment) {
      return toProto((Increment) mutation);
    }
    throw new IOException("unsupported Mutation subtype: " + mutation.getClass().getName());
  }

  static MutationProto toProto(Put put) {
    MutationProto.Builder b = baseBuilder(put, MutationProto.MutationType.PUT);
    if (put.getTimestamp() != Long.MAX_VALUE) {
      b.setTimestamp(put.getTimestamp());
    }
    appendCells(b, put.getFamilyCellMap(), MutationProto.MutationType.PUT);
    return b.build();
  }

  static MutationProto toProto(Delete delete) {
    MutationProto.Builder b = baseBuilder(delete, MutationProto.MutationType.DELETE);
    if (delete.getTimestamp() != Long.MAX_VALUE) {
      b.setTimestamp(delete.getTimestamp());
    }
    appendCells(b, delete.getFamilyCellMap(), MutationProto.MutationType.DELETE);
    return b.build();
  }

  static MutationProto toProto(Append append) {
    MutationProto.Builder b = baseBuilder(append, MutationProto.MutationType.APPEND);
    appendCells(b, append.getFamilyCellMap(), MutationProto.MutationType.APPEND);
    return b.build();
  }

  static MutationProto toProto(Increment increment) {
    MutationProto.Builder b = baseBuilder(increment, MutationProto.MutationType.INCREMENT);
    appendCells(b, increment.getFamilyCellMap(), MutationProto.MutationType.INCREMENT);
    return b.build();
  }

  private static MutationProto.Builder baseBuilder(
      Mutation m, MutationProto.MutationType mutationType) {
    MutationProto.Builder b =
        MutationProto.newBuilder()
            .setRow(ByteString.copyFrom(m.getRow()))
            .setMutateType(mutationType)
            .setDurability(toProtoDurability(m.getDurability()));
    appendAttributes(b, m);
    return b;
  }

  private static void appendAttributes(MutationProto.Builder b, Mutation m) {
    Map<String, byte[]> attrs = m.getAttributesMap();
    if (attrs.isEmpty()) {
      return;
    }
    for (String name : new java.util.TreeSet<>(attrs.keySet())) {
      byte[] value = attrs.get(name);
      if (value == null) {
        continue;
      }
      b.addAttribute(
          NameBytesPair.newBuilder().setName(name).setValue(ByteString.copyFrom(value)).build());
    }
  }

  private static void appendCells(
      MutationProto.Builder b,
      Map<byte[], List<Cell>> familyCellMap,
      MutationProto.MutationType mutationType) {
    for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
      MutationProto.ColumnValue.Builder cv =
          MutationProto.ColumnValue.newBuilder().setFamily(ByteString.copyFrom(entry.getKey()));
      for (Cell cell : entry.getValue()) {
        MutationProto.ColumnValue.QualifierValue.Builder qv =
            MutationProto.ColumnValue.QualifierValue.newBuilder()
                .setQualifier(ByteString.copyFrom(CellUtil.cloneQualifier(cell)))
                .setValue(ByteString.copyFrom(CellUtil.cloneValue(cell)))
                .setTimestamp(cell.getTimestamp());
        if (mutationType == MutationProto.MutationType.DELETE) {
          qv.setDeleteType(toProtoDeleteType(cell));
        }
        byte[] tags = PrivateCellUtil.cloneTags(cell);
        if (tags != null && tags.length > 0) {
          qv.setTags(ByteString.copyFrom(tags));
        }
        cv.addQualifierValue(qv);
      }
      b.addColumnValue(cv);
    }
  }

  private static MutationProto.DeleteType toProtoDeleteType(Cell cell) {
    KeyValue.Type t = KeyValue.Type.codeToType(cell.getTypeByte());
    switch (t) {
      case Delete:
        return MutationProto.DeleteType.DELETE_ONE_VERSION;
      case DeleteColumn:
        return MutationProto.DeleteType.DELETE_MULTIPLE_VERSIONS;
      case DeleteFamilyVersion:
        return MutationProto.DeleteType.DELETE_FAMILY_VERSION;
      case DeleteFamily:
        return MutationProto.DeleteType.DELETE_FAMILY;
      default:
        return MutationProto.DeleteType.DELETE_ONE_VERSION;
    }
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
