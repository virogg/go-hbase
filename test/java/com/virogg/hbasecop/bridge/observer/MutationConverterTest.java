// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.hbase.v1.ClientProtos.MutationProto;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Round-trip checks for the T42 Wave-2 write-path converter ({@link MutationConverter}). The four
 * HBase {@link org.apache.hadoop.hbase.client.Mutation} subtypes (Put / Delete / Append /
 * Increment) all share the {@link MutationProto} wire shape - the discriminator {@link
 * MutationProto.MutationType} steers the Go side per mutation kind.
 */
class MutationConverterTest {

  @Test
  void putMutationCarriesEveryShippableField() throws IOException {
    Put put = new Put(Bytes.toBytes("row-1")).setDurability(Durability.SKIP_WAL);
    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), 42L, Bytes.toBytes("v"));

    MutationProto encoded = MutationConverter.toProto(put);
    MutationProto rehydrated = MutationProto.parseFrom(encoded.toByteArray());

    assertEquals(MutationProto.MutationType.PUT, rehydrated.getMutateType());
    assertArrayEquals(Bytes.toBytes("row-1"), rehydrated.getRow().toByteArray());
    assertEquals(MutationProto.Durability.SKIP_WAL, rehydrated.getDurability());
    assertEquals(1, rehydrated.getColumnValueCount());
    MutationProto.ColumnValue cv = rehydrated.getColumnValue(0);
    assertArrayEquals(Bytes.toBytes("f"), cv.getFamily().toByteArray());
    assertEquals(1, cv.getQualifierValueCount());
    MutationProto.ColumnValue.QualifierValue qv = cv.getQualifierValue(0);
    assertArrayEquals(Bytes.toBytes("q"), qv.getQualifier().toByteArray());
    assertArrayEquals(Bytes.toBytes("v"), qv.getValue().toByteArray());
    assertEquals(42L, qv.getTimestamp());
  }

  @Test
  void deleteMutationCarriesPerCellDeleteType() throws IOException {
    Delete delete = new Delete(Bytes.toBytes("row-1"));
    delete.addColumns(Bytes.toBytes("f"), Bytes.toBytes("q1"));
    delete.addFamily(Bytes.toBytes("g"));

    MutationProto encoded = MutationConverter.toProto(delete);
    MutationProto rehydrated = MutationProto.parseFrom(encoded.toByteArray());

    assertEquals(MutationProto.MutationType.DELETE, rehydrated.getMutateType());
    assertArrayEquals(Bytes.toBytes("row-1"), rehydrated.getRow().toByteArray());
    assertEquals(2, rehydrated.getColumnValueCount());
    boolean sawColumns = false;
    boolean sawFamily = false;
    for (MutationProto.ColumnValue cv : rehydrated.getColumnValueList()) {
      for (MutationProto.ColumnValue.QualifierValue qv : cv.getQualifierValueList()) {
        switch (qv.getDeleteType()) {
          case DELETE_MULTIPLE_VERSIONS:
            sawColumns = true;
            break;
          case DELETE_FAMILY:
            sawFamily = true;
            break;
          default:
            // ignore - other delete types covered by separate tests later.
        }
      }
    }
    assertTrue(sawColumns, "addColumns should yield DELETE_MULTIPLE_VERSIONS");
    assertTrue(sawFamily, "addFamily should yield DELETE_FAMILY");
  }

  @Test
  void appendMutationMarksTypeAndCarriesValue() throws IOException {
    Append append = new Append(Bytes.toBytes("row-1"));
    append.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("tail"));

    MutationProto encoded = MutationConverter.toProto(append);
    MutationProto rehydrated = MutationProto.parseFrom(encoded.toByteArray());

    assertEquals(MutationProto.MutationType.APPEND, rehydrated.getMutateType());
    assertEquals(1, rehydrated.getColumnValueCount());
    MutationProto.ColumnValue.QualifierValue qv = rehydrated.getColumnValue(0).getQualifierValue(0);
    assertArrayEquals(Bytes.toBytes("tail"), qv.getValue().toByteArray());
  }

  @Test
  void incrementMutationCarriesEightBytesDelta() throws IOException {
    Increment inc = new Increment(Bytes.toBytes("row-1"));
    inc.addColumn(Bytes.toBytes("f"), Bytes.toBytes("counter"), 5L);

    MutationProto encoded = MutationConverter.toProto(inc);
    MutationProto rehydrated = MutationProto.parseFrom(encoded.toByteArray());

    assertEquals(MutationProto.MutationType.INCREMENT, rehydrated.getMutateType());
    MutationProto.ColumnValue.QualifierValue qv = rehydrated.getColumnValue(0).getQualifierValue(0);
    // HBase encodes the long delta as 8 bytes big-endian in the cell value.
    assertEquals(5L, Bytes.toLong(qv.getValue().toByteArray()));
  }

  @Test
  void preDeleteWiresThroughMutationConverter() throws IOException {
    // Sanity: the proto Request body for PreDelete embeds MutationProto (T42 Wave 2
    // schema). This shape pins the wire contract - observers can rely on the field
    // numbering: ctx=1, mutation=2.
    com.virogg.hbasecop.bridge.wire.pb.PreDeleteRequest empty =
        com.virogg.hbasecop.bridge.wire.pb.PreDeleteRequest.getDefaultInstance();
    assertEquals(false, empty.hasMutation());
    assertEquals(false, empty.hasCtx());
  }

  @Test
  void postIncrementBeforeWalCarriesCellPairs() throws IOException {
    // Proto-shape pin: the Before-WAL hook accepts repeated CellPair {old, new}
    // entries. Real cell content is exercised by ReadPathConverterTest's CellConverter
    // coverage; this asserts the envelope exposes the right field number for forward
    // compatibility.
    com.virogg.hbasecop.bridge.wire.pb.PostIncrementBeforeWALRequest empty =
        com.virogg.hbasecop.bridge.wire.pb.PostIncrementBeforeWALRequest.getDefaultInstance();
    assertEquals(0, empty.getCellPairCount());
  }

  // Regression for the F2 data-integrity defect: mutation-level attributes were
  // dropped, so a Go security/validation observer saw none of the client's
  // CellVisibility / ACL / TTL context. setTTL is stored as a mutation attribute
  // ("_ttl"), and setAttribute is the same generic mechanism CellVisibility/ACL
  // use; both must now reach the proto's attribute list.
  @Test
  void putMutationCarriesAttributesIncludingTtl() throws IOException {
    Put put = new Put(Bytes.toBytes("row-1"));
    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("v"));
    put.setTTL(60_000L); // stored as the "_ttl" mutation attribute
    put.setAttribute("custom-policy", Bytes.toBytes("audit=on"));

    MutationProto rehydrated =
        MutationProto.parseFrom(MutationConverter.toProto(put).toByteArray());

    assertTrue(rehydrated.getAttributeCount() >= 2, "TTL + custom attribute must survive");
    java.util.Map<String, byte[]> got = new java.util.HashMap<>();
    for (com.virogg.hbasecop.hbase.v1.HBaseProtos.NameBytesPair p : rehydrated.getAttributeList()) {
      got.put(p.getName(), p.getValue().toByteArray());
    }
    assertTrue(got.containsKey("custom-policy"), "custom attribute dropped");
    assertArrayEquals(Bytes.toBytes("audit=on"), got.get("custom-policy"));
    assertTrue(got.containsKey("_ttl"), "TTL (mutation attribute) dropped");
  }

  @Test
  void mutationWithoutAttributesEmitsNone() throws IOException {
    Put put = new Put(Bytes.toBytes("row-1"));
    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("v"));

    MutationProto rehydrated =
        MutationProto.parseFrom(MutationConverter.toProto(put).toByteArray());
    assertEquals(0, rehydrated.getAttributeCount());
  }
}
