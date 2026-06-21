// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.hbase.v1.ClientProtos;
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReverseGetConverterTest {

  @Test
  @DisplayName("vendored Get bytes parse into a native Get with the same row")
  void toNativeGetParsesVendoredGetBytes() throws Exception {
    byte[] row = Bytes.toBytes("row-7");
    byte[] wire =
        ClientProtos.Get.newBuilder().setRow(ByteString.copyFrom(row)).build().toByteArray();

    Get get = ReverseGetConverter.toNativeGet(wire);

    assertArrayEquals(row, get.getRow());
  }

  @Test
  @DisplayName("vendored Scan bytes parse into a native Scan with the same start row (TE33)")
  void toNativeScanParsesVendoredScanBytes() throws Exception {
    byte[] startRow = Bytes.toBytes("aaa");
    byte[] wire =
        ClientProtos.Scan.newBuilder()
            .setStartRow(ByteString.copyFrom(startRow))
            .build()
            .toByteArray();

    Scan scan = ReverseGetConverter.toNativeScan(wire);

    assertArrayEquals(startRow, scan.getStartRow());
  }

  @Test
  @DisplayName("native Result serializes to bytes a vendored Result parses identically")
  void toResultBytesRoundTripsThroughVendoredResult() throws Exception {
    byte[] row = Bytes.toBytes("row-7");
    byte[] cf = Bytes.toBytes("cf");
    byte[] q = Bytes.toBytes("q");
    byte[] val = Bytes.toBytes("hello");
    Cell cell = new KeyValue(row, cf, q, 1_700_000_000_000L, val);
    Result result = Result.create(new Cell[] {cell});

    byte[] bytes = ReverseGetConverter.toResultBytes(result);

    ClientProtos.Result parsed = ClientProtos.Result.parseFrom(bytes);
    assertEquals(1, parsed.getCellCount());
    assertArrayEquals(row, parsed.getCell(0).getRow().toByteArray());
    assertArrayEquals(val, parsed.getCell(0).getValue().toByteArray());
  }

  @Test
  @DisplayName("vendored PUT MutationProto parses into a native Put with the row + cell (TE41)")
  void toNativeMutationParsesPut() throws Exception {
    byte[] row = Bytes.toBytes("row-9");
    byte[] cf = Bytes.toBytes("cf");
    byte[] q = Bytes.toBytes("q");
    byte[] v = Bytes.toBytes("v");
    byte[] wire =
        ClientProtos.MutationProto.newBuilder()
            .setRow(ByteString.copyFrom(row))
            .setMutateType(ClientProtos.MutationProto.MutationType.PUT)
            .addColumnValue(
                ClientProtos.MutationProto.ColumnValue.newBuilder()
                    .setFamily(ByteString.copyFrom(cf))
                    .addQualifierValue(
                        ClientProtos.MutationProto.ColumnValue.QualifierValue.newBuilder()
                            .setQualifier(ByteString.copyFrom(q))
                            .setValue(ByteString.copyFrom(v))
                            .setTimestamp(1_700_000_000_000L)))
            .build()
            .toByteArray();

    Mutation m = ReverseGetConverter.toNativeMutation(wire);

    assertTrue(m instanceof Put, "want a native Put");
    assertArrayEquals(row, m.getRow());
    assertTrue(((Put) m).has(cf, q), "Put must carry the cf:q cell");
  }

  @Test
  @DisplayName("vendored DELETE MutationProto parses into a native Delete with the same row (TE41)")
  void toNativeMutationParsesDelete() throws Exception {
    byte[] row = Bytes.toBytes("row-d");
    byte[] wire =
        ClientProtos.MutationProto.newBuilder()
            .setRow(ByteString.copyFrom(row))
            .setMutateType(ClientProtos.MutationProto.MutationType.DELETE)
            .build()
            .toByteArray();

    Mutation m = ReverseGetConverter.toNativeMutation(wire);

    assertTrue(m instanceof Delete, "want a native Delete");
    assertArrayEquals(row, m.getRow());
  }

  @Test
  @DisplayName("an unsupported mutate type (APPEND) is a clean error (TE41)")
  void toNativeMutationRejectsUnsupportedType() {
    byte[] wire =
        ClientProtos.MutationProto.newBuilder()
            .setRow(ByteString.copyFrom(Bytes.toBytes("r")))
            .setMutateType(ClientProtos.MutationProto.MutationType.APPEND)
            .build()
            .toByteArray();

    assertThrows(IOException.class, () -> ReverseGetConverter.toNativeMutation(wire));
  }
}
