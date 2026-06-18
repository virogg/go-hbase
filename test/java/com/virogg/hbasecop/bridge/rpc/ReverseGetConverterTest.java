// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.virogg.hbasecop.hbase.v1.ClientProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * TE31 F4 acceptance: the converter bridges vendored-pb wire bytes and native HBase objects via the
 * shaded ProtobufUtil. Feeding bytes from the <em>vendored</em> {@code ClientProtos} (what the Go
 * side marshals) into the converter (which parses with HBase's <em>shaded</em> {@code
 * ClientProtos}) proves the byte-identity the reverse-RPC path relies on.
 */
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
}
