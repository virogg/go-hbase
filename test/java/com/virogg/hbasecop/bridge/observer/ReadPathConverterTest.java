// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.hbase.v1.CellProtos;
import com.virogg.hbasecop.hbase.v1.ClientProtos;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.junit.jupiter.api.Test;

/**
 * Round-trip checks for the T42 Wave-1 read-path converters: every shippable HBase native type used
 * by preGetOp / preExists / preScanner* is mapped to its proto twin and (where applicable) back.
 *
 * <p>The proto envelope is the wire contract; "round-trip" here is HBase→proto→bytes→proto and an
 * equals check on the proto side, because reconstructing the HBase native object on the Go side is
 * the Go runtime's job. Tests assert each populated field survives the encode step intact.
 */
class ReadPathConverterTest {

  @Test
  void getRoundTripCarriesEveryShippableField() throws IOException {
    Get hbaseGet =
        new Get(new byte[] {1, 2, 3})
            .addFamily(new byte[] {(byte) 'f'})
            .addColumn(new byte[] {(byte) 'f'}, new byte[] {(byte) 'q'})
            .setMaxVersions(7)
            .setCacheBlocks(false)
            .setMaxResultsPerColumnFamily(11)
            .setRowOffsetPerColumnFamily(2)
            .setCheckExistenceOnly(true)
            .setFilter(new PrefixFilter(new byte[] {(byte) 'p'}));
    hbaseGet.setTimeRange(10L, 100L);

    ClientProtos.Get encoded = GetConverter.toProto(hbaseGet);
    ClientProtos.Get redecoded = ClientProtos.Get.parseFrom(encoded.toByteArray());

    assertEquals(encoded, redecoded);
    assertArrayEquals(new byte[] {1, 2, 3}, redecoded.getRow().toByteArray());
    assertEquals(7, redecoded.getMaxVersions());
    assertEquals(false, redecoded.getCacheBlocks());
    assertEquals(11, redecoded.getStoreLimit());
    assertEquals(2, redecoded.getStoreOffset());
    assertTrue(redecoded.getExistenceOnly());
    assertEquals(10L, redecoded.getTimeRange().getFrom());
    assertEquals(100L, redecoded.getTimeRange().getTo());
    assertEquals(1, redecoded.getColumnCount());
    assertArrayEquals(new byte[] {(byte) 'f'}, redecoded.getColumn(0).getFamily().toByteArray());
    assertEquals(1, redecoded.getColumn(0).getQualifierCount());
    assertArrayEquals(
        new byte[] {(byte) 'q'}, redecoded.getColumn(0).getQualifier(0).toByteArray());
    assertEquals("org.apache.hadoop.hbase.filter.PrefixFilter", redecoded.getFilter().getName());
    assertTrue(redecoded.getFilter().hasSerializedFilter());
  }

  @Test
  void scanRoundTripCarriesEveryShippableField() throws IOException {
    Scan hbaseScan =
        new Scan()
            .withStartRow(new byte[] {(byte) 'a'}, true)
            .withStopRow(new byte[] {(byte) 'z'}, false)
            .addFamily(new byte[] {(byte) 'c', (byte) 'f'})
            .setReversed(true)
            .setCaching(50)
            .setCacheBlocks(false)
            .setBatch(7)
            .setMaxResultSize(1 << 20)
            .setAllowPartialResults(true)
            .setMaxVersions(3)
            .setLimit(11)
            .setMaxResultsPerColumnFamily(4)
            .setNeedCursorResult(true);
    hbaseScan.setTimeRange(0L, 999L);

    ClientProtos.Scan encoded = ScanConverter.toProto(hbaseScan);
    ClientProtos.Scan redecoded = ClientProtos.Scan.parseFrom(encoded.toByteArray());

    assertEquals(encoded, redecoded);
    assertArrayEquals(new byte[] {(byte) 'a'}, redecoded.getStartRow().toByteArray());
    assertArrayEquals(new byte[] {(byte) 'z'}, redecoded.getStopRow().toByteArray());
    assertTrue(redecoded.getReversed());
    assertEquals(50, redecoded.getCaching());
    assertEquals(false, redecoded.getCacheBlocks());
    assertEquals(7, redecoded.getBatchSize());
    assertEquals(1L << 20, redecoded.getMaxResultSize());
    assertTrue(redecoded.getAllowPartialResults());
    assertEquals(3, redecoded.getMaxVersions());
    assertTrue(redecoded.getNeedCursorResult());
    assertTrue(redecoded.getIncludeStartRow());
    assertEquals(false, redecoded.getIncludeStopRow());
    assertEquals(0L, redecoded.getTimeRange().getFrom());
    assertEquals(999L, redecoded.getTimeRange().getTo());
    assertEquals(1, redecoded.getColumnCount());
    // The Scan API "addFamily(cf)" registers the family with no
    // qualifiers; proto must reflect that.
    assertEquals(0, redecoded.getColumn(0).getQualifierCount());
  }

  @Test
  void resultRoundTripPreservesEveryCell() throws IOException {
    org.apache.hadoop.hbase.Cell cell =
        CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
            .setRow(new byte[] {(byte) 'r'})
            .setFamily(new byte[] {(byte) 'f'})
            .setQualifier(new byte[] {(byte) 'q'})
            .setTimestamp(42L)
            .setType(org.apache.hadoop.hbase.Cell.Type.Put)
            .setValue(new byte[] {(byte) 'v'})
            .build();
    Result hbaseResult = Result.create(Arrays.asList(cell));

    ClientProtos.Result encoded = ResultConverter.toProto(hbaseResult);
    ClientProtos.Result redecoded = ClientProtos.Result.parseFrom(encoded.toByteArray());

    assertEquals(encoded, redecoded);
    assertEquals(1, redecoded.getCellCount());
    CellProtos.Cell c = redecoded.getCell(0);
    assertArrayEquals(new byte[] {(byte) 'r'}, c.getRow().toByteArray());
    assertArrayEquals(new byte[] {(byte) 'f'}, c.getFamily().toByteArray());
    assertArrayEquals(new byte[] {(byte) 'q'}, c.getQualifier().toByteArray());
    assertArrayEquals(new byte[] {(byte) 'v'}, c.getValue().toByteArray());
    assertEquals(42L, c.getTimestamp());
    assertEquals(CellProtos.CellType.PUT, c.getCellType());
  }

  @Test
  void consistencyDefaultsToStrong() throws IOException {
    Get hbaseGet = new Get(new byte[] {1});
    ClientProtos.Get encoded = GetConverter.toProto(hbaseGet);
    assertEquals(ClientProtos.Consistency.STRONG, encoded.getConsistency());
  }
}
