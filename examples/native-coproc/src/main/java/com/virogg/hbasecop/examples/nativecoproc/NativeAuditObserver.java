// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.nativecoproc;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NativeAuditObserver implements RegionCoprocessor, RegionObserver {

  static final String MARKER = "native-audit: audit";

  private static final Logger LOG = LoggerFactory.getLogger(NativeAuditObserver.class);
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private final AtomicLong seq = new AtomicLong();

  public NativeAuditObserver() {}

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void postPut(
      ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) {
    emit("put", c, put.getRow(), put.size());
  }

  @Override
  public void postDelete(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Delete delete,
      WALEdit edit,
      Durability durability) {
    emit("delete", c, delete.getRow(), delete.size());
  }

  private void emit(String op, ObserverContext<RegionCoprocessorEnvironment> c, byte[] row, int cells) {
    String table = c.getEnvironment().getRegionInfo().getTable().getNameAsString();
    String region = c.getEnvironment().getRegionInfo().getEncodedName();
    String digest = (row != null && row.length > 0) ? rowDigest(row) : "";
    LOG.info(
        "{} op={} table={} region={} row_digest={} cells={} seq={}",
        MARKER,
        op,
        table,
        region,
        digest,
        cells,
        seq.incrementAndGet());
  }

  static String rowDigest(byte[] row) {
    byte[] sum;
    try {
      sum = MessageDigest.getInstance("SHA-256").digest(row);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
    char[] out = new char[16];
    for (int i = 0; i < 8; i++) {
      out[i * 2] = HEX[(sum[i] >> 4) & 0xf];
      out[i * 2 + 1] = HEX[sum[i] & 0xf];
    }
    return new String(out);
  }
}
