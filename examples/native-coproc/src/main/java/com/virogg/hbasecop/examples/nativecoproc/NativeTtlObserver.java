// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.examples.nativecoproc;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;

/**
 * Native (pure-Java) twin of the Go {@code examples/ttl-validator} observer. {@code prePut}
 * inspects every cell value and rejects the whole Put unless each value carries the {@code
 * ttl=<seconds>;} envelope — the first violation throws an {@link IOException}, which reaches the
 * HBase client and aborts the write, exactly as the Go strict pre-hook policy does.
 *
 * <p>The validation logic in {@link #validateSeconds(byte[])} mirrors {@code
 * examples/ttl-validator/ttl/ttl.go} {@code Validate} byte-for-byte (same min length, same
 * digit-by-digit parse, same {@code maxDigits=9} bound, same {@code seconds>0} rule), so the
 * accept/reject decision is identical across both arms for any input.
 */
public final class NativeTtlObserver implements RegionCoprocessor, RegionObserver {

  /** The TTL envelope prefix; matches the Go arm's {@code prefix}. */
  private static final byte[] PREFIX = {'t', 't', 'l', '='};

  /** Caps the seconds field at 9 digits (≈31.7 years); matches the Go arm's {@code maxDigits}. */
  private static final int MAX_DIGITS = 9;

  private final AtomicLong accepted = new AtomicLong();
  private final AtomicLong rejected = new AtomicLong();

  public NativeTtlObserver() {}

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void prePut(
      ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability)
      throws IOException {
    for (List<Cell> cells : put.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        try {
          validateSeconds(CellUtil.cloneValue(cell));
        } catch (IOException e) {
          rejected.incrementAndGet();
          // Error text names column coordinates (schema, not payload) — never the value bytes.
          throw new IOException(
              "native-ttl: "
                  + new String(CellUtil.cloneFamily(cell), java.nio.charset.StandardCharsets.UTF_8)
                  + ":"
                  + new String(
                      CellUtil.cloneQualifier(cell), java.nio.charset.StandardCharsets.UTF_8)
                  + " - "
                  + e.getMessage(),
              e);
        }
      }
    }
    accepted.incrementAndGet();
  }

  /**
   * Validates that {@code value} carries the {@code ttl=<seconds>;} envelope and returns the
   * declared seconds. Throws {@link IOException} on any violation. Never echoes the value bytes.
   *
   * <p>This is the line-for-line Java port of the Go {@code ttl.Validate}.
   */
  static long validateSeconds(byte[] value) throws IOException {
    if (value.length < PREFIX.length + 2) { // "ttl=" + at least one digit + ';'
      throw new IOException("value lacks the \"ttl=\" TTL envelope");
    }
    for (int j = 0; j < PREFIX.length; j++) {
      if (value[j] != PREFIX[j]) {
        throw new IOException("value does not start with \"ttl=\"");
      }
    }
    int rest = PREFIX.length;
    int restLen = value.length - rest;
    long seconds = 0;
    int i = 0;
    for (; i < restLen && i < MAX_DIGITS + 1; i++) {
      byte ch = value[rest + i];
      if (ch == ';') {
        break;
      }
      if (ch < '0' || ch > '9') {
        throw new IOException("TTL seconds must be digits terminated by ';'");
      }
      seconds = seconds * 10 + (ch - '0');
    }
    if (i == 0) {
      throw new IOException("TTL envelope has no digits");
    }
    if (i > MAX_DIGITS) {
      throw new IOException("TTL seconds field longer than " + MAX_DIGITS + " digits");
    }
    if (i >= restLen || value[rest + i] != ';') {
      throw new IOException("TTL envelope not terminated by ';'");
    }
    if (seconds == 0) {
      throw new IOException("TTL must be > 0 seconds");
    }
    return seconds;
  }

  long accepted() {
    return accepted.get();
  }

  long rejected() {
    return rejected.get();
  }
}
