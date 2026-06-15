// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.google.protobuf.ByteString;
import com.virogg.hbasecop.hbase.v1.ClientProtos;
import java.io.IOException;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * Maps an HBase {@link Filter} to its vendored proto envelope. The bridge ships (class-name,
 * self-serialized-bytes) - full reconstruction on the Go side waits for a per-filter mapper; the
 * opaque blob round-trips losslessly through the wire.
 */
final class FilterConverter {

  private FilterConverter() {}

  static ClientProtos.Filter toProto(Filter filter) throws IOException {
    ClientProtos.Filter.Builder b =
        ClientProtos.Filter.newBuilder().setName(filter.getClass().getName());
    byte[] serialized = filter.toByteArray();
    if (serialized != null) {
      b.setSerializedFilter(ByteString.copyFrom(serialized));
    }
    return b.build();
  }
}
