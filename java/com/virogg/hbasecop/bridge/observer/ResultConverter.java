// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import com.virogg.hbasecop.hbase.v1.ClientProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

final class ResultConverter {

  private ResultConverter() {}

  static ClientProtos.Result toProto(Result result) {
    ClientProtos.Result.Builder b =
        ClientProtos.Result.newBuilder()
            .setStale(result.isStale())
            .setPartial(result.mayHaveMoreCellsInRow());
    for (Cell cell : result.rawCells()) {
      b.addCell(CellConverter.toProto(cell));
    }
    return b.build();
  }
}
