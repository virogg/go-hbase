// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.wire.pb.BulkLoadFamilyPaths;
import com.virogg.hbasecop.bridge.wire.pb.CompactionRequestSummary;
import com.virogg.hbasecop.bridge.wire.pb.FamilyPath;
import com.virogg.hbasecop.bridge.wire.pb.PathPair;
import com.virogg.hbasecop.bridge.wire.pb.PostBulkLoadHFileRequest;
import com.virogg.hbasecop.bridge.wire.pb.PostCompactRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreBulkLoadHFileRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCloseRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCommitStoreFileRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreWALAppendRequest;
import com.virogg.hbasecop.bridge.wire.pb.StoreFilePathProto;
import com.virogg.hbasecop.bridge.wire.pb.WalEditProto;
import com.virogg.hbasecop.bridge.wire.pb.WalKeyProto;
import com.virogg.hbasecop.hbase.v1.CellProtos;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Wire-shape pins for the T42 Wave-4 hook payloads (lifecycle, compaction, WAL, bulk-load,
 * store-file). Asserts field numbering survives serialization round-trip.
 */
class Wave4ShapeTest {

  @Test
  void preCloseCarriesAbortFlag() throws IOException {
    PreCloseRequest msg = PreCloseRequest.newBuilder().setAbortRequested(true).build();
    assertTrue(PreCloseRequest.parseFrom(msg.toByteArray()).getAbortRequested());
  }

  @Test
  void postCompactCarriesResultFileAndRequest() throws IOException {
    PostCompactRequest msg =
        PostCompactRequest.newBuilder()
            .setColumnFamily(com.google.protobuf.ByteString.copyFromUtf8("cf"))
            .setResultFile(StoreFilePathProto.newBuilder().setPath("/foo.hfile").setSizeBytes(1024))
            .setRequest(
                CompactionRequestSummary.newBuilder()
                    .setIsMajor(true)
                    .setSize(42)
                    .setSelectionTime(7))
            .build();

    PostCompactRequest re = PostCompactRequest.parseFrom(msg.toByteArray());
    assertArrayEquals(Bytes.toBytes("cf"), re.getColumnFamily().toByteArray());
    assertEquals("/foo.hfile", re.getResultFile().getPath());
    assertEquals(1024L, re.getResultFile().getSizeBytes());
    assertTrue(re.getRequest().getIsMajor());
    assertEquals(42L, re.getRequest().getSize());
  }

  @Test
  void bulkLoadFamilyPathsRoundTrip() throws IOException {
    PreBulkLoadHFileRequest pre =
        PreBulkLoadHFileRequest.newBuilder()
            .addFamilyPath(
                FamilyPath.newBuilder()
                    .setFamily(com.google.protobuf.ByteString.copyFromUtf8("cf"))
                    .setPath("/staging/a.hfile"))
            .build();
    assertEquals(1, PreBulkLoadHFileRequest.parseFrom(pre.toByteArray()).getFamilyPathCount());

    PostBulkLoadHFileRequest post =
        PostBulkLoadHFileRequest.newBuilder()
            .addFinalPath(
                BulkLoadFamilyPaths.newBuilder()
                    .setFamily(com.google.protobuf.ByteString.copyFromUtf8("cf"))
                    .addPath("/final/a.hfile")
                    .addPath("/final/b.hfile"))
            .build();
    assertEquals(
        2, PostBulkLoadHFileRequest.parseFrom(post.toByteArray()).getFinalPath(0).getPathCount());
  }

  @Test
  void preCommitStoreFilePathPairs() throws IOException {
    PreCommitStoreFileRequest msg =
        PreCommitStoreFileRequest.newBuilder()
            .setFamily(com.google.protobuf.ByteString.copyFromUtf8("cf"))
            .addPair(PathPair.newBuilder().setSource("/src/a").setDestination("/dst/a"))
            .build();
    PathPair re = PreCommitStoreFileRequest.parseFrom(msg.toByteArray()).getPair(0);
    assertEquals("/src/a", re.getSource());
    assertEquals("/dst/a", re.getDestination());
  }

  @Test
  void preWALAppendCarriesWalKeyAndEdit() throws IOException {
    CellProtos.Cell cell =
        CellProtos.Cell.newBuilder()
            .setRow(com.google.protobuf.ByteString.copyFromUtf8("r"))
            .setFamily(com.google.protobuf.ByteString.copyFromUtf8("cf"))
            .setQualifier(com.google.protobuf.ByteString.copyFromUtf8("q"))
            .setValue(com.google.protobuf.ByteString.copyFromUtf8("v"))
            .setCellType(CellProtos.CellType.PUT)
            .build();
    PreWALAppendRequest msg =
        PreWALAppendRequest.newBuilder()
            .setLogKey(WalKeyProto.newBuilder().setLogSeqNum(99).setWriteTime(123))
            .setLogEdit(WalEditProto.newBuilder().addCell(cell))
            .build();
    PreWALAppendRequest re = PreWALAppendRequest.parseFrom(msg.toByteArray());
    assertEquals(99L, re.getLogKey().getLogSeqNum());
    assertEquals(123L, re.getLogKey().getWriteTime());
    assertEquals(1, re.getLogEdit().getCellCount());
  }
}
