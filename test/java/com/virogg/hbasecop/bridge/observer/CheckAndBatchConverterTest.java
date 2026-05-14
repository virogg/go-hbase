// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.virogg.hbasecop.bridge.wire.pb.CheckAndMutateAction;
import com.virogg.hbasecop.bridge.wire.pb.MutationOperation;
import com.virogg.hbasecop.bridge.wire.pb.PostBatchMutateIndispensablyRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreBatchMutateRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCheckAndMutateRequest;
import com.virogg.hbasecop.bridge.wire.pb.PreCheckAndPutRequest;
import com.virogg.hbasecop.hbase.v1.ClientProtos;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Wire-shape pins for the T42 Wave-3 hook payloads (CheckAnd* + Batch). Mostly default-instance
 * assertions on field numbering — full round-trip with real values is exercised through the adapter
 * integration tests where HBase native objects are available.
 */
class CheckAndBatchConverterTest {

  @Test
  void preCheckAndPutFieldNumbersAreStable() throws IOException {
    PreCheckAndPutRequest msg =
        PreCheckAndPutRequest.newBuilder()
            .setRow(com.google.protobuf.ByteString.copyFromUtf8("r"))
            .setFamily(com.google.protobuf.ByteString.copyFromUtf8("f"))
            .setQualifier(com.google.protobuf.ByteString.copyFromUtf8("q"))
            .setCompareOp(2)
            .setComparator(
                ClientProtos.Comparator.newBuilder()
                    .setName("org.apache.hadoop.hbase.filter.BinaryComparator")
                    .build())
            .setInputResult(true)
            .build();

    PreCheckAndPutRequest re = PreCheckAndPutRequest.parseFrom(msg.toByteArray());
    assertArrayEquals(Bytes.toBytes("r"), re.getRow().toByteArray());
    assertArrayEquals(Bytes.toBytes("f"), re.getFamily().toByteArray());
    assertArrayEquals(Bytes.toBytes("q"), re.getQualifier().toByteArray());
    assertEquals(2, re.getCompareOp());
    assertEquals("org.apache.hadoop.hbase.filter.BinaryComparator", re.getComparator().getName());
    assertTrue(re.getInputResult());
  }

  @Test
  void checkAndMutateActionCarriesEitherValueOrFilter() throws IOException {
    CheckAndMutateAction columnBased =
        CheckAndMutateAction.newBuilder()
            .setRow(com.google.protobuf.ByteString.copyFromUtf8("r"))
            .setFamily(com.google.protobuf.ByteString.copyFromUtf8("f"))
            .setQualifier(com.google.protobuf.ByteString.copyFromUtf8("q"))
            .setCompareOp(2)
            .setValue(com.google.protobuf.ByteString.copyFromUtf8("v"))
            .build();
    CheckAndMutateAction filterBased =
        CheckAndMutateAction.newBuilder()
            .setRow(com.google.protobuf.ByteString.copyFromUtf8("r"))
            .setFilter(
                ClientProtos.Filter.newBuilder()
                    .setName("org.apache.hadoop.hbase.filter.PrefixFilter")
                    .build())
            .build();

    assertArrayEquals(Bytes.toBytes("v"), columnBased.getValue().toByteArray());
    assertTrue(filterBased.getValue().isEmpty());
    assertEquals(false, columnBased.hasFilter());
    assertEquals(true, filterBased.hasFilter());
  }

  @Test
  void preCheckAndMutateWiresActionAndInputResult() throws IOException {
    PreCheckAndMutateRequest msg =
        PreCheckAndMutateRequest.newBuilder()
            .setAction(CheckAndMutateAction.newBuilder().setCompareOp(3))
            .setInputResult(
                com.virogg.hbasecop.bridge.wire.pb.CheckAndMutateResultProto.newBuilder()
                    .setSuccess(true))
            .build();

    PreCheckAndMutateRequest re = PreCheckAndMutateRequest.parseFrom(msg.toByteArray());
    assertEquals(3, re.getAction().getCompareOp());
    assertTrue(re.getInputResult().getSuccess());
  }

  @Test
  void preBatchMutateAccumulatesOperations() throws IOException {
    PreBatchMutateRequest msg =
        PreBatchMutateRequest.newBuilder()
            .addOperation(
                MutationOperation.newBuilder()
                    .setMutation(ClientProtos.MutationProto.getDefaultInstance())
                    .setOperationStatusCode(0))
            .addOperation(
                MutationOperation.newBuilder()
                    .setMutation(ClientProtos.MutationProto.getDefaultInstance())
                    .setOperationStatusCode(3))
            .build();

    PreBatchMutateRequest re = PreBatchMutateRequest.parseFrom(msg.toByteArray());
    assertEquals(2, re.getOperationCount());
    assertEquals(0, re.getOperation(0).getOperationStatusCode());
    assertEquals(3, re.getOperation(1).getOperationStatusCode());
  }

  @Test
  void postBatchMutateIndispensablyCarriesSuccessFlag() throws IOException {
    PostBatchMutateIndispensablyRequest msg =
        PostBatchMutateIndispensablyRequest.newBuilder().setSuccess(true).build();
    assertTrue(
        PostBatchMutateIndispensablyRequest.parseFrom(msg.toByteArray()).getSuccess(),
        "success flag must survive serialization (T42 Wave 3 contract)");
  }
}
