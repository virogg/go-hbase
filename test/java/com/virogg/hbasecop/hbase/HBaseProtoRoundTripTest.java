// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.virogg.hbasecop.bridge.wire.pb.HookContext;
import com.virogg.hbasecop.bridge.wire.pb.HookError;
import com.virogg.hbasecop.bridge.wire.pb.HookResponse;
import com.virogg.hbasecop.bridge.wire.pb.PrePutRequest;
import com.virogg.hbasecop.hbase.v1.CellProtos;
import com.virogg.hbasecop.hbase.v1.ClientProtos;
import com.virogg.hbasecop.hbase.v1.HBaseProtos;
import java.util.stream.Stream;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * T21 acceptance - per-message round-trip and deterministic re-encode for every vendored HBase type
 * and the Phase 2 hook envelopes. We keep upstream field numbers byte-identical (see
 * proto/hbase/UPSTREAM.md) so anything that survives this test is wire-compatible with HBase itself
 * and with the protoc-gen-go side (cf. {@code internal/wire/hbasepb/hbase_roundtrip_test.go}).
 */
class HBaseProtoRoundTripTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("fixtures")
  @DisplayName("HBase + hook proto round-trip is identity + deterministic")
  void roundTrip(String name, Message msg) throws Exception {
    byte[] encoded = msg.toByteArray();
    assertNotEquals(0, encoded.length, () -> name + ": empty wire for non-empty message");

    Message decoded = msg.getParserForType().parseFrom(encoded);
    assertEquals(msg, decoded, () -> name + ": round-trip diverged");

    byte[] reEncoded = decoded.toByteArray();
    assertArrayEquals(encoded, reEncoded, () -> name + ": re-encode mismatch");
  }

  private static Stream<Arguments> fixtures() {
    return Stream.of(
        Arguments.of("Cell_put", cellPut()),
        Arguments.of("TableName_default_ns", tableName()),
        Arguments.of("TimeRange_bounded", timeRange()),
        Arguments.of("NameBytesPair_with_value", nameBytesPair()),
        Arguments.of("MutationProto_put_two_cols", putMutation()),
        Arguments.of("PrePutRequest_wrapped", prePutRequest()),
        Arguments.of("HookResponse_bypass_with_error", hookResponseWithError()));
  }

  private static CellProtos.Cell cellPut() {
    return CellProtos.Cell.newBuilder()
        .setRow(ByteString.copyFromUtf8("row-7"))
        .setFamily(ByteString.copyFromUtf8("cf"))
        .setQualifier(ByteString.copyFromUtf8("q1"))
        .setTimestamp(1_700_000_000_000L)
        .setCellType(CellProtos.CellType.PUT)
        .setValue(ByteString.copyFromUtf8("hello"))
        .build();
  }

  private static HBaseProtos.TableName tableName() {
    return HBaseProtos.TableName.newBuilder()
        .setNamespace(ByteString.copyFromUtf8("default"))
        .setQualifier(ByteString.copyFromUtf8("users"))
        .build();
  }

  private static HBaseProtos.TimeRange timeRange() {
    return HBaseProtos.TimeRange.newBuilder()
        .setFrom(1_700_000_000_000L)
        .setTo(1_800_000_000_000L)
        .build();
  }

  private static HBaseProtos.NameBytesPair nameBytesPair() {
    return HBaseProtos.NameBytesPair.newBuilder()
        .setName("audit-tag")
        .setValue(ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03}))
        .build();
  }

  private static ClientProtos.MutationProto putMutation() {
    ClientProtos.MutationProto.ColumnValue.QualifierValue.Builder qv1 =
        ClientProtos.MutationProto.ColumnValue.QualifierValue.newBuilder()
            .setQualifier(ByteString.copyFromUtf8("q1"))
            .setValue(ByteString.copyFromUtf8("v1"))
            .setTimestamp(1_700_000_000_000L);
    ClientProtos.MutationProto.ColumnValue.QualifierValue.Builder qv2 =
        ClientProtos.MutationProto.ColumnValue.QualifierValue.newBuilder()
            .setQualifier(ByteString.copyFromUtf8("q2"))
            .setValue(ByteString.copyFromUtf8("v2"))
            .setTimestamp(1_700_000_000_001L);
    ClientProtos.MutationProto.ColumnValue cf =
        ClientProtos.MutationProto.ColumnValue.newBuilder()
            .setFamily(ByteString.copyFromUtf8("cf"))
            .addQualifierValue(qv1)
            .addQualifierValue(qv2)
            .build();
    ClientProtos.MutationProto.ColumnValue meta =
        ClientProtos.MutationProto.ColumnValue.newBuilder()
            .setFamily(ByteString.copyFromUtf8("meta"))
            .addQualifierValue(
                ClientProtos.MutationProto.ColumnValue.QualifierValue.newBuilder()
                    .setQualifier(ByteString.copyFromUtf8("source"))
                    .setValue(ByteString.copyFromUtf8("audit"))
                    .setTimestamp(1_700_000_000_002L))
            .build();
    return ClientProtos.MutationProto.newBuilder()
        .setRow(ByteString.copyFromUtf8("row-7"))
        .setMutateType(ClientProtos.MutationProto.MutationType.PUT)
        .setTimestamp(1_700_000_000_000L)
        .setDurability(ClientProtos.MutationProto.Durability.USE_DEFAULT)
        .addColumnValue(cf)
        .addColumnValue(meta)
        .addAttribute(
            HBaseProtos.NameBytesPair.newBuilder()
                .setName("trace_id")
                .setValue(ByteString.copyFromUtf8("abc-123")))
        .build();
  }

  private static PrePutRequest prePutRequest() {
    return PrePutRequest.newBuilder()
        .setCtx(
            HookContext.newBuilder()
                .setTableName(tableName())
                .setRegionName(ByteString.copyFromUtf8("users,,1700000000000.abcd."))
                .setRequestId(4242L))
        .setMutation(putMutation())
        .build();
  }

  private static HookResponse hookResponseWithError() {
    return HookResponse.newBuilder()
        .setBypass(true)
        .setError(
            HookError.newBuilder().setCode(7).setMessage("policy rejected: missing TTL marker"))
        .build();
  }
}
