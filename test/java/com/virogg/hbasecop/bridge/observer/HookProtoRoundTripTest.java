// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.virogg.hbasecop.bridge.wire.pb.HooksProto;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * T42 Wave-5 "100% coverage" gate (Java side): every proto message under {@code virogg.hbasecop.v1}
 * round-trips via {@code Message → bytes → Message} at its default-instance shape and with a
 * synthetic value populated on every scalar field. Pairs with the Go-side counterpart {@code
 * internal/wire/hookpb/hooks_roundtrip_test.go}; both walk the proto registry so a new Request type
 * added to {@code proto/hooks.proto} flows into the gate automatically.
 */
class HookProtoRoundTripTest {

  @Test
  void everyHookMessageRoundTrips() throws Exception {
    Descriptors.FileDescriptor hooks = HooksProto.getDescriptor();
    List<Descriptors.Descriptor> messages = collectAllMessages(hooks);
    assertTrue(
        messages.size() >= 70,
        "expected 70+ messages in proto/hooks.proto for T42 coverage gate, got " + messages.size());

    int defaulted = 0;
    int mutated = 0;
    for (Descriptors.Descriptor d : messages) {
      Message defaultInstance = DynamicMessage.getDefaultInstance(d);

      // Default-instance round-trip.
      byte[] bytes = defaultInstance.toByteArray();
      Message parsed = DynamicMessage.parseFrom(d, bytes);
      assertEquals(
          defaultInstance.toByteString(),
          parsed.toByteString(),
          "default-instance round-trip drift for " + d.getFullName());
      defaulted++;

      // Mutated round-trip — populate every scalar field with a small
      // typed default so the wire bytes are non-empty.
      DynamicMessage.Builder mb = DynamicMessage.newBuilder(d);
      boolean touched = false;
      for (Descriptors.FieldDescriptor fd : d.getFields()) {
        if (fd.isRepeated() || fd.isMapField()) {
          continue;
        }
        Object value = scalarValue(fd);
        if (value == null) {
          continue;
        }
        mb.setField(fd, value);
        touched = true;
      }
      if (!touched) {
        // No scalars on this message — default-instance check above is the only path.
        continue;
      }
      Message mutatedMsg = mb.build();
      Message reparsed = DynamicMessage.parseFrom(d, mutatedMsg.toByteArray());
      assertEquals(
          mutatedMsg.toByteString(),
          reparsed.toByteString(),
          "mutated round-trip drift for " + d.getFullName());
      mutated++;
    }

    if (defaulted < 70 || mutated < 30) {
      fail(
          "T42 Wave-5 coverage gate too thin: defaulted="
              + defaulted
              + " mutated="
              + mutated
              + " (expected 70+ / 30+; many ctx-only stubs are scalar-free)");
    }
  }

  private static List<Descriptors.Descriptor> collectAllMessages(Descriptors.FileDescriptor file) {
    List<Descriptors.Descriptor> out = new ArrayList<>();
    for (Descriptors.Descriptor d : file.getMessageTypes()) {
      out.add(d);
      out.addAll(d.getNestedTypes());
    }
    return out;
  }

  private static Object scalarValue(Descriptors.FieldDescriptor fd) {
    switch (fd.getJavaType()) {
      case BOOLEAN:
        return Boolean.TRUE;
      case INT:
        return Integer.valueOf(7);
      case LONG:
        return Long.valueOf(7L);
      case FLOAT:
        return Float.valueOf(7f);
      case DOUBLE:
        return Double.valueOf(7d);
      case STRING:
        return "rt";
      case BYTE_STRING:
        return ByteString.copyFromUtf8("rt");
      case ENUM:
        return fd.getEnumType().getValues().get(0);
      case MESSAGE:
        // Message fields are exercised by their own round-trip; leave default.
        return null;
      default:
        return null;
    }
  }
}
