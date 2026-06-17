// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.pb.RpcRequest;
import com.virogg.hbasecop.bridge.wire.pb.RpcResponse;
import com.virogg.hbasecop.hbase.v1.ClientProtos;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * TE31 - {@link ReverseRpcServicer} acceptance: resolves the region, runs the GET, replies OK; and
 * is fail-closed (an ERROR reply, not a block) on an unknown region or a saturated pool.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReverseRpcServicerTest {

  @Mock private Region region;

  private final LinkedBlockingQueue<Message> replies = new LinkedBlockingQueue<>();
  private ReverseRpcServicer servicer;

  @AfterEach
  void tearDown() {
    if (servicer != null) {
      servicer.close();
    }
  }

  private static Message getRequest(long reqId, int regionId, String row) {
    byte[] getBytes =
        ClientProtos.Get.newBuilder()
            .setRow(ByteString.copyFrom(Bytes.toBytes(row)))
            .build()
            .toByteArray();
    byte[] rr =
        RpcRequest.newBuilder()
            .setOp(RpcRequest.Op.GET)
            .setOpPayload(ByteString.copyFrom(getBytes))
            .build()
            .toByteArray();
    return new Message(FrameType.RPC_REQUEST, reqId, regionId, (byte) 0, rr);
  }

  private RpcResponse poll() throws Exception {
    Message m = replies.poll(2, TimeUnit.SECONDS);
    assertNotNull(m, "expected a reverse-RPC reply");
    assertEquals(FrameType.RPC_RESPONSE, m.type());
    return RpcResponse.parseFrom(m.payload());
  }

  @Test
  void getResolvesRegionAndReturnsResult() throws Exception {
    RegionRegistry registry = new RegionRegistry();
    registry.register(7, region);
    Cell cell =
        new KeyValue(
            Bytes.toBytes("row-1"),
            Bytes.toBytes("cf"),
            Bytes.toBytes("q"),
            1L,
            Bytes.toBytes("hello"));
    when(region.get(any(Get.class))).thenReturn(Result.create(new Cell[] {cell}));

    servicer = new ReverseRpcServicer(registry, replies::add, 4, 16, Duration.ofSeconds(5));
    servicer.accept(getRequest(1, 7, "row-1"));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.OK, resp.getStatus());
    ClientProtos.Result parsed = ClientProtos.Result.parseFrom(resp.getPayload());
    assertEquals(1, parsed.getCellCount());
    assertEquals("hello", parsed.getCell(0).getValue().toStringUtf8());
  }

  @Test
  void unknownRegionRepliesError() throws Exception {
    servicer =
        new ReverseRpcServicer(new RegionRegistry(), replies::add, 4, 16, Duration.ofSeconds(5));
    servicer.accept(getRequest(1, 99, "row-1"));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.ERROR, resp.getStatus());
    assertTrue(
        resp.getPayload().toStringUtf8().contains("no region"),
        () -> "error detail: " + resp.getPayload().toStringUtf8());
  }

  @Test
  void saturatedPoolRepliesErrorWithoutBlocking() throws Exception {
    RegionRegistry registry = new RegionRegistry();
    registry.register(7, region);

    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch block = new CountDownLatch(1);
    when(region.get(any(Get.class)))
        .thenAnswer(
            inv -> {
              started.countDown();
              block.await();
              return Result.EMPTY_RESULT;
            });

    // pool size 1 + queue depth 1: the 3rd concurrent request has nowhere to go.
    servicer = new ReverseRpcServicer(registry, replies::add, 1, 1, Duration.ofSeconds(5));
    try {
      servicer.accept(getRequest(1, 7, "row-1")); // occupies the single thread, then blocks
      assertTrue(started.await(2, TimeUnit.SECONDS), "first task did not start");
      servicer.accept(getRequest(2, 7, "row-2")); // fills the bounded queue
      servicer.accept(getRequest(3, 7, "row-3")); // rejected -> fail-closed ERROR

      // The only reply while the workers are blocked is the rejected request's ERROR — proving
      // accept() did not block the (reader) thread.
      RpcResponse resp = poll();
      assertEquals(RpcResponse.Status.ERROR, resp.getStatus());
      assertTrue(
          resp.getPayload().toStringUtf8().contains("saturated"),
          () -> "error detail: " + resp.getPayload().toStringUtf8());
    } finally {
      block.countDown();
    }
  }
}
