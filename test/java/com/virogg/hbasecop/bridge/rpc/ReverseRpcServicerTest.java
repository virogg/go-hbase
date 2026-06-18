// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.pb.RpcRequest;
import com.virogg.hbasecop.bridge.wire.pb.RpcResponse;
import com.virogg.hbasecop.hbase.v1.ClientProtos;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
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
 * {@link ReverseRpcServicer} acceptance: resolves the region and runs GET (TE31) / pull-scan
 * (TE33), replying OK; fail-closed (an ERROR reply, not a block) on an unknown region, unknown
 * scanner, or a saturated pool.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReverseRpcServicerTest {

  private static final int SLOT = 1 << 20;

  @Mock private Region region;
  @Mock private RegionScanner scanner;

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

    servicer =
        new ReverseRpcServicer(
            registry, new ScannerRegistry(), replies::add, SLOT, 4, 16, Duration.ofSeconds(5));
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
        new ReverseRpcServicer(
            new RegionRegistry(),
            new ScannerRegistry(),
            replies::add,
            SLOT,
            4,
            16,
            Duration.ofSeconds(5));
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
    servicer =
        new ReverseRpcServicer(
            registry, new ScannerRegistry(), replies::add, SLOT, 1, 1, Duration.ofSeconds(5));
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

  private static Message scanReq(
      long reqId, int regionId, RpcRequest.Op op, long callId, long scannerId) {
    RpcRequest.Builder b =
        RpcRequest.newBuilder().setOp(op).setCallId(callId).setScannerId(scannerId);
    if (op == RpcRequest.Op.SCAN_OPEN) {
      b.setOpPayload(ByteString.copyFrom(ClientProtos.Scan.newBuilder().build().toByteArray()));
    }
    return new Message(FrameType.RPC_REQUEST, reqId, regionId, (byte) 0, b.build().toByteArray());
  }

  @Test
  void scanOpenRegistersScannerAndReturnsId() throws Exception {
    RegionRegistry regions = new RegionRegistry();
    regions.register(7, region);
    ScannerRegistry scanners = new ScannerRegistry();
    when(region.getScanner(any(Scan.class))).thenReturn(scanner);

    servicer =
        new ReverseRpcServicer(regions, scanners, replies::add, SLOT, 4, 16, Duration.ofSeconds(5));
    servicer.accept(scanReq(1, 7, RpcRequest.Op.SCAN_OPEN, 100, 0));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.OK, resp.getStatus());
    assertTrue(resp.getScannerId() > 0, "SCAN_OPEN must return a scanner id");
    assertEquals(1, scanners.size());
  }

  @Test
  void scanNextReturnsBatchAndHasMore() throws Exception {
    RegionRegistry regions = new RegionRegistry();
    regions.register(7, region);
    ScannerRegistry scanners = new ScannerRegistry();
    long sid = scanners.register(100, scanner);
    // One row, then "more rows remain".
    when(scanner.nextRaw(anyList(), any(ScannerContext.class)))
        .thenAnswer(
            inv -> {
              List<Cell> out = inv.getArgument(0);
              out.add(
                  new KeyValue(
                      Bytes.toBytes("r1"),
                      Bytes.toBytes("cf"),
                      Bytes.toBytes("q"),
                      1L,
                      Bytes.toBytes("v1")));
              return true;
            });

    servicer =
        new ReverseRpcServicer(regions, scanners, replies::add, SLOT, 4, 16, Duration.ofSeconds(5));
    servicer.accept(scanReq(1, 7, RpcRequest.Op.SCAN_NEXT, 100, sid));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.OK, resp.getStatus());
    assertTrue(resp.getHasMore(), "has_more must reflect that more rows remain");
    ClientProtos.Result batch = ClientProtos.Result.parseFrom(resp.getPayload());
    assertEquals(1, batch.getCellCount());
    assertEquals("v1", batch.getCell(0).getValue().toStringUtf8());
    verify(region).startRegionOperation();
    verify(region).closeRegionOperation();
  }

  @Test
  void scanNextUnknownScannerRepliesError() throws Exception {
    RegionRegistry regions = new RegionRegistry();
    regions.register(7, region);
    servicer =
        new ReverseRpcServicer(
            regions, new ScannerRegistry(), replies::add, SLOT, 4, 16, Duration.ofSeconds(5));
    servicer.accept(scanReq(1, 7, RpcRequest.Op.SCAN_NEXT, 100, 999));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.ERROR, resp.getStatus());
    assertTrue(resp.getPayload().toStringUtf8().contains("unknown scanner"));
  }

  @Test
  void scanCloseClosesAndDeregisters() throws Exception {
    ScannerRegistry scanners = new ScannerRegistry();
    long sid = scanners.register(100, scanner);
    servicer =
        new ReverseRpcServicer(
            new RegionRegistry(), scanners, replies::add, SLOT, 4, 16, Duration.ofSeconds(5));
    servicer.accept(scanReq(1, 7, RpcRequest.Op.SCAN_CLOSE, 100, sid));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.OK, resp.getStatus());
    verify(scanner).close();
    assertEquals(0, scanners.size());
  }
}
