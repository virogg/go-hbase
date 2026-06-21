// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.pb.RpcRequest;
import com.virogg.hbasecop.bridge.wire.pb.RpcResponse;
import com.virogg.hbasecop.hbase.v1.ClientProtos;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

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

    servicer =
        new ReverseRpcServicer(
            registry, new ScannerRegistry(), replies::add, SLOT, 1, 1, Duration.ofSeconds(5));
    try {
      servicer.accept(getRequest(1, 7, "row-1"));
      assertTrue(started.await(2, TimeUnit.SECONDS), "first task did not start");
      servicer.accept(getRequest(2, 7, "row-2"));
      servicer.accept(getRequest(3, 7, "row-3"));

      RpcResponse resp = poll();
      assertEquals(RpcResponse.Status.ERROR, resp.getStatus());
      assertTrue(
          resp.getPayload().toStringUtf8().contains("saturated"),
          () -> "error detail: " + resp.getPayload().toStringUtf8());
    } finally {
      block.countDown();
    }
  }

  @Test
  void saturationReplyDoesNotBlockTheReaderThread() throws Exception {
    RegionRegistry registry = new RegionRegistry();
    registry.register(7, region);

    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch workerBlock = new CountDownLatch(1);
    when(region.get(any(Get.class)))
        .thenAnswer(
            inv -> {
              started.countDown();
              workerBlock.await();
              return Result.EMPTY_RESULT;
            });

    CountDownLatch sinkBlock = new CountDownLatch(1);
    Consumer<Message> blockingSink =
        m -> {
          try {
            sinkBlock.await();
            replies.add(m);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };

    servicer =
        new ReverseRpcServicer(
            registry, new ScannerRegistry(), blockingSink, SLOT, 1, 1, Duration.ofSeconds(5));
    try {
      servicer.accept(getRequest(1, 7, "r1"));
      assertTrue(started.await(2, TimeUnit.SECONDS), "first task did not start");
      servicer.accept(getRequest(2, 7, "r2"));
      long t0 = System.nanoTime();
      servicer.accept(getRequest(3, 7, "r3"));
      long ms = (System.nanoTime() - t0) / 1_000_000L;
      assertTrue(
          ms < 1_000, "accept() blocked " + ms + "ms on the reply sink; the reader must not block");
    } finally {
      sinkBlock.countDown();
      workerBlock.countDown();
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
  void scanOpenBeyondPerCallCapRepliesError() throws Exception {
    RegionRegistry regions = new RegionRegistry();
    regions.register(7, region);
    ScannerRegistry scanners = new ScannerRegistry(1, Long.MAX_VALUE, System::currentTimeMillis);
    when(region.getScanner(any(Scan.class))).thenReturn(scanner);

    servicer =
        new ReverseRpcServicer(regions, scanners, replies::add, SLOT, 4, 16, Duration.ofSeconds(5));
    servicer.accept(scanReq(1, 7, RpcRequest.Op.SCAN_OPEN, 100, 0)); // 1st: ok (cap 1)
    assertEquals(RpcResponse.Status.OK, poll().getStatus());

    servicer.accept(scanReq(2, 7, RpcRequest.Op.SCAN_OPEN, 100, 0)); // 2nd on same call: over cap
    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.ERROR, resp.getStatus());
    assertTrue(
        resp.getPayload().toStringUtf8().contains("max scanners per call"),
        () -> "error detail: " + resp.getPayload().toStringUtf8());
  }

  private void stubOneRowPerNext() throws Exception {
    when(scanner.nextRaw(anyList()))
        .thenAnswer(
            inv -> {
              List<Cell> out = inv.getArgument(0);
              int i = nextRawCalls.getAndIncrement();
              out.add(
                  new KeyValue(
                      Bytes.toBytes("r" + i),
                      Bytes.toBytes("cf"),
                      Bytes.toBytes("q"),
                      1L,
                      Bytes.toBytes("v" + i)));
              return true;
            });
  }

  private final java.util.concurrent.atomic.AtomicInteger nextRawCalls =
      new java.util.concurrent.atomic.AtomicInteger();

  @Test
  void scanNextReturnsBatchAndHasMore() throws Exception {
    RegionRegistry regions = new RegionRegistry();
    regions.register(7, region);
    ScannerRegistry scanners = new ScannerRegistry();
    long sid = scanners.register(100, scanner);
    stubOneRowPerNext();

    servicer =
        new ReverseRpcServicer(
            regions, scanners, replies::add, SLOT, 4, 16, Duration.ofSeconds(5), false, SLOT, 1);
    servicer.accept(scanReq(1, 7, RpcRequest.Op.SCAN_NEXT, 100, sid));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.OK, resp.getStatus());
    assertTrue(resp.getHasMore(), "has_more must reflect that more rows remain");
    ClientProtos.Result batch = ClientProtos.Result.parseFrom(resp.getPayload());
    assertEquals(1, batch.getCellCount());
    verify(region).startRegionOperation();
    verify(region).closeRegionOperation();
  }

  @Test
  void scanNextStopsAtMaxRowsPerNext() throws Exception {
    RegionRegistry regions = new RegionRegistry();
    regions.register(7, region);
    ScannerRegistry scanners = new ScannerRegistry();
    long sid = scanners.register(100, scanner);
    stubOneRowPerNext();

    servicer =
        new ReverseRpcServicer(
            regions, scanners, replies::add, SLOT, 4, 16, Duration.ofSeconds(5), false, SLOT, 3);
    servicer.accept(scanReq(1, 7, RpcRequest.Op.SCAN_NEXT, 100, sid));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.OK, resp.getStatus());
    assertTrue(
        resp.getHasMore(), "has_more must be true when the row cap stops a non-exhausted scan");
    ClientProtos.Result batch = ClientProtos.Result.parseFrom(resp.getPayload());
    assertEquals(3, batch.getCellCount(), "row cap of 3 yields exactly 3 rows (one cell each)");
  }

  @Test
  void scanNextStopsAtMaxBytesPerResp() throws Exception {
    RegionRegistry regions = new RegionRegistry();
    regions.register(7, region);
    ScannerRegistry scanners = new ScannerRegistry();
    long sid = scanners.register(100, scanner);
    stubOneRowPerNext();

    servicer =
        new ReverseRpcServicer(
            regions,
            scanners,
            replies::add,
            SLOT,
            4,
            16,
            Duration.ofSeconds(5),
            false,
            1,
            Integer.MAX_VALUE);
    servicer.accept(scanReq(1, 7, RpcRequest.Op.SCAN_NEXT, 100, sid));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.OK, resp.getStatus());
    assertTrue(resp.getHasMore(), "has_more must be true when the byte ceiling stops the scan");
    ClientProtos.Result batch = ClientProtos.Result.parseFrom(resp.getPayload());
    assertEquals(1, batch.getCellCount(), "a 1-byte ceiling yields one row then stops");
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
  void scanNextOversizedRowRepliesErrorAndClosesScanner() throws Exception {
    RegionRegistry regions = new RegionRegistry();
    regions.register(7, region);
    ScannerRegistry scanners = new ScannerRegistry();
    long sid = scanners.register(100, scanner);
    byte[] big = new byte[4096];
    when(scanner.nextRaw(anyList()))
        .thenAnswer(
            inv -> {
              List<Cell> out = inv.getArgument(0);
              out.add(
                  new KeyValue(
                      Bytes.toBytes("r"), Bytes.toBytes("cf"), Bytes.toBytes("q"), 1L, big));
              return false;
            });

    int slot = 64 * 1024 + 1024;
    servicer =
        new ReverseRpcServicer(
            regions, scanners, replies::add, slot, 4, 16, Duration.ofSeconds(5), false, slot, 4);
    servicer.accept(scanReq(1, 7, RpcRequest.Op.SCAN_NEXT, 100, sid));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.ERROR, resp.getStatus());
    assertTrue(
        resp.getPayload().toStringUtf8().contains("exceeds ring slot"),
        "oversized row must surface a clean 'exceeds ring slot' error");
    verify(scanner).close();
    assertNull(scanners.lookup(100, sid), "the oversized-row scanner must be deregistered");
  }

  private static Message mutateReq(
      long reqId, int regionId, ClientProtos.MutationProto.MutationType type, String row) {
    ClientProtos.MutationProto.Builder mp =
        ClientProtos.MutationProto.newBuilder()
            .setRow(ByteString.copyFrom(Bytes.toBytes(row)))
            .setMutateType(type);
    if (type == ClientProtos.MutationProto.MutationType.PUT) {
      mp.addColumnValue(
          ClientProtos.MutationProto.ColumnValue.newBuilder()
              .setFamily(ByteString.copyFrom(Bytes.toBytes("cf")))
              .addQualifierValue(
                  ClientProtos.MutationProto.ColumnValue.QualifierValue.newBuilder()
                      .setQualifier(ByteString.copyFrom(Bytes.toBytes("q")))
                      .setValue(ByteString.copyFrom(Bytes.toBytes("v")))
                      .setTimestamp(1L)));
    }
    byte[] rr =
        RpcRequest.newBuilder()
            .setOp(RpcRequest.Op.MUTATE)
            .setOpPayload(ByteString.copyFrom(mp.build().toByteArray()))
            .build()
            .toByteArray();
    return new Message(FrameType.RPC_REQUEST, reqId, regionId, (byte) 0, rr);
  }

  @Test
  void mutateDisabledRepliesErrorWithoutWriting() throws Exception {
    RegionRegistry registry = new RegionRegistry();
    registry.register(7, region);
    servicer =
        new ReverseRpcServicer(
            registry, new ScannerRegistry(), replies::add, SLOT, 4, 16, Duration.ofSeconds(5));
    servicer.accept(mutateReq(1, 7, ClientProtos.MutationProto.MutationType.PUT, "row-1"));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.ERROR, resp.getStatus());
    assertTrue(
        resp.getPayload().toStringUtf8().contains("allow-mutate"),
        () -> "error detail: " + resp.getPayload().toStringUtf8());
    verify(region, never()).put(any(Put.class));
  }

  @Test
  void mutatePutAppliesToRegionWhenAllowed() throws Exception {
    RegionRegistry registry = new RegionRegistry();
    registry.register(7, region);
    servicer =
        new ReverseRpcServicer(
            registry,
            new ScannerRegistry(),
            replies::add,
            SLOT,
            4,
            16,
            Duration.ofSeconds(5),
            true);
    servicer.accept(mutateReq(1, 7, ClientProtos.MutationProto.MutationType.PUT, "row-1"));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.OK, resp.getStatus());
    verify(region).put(any(Put.class));
  }

  @Test
  void mutateDeleteAppliesToRegionWhenAllowed() throws Exception {
    RegionRegistry registry = new RegionRegistry();
    registry.register(7, region);
    servicer =
        new ReverseRpcServicer(
            registry,
            new ScannerRegistry(),
            replies::add,
            SLOT,
            4,
            16,
            Duration.ofSeconds(5),
            true);
    servicer.accept(mutateReq(1, 7, ClientProtos.MutationProto.MutationType.DELETE, "row-1"));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.OK, resp.getStatus());
    verify(region).delete(any(Delete.class));
  }

  @Test
  void mutateUnknownRegionRepliesError() throws Exception {
    servicer =
        new ReverseRpcServicer(
            new RegionRegistry(),
            new ScannerRegistry(),
            replies::add,
            SLOT,
            4,
            16,
            Duration.ofSeconds(5),
            true);
    servicer.accept(mutateReq(1, 99, ClientProtos.MutationProto.MutationType.PUT, "row-1"));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.ERROR, resp.getStatus());
    assertTrue(resp.getPayload().toStringUtf8().contains("no region"));
  }

  private void stubTableAllowMutate(String value) {
    CoprocessorDescriptor cd = mock(CoprocessorDescriptor.class);
    when(cd.getProperties())
        .thenReturn(value == null ? Map.of() : Map.of("hbasecop.endpoint.allow-mutate", value));
    TableDescriptor td = mock(TableDescriptor.class);
    when(td.getCoprocessorDescriptors()).thenReturn(List.of(cd));
    when(region.getTableDescriptor()).thenReturn(td);
  }

  @Test
  void mutateAllowedWhenTablePropertyOnEvenIfClusterOff() throws Exception {
    RegionRegistry registry = new RegionRegistry();
    registry.register(7, region);
    stubTableAllowMutate("true");
    servicer =
        new ReverseRpcServicer(
            registry, new ScannerRegistry(), replies::add, SLOT, 4, 16, Duration.ofSeconds(5));
    servicer.accept(mutateReq(1, 7, ClientProtos.MutationProto.MutationType.PUT, "row-1"));

    assertEquals(RpcResponse.Status.OK, poll().getStatus());
    verify(region).put(any(Put.class));
  }

  @Test
  void mutateDeniedWhenTablePropertyOffEvenIfClusterOn() throws Exception {
    RegionRegistry registry = new RegionRegistry();
    registry.register(7, region);
    stubTableAllowMutate("false");
    servicer =
        new ReverseRpcServicer(
            registry,
            new ScannerRegistry(),
            replies::add,
            SLOT,
            4,
            16,
            Duration.ofSeconds(5),
            true);
    servicer.accept(mutateReq(1, 7, ClientProtos.MutationProto.MutationType.PUT, "row-1"));

    RpcResponse resp = poll();
    assertEquals(RpcResponse.Status.ERROR, resp.getStatus());
    assertTrue(resp.getPayload().toStringUtf8().contains("allow-mutate"));
    verify(region, never()).put(any(Put.class));
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
