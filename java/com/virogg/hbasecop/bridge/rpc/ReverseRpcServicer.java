// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.pb.RpcRequest;
import com.virogg.hbasecop.bridge.wire.pb.RpcResponse;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

public final class ReverseRpcServicer {

  private static final Logger LOG = System.getLogger(ReverseRpcServicer.class.getName());

  private static final int SLOT_HEADROOM = 64 * 1024;

  private static final String ALLOW_MUTATE_PROPERTY = "hbasecop.endpoint.allow-mutate";

  private final RegionRegistry regionRegistry;
  private final ScannerRegistry scannerRegistry;
  private final Consumer<Message> replySink;
  private final int slotMaxObjectSize;
  private final ThreadPoolExecutor pool;
  private final ThreadPoolExecutor overflowReplies;
  private final Duration shutdownTimeout;
  private final boolean allowMutate;
  private final int maxBytesPerResp;
  private final int maxRowsPerNext;

  public ReverseRpcServicer(
      RegionRegistry regionRegistry,
      ScannerRegistry scannerRegistry,
      Consumer<Message> replySink,
      int slotMaxObjectSize,
      int poolSize,
      int queueDepth,
      Duration shutdownTimeout) {
    this(
        regionRegistry,
        scannerRegistry,
        replySink,
        slotMaxObjectSize,
        poolSize,
        queueDepth,
        shutdownTimeout,
        false);
  }

  public ReverseRpcServicer(
      RegionRegistry regionRegistry,
      ScannerRegistry scannerRegistry,
      Consumer<Message> replySink,
      int slotMaxObjectSize,
      int poolSize,
      int queueDepth,
      Duration shutdownTimeout,
      boolean allowMutate) {
    this(
        regionRegistry,
        scannerRegistry,
        replySink,
        slotMaxObjectSize,
        poolSize,
        queueDepth,
        shutdownTimeout,
        allowMutate,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE);
  }

  public ReverseRpcServicer(
      RegionRegistry regionRegistry,
      ScannerRegistry scannerRegistry,
      Consumer<Message> replySink,
      int slotMaxObjectSize,
      int poolSize,
      int queueDepth,
      Duration shutdownTimeout,
      boolean allowMutate,
      int maxBytesPerResp,
      int maxRowsPerNext) {
    this.allowMutate = allowMutate;
    if (maxBytesPerResp <= 0) {
      throw new IllegalArgumentException("maxBytesPerResp must be > 0, got " + maxBytesPerResp);
    }
    if (maxRowsPerNext <= 0) {
      throw new IllegalArgumentException("maxRowsPerNext must be > 0, got " + maxRowsPerNext);
    }
    this.maxBytesPerResp = maxBytesPerResp;
    this.maxRowsPerNext = maxRowsPerNext;
    this.regionRegistry = Objects.requireNonNull(regionRegistry, "regionRegistry");
    this.scannerRegistry = Objects.requireNonNull(scannerRegistry, "scannerRegistry");
    this.replySink = Objects.requireNonNull(replySink, "replySink");
    this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
    if (slotMaxObjectSize <= SLOT_HEADROOM) {
      throw new IllegalArgumentException("slotMaxObjectSize must be > " + SLOT_HEADROOM);
    }
    this.slotMaxObjectSize = slotMaxObjectSize;
    if (poolSize <= 0) {
      throw new IllegalArgumentException("poolSize must be > 0, got " + poolSize);
    }
    if (queueDepth <= 0) {
      throw new IllegalArgumentException("queueDepth must be > 0, got " + queueDepth);
    }
    AtomicInteger n = new AtomicInteger();
    this.pool =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueDepth),
            r -> {
              Thread t = new Thread(r, "hbasecop-rpcsvc-" + n.incrementAndGet());
              t.setDaemon(true);
              return t;
            },
            new ThreadPoolExecutor.AbortPolicy());
    this.pool.allowCoreThreadTimeOut(true);

    this.overflowReplies =
        new ThreadPoolExecutor(
            1,
            1,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueDepth),
            r -> {
              Thread t = new Thread(r, "hbasecop-rpcsvc-overflow");
              t.setDaemon(true);
              return t;
            },
            new ThreadPoolExecutor.AbortPolicy());
    this.overflowReplies.allowCoreThreadTimeOut(true);
  }

  public void accept(Message request) {
    try {
      pool.execute(() -> service(request));
    } catch (RejectedExecutionException e) {
      try {
        overflowReplies.execute(() -> replyError(request, "reverse RPC servicing pool saturated"));
      } catch (RejectedExecutionException drop) {
        LOG.log(Level.WARNING, "hbasecop: dropped reverse-RPC saturation reply (overflow full)");
      }
    }
  }

  private void service(Message request) {
    RpcRequest req;
    try {
      req = RpcRequest.parseFrom(request.payload());
    } catch (Throwable t) {
      replyError(request, "invalid RpcRequest: " + messageOf(t));
      return;
    }
    try {
      switch (req.getOp()) {
        case GET:
          serviceGet(request, req);
          break;
        case SCAN_OPEN:
          serviceScanOpen(request, req);
          break;
        case SCAN_NEXT:
          serviceScanNext(request, req);
          break;
        case SCAN_CLOSE:
          serviceScanClose(request, req);
          break;
        case MUTATE:
          serviceMutate(request, req);
          break;
        default:
          replyError(request, "unsupported reverse op " + req.getOp());
      }
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "hbasecop: reverse " + req.getOp() + " servicing failed", t);
      replyError(request, "reverse " + req.getOp() + " failed: " + messageOf(t));
    }
  }

  private void serviceGet(Message request, RpcRequest req) throws IOException {
    Region region = region(request);
    if (region == null) {
      return;
    }
    Get get = ReverseGetConverter.toNativeGet(req.getOpPayload().toByteArray());
    Result result = region.get(get);
    reply(
        request,
        RpcResponse.Status.OK,
        ByteString.copyFrom(ReverseGetConverter.toResultBytes(result)));
  }

  private void serviceScanOpen(Message request, RpcRequest req) throws IOException {
    Region region = region(request);
    if (region == null) {
      return;
    }
    Scan scan = ReverseGetConverter.toNativeScan(req.getOpPayload().toByteArray());
    RegionScanner scanner = region.getScanner(scan);
    long scannerId = scannerRegistry.register(req.getCallId(), scanner);
    if (scannerId == ScannerRegistry.REJECTED) {
      replyError(
          request, "scanner registry reaping call " + req.getCallId() + "; SCAN_OPEN rejected");
      return;
    }
    if (scannerId == ScannerRegistry.AT_CAPACITY) {
      replyError(request, "max scanners per call exceeded for call " + req.getCallId());
      return;
    }
    sendReply(
        request,
        RpcResponse.newBuilder().setStatus(RpcResponse.Status.OK).setScannerId(scannerId).build());
  }

  private void serviceScanNext(Message request, RpcRequest req) throws IOException {
    long callId = req.getCallId();
    long scannerId = req.getScannerId();
    Region region = region(request);
    if (region == null) {
      closeQuietly(scannerRegistry.remove(callId, scannerId));
      return;
    }
    RegionScanner scanner = scannerRegistry.lookup(callId, scannerId);
    if (scanner == null) {
      replyError(request, "unknown scanner (call_id=" + callId + ", scanner_id=" + scannerId + ")");
      return;
    }

    long byteCeiling =
        Math.max(
            1L, Math.min((long) (slotMaxObjectSize - SLOT_HEADROOM) / 2L, (long) maxBytesPerResp));
    List<Cell> cells = new ArrayList<>();
    List<Cell> rowCells = new ArrayList<>();
    long bytes = 0;
    int rows = 0;
    boolean moreRows = false;
    try {
      region.startRegionOperation();
      try {
        do {
          rowCells.clear();
          moreRows = scanner.nextRaw(rowCells);
          if (!rowCells.isEmpty()) {
            cells.addAll(rowCells);
            for (Cell c : rowCells) {
              bytes += CellUtil.estimatedSerializedSizeOf(c);
            }
            rows++;
          }
        } while (moreRows && rows < maxRowsPerNext && bytes < byteCeiling);
      } finally {
        region.closeRegionOperation();
      }
    } catch (IOException e) {
      closeQuietly(scannerRegistry.remove(callId, scannerId));
      throw e;
    }

    byte[] payload = ReverseGetConverter.toResultBytes(Result.create(cells));
    int max = slotMaxObjectSize - SLOT_HEADROOM;
    if (payload.length > max) {
      RegionScanner dead = scannerRegistry.remove(callId, scannerId);
      closeQuietly(dead);
      String detail =
          rows <= 1
              ? "scan row exceeds ring slot"
              : "scan batch exceeds ring slot (" + rows + " rows; byte-estimate undercount)";
      replyError(request, detail + " (" + payload.length + " > " + max + " bytes)");
      return;
    }
    sendReply(
        request,
        RpcResponse.newBuilder()
            .setStatus(RpcResponse.Status.OK)
            .setPayload(ByteString.copyFrom(payload))
            .setHasMore(moreRows)
            .build());
  }

  private void serviceScanClose(Message request, RpcRequest req) {
    closeQuietly(scannerRegistry.remove(req.getCallId(), req.getScannerId()));
    sendReply(request, RpcResponse.newBuilder().setStatus(RpcResponse.Status.OK).build());
  }

  private void serviceMutate(Message request, RpcRequest req) throws IOException {
    Region region = region(request);
    if (region == null) {
      return; // region() already replied ERROR
    }
    if (!allowMutateFor(region)) {
      replyError(request, "reverse MUTATE disabled (set hbasecop.endpoint.allow-mutate=true)");
      return;
    }
    Mutation m = ReverseGetConverter.toNativeMutation(req.getOpPayload().toByteArray());
    if (m instanceof Put) {
      region.put((Put) m);
    } else {
      region.delete((Delete) m); // toNativeMutation only ever returns Put or Delete
    }
    sendReply(request, RpcResponse.newBuilder().setStatus(RpcResponse.Status.OK).build());
  }

  private boolean allowMutateFor(Region region) {
    try {
      for (org.apache.hadoop.hbase.client.CoprocessorDescriptor cd :
          region.getTableDescriptor().getCoprocessorDescriptors()) {
        String v = cd.getProperties().get(ALLOW_MUTATE_PROPERTY);
        if (v != null) {
          return Boolean.parseBoolean(v);
        }
      }
    } catch (RuntimeException e) {
      LOG.log(Level.WARNING, "hbasecop: reading per-table allow-mutate failed; using default", e);
    }
    return allowMutate;
  }

  private Region region(Message request) {
    Region region = regionRegistry.lookup(request.regionId());
    if (region == null) {
      replyError(request, "no region registered for region_id " + request.regionId());
    }
    return region;
  }

  private void replyError(Message request, String detail) {
    reply(request, RpcResponse.Status.ERROR, ByteString.copyFromUtf8(detail));
  }

  private void reply(Message request, RpcResponse.Status status, ByteString payload) {
    sendReply(request, RpcResponse.newBuilder().setStatus(status).setPayload(payload).build());
  }

  private void sendReply(Message request, RpcResponse resp) {
    replySink.accept(
        new Message(
            FrameType.RPC_RESPONSE,
            request.reqId(),
            request.regionId(),
            (byte) 0,
            resp.toByteArray()));
  }

  private static void closeQuietly(RegionScanner scanner) {
    if (scanner == null) {
      return;
    }
    try {
      scanner.close();
    } catch (IOException e) {
      LOG.log(Level.WARNING, "hbasecop: closing an oversized-row scanner threw", e);
    }
  }

  private static String messageOf(Throwable t) {
    return t.getMessage() != null ? t.getMessage() : t.toString();
  }

  public void close() {
    pool.shutdownNow();
    overflowReplies.shutdownNow();
    try {
      if (!pool.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "hbasecop: reverse-RPC servicing pool did not drain in time");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
