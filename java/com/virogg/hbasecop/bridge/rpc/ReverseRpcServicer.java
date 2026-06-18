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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

/**
 * Services Go-initiated reverse RPCs (Tier 2): a running endpoint handler asks the bridge to read
 * region-local data — GET (TE31) and pull-scan SCAN_OPEN/NEXT/CLOSE (TE33). Requests are handed off
 * by the reader thread via {@link #accept(Message)} and executed on a bounded, dedicated thread
 * pool — never the reader thread (which must keep routing hooks/heartbeats) and never the
 * RegionServer handler thread blocked awaiting the endpoint result (A-1, anti-deadlock).
 *
 * <p>The pool is <em>fail-closed</em>: when both the pool and its bounded queue are saturated, a
 * request is rejected and answered with an {@code RpcResponse{ERROR}} rather than blocking.
 *
 * <p>Scans batch bytes-primary and resumably: each SCAN_NEXT pulls rows up to a size limit (via
 * {@link ScannerContext}, row-aligned so a row is never split) that keeps the reply within one ring
 * slot; {@code has_more} tells the caller whether to issue another SCAN_NEXT. A single row whose
 * cells exceed one slot is a clean error (the scanner is closed) rather than a crash loop. Open
 * scanners live in the shared {@link ScannerRegistry} and are reaped on Go-process crash. MUTATE is
 * TE41.
 */
public final class ReverseRpcServicer {

  private static final Logger LOG = System.getLogger(ReverseRpcServicer.class.getName());

  // Headroom reserved within a ring slot for the frame header + RpcResponse fields around the
  // Result payload, so a batch that fits our threshold also fits the slot once encoded.
  private static final int SLOT_HEADROOM = 64 * 1024;

  private final RegionRegistry regionRegistry;
  private final ScannerRegistry scannerRegistry;
  private final Consumer<Message> replySink;
  private final int slotMaxObjectSize;
  private final ThreadPoolExecutor pool;
  // Single dedicated thread for sending pool-saturation ERROR replies, so the reject path never
  // sends inline on the reader thread (the reply is itself a bulk-ring send that can block).
  private final ThreadPoolExecutor overflowReplies;
  private final Duration shutdownTimeout;

  /**
   * @param regionRegistry resolves a request's {@code region_id} to its live {@link Region}
   * @param scannerRegistry holds open server-side scanners keyed by {@code (call_id, scanner_id)}
   * @param replySink ships an {@code RpcResponse} {@link Message} back over the bulk ring; must not
   *     block (it runs on a servicing-pool thread)
   * @param slotMaxObjectSize the bulk ring slot size in bytes; bounds a scan batch
   * @param poolSize max concurrent servicing threads
   * @param queueDepth bounded backlog before requests are rejected (fail-closed)
   * @param shutdownTimeout how long {@link #close()} waits for in-flight tasks to drain
   */
  public ReverseRpcServicer(
      RegionRegistry regionRegistry,
      ScannerRegistry scannerRegistry,
      Consumer<Message> replySink,
      int slotMaxObjectSize,
      int poolSize,
      int queueDepth,
      Duration shutdownTimeout) {
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
    // Let idle threads die so a pure-observer deployment that never issues a reverse RPC keeps no
    // servicing threads parked; they respawn on demand up to poolSize.
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

  /**
   * Hand off one reverse-RPC request for servicing. Called on the reader thread, so it only submits
   * (cheap) and never blocks: on pool+queue saturation it fails closed with an ERROR reply.
   */
  public void accept(Message request) {
    try {
      pool.execute(() -> service(request));
    } catch (RejectedExecutionException e) {
      // Fail closed WITHOUT blocking the reader thread. The ERROR reply is itself a (possibly
      // blocking) bulk-ring send, so hand it to a dedicated thread — accept() must stay O(1), else
      // it stalls the reader, starves heartbeat accounting, and risks a false watchdog kill of a
      // healthy Go process (A-1).
      try {
        overflowReplies.execute(() -> replyError(request, "reverse RPC servicing pool saturated"));
      } catch (RejectedExecutionException drop) {
        // Even the overflow path is saturated (extreme, sustained load): drop the reply; the Go
        // caller's reverse-call deadline surfaces it as a clean error.
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
        default:
          replyError(
              request,
              "unsupported reverse op " + req.getOp() + " (GET/SCAN in TE33; MUTATE TE41)");
      }
    } catch (Throwable t) {
      // A worker must never die silently or leak the exception: turn any failure into an ERROR
      // reply
      // so the Go caller sees a clean error instead of hanging until its deadline.
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
    sendReply(
        request,
        RpcResponse.newBuilder().setStatus(RpcResponse.Status.OK).setScannerId(scannerId).build());
  }

  private void serviceScanNext(Message request, RpcRequest req) throws IOException {
    Region region = region(request);
    if (region == null) {
      return;
    }
    long callId = req.getCallId();
    long scannerId = req.getScannerId();
    RegionScanner scanner = scannerRegistry.lookup(callId, scannerId);
    if (scanner == null) {
      replyError(request, "unknown scanner (call_id=" + callId + ", scanner_id=" + scannerId + ")");
      return;
    }

    // Bytes-primary, row-aligned batch (BETWEEN_ROWS: never split a row), sized so the encoded
    // reply
    // fits one ring slot with headroom for a trailing row.
    long sizeLimit = Math.max(1L, (long) (slotMaxObjectSize - SLOT_HEADROOM) / 2L);
    ScannerContext ctx =
        ScannerContext.newBuilder()
            .setSizeLimit(ScannerContext.LimitScope.BETWEEN_ROWS, sizeLimit, sizeLimit)
            .build();
    List<Cell> cells = new ArrayList<>();
    boolean moreRows;
    region.startRegionOperation();
    try {
      moreRows = scanner.nextRaw(cells, ctx);
    } finally {
      region.closeRegionOperation();
    }

    byte[] payload = ReverseGetConverter.toResultBytes(Result.create(cells));
    int max = slotMaxObjectSize - SLOT_HEADROOM;
    if (payload.length > max) {
      // A single row's cells exceed one ring slot: clean error and close the scanner so the client
      // does not retry into a crash loop. (Cross-slot reassembly is unsupported.)
      RegionScanner dead = scannerRegistry.remove(callId, scannerId);
      closeQuietly(dead);
      replyError(
          request, "scan row exceeds ring slot (" + payload.length + " > " + max + " bytes)");
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
    // closeQuietly: if close() throws, the scanner is already removed from the registry, so swallow
    // (and log) rather than leak the exception into an ERROR reply for an otherwise-successful
    // close.
    closeQuietly(scannerRegistry.remove(req.getCallId(), req.getScannerId()));
    sendReply(request, RpcResponse.newBuilder().setStatus(RpcResponse.Status.OK).build());
  }

  /**
   * Resolve the request's region, replying ERROR (and returning null) when it is not registered.
   */
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

  /** Stop servicing and wait briefly for in-flight tasks to drain. Idempotent. */
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
