// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.rpc;

import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.pb.RpcRequest;
import com.virogg.hbasecop.bridge.wire.pb.RpcResponse;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

/**
 * Services Go-initiated reverse RPCs (Tier 2, TE31): a running endpoint handler asks the bridge to
 * read region-local data. Requests are handed off by the reader thread via {@link #accept(Message)}
 * and executed on a bounded, dedicated thread pool — never the reader thread (which must keep
 * routing hooks/heartbeats) and never the RegionServer handler thread blocked awaiting the endpoint
 * result (A-1, anti-deadlock).
 *
 * <p>The pool is <em>fail-closed</em>: when both the pool and its bounded queue are saturated, a
 * request is rejected and answered with an {@code RpcResponse{ERROR}} rather than blocking, so a
 * burst of reverse RPCs degrades to errors instead of stalling the reader or exhausting memory.
 *
 * <p>TE31 implements {@code GET} only; SCAN lands in TE33 and MUTATE in TE41.
 */
public final class ReverseRpcServicer {

  private static final Logger LOG = System.getLogger(ReverseRpcServicer.class.getName());

  private final RegionRegistry regionRegistry;
  private final Consumer<Message> replySink;
  private final ThreadPoolExecutor pool;
  private final Duration shutdownTimeout;

  /**
   * @param regionRegistry resolves a request's {@code region_id} to its live {@link Region}
   * @param replySink ships an {@code RpcResponse} {@link Message} back over the bulk ring; must not
   *     block (it runs on a servicing-pool thread)
   * @param poolSize max concurrent servicing threads
   * @param queueDepth bounded backlog before requests are rejected (fail-closed)
   * @param shutdownTimeout how long {@link #close()} waits for in-flight tasks to drain
   */
  public ReverseRpcServicer(
      RegionRegistry regionRegistry,
      Consumer<Message> replySink,
      int poolSize,
      int queueDepth,
      Duration shutdownTimeout) {
    this.regionRegistry = Objects.requireNonNull(regionRegistry, "regionRegistry");
    this.replySink = Objects.requireNonNull(replySink, "replySink");
    this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
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
  }

  /**
   * Hand off one reverse-RPC request for servicing. Called on the reader thread, so it only submits
   * (cheap) and never blocks: on pool+queue saturation it fails closed with an ERROR reply.
   */
  public void accept(Message request) {
    try {
      pool.execute(() -> service(request));
    } catch (RejectedExecutionException e) {
      replyError(request, "reverse RPC servicing pool saturated");
    }
  }

  private void service(Message request) {
    try {
      RpcRequest req = RpcRequest.parseFrom(request.payload());
      if (req.getOp() != RpcRequest.Op.GET) {
        replyError(request, "unsupported reverse op " + req.getOp() + " (GET only in TE31)");
        return;
      }
      Region region = regionRegistry.lookup(request.regionId());
      if (region == null) {
        replyError(request, "no region registered for region_id " + request.regionId());
        return;
      }
      Get get = ReverseGetConverter.toNativeGet(req.getOpPayload().toByteArray());
      Result result = region.get(get);
      byte[] payload = ReverseGetConverter.toResultBytes(result);
      reply(request, RpcResponse.Status.OK, ByteString.copyFrom(payload));
    } catch (Throwable t) {
      // A worker must never die silently or leak the exception: turn any failure into an ERROR
      // reply so the Go caller sees a clean error instead of hanging until its deadline.
      LOG.log(Level.WARNING, "hbasecop: reverse GET servicing failed", t);
      replyError(request, "reverse GET failed: " + messageOf(t));
    }
  }

  private void replyError(Message request, String detail) {
    reply(request, RpcResponse.Status.ERROR, ByteString.copyFromUtf8(detail));
  }

  private void reply(Message request, RpcResponse.Status status, ByteString payload) {
    RpcResponse resp = RpcResponse.newBuilder().setStatus(status).setPayload(payload).build();
    replySink.accept(
        new Message(
            FrameType.RPC_RESPONSE,
            request.reqId(),
            request.regionId(),
            (byte) 0,
            resp.toByteArray()));
  }

  private static String messageOf(Throwable t) {
    return t.getMessage() != null ? t.getMessage() : t.toString();
  }

  /** Stop servicing and wait briefly for in-flight tasks to drain. Idempotent. */
  public void close() {
    pool.shutdownNow();
    try {
      if (!pool.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "hbasecop: reverse-RPC servicing pool did not drain in time");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
