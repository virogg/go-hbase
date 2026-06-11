// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

import com.jgshmem.message.PayloadMessage;
import com.jgshmem.ring.WaitingRingConsumer;
import com.jgshmem.ring.WaitingRingProducer;
import com.jgshmem.utils.Builder;
import java.util.Objects;
import java.util.Optional;

/**
 * Thin typed wrapper over {@code com.jgshmem.ring.WaitingRingProducer / WaitingRingConsumer}. One
 * {@code Channel} is a single-direction endpoint over one shared-memory ring.
 *
 * <p>Mirrors {@code internal/shmem.Channel} on the Go side, including the {@link Role}-based
 * read/write asymmetry and the sentinel-style "no data" / "ring full" semantics. The wrapper is
 * payload-agnostic: framing and serialization are the caller's responsibility (see {@code
 * com.virogg.hbasecop.bridge.wire}).
 *
 * <p>Instances are not thread-safe — pair each Channel with a single owning thread.
 */
public final class Channel implements AutoCloseable {

  private final Role role;
  private final int maxPayloadSize;
  private final WaitingRingProducer<PayloadMessage> producer;
  private final WaitingRingConsumer<PayloadMessage> consumer;

  private Channel(
      Role role,
      int maxPayloadSize,
      WaitingRingProducer<PayloadMessage> producer,
      WaitingRingConsumer<PayloadMessage> consumer) {
    this.role = role;
    this.maxPayloadSize = maxPayloadSize;
    this.producer = producer;
    this.consumer = consumer;
  }

  /**
   * Opens one endpoint of a shmem ring. The underlying region is auto-created on first open and is
   * shared with the peer endpoint opened against the same {@code filename}/{@code shmName}.
   */
  public static Channel open(Config cfg) {
    validate(cfg);

    int maxPayload = cfg.maxObjectSize() - 4; // 4-byte length prefix per PayloadMessage layout
    if (maxPayload <= 0) {
      throw new IllegalArgumentException(
          "shmem: maxObjectSize=" + cfg.maxObjectSize() + " leaves no room for payload");
    }

    com.jgshmem.config.Config.Ring ring = toUpstreamRing(cfg);
    Builder<PayloadMessage> msgBuilder = () -> new PayloadMessage(maxPayload);

    if (cfg.role() == Role.PRODUCER) {
      WaitingRingProducer<PayloadMessage> p =
          new WaitingRingProducer<>(cfg.capacity(), cfg.maxObjectSize(), msgBuilder, ring);
      return new Channel(Role.PRODUCER, maxPayload, p, null);
    }

    WaitingRingConsumer<PayloadMessage> c =
        new WaitingRingConsumer<>(cfg.capacity(), cfg.maxObjectSize(), msgBuilder, ring);
    return new Channel(Role.CONSUMER, maxPayload, null, c);
  }

  /** Capacity of the underlying ring in slots. */
  public int capacity() {
    return role == Role.PRODUCER ? producer.getCapacity() : consumer.getCapacity();
  }

  /**
   * Publish one frame.
   *
   * @throws IllegalStateException if this Channel is not a producer
   * @throws FrameTooLargeException if {@code frame.length > maxObjectSize - 4}
   * @throws RingFullException if the consumer has not yet caught up
   */
  public void send(byte[] frame) throws ShmemException {
    if (role != Role.PRODUCER) {
      throw new IllegalStateException("shmem: send on " + role + " channel");
    }
    Objects.requireNonNull(frame, "frame");
    if (frame.length > maxPayloadSize) {
      throw new FrameTooLargeException(frame.length, maxPayloadSize);
    }

    PayloadMessage msg = producer.nextToDispatch();
    if (msg == null) {
      throw new RingFullException();
    }
    msg.payloadSize = frame.length;
    msg.payload.clear();
    if (frame.length > 0) {
      msg.payload.put(frame);
    }
    msg.payload.flip();
    producer.flush();
  }

  /**
   * Fetch the next available frame.
   *
   * @return the frame as a fresh byte[] copy, or {@link Optional#empty()} if the ring is empty
   * @throws IllegalStateException if this Channel is not a consumer
   */
  public Optional<byte[]> recv() {
    if (role != Role.CONSUMER) {
      throw new IllegalStateException("shmem: recv on " + role + " channel");
    }
    if (consumer.availableToFetch() <= 0) {
      return Optional.empty();
    }

    PayloadMessage msg = consumer.fetch();
    byte[] out = new byte[msg.payloadSize];
    if (msg.payloadSize > 0) {
      msg.payload.position(0);
      msg.payload.limit(msg.payloadSize);
      msg.payload.get(out);
    }
    consumer.doneFetching();
    return Optional.of(out);
  }

  /**
   * Releases the mmap/shm region for this endpoint. The underlying file or shm object is left in
   * place; the supervisor (T18) decides when to unlink. Idempotent.
   */
  @Override
  public void close() {
    if (producer != null) {
      producer.close(false);
    } else if (consumer != null) {
      consumer.close(false);
    }
  }

  private static void validate(Config cfg) {
    Objects.requireNonNull(cfg, "cfg");
    if (cfg.role() == null) {
      throw new IllegalArgumentException("shmem: role required");
    }
    if (cfg.capacity() <= 0) {
      throw new IllegalArgumentException("shmem: capacity must be > 0, got " + cfg.capacity());
    }
    if (cfg.maxObjectSize() <= 0) {
      throw new IllegalArgumentException(
          "shmem: maxObjectSize must be > 0, got " + cfg.maxObjectSize());
    }

    String backend = cfg.backend() == null ? Config.BACKEND_MMAP : cfg.backend();
    switch (backend) {
      case Config.BACKEND_MMAP:
        if (cfg.filename() == null || cfg.filename().isEmpty()) {
          throw new IllegalArgumentException("shmem: filename required for mmap backend");
        }
        break;
      case Config.BACKEND_POSIX_SHM:
        if (cfg.shmName() == null || cfg.shmName().isEmpty()) {
          throw new IllegalArgumentException("shmem: shmName required for posix_shm backend");
        }
        break;
      default:
        throw new IllegalArgumentException("shmem: unknown backend: " + backend);
    }
  }

  private static com.jgshmem.config.Config.Ring toUpstreamRing(Config cfg) {
    com.jgshmem.config.Config.Ring ring = new com.jgshmem.config.Config.Ring();
    ring.backend = cfg.backend() == null ? Config.BACKEND_MMAP : cfg.backend();
    ring.filename = cfg.filename();
    ring.shmName = cfg.shmName();
    ring.capacity = cfg.capacity();
    ring.maxObjectSize = cfg.maxObjectSize();
    return ring;
  }
}
