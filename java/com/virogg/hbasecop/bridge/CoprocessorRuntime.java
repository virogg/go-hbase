// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge;

import com.virogg.hbasecop.bridge.observer.HookDispatcher;
import com.virogg.hbasecop.bridge.observer.MuxHookDispatcher;
import com.virogg.hbasecop.bridge.observer.RegionObserverAdapter;
import com.virogg.hbasecop.bridge.shmem.Channel;
import com.virogg.hbasecop.bridge.shmem.Role;
import com.virogg.hbasecop.bridge.shmem.ShmemException;
import com.virogg.hbasecop.bridge.supervisor.GoProcess;
import com.virogg.hbasecop.bridge.supervisor.GoProcessConfig;
import com.virogg.hbasecop.bridge.wire.Decoder;
import com.virogg.hbasecop.bridge.wire.Encoder;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.WireException;
import com.virogg.hbasecop.multiplex.Multiplexer;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

/**
 * In-RegionServer bridge runtime: ties together the {@link GoProcess} supervisor, the shmem {@link
 * Channel} pair, a {@link Multiplexer}-backed {@link HookDispatcher} and a single reader thread
 * draining inbound frames, and finally exposes the {@link RegionObserver} that the host coproc
 * delegates to.
 *
 * <p>One instance owns one Go process and one ring pair; the lifecycle is {@code start() → use →
 * stop()}. Concrete HBase {@code RegionCoprocessor} classes (e.g. the counter-observer example)
 * delegate their {@code start(env) / stop(env) / getRegionObserver()} hooks to this class.
 *
 * <p>Instances are not thread-safe across {@link #start()} / {@link #stop()}, but {@link
 * #getRegionObserver()} may be called from any thread once {@link #start()} has returned.
 */
public final class CoprocessorRuntime implements AutoCloseable {

  private static final Logger LOG = System.getLogger(CoprocessorRuntime.class.getName());

  private final Config cfg;

  private Channel javaToGo;
  private Channel goToJava;
  private GoProcess goProcess;
  private Multiplexer mux;
  private ChannelReader reader;
  private Thread readerThread;
  private RegionObserver observer;
  private boolean started;

  public CoprocessorRuntime(Config cfg) {
    this.cfg = Objects.requireNonNull(cfg, "cfg");
  }

  /**
   * Opens both rings, spawns the Go runtime, stands up the multiplexer + reader thread, and builds
   * the {@link RegionObserverAdapter}. After this returns successfully, {@link
   * #getRegionObserver()} is wired and HBase may invoke it.
   */
  public synchronized void start() throws IOException {
    if (started) {
      throw new IllegalStateException("CoprocessorRuntime already started");
    }

    boolean ok = false;
    try {
      javaToGo =
          Channel.open(
              shmemConfig(
                  cfg.javaToGoFile(), Role.PRODUCER, cfg.ringCapacity(), cfg.ringMaxObjectSize()));
      goToJava =
          Channel.open(
              shmemConfig(
                  cfg.goToJavaFile(), Role.CONSUMER, cfg.ringCapacity(), cfg.ringMaxObjectSize()));

      GoProcessConfig procCfg =
          GoProcessConfig.builder()
              .binaryResourcePath(cfg.binaryResourcePath())
              .javaToGoFile(cfg.javaToGoFile())
              .goToJavaFile(cfg.goToJavaFile())
              .capacity(cfg.ringCapacity())
              .maxObjectSize(cfg.ringMaxObjectSize())
              .heartbeatPeriodMs(cfg.heartbeatPeriodMs())
              .gracefulShutdownTimeout(cfg.gracefulShutdownTimeout())
              .build();
      goProcess = new GoProcess(procCfg, javaToGo);
      goProcess.start();

      Encoder enc = new Encoder();
      mux = new Multiplexer(msg -> sendOnChannel(javaToGo, enc, msg));

      reader = new ChannelReader(goToJava, new Decoder(), mux);
      readerThread = new Thread(reader, "hbasecop-reader");
      readerThread.setDaemon(true);
      readerThread.start();

      HookDispatcher dispatcher = new MuxHookDispatcher(mux);
      observer = new RegionObserverAdapter(dispatcher, cfg.hookTimeout());

      started = true;
      ok = true;
      LOG.log(Level.INFO, "CoprocessorRuntime started: pid={0}", goProcess.pid());
    } finally {
      if (!ok) {
        closeQuietly();
      }
    }
  }

  /** True between a successful {@link #start()} and {@link #stop()}. */
  public synchronized boolean isStarted() {
    return started;
  }

  /** True iff the supervised Go process is alive. */
  public synchronized boolean isAlive() {
    return goProcess != null && goProcess.isAlive();
  }

  /** The RegionObserver to expose to HBase; null until {@link #start()} succeeds. */
  public RegionObserver getRegionObserver() {
    return observer;
  }

  /**
   * Tears down the runtime in reverse order: stop reader thread, close mux (failing inflight), send
   * SHUTDOWN to Go via {@link GoProcess#stop()}, close shmem channels. Idempotent.
   */
  public synchronized void stop() throws IOException, InterruptedException {
    if (!started) {
      return;
    }
    started = false;
    closeQuietly();
  }

  @Override
  public void close() throws IOException, InterruptedException {
    stop();
  }

  /** Visible-for-testing: lets tests assert the reader has joined after {@link #stop()}. */
  Thread readerThreadForTesting() {
    return readerThread;
  }

  private void closeQuietly() {
    if (reader != null) {
      reader.shutdown();
    }
    if (readerThread != null) {
      try {
        readerThread.join(2_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (mux != null) {
      mux.close();
    }
    if (goProcess != null) {
      try {
        goProcess.stop();
      } catch (IOException | InterruptedException e) {
        LOG.log(Level.WARNING, "CoprocessorRuntime: GoProcess.stop failed", e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
    closeChannelQuietly(goToJava);
    closeChannelQuietly(javaToGo);

    reader = null;
    readerThread = null;
    mux = null;
    goProcess = null;
    goToJava = null;
    javaToGo = null;
    observer = null;
  }

  private static void closeChannelQuietly(Channel ch) {
    if (ch == null) {
      return;
    }
    try {
      ch.close();
    } catch (RuntimeException e) {
      LOG.log(Level.WARNING, "CoprocessorRuntime: channel close failed", e);
    }
  }

  private static com.virogg.hbasecop.bridge.shmem.Config shmemConfig(
      Path file, Role role, int capacity, int maxObjectSize) {
    return com.virogg.hbasecop.bridge.shmem.Config.builder()
        .filename(file.toString())
        .capacity(capacity)
        .maxObjectSize(maxObjectSize)
        .role(role)
        .build();
  }

  private static void sendOnChannel(Channel ch, Encoder enc, Message msg)
      throws ShmemException, WireException {
    ByteBuffer bb = enc.encode(msg);
    byte[] frame = new byte[bb.remaining()];
    bb.get(frame);
    // Backpressure: if the ring is full the Go side hasn't drained yet — spin briefly. Producer
    // contention here is low for prePut (one in-flight call per region thread).
    while (true) {
      try {
        ch.send(frame);
        return;
      } catch (com.virogg.hbasecop.bridge.shmem.RingFullException e) {
        Thread.onSpinWait();
      }
    }
  }

  /** Reader thread: drains the Go→Java ring and routes frames to the multiplexer. */
  private static final class ChannelReader implements Runnable {
    private final Channel ch;
    private final Decoder decoder;
    private final Multiplexer mux;
    private volatile boolean stop;

    ChannelReader(Channel ch, Decoder decoder, Multiplexer mux) {
      this.ch = ch;
      this.decoder = decoder;
      this.mux = mux;
    }

    void shutdown() {
      stop = true;
    }

    @Override
    public void run() {
      while (!stop) {
        Optional<byte[]> raw;
        try {
          raw = ch.recv();
        } catch (RuntimeException e) {
          LOG.log(Level.WARNING, "CoprocessorRuntime: recv failed", e);
          return;
        }
        if (raw.isEmpty()) {
          Thread.onSpinWait();
          continue;
        }
        final Message m;
        try {
          m = decoder.decode(ByteBuffer.wrap(raw.get()));
        } catch (WireException e) {
          LOG.log(Level.WARNING, "CoprocessorRuntime: malformed frame discarded", e);
          continue;
        }
        if (m == null) {
          // Partial frame — Decoder will resume on next chunk; here we treat as "wait".
          continue;
        }
        switch (m.type()) {
          case RESPONSE:
          case ERROR:
            // Both carry the same req_id; the Go side picks ERROR when a hook
            // call fails (e.g. unknown hook, malformed payload, marshal fail).
            // MuxHookDispatcher inspects FrameType and surfaces an IOException.
            if (!mux.deliver(m)) {
              LOG.log(
                  Level.DEBUG, "CoprocessorRuntime: unmatched {0} req_id={1}", m.type(), m.reqId());
            }
            break;
          case HEARTBEAT:
            // T33 will add a watchdog; for now we just acknowledge by ignoring.
            break;
          default:
            LOG.log(Level.WARNING, "CoprocessorRuntime: unexpected frame type {0}", m.type());
        }
      }
    }
  }

  /** Immutable runtime configuration. Use {@link #builder()} to construct. */
  public static final class Config {

    private final String binaryResourcePath;
    private final Path javaToGoFile;
    private final Path goToJavaFile;
    private final int ringCapacity;
    private final int ringMaxObjectSize;
    private final long heartbeatPeriodMs;
    private final Duration hookTimeout;
    private final Duration gracefulShutdownTimeout;

    private Config(Builder b) {
      this.binaryResourcePath = b.binaryResourcePath;
      this.javaToGoFile = Objects.requireNonNull(b.javaToGoFile, "javaToGoFile");
      this.goToJavaFile = Objects.requireNonNull(b.goToJavaFile, "goToJavaFile");
      if (b.ringCapacity <= 0) {
        throw new IllegalArgumentException("ringCapacity must be > 0");
      }
      if (b.ringMaxObjectSize <= 0) {
        throw new IllegalArgumentException("ringMaxObjectSize must be > 0");
      }
      this.ringCapacity = b.ringCapacity;
      this.ringMaxObjectSize = b.ringMaxObjectSize;
      this.heartbeatPeriodMs = b.heartbeatPeriodMs;
      this.hookTimeout = Objects.requireNonNull(b.hookTimeout, "hookTimeout");
      this.gracefulShutdownTimeout =
          Objects.requireNonNull(b.gracefulShutdownTimeout, "gracefulShutdownTimeout");
    }

    public String binaryResourcePath() {
      return binaryResourcePath;
    }

    public Path javaToGoFile() {
      return javaToGoFile;
    }

    public Path goToJavaFile() {
      return goToJavaFile;
    }

    public int ringCapacity() {
      return ringCapacity;
    }

    public int ringMaxObjectSize() {
      return ringMaxObjectSize;
    }

    public long heartbeatPeriodMs() {
      return heartbeatPeriodMs;
    }

    public Duration hookTimeout() {
      return hookTimeout;
    }

    public Duration gracefulShutdownTimeout() {
      return gracefulShutdownTimeout;
    }

    public static Builder builder() {
      return new Builder();
    }

    /** Mutable builder; not thread-safe. */
    public static final class Builder {
      private String binaryResourcePath = "bin/linux-amd64/hbasecop-runtime";
      private Path javaToGoFile;
      private Path goToJavaFile;
      private int ringCapacity = 16;
      private int ringMaxObjectSize = 1 << 20; // 1 MiB
      private long heartbeatPeriodMs = 0L;
      private Duration hookTimeout = Duration.ofSeconds(5);
      private Duration gracefulShutdownTimeout = Duration.ofSeconds(2);

      public Builder binaryResourcePath(String s) {
        this.binaryResourcePath = s;
        return this;
      }

      public Builder javaToGoFile(Path p) {
        this.javaToGoFile = p;
        return this;
      }

      public Builder goToJavaFile(Path p) {
        this.goToJavaFile = p;
        return this;
      }

      public Builder ringCapacity(int n) {
        this.ringCapacity = n;
        return this;
      }

      public Builder ringMaxObjectSize(int n) {
        this.ringMaxObjectSize = n;
        return this;
      }

      public Builder heartbeatPeriodMs(long ms) {
        this.heartbeatPeriodMs = ms;
        return this;
      }

      public Builder hookTimeout(Duration d) {
        this.hookTimeout = d;
        return this;
      }

      public Builder gracefulShutdownTimeout(Duration d) {
        this.gracefulShutdownTimeout = d;
        return this;
      }

      public Config build() {
        return new Config(this);
      }
    }
  }
}
