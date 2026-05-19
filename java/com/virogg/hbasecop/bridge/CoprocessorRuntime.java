// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge;

import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.observer.BulkLoadObserverAdapter;
import com.virogg.hbasecop.bridge.observer.HookDispatcher;
import com.virogg.hbasecop.bridge.observer.MasterObserverAdapter;
import com.virogg.hbasecop.bridge.observer.MuxHookDispatcher;
import com.virogg.hbasecop.bridge.observer.RegionObserverAdapter;
import com.virogg.hbasecop.bridge.observer.RegionServerObserverAdapter;
import com.virogg.hbasecop.bridge.observer.WALObserverAdapter;
import com.virogg.hbasecop.bridge.shmem.Channel;
import com.virogg.hbasecop.bridge.shmem.Role;
import com.virogg.hbasecop.bridge.shmem.ShmemException;
import com.virogg.hbasecop.bridge.supervisor.GoProcess;
import com.virogg.hbasecop.bridge.supervisor.GoProcessConfig;
import com.virogg.hbasecop.bridge.supervisor.HeartbeatWatchdog;
import com.virogg.hbasecop.bridge.supervisor.RestartConfig;
import com.virogg.hbasecop.bridge.supervisor.RestartController;
import com.virogg.hbasecop.bridge.wire.Decoder;
import com.virogg.hbasecop.bridge.wire.Encoder;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.WireException;
import com.virogg.hbasecop.multiplex.GoSideCrashedException;
import com.virogg.hbasecop.multiplex.Multiplexer;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
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

  /** Configuration key: heartbeat period (Hadoop-style duration, e.g. {@code 500ms}). */
  public static final String KEY_HEARTBEAT_PERIOD = "hbasecop.heartbeat.period";

  /** Configuration key: consecutive missed heartbeats before the watchdog fires. */
  public static final String KEY_HEARTBEAT_MISS_THRESHOLD = "hbasecop.heartbeat.miss-threshold";

  /** Default heartbeat period when neither Configuration nor builder pin one. */
  public static final Duration DEFAULT_HEARTBEAT_PERIOD = Duration.ofMillis(500);

  /** Default miss threshold for the watchdog. */
  public static final int DEFAULT_HEARTBEAT_MISS_THRESHOLD = 3;

  /** Configuration key: max consecutive restart failures before declaring the runtime unhealthy. */
  public static final String KEY_RESTART_MAX_FAILS = "hbasecop.restart.max-fails";

  /** Configuration key: probe interval after the runtime is declared unhealthy. */
  public static final String KEY_RESTART_PROBE_INTERVAL = "hbasecop.restart.probe-interval";

  /** Configuration key: initial delay before the first restart attempt. */
  public static final String KEY_RESTART_INITIAL_DELAY = "hbasecop.restart.initial-delay";

  /** Configuration key: cap on the per-attempt restart delay. */
  public static final String KEY_RESTART_MAX_DELAY = "hbasecop.restart.max-delay";

  /** Configuration key: deadline for deferred calls during a paused-by-crash window. */
  public static final String KEY_RESTART_DEADLINE = "hbasecop.restart.deadline";

  /** Default deadline a call issued during a paused-by-crash window waits before failing. */
  public static final Duration DEFAULT_RESTART_DEADLINE = Duration.ofSeconds(3);

  private final Config cfg;

  private Channel javaToGo;
  private Channel goToJava;
  private GoProcess goProcess;
  private Multiplexer mux;
  private ChannelReader reader;
  private Thread readerThread;
  private RegionObserver observer;
  private org.apache.hadoop.hbase.coprocessor.MasterObserver masterObserver;
  private org.apache.hadoop.hbase.coprocessor.RegionServerObserver regionServerObserver;
  private org.apache.hadoop.hbase.coprocessor.WALObserver walObserver;
  private org.apache.hadoop.hbase.coprocessor.BulkLoadObserver bulkLoadObserver;
  private HeartbeatWatchdog watchdog;
  private ScheduledExecutorService watchdogScheduler;
  private ScheduledFuture<?> watchdogTask;
  private RestartController restartController;
  private long effectiveHeartbeatMs;
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

      effectiveHeartbeatMs = resolveHeartbeatPeriodMs(cfg);
      goProcess = buildGoProcess();
      goProcess.start();

      watchdog = maybeBuildWatchdog(cfg, effectiveHeartbeatMs);
      restartController = buildRestartController(cfg);
      if (watchdog != null) {
        watchdogScheduler =
            Executors.newSingleThreadScheduledExecutor(
                r -> {
                  Thread t = new Thread(r, "hbasecop-watchdog");
                  t.setDaemon(true);
                  return t;
                });
        long tickMs = watchdog.period().toMillis();
        watchdogTask =
            watchdogScheduler.scheduleAtFixedRate(
                this::tickSchedulerTask, tickMs, tickMs, TimeUnit.MILLISECONDS);
      }

      Encoder enc = new Encoder();
      long restartDeadlineMs = resolveRestartDeadlineMs(cfg);
      // The shmem Channel is single-producer (see Channel javadoc). Under T63 sharing the same
      // CoprocessorRuntime is fed by hook threads from N regions concurrently, so we serialize
      // every send through this lock — the ring stays effectively SPSC from the producer's view.
      final Object sendLock = new Object();
      final Channel javaToGoRef = javaToGo;
      mux =
          Multiplexer.builder(
                  msg -> {
                    synchronized (sendLock) {
                      sendOnChannel(javaToGoRef, enc, msg);
                    }
                  })
              .restartDeadlineMs(restartDeadlineMs)
              .scheduler(watchdogScheduler)
              .build();

      reader = new ChannelReader(goToJava, new Decoder(), mux, watchdog);
      readerThread = new Thread(reader, "hbasecop-reader");
      readerThread.setDaemon(true);
      readerThread.start();

      HookDispatcher dispatcher = new MuxHookDispatcher(mux);
      com.virogg.hbasecop.bridge.config.PolicyConfig policy = buildPolicyConfig(cfg);
      observer = new RegionObserverAdapter(dispatcher, policy);
      masterObserver = new MasterObserverAdapter(dispatcher, policy);
      regionServerObserver = new RegionServerObserverAdapter(dispatcher, policy);
      walObserver = new WALObserverAdapter(dispatcher, policy);
      bulkLoadObserver = new BulkLoadObserverAdapter(dispatcher, policy);

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

  /**
   * True iff the restart controller has exhausted its consecutive-failure budget and the runtime is
   * in the probing-only {@code UNHEALTHY} state. Hook dispatch can use this to short-circuit calls
   * by policy without waiting on dead transport.
   */
  public boolean isUnhealthy() {
    RestartController c = restartController;
    return c != null && c.isUnhealthy();
  }

  /** The RegionObserver to expose to HBase; null until {@link #start()} succeeds. */
  public RegionObserver getRegionObserver() {
    return observer;
  }

  /** The MasterObserver to expose to HBase (T51); null until {@link #start()} succeeds. */
  public org.apache.hadoop.hbase.coprocessor.MasterObserver getMasterObserver() {
    return masterObserver;
  }

  /** The RegionServerObserver to expose to HBase (T52); null until {@link #start()} succeeds. */
  public org.apache.hadoop.hbase.coprocessor.RegionServerObserver getRegionServerObserver() {
    return regionServerObserver;
  }

  /** The WALObserver to expose to HBase (T53); null until {@link #start()} succeeds. */
  public org.apache.hadoop.hbase.coprocessor.WALObserver getWALObserver() {
    return walObserver;
  }

  /** The BulkLoadObserver to expose to HBase (T54); null until {@link #start()} succeeds. */
  public org.apache.hadoop.hbase.coprocessor.BulkLoadObserver getBulkLoadObserver() {
    return bulkLoadObserver;
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

  /** Visible-for-testing: current Go pid, or {@code -1} if no process is running. */
  synchronized long goProcessPidForTesting() {
    return goProcess == null ? -1L : goProcess.pid();
  }

  /** Visible-for-testing: the active multiplexer, or {@code null} before {@link #start()}. */
  synchronized Multiplexer multiplexerForTesting() {
    return mux;
  }

  /**
   * Visible-for-testing: the active restart controller, or {@code null} before {@link #start()}.
   */
  RestartController restartControllerForTesting() {
    return restartController;
  }

  /**
   * Visible-for-testing: SIGKILLs the underlying Go process to simulate a crash. The watchdog
   * scheduler will detect the dead process on its next tick and notify the restart controller.
   */
  synchronized void crashGoProcessForTesting() {
    if (goProcess != null) {
      goProcess.destroyForcibly();
    }
  }

  private void closeQuietly() {
    if (restartController != null) {
      restartController.stop();
    }
    if (watchdogTask != null) {
      watchdogTask.cancel(false);
      watchdogTask = null;
    }
    if (watchdogScheduler != null) {
      watchdogScheduler.shutdownNow();
      try {
        if (!watchdogScheduler.awaitTermination(1, TimeUnit.SECONDS)) {
          LOG.log(Level.WARNING, "CoprocessorRuntime: watchdog scheduler did not shut down in 1s");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      watchdogScheduler = null;
    }
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
    masterObserver = null;
    regionServerObserver = null;
    walObserver = null;
    bulkLoadObserver = null;
    watchdog = null;
    restartController = null;
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

  private void tickSchedulerTask() {
    HeartbeatWatchdog wd = watchdog;
    if (wd != null) {
      try {
        wd.tick();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "CoprocessorRuntime: watchdog tick threw", e);
      }
    }
    // Detect process-exit independently of heartbeats so {@code exit 1} from the Go side
    // also triggers a restart (the watchdog only catches "hung" — no heartbeats over the
    // miss window — not an outright exit).
    detectExitedGoProcess();
    RestartController c = restartController;
    if (c != null) {
      try {
        c.tick();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "CoprocessorRuntime: restart controller tick threw", e);
      }
    }
  }

  private void detectExitedGoProcess() {
    GoProcess gp;
    RestartController c;
    Multiplexer m;
    long pid;
    synchronized (this) {
      gp = goProcess;
      c = restartController;
      m = mux;
      pid = gp == null ? -1L : gp.pid();
    }
    if (gp != null && !gp.isAlive()) {
      if (m != null) {
        m.pauseInflightFailing(
            new GoSideCrashedException("Go process pid=" + pid + " exited unexpectedly"));
      }
      if (c != null) {
        c.notifyDead();
      }
    }
  }

  /**
   * Watchdog miss action: SIGKILL the hung Go process (so the OS reaps it) and notify the restart
   * controller, which schedules the actual respawn through {@link #attemptRestart()}.
   */
  private void onHung(long elapsedMs) {
    GoProcess gp;
    Multiplexer m;
    synchronized (this) {
      gp = goProcess;
      m = mux;
    }
    long pid = gp == null ? -1L : gp.pid();
    LOG.log(
        Level.WARNING,
        "CoprocessorRuntime: heartbeat watchdog fired (pid={0}, elapsed={1}ms) — SIGKILL",
        pid,
        elapsedMs);
    if (gp != null) {
      gp.destroyForcibly();
    }
    if (m != null) {
      m.pauseInflightFailing(
          new GoSideCrashedException(
              "Go process pid=" + pid + " hung for " + elapsedMs + "ms, SIGKILLed"));
    }
    RestartController c = restartController;
    if (c != null) {
      c.notifyDead();
    }
  }

  /**
   * Attempt one restart cycle: clean up the old Go process and spawn a fresh one against the same
   * shmem rings. Returns {@code true} on success. Called by {@link RestartController} from the
   * watchdog scheduler thread.
   *
   * <p>Inflight requests waiting on the multiplexer are <em>not</em> cancelled here — T35 handles
   * mux teardown on crash.
   */
  private boolean attemptRestart() {
    synchronized (this) {
      if (!started) {
        return false;
      }
      long oldPid = goProcess == null ? -1L : goProcess.pid();
      if (goProcess != null) {
        try {
          goProcess.stop();
        } catch (IOException e) {
          LOG.log(Level.WARNING, "CoprocessorRuntime: old GoProcess.stop failed", e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      goProcess = null;
      try {
        GoProcess fresh = buildGoProcess();
        fresh.start();
        goProcess = fresh;
        // Re-arm the watchdog: a fresh process has not sent any heartbeats yet, but we treat the
        // successful spawn moment as a heartbeat so the watchdog does not immediately re-fire
        // within the first miss-threshold window.
        HeartbeatWatchdog wd = watchdog;
        if (wd != null) {
          wd.recordHeartbeat();
        }
        // Let any deferred (post-crash) calls go through to the new process.
        Multiplexer m = mux;
        if (m != null) {
          m.resume();
        }
        LOG.log(
            Level.INFO,
            "CoprocessorRuntime: restart succeeded (old pid={0}, new pid={1})",
            oldPid,
            fresh.pid());
        return true;
      } catch (IOException | RuntimeException e) {
        LOG.log(Level.WARNING, "CoprocessorRuntime: restart attempt failed", e);
        return false;
      }
    }
  }

  private GoProcess buildGoProcess() {
    GoProcessConfig procCfg =
        GoProcessConfig.builder()
            .binaryResourcePath(cfg.binaryResourcePath())
            .javaToGoFile(cfg.javaToGoFile())
            .goToJavaFile(cfg.goToJavaFile())
            .capacity(cfg.ringCapacity())
            .maxObjectSize(cfg.ringMaxObjectSize())
            .heartbeatPeriodMs(effectiveHeartbeatMs)
            .gracefulShutdownTimeout(cfg.gracefulShutdownTimeout())
            .extraEnv(cfg.extraEnv())
            .build();
    return new GoProcess(procCfg, javaToGo);
  }

  private RestartController buildRestartController(Config cfg) {
    RestartConfig restartCfg = resolveRestartConfig(cfg);
    return new RestartController(
        restartCfg,
        System::currentTimeMillis,
        this::attemptRestart,
        ThreadLocalRandom.current()::nextDouble);
  }

  private static long resolveRestartDeadlineMs(Config cfg) {
    if (cfg.restartDeadline() != null) {
      return cfg.restartDeadline().toMillis();
    }
    Configuration conf = cfg.configuration();
    if (conf == null) {
      return DEFAULT_RESTART_DEADLINE.toMillis();
    }
    return conf.getTimeDuration(
        KEY_RESTART_DEADLINE, DEFAULT_RESTART_DEADLINE.toMillis(), TimeUnit.MILLISECONDS);
  }

  private static RestartConfig resolveRestartConfig(Config cfg) {
    if (cfg.restartConfig() != null) {
      return cfg.restartConfig();
    }
    Configuration conf = cfg.configuration();
    if (conf == null) {
      return RestartConfig.defaults();
    }
    return RestartConfig.builder()
        .initialDelayMs(
            conf.getTimeDuration(
                KEY_RESTART_INITIAL_DELAY,
                RestartConfig.DEFAULT_INITIAL_DELAY_MS,
                TimeUnit.MILLISECONDS))
        .maxDelayMs(
            conf.getTimeDuration(
                KEY_RESTART_MAX_DELAY, RestartConfig.DEFAULT_MAX_DELAY_MS, TimeUnit.MILLISECONDS))
        .maxConsecutiveFails(
            conf.getInt(KEY_RESTART_MAX_FAILS, RestartConfig.DEFAULT_MAX_CONSECUTIVE_FAILS))
        .probeIntervalMs(
            conf.getTimeDuration(
                KEY_RESTART_PROBE_INTERVAL,
                RestartConfig.DEFAULT_PROBE_INTERVAL_MS,
                TimeUnit.MILLISECONDS))
        .build();
  }

  private static long resolveHeartbeatPeriodMs(Config cfg) {
    Configuration conf = cfg.configuration();
    if (conf != null && conf.get(KEY_HEARTBEAT_PERIOD) != null) {
      long ms = conf.getTimeDuration(KEY_HEARTBEAT_PERIOD, 0L, TimeUnit.MILLISECONDS);
      return ms;
    }
    return cfg.heartbeatPeriodMs();
  }

  private HeartbeatWatchdog maybeBuildWatchdog(Config cfg, long effectiveHeartbeatMs) {
    if (effectiveHeartbeatMs <= 0) {
      // Heartbeats explicitly disabled — no watchdog.
      return null;
    }
    Duration period = Duration.ofMillis(effectiveHeartbeatMs);
    Configuration conf = cfg.configuration();
    int threshold =
        conf == null
            ? DEFAULT_HEARTBEAT_MISS_THRESHOLD
            : conf.getInt(KEY_HEARTBEAT_MISS_THRESHOLD, DEFAULT_HEARTBEAT_MISS_THRESHOLD);
    if (threshold < 1) {
      throw new IllegalArgumentException(
          KEY_HEARTBEAT_MISS_THRESHOLD + " must be ≥ 1, got " + threshold);
    }
    return new HeartbeatWatchdog(period, threshold, System::currentTimeMillis, this::onHung);
  }

  private static PolicyConfig buildPolicyConfig(Config cfg) {
    Configuration src = cfg.configuration();
    // Clone via iterator rather than `new Configuration(src)`: HBase wraps the per-region coproc
    // env in a CompoundConfiguration that merges TableDescriptor.setValue keys dynamically inside
    // get(), and the Configuration copy-constructor only clones the base `properties` map — it
    // would silently drop those merged values (per-table policy / timeout overrides).
    Configuration conf = new Configuration(false);
    if (src != null) {
      for (java.util.Map.Entry<String, String> e : src) {
        conf.set(e.getKey(), e.getValue());
      }
    }
    // The configured hookTimeout is a global default — only inject it when the caller's
    // Configuration does not already pin per-hook or global hbasecop.timeout.* keys, so
    // explicit Configuration overrides always win.
    if (conf.get(PolicyConfig.KEY_TIMEOUT_DEFAULT) == null) {
      conf.setTimeDuration(
          PolicyConfig.KEY_TIMEOUT_DEFAULT, cfg.hookTimeout().toNanos(), TimeUnit.NANOSECONDS);
    }
    return new PolicyConfig(conf);
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
    private final HeartbeatWatchdog watchdog;
    private volatile boolean stop;

    ChannelReader(Channel ch, Decoder decoder, Multiplexer mux, HeartbeatWatchdog watchdog) {
      this.ch = ch;
      this.decoder = decoder;
      this.mux = mux;
      this.watchdog = watchdog;
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
            if (watchdog != null) {
              watchdog.recordHeartbeat();
            }
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
    private final Configuration configuration;
    private final RestartConfig restartConfig;
    private final Duration restartDeadline;
    private final Map<String, String> extraEnv;

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
      this.configuration = b.configuration;
      this.restartConfig = b.restartConfig;
      this.restartDeadline = b.restartDeadline;
      this.extraEnv =
          b.extraEnv.isEmpty()
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new LinkedHashMap<>(b.extraEnv));
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

    /**
     * The HBase {@link Configuration} forwarded to {@link PolicyConfig}; may be {@code null} when
     * the runtime is driven from a context that does not own a Configuration (e.g. raw bridge
     * tests). When null, an empty configuration is used and the builder's {@link #hookTimeout()} is
     * treated as the global default.
     */
    public Configuration configuration() {
      return configuration;
    }

    /**
     * Explicit {@link RestartConfig}; {@code null} → derive from {@link #configuration()} or fall
     * back to {@link RestartConfig#defaults()}.
     */
    public RestartConfig restartConfig() {
      return restartConfig;
    }

    /**
     * Deadline a call issued during a paused-by-crash window waits for restart before failing with
     * {@link GoSideCrashedException}. {@code null} → derive from {@link #configuration()} via
     * {@link #KEY_RESTART_DEADLINE} or fall back to {@link #DEFAULT_RESTART_DEADLINE}.
     */
    public Duration restartDeadline() {
      return restartDeadline;
    }

    /**
     * Extra environment variables passed to the spawned Go process on top of the JVM's environment.
     * Useful for forwarding coprocessor-specific tokens (e.g. {@code HBASECOP_FAULT_MODE} from the
     * T36 fault-observer) into the child.
     */
    public Map<String, String> extraEnv() {
      return extraEnv;
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
      private Configuration configuration;
      private RestartConfig restartConfig;
      private Duration restartDeadline;
      private Map<String, String> extraEnv = new LinkedHashMap<>();

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

      /**
       * Supplies the HBase {@link Configuration} that drives per-hook policy + timeout resolution.
       * Optional: when omitted, the runtime falls back to defaults and treats {@link
       * #hookTimeout(Duration)} as the global default timeout.
       */
      public Builder configuration(Configuration c) {
        this.configuration = c;
        return this;
      }

      /**
       * Override the restart controller tunables. When omitted, the runtime derives them from the
       * supplied {@link #configuration(Configuration)} via {@code hbasecop.restart.*} keys, or
       * falls back to {@link RestartConfig#defaults()}.
       */
      public Builder restartConfig(RestartConfig rc) {
        this.restartConfig = rc;
        return this;
      }

      /**
       * Override the deadline a call waits during a paused-by-crash window before failing with
       * {@link GoSideCrashedException}. When omitted, the runtime derives it from {@link
       * #configuration(Configuration)} via {@code hbasecop.restart.deadline}, falling back to
       * {@link #DEFAULT_RESTART_DEADLINE}.
       */
      public Builder restartDeadline(Duration d) {
        this.restartDeadline = d;
        return this;
      }

      /**
       * Replace the extra-env map. {@code null} clears it. The map is defensively copied at {@link
       * #build()}, so post-build mutations of the caller's map do not leak through.
       */
      public Builder extraEnv(Map<String, String> env) {
        this.extraEnv = env == null ? new LinkedHashMap<>() : new LinkedHashMap<>(env);
        return this;
      }

      public Config build() {
        return new Config(this);
      }
    }
  }
}
