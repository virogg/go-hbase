// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge;

import com.virogg.hbasecop.bridge.config.ConfigPreflight;
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
import com.virogg.hbasecop.bridge.supervisor.ManifestBinaryDescriptor;
import com.virogg.hbasecop.bridge.supervisor.RestartConfig;
import com.virogg.hbasecop.bridge.supervisor.RestartController;
import com.virogg.hbasecop.bridge.wire.Decoder;
import com.virogg.hbasecop.bridge.wire.Encoder;
import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.WireException;
import com.virogg.hbasecop.bridge.wire.pb.EndpointInvoke;
import com.virogg.hbasecop.bridge.wire.pb.EndpointResult;
import com.virogg.hbasecop.multiplex.ChannelClosedException;
import com.virogg.hbasecop.multiplex.GoSideCrashedException;
import com.virogg.hbasecop.multiplex.Multiplexer;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.Manifest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

public final class CoprocessorRuntime implements AutoCloseable {

  private static final Logger LOG = System.getLogger(CoprocessorRuntime.class.getName());

  public static final String KEY_HEARTBEAT_PERIOD = "hbasecop.heartbeat.period";

  public static final String KEY_HEARTBEAT_MISS_THRESHOLD = "hbasecop.heartbeat.miss-threshold";

  public static final Duration DEFAULT_HEARTBEAT_PERIOD = Duration.ofMillis(500);

  public static final int DEFAULT_HEARTBEAT_MISS_THRESHOLD = 3;

  private static final long DEFAULT_CRASH_PROBE_MS = 500L;

  public static final String KEY_RESTART_MAX_FAILS = "hbasecop.restart.max-fails";

  public static final String KEY_RESTART_PROBE_INTERVAL = "hbasecop.restart.probe-interval";

  public static final String KEY_RESTART_INITIAL_DELAY = "hbasecop.restart.initial-delay";

  public static final String KEY_RESTART_MAX_DELAY = "hbasecop.restart.max-delay";

  public static final String KEY_RESTART_DEADLINE = "hbasecop.restart.deadline";

  public static final Duration DEFAULT_RESTART_DEADLINE = Duration.ofSeconds(3);

  private final Config cfg;

  private final com.virogg.hbasecop.multiplex.RegionIdAllocator regionIdAllocator =
      new com.virogg.hbasecop.multiplex.RegionIdAllocator();
  private final com.virogg.hbasecop.bridge.rpc.RegionRegistry regionRegistry =
      new com.virogg.hbasecop.bridge.rpc.RegionRegistry();
  private final com.virogg.hbasecop.bridge.rpc.ScannerRegistry scannerRegistry;
  private final java.util.concurrent.Semaphore endpointAdmission;

  private Channel javaToGo;
  private Channel goToJava;
  private Channel javaToGoBulk;
  private com.virogg.hbasecop.bridge.rpc.ReverseRpcServicer reverseRpcServicer;
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
    this.scannerRegistry =
        new com.virogg.hbasecop.bridge.rpc.ScannerRegistry(
            cfg.maxScannersPerCall(), cfg.scannerIdleLease().toMillis(), System::currentTimeMillis);
    this.endpointAdmission = new java.util.concurrent.Semaphore(cfg.maxConcurrentCalls());
  }

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
      javaToGoBulk =
          Channel.open(
              shmemConfig(
                  cfg.javaToGoBulkFile(),
                  Role.PRODUCER,
                  cfg.bulkRingCapacity(),
                  cfg.bulkRingMaxObjectSize()));

      effectiveHeartbeatMs = resolveHeartbeatPeriodMs(cfg);
      goProcess = buildGoProcess();
      goProcess.start();

      watchdog = maybeBuildWatchdog(cfg, effectiveHeartbeatMs);
      restartController = buildRestartController(cfg);
      long tickMs = effectiveHeartbeatMs > 0 ? effectiveHeartbeatMs : DEFAULT_CRASH_PROBE_MS;
      watchdogScheduler =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "hbasecop-supervisor");
                t.setDaemon(true);
                return t;
              });
      watchdogTask =
          watchdogScheduler.scheduleAtFixedRate(
              this::tickSchedulerTask, tickMs, tickMs, TimeUnit.MILLISECONDS);

      Encoder enc = new Encoder();
      long restartDeadlineMs = resolveRestartDeadlineMs(cfg);
      final Object sendLock = new Object();
      final Channel javaToGoRef = javaToGo;
      final long sendDeadlineMs = restartDeadlineMs;
      mux =
          Multiplexer.builder(
                  msg -> {
                    synchronized (sendLock) {
                      sendOnChannel(javaToGoRef, enc, msg, sendDeadlineMs);
                    }
                  })
              .restartDeadlineMs(restartDeadlineMs)
              .scheduler(watchdogScheduler)
              .build();

      final Encoder bulkEnc = new Encoder();
      final Object bulkSendLock = new Object();
      final Channel javaToGoBulkRef = javaToGoBulk;
      java.util.function.Consumer<Message> bulkReplySink =
          msg -> {
            synchronized (bulkSendLock) {
              try {
                sendOnChannel(javaToGoBulkRef, bulkEnc, msg, sendDeadlineMs);
              } catch (ShmemException | WireException e) {
                LOG.log(Level.WARNING, "CoprocessorRuntime: reverse-RPC reply send failed", e);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.log(
                    Level.WARNING, "CoprocessorRuntime: interrupted sending reverse-RPC reply", e);
              }
            }
          };
      reverseRpcServicer =
          new com.virogg.hbasecop.bridge.rpc.ReverseRpcServicer(
              regionRegistry,
              scannerRegistry,
              bulkReplySink,
              cfg.bulkRingMaxObjectSize(),
              cfg.servicingPoolSize(),
              cfg.servicingQueueDepth(),
              cfg.servicingTimeout(),
              cfg.allowMutate(),
              cfg.maxBytesPerResp(),
              cfg.maxRowsPerNext());

      reader =
          new ChannelReader(goToJava, new Decoder(), mux, watchdog, reverseRpcServicer::accept);
      readerThread = new Thread(reader, "hbasecop-reader");
      readerThread.setDaemon(true);
      readerThread.start();

      HookDispatcher dispatcher = new MuxHookDispatcher(mux);
      com.virogg.hbasecop.bridge.config.PolicyConfig policy = buildPolicyConfig(cfg);
      observer = new RegionObserverAdapter(dispatcher, policy, regionIdAllocator, regionRegistry);
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

  public synchronized boolean isStarted() {
    return started;
  }

  public synchronized boolean isAlive() {
    return goProcess != null && goProcess.isAlive();
  }

  public boolean isUnhealthy() {
    RestartController c = restartController;
    return c != null && c.isUnhealthy();
  }

  public RegionObserver getRegionObserver() {
    return observer;
  }

  public org.apache.hadoop.hbase.coprocessor.MasterObserver getMasterObserver() {
    return masterObserver;
  }

  public org.apache.hadoop.hbase.coprocessor.RegionServerObserver getRegionServerObserver() {
    return regionServerObserver;
  }

  public org.apache.hadoop.hbase.coprocessor.WALObserver getWALObserver() {
    return walObserver;
  }

  public org.apache.hadoop.hbase.coprocessor.BulkLoadObserver getBulkLoadObserver() {
    return bulkLoadObserver;
  }

  public byte[] invokeEndpoint(EndpointInvoke invoke) throws IOException {
    return invokeEndpoint(invoke, 0);
  }

  public int regionIdFor(String encodedRegionName) {
    return regionIdAllocator.idFor(encodedRegionName);
  }

  public byte[] invokeEndpoint(EndpointInvoke invoke, int regionId) throws IOException {
    Multiplexer m = mux;
    if (m == null) {
      throw new IOException("hbasecop: endpoint invoked before runtime start");
    }
    if (!endpointAdmission.tryAcquire()) {
      throw new IOException(
          "hbasecop: endpoint max-concurrent-calls ("
              + cfg.maxConcurrentCalls()
              + ") exceeded; rejecting "
              + invoke.getMethod());
    }
    Message req =
        new Message(FrameType.ENDPOINT_INVOKE, 0L, regionId, (byte) 0, invoke.toByteArray());
    Multiplexer.Call call = null;
    try {
      call = m.callTracked(req);
      CompletableFuture<Message> fut = call.future;

      final Message resp;
      try {
        resp = fut.get(cfg.endpointTimeout().toMillis(), TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        fut.cancel(false);
        m.cancel(call.reqId);
        throw new IOException("hbasecop: endpoint " + invoke.getMethod() + " timed out", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("hbasecop: endpoint " + invoke.getMethod() + " interrupted", e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ChannelClosedException) {
          throw new IOException("hbasecop: channel closed during endpoint call", cause);
        }
        if (cause instanceof IOException) {
          throw (IOException) cause;
        }
        throw new IOException(
            "hbasecop: endpoint " + invoke.getMethod() + " dispatch failed", cause);
      }

      if (resp.type() == FrameType.ERROR) {
        try {
          com.virogg.hbasecop.bridge.wire.pb.Error err =
              com.virogg.hbasecop.bridge.wire.pb.Error.parseFrom(resp.payload());
          throw new IOException(
              "hbasecop: endpoint "
                  + invoke.getMethod()
                  + " returned error (code="
                  + err.getCode()
                  + "): "
                  + err.getMessage());
        } catch (InvalidProtocolBufferException e) {
          throw new IOException("hbasecop: malformed endpoint Error payload", e);
        }
      }
      try {
        return EndpointResult.parseFrom(resp.payload()).getPayload().toByteArray();
      } catch (InvalidProtocolBufferException e) {
        throw new IOException("hbasecop: malformed EndpointResult payload", e);
      }
    } finally {
      if (call != null) {
        int reaped = scannerRegistry.closeForCall(call.reqId);
        if (reaped > 0) {
          LOG.log(
              Level.WARNING,
              "CoprocessorRuntime: reaped {0} scanner(s) left open by endpoint call (req_id={1})",
              reaped,
              call.reqId);
        }
      }
      endpointAdmission.release();
    }
  }

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

  Thread readerThreadForTesting() {
    return readerThread;
  }

  synchronized long goProcessPidForTesting() {
    return goProcess == null ? -1L : goProcess.pid();
  }

  synchronized Multiplexer multiplexerForTesting() {
    return mux;
  }

  RestartController restartControllerForTesting() {
    return restartController;
  }

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
    if (reverseRpcServicer != null) {
      reverseRpcServicer.close();
    }
    reapScanners("runtime teardown");
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
    closeChannelQuietly(javaToGoBulk);

    reader = null;
    readerThread = null;
    reverseRpcServicer = null;
    mux = null;
    goProcess = null;
    goToJava = null;
    javaToGo = null;
    javaToGoBulk = null;
    observer = null;
    masterObserver = null;
    regionServerObserver = null;
    walObserver = null;
    bulkLoadObserver = null;
    watchdog = null;
    restartController = null;
  }

  private void reapScanners(String reason) {
    int n = scannerRegistry.closeAll();
    if (n > 0) {
      LOG.log(Level.INFO, "CoprocessorRuntime: reaped {0} orphaned scanner(s) ({1})", n, reason);
    }
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
    detectExitedGoProcess();
    RestartController c = restartController;
    if (c != null) {
      try {
        c.tick();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "CoprocessorRuntime: restart controller tick threw", e);
      }
    }
    try {
      int evicted = scannerRegistry.evictIdle();
      if (evicted > 0) {
        LOG.log(Level.INFO, "CoprocessorRuntime: evicted {0} idle scanner(s)", evicted);
      }
    } catch (RuntimeException e) {
      LOG.log(Level.WARNING, "CoprocessorRuntime: scanner idle-evict threw", e);
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
      reapScanners("exited pid=" + pid);
      if (c != null) {
        c.notifyDead();
      }
    }
  }

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
        "CoprocessorRuntime: heartbeat watchdog fired (pid={0}, elapsed={1}ms) - SIGKILL",
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
    reapScanners("hung pid=" + pid);
    RestartController c = restartController;
    if (c != null) {
      c.notifyDead();
    }
  }

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
        HeartbeatWatchdog wd = watchdog;
        if (wd != null) {
          wd.recordHeartbeat();
        }
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
    Map<String, String> env = new LinkedHashMap<>(cfg.extraEnv());
    env.put("HBASECOP_SHMEM_BULK_PATH", cfg.javaToGoBulkFile().toString());
    env.put("HBASECOP_BULK_RING_CAPACITY", Integer.toString(cfg.bulkRingCapacity()));
    env.put("HBASECOP_BULK_RING_MAX_OBJECT_SIZE", Integer.toString(cfg.bulkRingMaxObjectSize()));
    env.put("HBASECOP_REVERSE_CALL_TIMEOUT_MS", Long.toString(cfg.endpointTimeout().toMillis()));
    GoProcessConfig procCfg =
        GoProcessConfig.builder()
            .binaryResourcePath(cfg.binaryResourcePath())
            .javaToGoFile(cfg.javaToGoFile())
            .goToJavaFile(cfg.goToJavaFile())
            .capacity(cfg.ringCapacity())
            .maxObjectSize(cfg.ringMaxObjectSize())
            .heartbeatPeriodMs(effectiveHeartbeatMs)
            .gracefulShutdownTimeout(cfg.gracefulShutdownTimeout())
            .expectedBinarySha256(resolveExpectedBinarySha256())
            .extraEnv(env)
            .build();
    return new GoProcess(procCfg, javaToGo);
  }

  private String resolveExpectedBinarySha256() {
    String override = cfg.expectedBinarySha256();
    if (override != null && !override.isEmpty()) {
      return override;
    }
    String resourcePath = cfg.binaryResourcePath();
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) {
        cl = CoprocessorRuntime.class.getClassLoader();
      }
      URL res = cl.getResource(resourcePath);
      if (res != null) {
        URLConnection conn = res.openConnection();
        if (conn instanceof JarURLConnection) {
          Manifest mf = ((JarURLConnection) conn).getManifest();
          if (mf != null) {
            ManifestBinaryDescriptor d =
                ManifestBinaryDescriptor.fromAttributes(mf.getMainAttributes());
            if (d != null && d.binarySha256() != null) {
              return d.binarySha256();
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.log(
          Level.WARNING,
          "CoprocessorRuntime: failed to read coproc-jar manifest for ELF checksum; skipping verification",
          e);
      return null;
    }
    LOG.log(
        Level.WARNING,
        "CoprocessorRuntime: no HbaseCop-Go-Bin-SHA256 manifest attribute for resource {0}; "
            + "ELF checksum verification skipped (expected only for dev/uninstrumented classpaths)",
        resourcePath);
    return null;
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
    Configuration conf = new Configuration(false);
    if (src != null) {
      for (java.util.Map.Entry<String, String> e : src) {
        conf.set(e.getKey(), e.getValue());
      }
    }
    if (conf.get(PolicyConfig.KEY_TIMEOUT_DEFAULT) == null) {
      conf.setTimeDuration(
          PolicyConfig.KEY_TIMEOUT_DEFAULT, cfg.hookTimeout().toNanos(), TimeUnit.NANOSECONDS);
    }
    ConfigPreflight.validate(conf, LOG);
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

  private static void sendOnChannel(Channel ch, Encoder enc, Message msg, long deadlineMs)
      throws ShmemException, WireException, InterruptedException {
    ByteBuffer bb = enc.encode(msg);
    byte[] frame = new byte[bb.remaining()];
    bb.get(frame);
    sendWithDeadline(() -> ch.send(frame), deadlineMs, System::nanoTime);
  }

  @FunctionalInterface
  interface RingSend {
    void send() throws ShmemException;
  }

  static void sendWithDeadline(
      RingSend send, long deadlineMs, java.util.function.LongSupplier nanoClock)
      throws ShmemException, InterruptedException {
    long deadlineNanos =
        nanoClock.getAsLong() + TimeUnit.MILLISECONDS.toNanos(Math.max(1L, deadlineMs));
    while (true) {
      try {
        send.send();
        return;
      } catch (com.virogg.hbasecop.bridge.shmem.RingFullException e) {
        if (Thread.interrupted()) {
          throw new InterruptedException("hbasecop: interrupted while waiting for ring space");
        }
        if (nanoClock.getAsLong() - deadlineNanos >= 0) {
          throw new ShmemException(
              "hbasecop: outbound ring full for >" + deadlineMs + "ms; Go side not draining");
        }
        Thread.onSpinWait();
      }
    }
  }

  @FunctionalInterface
  interface ReverseRpcSink {
    void accept(Message m);
  }

  static void routeFrame(
      Message m, Multiplexer mux, HeartbeatWatchdog watchdog, ReverseRpcSink reverseRpc) {
    switch (m.type()) {
      case RESPONSE:
      case ERROR:
      case ENDPOINT_RESULT:
        if (!mux.deliver(m)) {
          LOG.log(Level.DEBUG, "CoprocessorRuntime: unmatched {0} req_id={1}", m.type(), m.reqId());
        }
        break;
      case HEARTBEAT:
        if (watchdog != null) {
          watchdog.recordHeartbeat();
        }
        break;
      case RPC_REQUEST:
        reverseRpc.accept(m);
        break;
      default:
        LOG.log(Level.WARNING, "CoprocessorRuntime: unexpected frame type {0}", m.type());
    }
  }

  private static final class ChannelReader implements Runnable {
    private final Channel ch;
    private final Decoder decoder;
    private final Multiplexer mux;
    private final HeartbeatWatchdog watchdog;
    private final ReverseRpcSink reverseRpc;
    private volatile boolean stop;

    ChannelReader(
        Channel ch,
        Decoder decoder,
        Multiplexer mux,
        HeartbeatWatchdog watchdog,
        ReverseRpcSink reverseRpc) {
      this.ch = ch;
      this.decoder = decoder;
      this.mux = mux;
      this.watchdog = watchdog;
      this.reverseRpc = reverseRpc;
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
          continue;
        }
        routeFrame(m, mux, watchdog, reverseRpc);
      }
    }
  }

  public static final class Config {

    private final String binaryResourcePath;
    private final String expectedBinarySha256;
    private final Path javaToGoFile;
    private final Path goToJavaFile;
    private final int ringCapacity;
    private final int ringMaxObjectSize;
    private final long heartbeatPeriodMs;
    private final Duration hookTimeout;
    private final Duration endpointTimeout;
    private final Duration gracefulShutdownTimeout;
    private final Configuration configuration;
    private final RestartConfig restartConfig;
    private final Duration restartDeadline;
    private final Map<String, String> extraEnv;
    private final int servicingPoolSize;
    private final int servicingQueueDepth;
    private final Duration servicingTimeout;
    private final int bulkRingCapacity;
    private final int bulkRingMaxObjectSize;
    private final boolean allowMutate;
    private final int maxConcurrentCalls;
    private final int maxScannersPerCall;
    private final int maxBytesPerResp;
    private final int maxRowsPerNext;
    private final Duration scannerIdleLease;

    private Config(Builder b) {
      this.binaryResourcePath = b.binaryResourcePath;
      this.expectedBinarySha256 = b.expectedBinarySha256;
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
      this.endpointTimeout = Objects.requireNonNull(b.endpointTimeout, "endpointTimeout");
      this.gracefulShutdownTimeout =
          Objects.requireNonNull(b.gracefulShutdownTimeout, "gracefulShutdownTimeout");
      this.configuration = b.configuration;
      this.restartConfig = b.restartConfig;
      this.restartDeadline = b.restartDeadline;
      this.extraEnv =
          b.extraEnv.isEmpty()
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new LinkedHashMap<>(b.extraEnv));
      if (b.servicingPoolSize <= 0) {
        throw new IllegalArgumentException("servicingPoolSize must be > 0");
      }
      if (b.servicingQueueDepth <= 0) {
        throw new IllegalArgumentException("servicingQueueDepth must be > 0");
      }
      if (b.bulkRingCapacity <= 0) {
        throw new IllegalArgumentException("bulkRingCapacity must be > 0");
      }
      if (b.bulkRingMaxObjectSize <= 0) {
        throw new IllegalArgumentException("bulkRingMaxObjectSize must be > 0");
      }
      this.servicingPoolSize = b.servicingPoolSize;
      this.servicingQueueDepth = b.servicingQueueDepth;
      this.servicingTimeout = Objects.requireNonNull(b.servicingTimeout, "servicingTimeout");
      this.bulkRingCapacity = b.bulkRingCapacity;
      this.bulkRingMaxObjectSize = b.bulkRingMaxObjectSize;
      this.allowMutate = b.allowMutate;
      if (b.maxConcurrentCalls <= 0) {
        throw new IllegalArgumentException("maxConcurrentCalls must be > 0");
      }
      if (b.maxScannersPerCall <= 0) {
        throw new IllegalArgumentException("maxScannersPerCall must be > 0");
      }
      if (b.maxBytesPerResp <= 0) {
        throw new IllegalArgumentException("maxBytesPerResp must be > 0");
      }
      if (b.maxRowsPerNext <= 0) {
        throw new IllegalArgumentException("maxRowsPerNext must be > 0");
      }
      this.maxConcurrentCalls = b.maxConcurrentCalls;
      this.maxScannersPerCall = b.maxScannersPerCall;
      this.maxBytesPerResp = b.maxBytesPerResp;
      this.maxRowsPerNext = b.maxRowsPerNext;
      this.scannerIdleLease = Objects.requireNonNull(b.scannerIdleLease, "scannerIdleLease");
    }

    public String binaryResourcePath() {
      return binaryResourcePath;
    }

    public String expectedBinarySha256() {
      return expectedBinarySha256;
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

    public Duration endpointTimeout() {
      return endpointTimeout;
    }

    public Duration gracefulShutdownTimeout() {
      return gracefulShutdownTimeout;
    }

    public Configuration configuration() {
      return configuration;
    }

    public RestartConfig restartConfig() {
      return restartConfig;
    }

    public Duration restartDeadline() {
      return restartDeadline;
    }

    public Map<String, String> extraEnv() {
      return extraEnv;
    }

    public int servicingPoolSize() {
      return servicingPoolSize;
    }

    public int servicingQueueDepth() {
      return servicingQueueDepth;
    }

    public Duration servicingTimeout() {
      return servicingTimeout;
    }

    public int bulkRingCapacity() {
      return bulkRingCapacity;
    }

    public int bulkRingMaxObjectSize() {
      return bulkRingMaxObjectSize;
    }

    public boolean allowMutate() {
      return allowMutate;
    }

    public int maxConcurrentCalls() {
      return maxConcurrentCalls;
    }

    public int maxScannersPerCall() {
      return maxScannersPerCall;
    }

    public int maxBytesPerResp() {
      return maxBytesPerResp;
    }

    public int maxRowsPerNext() {
      return maxRowsPerNext;
    }

    public Duration scannerIdleLease() {
      return scannerIdleLease;
    }

    public Path javaToGoBulkFile() {
      return Path.of(javaToGoFile.toString() + ".bulk");
    }

    public static Builder builder() {
      return new Builder();
    }

    public static final class Builder {
      private String binaryResourcePath = "bin/linux-amd64/hbasecop-runtime";
      private String expectedBinarySha256;
      private Path javaToGoFile;
      private Path goToJavaFile;
      private int ringCapacity = 16;
      private int ringMaxObjectSize = 1 << 20; // 1 MiB
      private long heartbeatPeriodMs = 0L;
      private Duration hookTimeout = Duration.ofSeconds(5);
      private Duration endpointTimeout = Duration.ofSeconds(30);
      private Duration gracefulShutdownTimeout = Duration.ofSeconds(2);
      private Configuration configuration;
      private RestartConfig restartConfig;
      private Duration restartDeadline;
      private Map<String, String> extraEnv = new LinkedHashMap<>();
      private int servicingPoolSize = 8;
      private int servicingQueueDepth = 64;
      private Duration servicingTimeout = Duration.ofSeconds(30);
      private int bulkRingCapacity = 16;
      private int bulkRingMaxObjectSize = 1 << 20; // 1 MiB
      private boolean allowMutate = false;
      private int maxConcurrentCalls = 8;
      private int maxScannersPerCall = 16;
      private int maxBytesPerResp = 1 << 20; // 1 MiB
      private int maxRowsPerNext = 1000;
      private Duration scannerIdleLease = Duration.ofMinutes(2);

      public Builder binaryResourcePath(String s) {
        this.binaryResourcePath = s;
        return this;
      }

      public Builder expectedBinarySha256(String hex) {
        this.expectedBinarySha256 = hex;
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

      public Builder endpointTimeout(Duration d) {
        this.endpointTimeout = d;
        return this;
      }

      public Builder gracefulShutdownTimeout(Duration d) {
        this.gracefulShutdownTimeout = d;
        return this;
      }

      public Builder configuration(Configuration c) {
        this.configuration = c;
        return this;
      }

      public Builder restartConfig(RestartConfig rc) {
        this.restartConfig = rc;
        return this;
      }

      public Builder restartDeadline(Duration d) {
        this.restartDeadline = d;
        return this;
      }

      public Builder extraEnv(Map<String, String> env) {
        this.extraEnv = env == null ? new LinkedHashMap<>() : new LinkedHashMap<>(env);
        return this;
      }

      public Builder servicingPoolSize(int n) {
        this.servicingPoolSize = n;
        return this;
      }

      public Builder servicingQueueDepth(int n) {
        this.servicingQueueDepth = n;
        return this;
      }

      public Builder servicingTimeout(Duration d) {
        this.servicingTimeout = d;
        return this;
      }

      public Builder bulkRingCapacity(int n) {
        this.bulkRingCapacity = n;
        return this;
      }

      public Builder bulkRingMaxObjectSize(int n) {
        this.bulkRingMaxObjectSize = n;
        return this;
      }

      public Builder allowMutate(boolean enabled) {
        this.allowMutate = enabled;
        return this;
      }

      public Builder maxConcurrentCalls(int n) {
        this.maxConcurrentCalls = n;
        return this;
      }

      public Builder maxScannersPerCall(int n) {
        this.maxScannersPerCall = n;
        return this;
      }

      public Builder maxBytesPerResp(int n) {
        this.maxBytesPerResp = n;
        return this;
      }

      public Builder maxRowsPerNext(int n) {
        this.maxRowsPerNext = n;
        return this;
      }

      public Builder scannerIdleLease(Duration d) {
        this.scannerIdleLease = d;
        return this;
      }

      public Config build() {
        return new Config(this);
      }
    }
  }
}
