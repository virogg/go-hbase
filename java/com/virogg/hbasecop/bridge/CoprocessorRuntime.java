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

/**
 * In-RegionServer bridge runtime: ties together the {@link GoProcess} supervisor, the shmem {@link
 * Channel} pair, a {@link Multiplexer}-backed {@link HookDispatcher} and a single reader thread
 * draining inbound frames, and finally exposes the {@link RegionObserver} that the host coproc
 * delegates to.
 *
 * <p>One instance owns one Go process and one ring pair; the lifecycle is {@code start()}, use,
 * {@code stop()}. Concrete HBase {@code RegionCoprocessor} classes (e.g. the counter-observer
 * example) delegate their {@code start(env) / stop(env) / getRegionObserver()} hooks to this class.
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

  /**
   * Tick cadence for the supervisor scheduler when heartbeats are disabled. Crash detection and
   * restart still run at this interval so disabling heartbeats never disables auto-restart.
   */
  private static final long DEFAULT_CRASH_PROBE_MS = 500L;

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

  // TE31: shared per-process region_id space + region_id -> live Region map. The
  // RegionObserverAdapter populates both over the region open/close lifecycle;
  // the endpoint path resolves region_id (regionIdFor) to stamp reverse RPCs and
  // the servicing pool resolves the Region (regionRegistry) to execute them.
  private final com.virogg.hbasecop.multiplex.RegionIdAllocator regionIdAllocator =
      new com.virogg.hbasecop.multiplex.RegionIdAllocator();
  private final com.virogg.hbasecop.bridge.rpc.RegionRegistry regionRegistry =
      new com.virogg.hbasecop.bridge.rpc.RegionRegistry();
  // TE33: open server-side scanners, reaped on Go-process crash so no RegionScanner leaks.
  // TE42: bounded per call + idle-lease evicted (constructed from cfg).
  private final com.virogg.hbasecop.bridge.rpc.ScannerRegistry scannerRegistry;

  private Channel javaToGo;
  private Channel goToJava;
  private Channel javaToGoBulk; // TE31: bulk J->G ring carrying reverse-RPC replies
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
      // TE31: dedicated bulk J->G ring for reverse-RPC replies (Result data), isolated from the
      // control ring so a large reply never queues behind a hook invoke or starves the heartbeat.
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
      // Scheduler drives crash detection (detectExitedGoProcess) and restart
      // (restartController.tick); watchdog tick is optional. Must run even with heartbeats
      // disabled, else a crashed/exited Go process is never detected or restarted (strict
      // hooks fail forever). Created unconditionally; with no watchdog it ticks at a
      // crash-probe cadence. tickSchedulerTask() null-checks the watchdog.
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
      // The shmem Channel is single-producer (see Channel javadoc). Under T63, a shared
      // CoprocessorRuntime is fed by hook threads from N regions concurrently, so we serialize
      // every send through this lock; the ring stays effectively SPSC from the producer's view.
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

      // TE31: reverse-RPC replies ride a SECOND send funnel on the dedicated bulk ring, so a large
      // Result never contends with hook/endpoint/heartbeat traffic under the control sendLock. The
      // bulk ring has one logical producer; the lock guards the concurrent servicing-pool threads.
      final Encoder bulkEnc = new Encoder();
      final Object bulkSendLock = new Object();
      final Channel javaToGoBulkRef = javaToGoBulk;
      java.util.function.Consumer<Message> bulkReplySink =
          msg -> {
            synchronized (bulkSendLock) {
              try {
                sendOnChannel(javaToGoBulkRef, bulkEnc, msg, sendDeadlineMs);
              } catch (ShmemException | WireException e) {
                // The Go caller's reverse RPC will fail on its own deadline; a lost reply must not
                // take down the servicing thread or the bridge.
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

      // TE31: route reverse-RPC requests to the bounded, fail-closed servicing pool (never the
      // reader thread). accept() only submits, so the reader keeps routing hooks/heartbeats.
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
   * in the probing-only {@code UNHEALTHY} state. Hook dispatch can short-circuit calls by policy
   * without waiting on dead transport.
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
   * Forwards a Tier 2 endpoint invocation to the Go side and returns the result payload. Sends an
   * {@code ENDPOINT_INVOKE} frame through the same multiplexer + send lock as hooks, blocks for the
   * matching {@code ENDPOINT_RESULT} (or an {@code ERROR}) up to the configured hook timeout, and
   * unwraps the result. Throws {@link IOException} on timeout, a Go-side error, channel close, or a
   * malformed reply, which {@code GoEndpointServiceImpl} surfaces to the client as an endpoint
   * error.
   */
  public byte[] invokeEndpoint(EndpointInvoke invoke) throws IOException {
    return invokeEndpoint(invoke, 0);
  }

  /**
   * The region id under which the named region was allocated (TE61/TE31), or 0 if it is not
   * currently open here. The endpoint path stamps this onto an {@link EndpointInvoke} so a Go
   * handler's reverse RPCs target the region the client invoked the endpoint on.
   */
  public int regionIdFor(String encodedRegionName) {
    return regionIdAllocator.idFor(encodedRegionName);
  }

  /**
   * TE31 variant: forward an endpoint invocation stamped with {@code regionId}, so a Go endpoint
   * handler's reverse RPCs (reads of region-local data) resolve to the originating region. {@code
   * regionId} 0 means no region scope (e.g. master endpoints).
   */
  public byte[] invokeEndpoint(EndpointInvoke invoke, int regionId) throws IOException {
    Multiplexer m = mux;
    if (m == null) {
      throw new IOException("hbasecop: endpoint invoked before runtime start");
    }
    Message req =
        new Message(FrameType.ENDPOINT_INVOKE, 0L, regionId, (byte) 0, invoke.toByteArray());
    Multiplexer.Call call = m.callTracked(req);
    try {
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
      // TE33: reap any scanners this endpoint call opened but did not close (handler bug, early
      // return, panic, or timeout) so a RegionScanner read point never leaks past the call.
      int reaped = scannerRegistry.closeForCall(call.reqId);
      if (reaped > 0) {
        LOG.log(
            Level.WARNING,
            "CoprocessorRuntime: reaped {0} scanner(s) left open by endpoint call (req_id={1})",
            reaped,
            call.reqId);
      }
    }
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
    // TE31: stop the reverse-RPC servicing pool after the reader has joined (no new requests are
    // handed off) and before the bulk channel is closed (in-flight replies still have a sink).
    if (reverseRpcServicer != null) {
      reverseRpcServicer.close();
    }
    // TE33: close any scanners still open at teardown (clean stop, not a crash) so none leak.
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

  /**
   * TE33: close every open server-side scanner when the Go process dies, so no {@link
   * org.apache.hadoop.hbase.regionserver.RegionScanner} — and the MVCC read point it pins — leaks.
   * Invoked alongside {@code pauseInflightFailing} on the crash path; logs the count so a leak
   * regression is observable in the RegionServer log.
   */
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
    // Detect process-exit independently of heartbeats so `exit 1` from the Go side also
    // triggers a restart. The watchdog only catches "hung" (no heartbeats over the miss
    // window), not an outright exit.
    detectExitedGoProcess();
    RestartController c = restartController;
    if (c != null) {
      try {
        c.tick();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "CoprocessorRuntime: restart controller tick threw", e);
      }
    }
    // TE42: reap scanners idle past their lease (an endpoint that opened a scanner and never
    // closed it, or a reaping-race straggler).
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

  /**
   * Attempt one restart cycle: clean up the old Go process and spawn a fresh one against the same
   * shmem rings. Returns {@code true} on success. Called by {@link RestartController} from the
   * watchdog scheduler thread.
   *
   * <p>Inflight requests waiting on the multiplexer are <em>not</em> cancelled here; T35 handles
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
    // TE31: tell the Go child where the bulk ring is and how it is sized, so it opens the matching
    // consumer endpoint and stands up its reverse in-pump. Carried as extra env on top of the
    // caller's, so no GoProcess change is needed.
    Map<String, String> env = new LinkedHashMap<>(cfg.extraEnv());
    env.put("HBASECOP_SHMEM_BULK_PATH", cfg.javaToGoBulkFile().toString());
    env.put("HBASECOP_BULK_RING_CAPACITY", Integer.toString(cfg.bulkRingCapacity()));
    env.put("HBASECOP_BULK_RING_MAX_OBJECT_SIZE", Integer.toString(cfg.bulkRingMaxObjectSize()));
    GoProcessConfig procCfg =
        GoProcessConfig.builder()
            .binaryResourcePath(cfg.binaryResourcePath())
            .javaToGoFile(cfg.javaToGoFile())
            .goToJavaFile(cfg.goToJavaFile())
            .capacity(cfg.ringCapacity())
            .maxObjectSize(cfg.ringMaxObjectSize())
            .heartbeatPeriodMs(effectiveHeartbeatMs)
            .gracefulShutdownTimeout(cfg.gracefulShutdownTimeout())
            // T71/CP-ε3: pass the manifest's HbaseCop-Go-Bin-SHA256 so GoProcess validates the
            // extracted ELF before exec and fails closed on a corrupt/wrong-arch/tampered binary.
            // Without this, the checksum hbasecop-build writes is never verified at runtime.
            .expectedBinarySha256(resolveExpectedBinarySha256())
            .extraEnv(env)
            .build();
    return new GoProcess(procCfg, javaToGo);
  }

  /**
   * Resolve the expected SHA-256 of the embedded Go ELF. Precedence: explicit override on the
   * {@link Config}, else the {@code HbaseCop-Go-Bin-SHA256} attribute from the manifest of the
   * <em>same</em> jar that provides {@link Config#binaryResourcePath()} (binds the digest to the
   * same artifact as the ELF). Returns {@code null} (checksum skipped, with a WARN) only for
   * dev/uninstrumented classpaths (e.g. {@code target/classes}) that carry no HbaseCop manifest
   * attributes; production coproc-jars from {@code hbasecop-build} always carry the attribute.
   */
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
      // Heartbeats explicitly disabled: no watchdog.
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
    // get(); the copy-constructor only clones the base `properties` map, so it would silently drop
    // those merged values (per-table policy and timeout overrides).
    Configuration conf = new Configuration(false);
    if (src != null) {
      for (java.util.Map.Entry<String, String> e : src) {
        conf.set(e.getKey(), e.getValue());
      }
    }
    // The configured hookTimeout is a global default: only inject it when the caller's
    // Configuration does not already pin per-hook or global hbasecop.timeout.* keys, so
    // explicit overrides always win.
    if (conf.get(PolicyConfig.KEY_TIMEOUT_DEFAULT) == null) {
      conf.setTimeDuration(
          PolicyConfig.KEY_TIMEOUT_DEFAULT, cfg.hookTimeout().toNanos(), TimeUnit.NANOSECONDS);
    }
    // Fail fast at start on a malformed hbasecop.* value (else it would surface
    // lazily on the first hook of a live write path); WARN on unknown keys.
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

  /** A single non-blocking ring write; throws {@link RingFullException} when no slot is free. */
  @FunctionalInterface
  interface RingSend {
    void send() throws ShmemException;
  }

  /**
   * Retry a non-blocking ring write until it succeeds, the deadline passes, or the thread is
   * interrupted. The spin must be bounded: {@link Channel#send} throws {@link RingFullException}
   * immediately (never blocks) and the production caller holds the shared {@code sendLock}, so an
   * unbounded spin against a full/hung/dead Go side would pin this RegionServer RPC-handler thread
   * at 100% CPU and starve every other region's send. On timeout throws a clear {@link
   * ShmemException} so the hook fails by policy instead of hanging forever. {@code nanoClock} is
   * injected for testability.
   */
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

  /**
   * Sink for inbound {@code RPC_REQUEST} frames: a Go-initiated reverse RPC (Tier 2). The bounded
   * servicing pool that executes scan/get/mutate against the region lands in TE31; in E1 this is a
   * stub. Correlation is by the wire-header {@code req_id} carried on the {@link Message}; the
   * router does not unmarshal the protobuf payload.
   */
  @FunctionalInterface
  interface ReverseRpcSink {
    void accept(Message m);
  }

  /**
   * Routes one decoded Go→Java frame. Package-private and static so it can be unit-tested by
   * injecting Messages without standing up a Channel/Thread.
   */
  static void routeFrame(
      Message m, Multiplexer mux, HeartbeatWatchdog watchdog, ReverseRpcSink reverseRpc) {
    switch (m.type()) {
      case RESPONSE:
      case ERROR:
      case ENDPOINT_RESULT:
        // All carry the req_id of a pending mux call: a hook RESPONSE, an
        // ENDPOINT_RESULT for an endpoint invoke, or an ERROR for either. The
        // Go side picks ERROR when the call fails; the waiting caller
        // (MuxHookDispatcher / invokeEndpoint) inspects FrameType.
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
        // Go-initiated reverse RPC (Tier 2); hand to the stub sink keyed by
        // req_id. No PB decode here.
        reverseRpc.accept(m);
        break;
      default:
        LOG.log(Level.WARNING, "CoprocessorRuntime: unexpected frame type {0}", m.type());
    }
  }

  /** Reader thread: drains the Go→Java ring and routes frames to the multiplexer. */
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
          // Partial frame: Decoder will resume on next chunk; here we treat as "wait".
          continue;
        }
        routeFrame(m, mux, watchdog, reverseRpc);
      }
    }
  }

  /** Immutable runtime configuration. Use {@link #builder()} to construct. */
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
    // TE31 reverse-RPC servicing pool + bulk ring sizing.
    private final int servicingPoolSize;
    private final int servicingQueueDepth;
    private final Duration servicingTimeout;
    private final int bulkRingCapacity;
    private final int bulkRingMaxObjectSize;
    // TE41: gate for reverse MUTATE (endpoint writes); off by default.
    private final boolean allowMutate;
    // TE42 endpoint limits + admission.
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

    /**
     * Optional explicit override for the expected ELF SHA-256. When unset, the runtime resolves it
     * from the coproc-jar manifest ({@code HbaseCop-Go-Bin-SHA256}).
     */
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

    /** Wall-clock bound on a Tier 2 endpoint call awaiting its EndpointResult. */
    public Duration endpointTimeout() {
      return endpointTimeout;
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
     * Explicit {@link RestartConfig}; {@code null} means derive from {@link #configuration()} or
     * fall back to {@link RestartConfig#defaults()}.
     */
    public RestartConfig restartConfig() {
      return restartConfig;
    }

    /**
     * Deadline a call issued during a paused-by-crash window waits for restart before failing with
     * {@link GoSideCrashedException}. {@code null} means derive from {@link #configuration()} via
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

    /** TE31: max concurrent reverse-RPC servicing threads. */
    public int servicingPoolSize() {
      return servicingPoolSize;
    }

    /** TE31: bounded reverse-RPC backlog before requests fail closed. */
    public int servicingQueueDepth() {
      return servicingQueueDepth;
    }

    /** TE31: how long teardown waits for in-flight reverse-RPC tasks to drain. */
    public Duration servicingTimeout() {
      return servicingTimeout;
    }

    /** TE31: slot count of the dedicated bulk Java->Go ring carrying reverse-RPC replies. */
    public int bulkRingCapacity() {
      return bulkRingCapacity;
    }

    /** TE31: slot byte size of the bulk Java->Go ring. */
    public int bulkRingMaxObjectSize() {
      return bulkRingMaxObjectSize;
    }

    /** TE41: whether reverse MUTATE (endpoint writes) is enabled; off by default. */
    public boolean allowMutate() {
      return allowMutate;
    }

    /** TE42: max concurrent client endpoint calls (bounds blocked RS handler threads). */
    public int maxConcurrentCalls() {
      return maxConcurrentCalls;
    }

    /** TE42: max open server-side scanners per endpoint call. */
    public int maxScannersPerCall() {
      return maxScannersPerCall;
    }

    /** TE42: max bytes in a single SCAN_NEXT reply (clamped to the bulk-ring slot). */
    public int maxBytesPerResp() {
      return maxBytesPerResp;
    }

    /** TE42: max rows in a single SCAN_NEXT batch. */
    public int maxRowsPerNext() {
      return maxRowsPerNext;
    }

    /** TE42: idle scanner lease before reaping. */
    public Duration scannerIdleLease() {
      return scannerIdleLease;
    }

    /** TE31: the bulk ring's segment path, derived next to the control Java->Go segment. */
    public Path javaToGoBulkFile() {
      return Path.of(javaToGoFile.toString() + ".bulk");
    }

    public static Builder builder() {
      return new Builder();
    }

    /** Mutable builder; not thread-safe. */
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

      /**
       * Override the expected ELF SHA-256 (64 lower-case hex chars). Normally left unset so the
       * runtime reads it from the coproc-jar manifest; useful for tests and embedders that supply
       * the digest out of band.
       */
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

      /** TE31: max concurrent reverse-RPC servicing threads (default 8). */
      public Builder servicingPoolSize(int n) {
        this.servicingPoolSize = n;
        return this;
      }

      /** TE31: bounded reverse-RPC backlog before requests fail closed (default 64). */
      public Builder servicingQueueDepth(int n) {
        this.servicingQueueDepth = n;
        return this;
      }

      /** TE31: teardown drain timeout for in-flight reverse-RPC tasks (default 30s). */
      public Builder servicingTimeout(Duration d) {
        this.servicingTimeout = d;
        return this;
      }

      /** TE31: bulk ring slot count (default 16). */
      public Builder bulkRingCapacity(int n) {
        this.bulkRingCapacity = n;
        return this;
      }

      /** TE31: bulk ring slot byte size (default 1 MiB). */
      public Builder bulkRingMaxObjectSize(int n) {
        this.bulkRingMaxObjectSize = n;
        return this;
      }

      /** TE41: enable reverse MUTATE (endpoint writes); off by default. */
      public Builder allowMutate(boolean enabled) {
        this.allowMutate = enabled;
        return this;
      }

      /** TE42: max concurrent client endpoint calls (default 8). */
      public Builder maxConcurrentCalls(int n) {
        this.maxConcurrentCalls = n;
        return this;
      }

      /** TE42: max open scanners per endpoint call (default 16). */
      public Builder maxScannersPerCall(int n) {
        this.maxScannersPerCall = n;
        return this;
      }

      /** TE42: max bytes per SCAN_NEXT reply (default 1 MiB, clamped to slot). */
      public Builder maxBytesPerResp(int n) {
        this.maxBytesPerResp = n;
        return this;
      }

      /** TE42: max rows per SCAN_NEXT batch (default 1000). */
      public Builder maxRowsPerNext(int n) {
        this.maxRowsPerNext = n;
        return this;
      }

      /** TE42: idle scanner lease before reaping (default 2m). */
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
