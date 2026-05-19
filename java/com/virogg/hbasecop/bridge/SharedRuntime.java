// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.WALObserver;

/**
 * T63 — process-wide refcounted lookup for {@link CoprocessorRuntime}.
 *
 * <p>HBase loads a fresh {@code RegionCoprocessor} instance for each region attached on a
 * RegionServer. Without sharing, every region would spawn its own Go process — for a host serving
 * dozens or hundreds of regions, that is dozens of redundant ELFs, shmem pairs, and watchdog
 * schedulers. {@code SharedRuntime} lets a coprocessor wrapper acquire a runtime by key (typically
 * the coproc-id or class name): the first {@link #acquire} on a key spawns the Go process and
 * stands up the runtime; subsequent acquires bump a refcount and hand back the same instance; the
 * last {@link Handle#release} sends {@code SHUTDOWN} and waits for the process to exit.
 *
 * <p>Thread-safe across acquire/release. The registry is JVM-wide.
 */
public final class SharedRuntime {

  private static final Logger LOG = System.getLogger(SharedRuntime.class.getName());

  private static final Object LOCK = new Object();
  private static final Map<String, Entry> ENTRIES = new HashMap<>();

  private SharedRuntime() {}

  /** Supplier of a {@link CoprocessorRuntime.Config}; only called on first acquire for a key. */
  @FunctionalInterface
  public interface ConfigSupplier {
    CoprocessorRuntime.Config get() throws IOException;
  }

  /**
   * Acquire a shared runtime for {@code key}. The first call spawns the Go process and starts a
   * fresh {@link CoprocessorRuntime}; subsequent calls return a new {@link Handle} bound to the
   * existing runtime and increment its refcount.
   *
   * @throws IOException if this is the first acquire for the key and either the supplier or the
   *     runtime fails to start; in that case the registry stays clean and a retry may succeed.
   */
  public static Handle acquire(String key, ConfigSupplier supplier) throws IOException {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(supplier, "supplier");

    Entry entry;
    boolean created = false;
    synchronized (LOCK) {
      entry = ENTRIES.get(key);
      if (entry == null) {
        CoprocessorRuntime.Config cfg = supplier.get();
        CoprocessorRuntime rt = new CoprocessorRuntime(cfg);
        boolean ok = false;
        try {
          rt.start();
          entry = new Entry(rt);
          ENTRIES.put(key, entry);
          ok = true;
          created = true;
        } finally {
          if (!ok) {
            // start() already cleaned up its own partial state; swallow any close errors so the
            // original IOException from start() reaches the caller unwrapped.
            try {
              rt.close();
            } catch (IOException | InterruptedException ignored) {
              if (ignored instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              }
            }
          }
        }
      }
      entry.refcount++;
    }
    if (created) {
      LOG.log(Level.INFO, "SharedRuntime: spawned runtime for key={0}", key);
    } else {
      LOG.log(
          Level.DEBUG,
          "SharedRuntime: joined existing runtime for key={0} (refcount={1})",
          key,
          entry.refcount);
    }
    return new Handle(key, entry.runtime);
  }

  /** Visible-for-testing: current refcount on a key, or {@code 0} if the key has no entry. */
  static int refcountForTesting(String key) {
    synchronized (LOCK) {
      Entry e = ENTRIES.get(key);
      return e == null ? 0 : e.refcount;
    }
  }

  /** Visible-for-testing: number of distinct live keys in the registry. */
  static int activeKeyCountForTesting() {
    synchronized (LOCK) {
      return ENTRIES.size();
    }
  }

  private static void release(String key) {
    CoprocessorRuntime toStop = null;
    synchronized (LOCK) {
      Entry e = ENTRIES.get(key);
      if (e == null) {
        return;
      }
      e.refcount--;
      if (e.refcount <= 0) {
        ENTRIES.remove(key);
        toStop = e.runtime;
      }
    }
    if (toStop != null) {
      try {
        toStop.stop();
        LOG.log(Level.INFO, "SharedRuntime: stopped runtime for key={0}", key);
      } catch (IOException e) {
        LOG.log(Level.WARNING, "SharedRuntime: stop failed for key={0}", key, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.log(Level.WARNING, "SharedRuntime: stop interrupted for key={0}", key, e);
      }
    }
  }

  private static final class Entry {
    final CoprocessorRuntime runtime;
    int refcount;

    Entry(CoprocessorRuntime runtime) {
      this.runtime = runtime;
    }
  }

  /**
   * A refcount-holding reference to a shared {@link CoprocessorRuntime}. Each handle must be
   * {@linkplain #release() released} (or {@linkplain #close() closed}) exactly once; further calls
   * are no-ops.
   */
  public static final class Handle implements AutoCloseable {

    private final String key;
    private final CoprocessorRuntime runtime;
    private boolean released;

    private Handle(String key, CoprocessorRuntime runtime) {
      this.key = key;
      this.runtime = runtime;
    }

    /**
     * The shared runtime's {@link RegionObserver}, or {@code null} after this handle is released.
     */
    public RegionObserver getRegionObserver() {
      return released ? null : runtime.getRegionObserver();
    }

    /**
     * The shared runtime's {@link MasterObserver}, or {@code null} after this handle is released.
     */
    public MasterObserver getMasterObserver() {
      return released ? null : runtime.getMasterObserver();
    }

    /**
     * The shared runtime's {@link RegionServerObserver}, or {@code null} after this handle is
     * released.
     */
    public RegionServerObserver getRegionServerObserver() {
      return released ? null : runtime.getRegionServerObserver();
    }

    /** The shared runtime's {@link WALObserver}, or {@code null} after this handle is released. */
    public WALObserver getWALObserver() {
      return released ? null : runtime.getWALObserver();
    }

    /**
     * The shared runtime's {@link BulkLoadObserver}, or {@code null} after this handle is released.
     */
    public BulkLoadObserver getBulkLoadObserver() {
      return released ? null : runtime.getBulkLoadObserver();
    }

    /**
     * Decrement the refcount on this handle's runtime. When the last handle for a key releases, the
     * runtime is stopped (SHUTDOWN frame + {@code process.waitFor}). Idempotent: subsequent calls
     * are no-ops.
     */
    public synchronized void release() {
      if (released) {
        return;
      }
      released = true;
      SharedRuntime.release(key);
    }

    @Override
    public void close() {
      release();
    }

    /**
     * Visible-for-testing: direct access to the underlying runtime, for assertions on pid / isAlive
     * / etc. Production callers should go through the {@code getXObserver()} accessors.
     */
    CoprocessorRuntime runtimeForTesting() {
      return runtime;
    }
  }
}
