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

public final class SharedRuntime {

  private static final Logger LOG = System.getLogger(SharedRuntime.class.getName());

  private static final Object LOCK = new Object();
  private static final Map<String, Entry> ENTRIES = new HashMap<>();

  private SharedRuntime() {}

  @FunctionalInterface
  public interface ConfigSupplier {
    Spec get() throws IOException;
  }

  public static final class Spec {
    private final CoprocessorRuntime.Config config;
    private final Runnable onStop;

    private Spec(CoprocessorRuntime.Config config, Runnable onStop) {
      this.config = Objects.requireNonNull(config, "config");
      this.onStop = Objects.requireNonNull(onStop, "onStop");
    }

    public static Spec of(CoprocessorRuntime.Config config) {
      return new Spec(config, () -> {});
    }

    public static Spec of(CoprocessorRuntime.Config config, Runnable onStop) {
      return new Spec(config, onStop);
    }

    public CoprocessorRuntime.Config config() {
      return config;
    }

    public Runnable onStop() {
      return onStop;
    }
  }

  public static Handle acquire(String key, ConfigSupplier supplier) throws IOException {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(supplier, "supplier");

    Entry entry;
    boolean created = false;
    synchronized (LOCK) {
      entry = ENTRIES.get(key);
      if (entry == null) {
        Spec spec = supplier.get();
        CoprocessorRuntime rt = new CoprocessorRuntime(spec.config());
        boolean ok = false;
        try {
          rt.start();
          entry = new Entry(rt, spec.onStop());
          ENTRIES.put(key, entry);
          ok = true;
          created = true;
        } finally {
          if (!ok) {
            try {
              rt.close();
            } catch (IOException | InterruptedException ignored) {
              if (ignored instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              }
            }
            try {
              spec.onStop().run();
            } catch (RuntimeException cleanupErr) {
              LOG.log(
                  Level.WARNING,
                  "SharedRuntime: onStop cleanup after failed start threw for key={0}",
                  key,
                  cleanupErr);
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

  static int refcountForTesting(String key) {
    synchronized (LOCK) {
      Entry e = ENTRIES.get(key);
      return e == null ? 0 : e.refcount;
    }
  }

  static int activeKeyCountForTesting() {
    synchronized (LOCK) {
      return ENTRIES.size();
    }
  }

  private static void release(String key) {
    Entry toStop = null;
    synchronized (LOCK) {
      Entry e = ENTRIES.get(key);
      if (e == null) {
        return;
      }
      e.refcount--;
      if (e.refcount <= 0) {
        ENTRIES.remove(key);
        toStop = e;
      }
    }
    if (toStop != null) {
      try {
        toStop.runtime.stop();
        LOG.log(Level.INFO, "SharedRuntime: stopped runtime for key={0}", key);
      } catch (IOException e) {
        LOG.log(Level.WARNING, "SharedRuntime: stop failed for key={0}", key, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.log(Level.WARNING, "SharedRuntime: stop interrupted for key={0}", key, e);
      }
      try {
        toStop.onStop.run();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "SharedRuntime: onStop cleanup threw for key={0}", key, e);
      }
    }
  }

  private static final class Entry {
    final CoprocessorRuntime runtime;
    final Runnable onStop;
    int refcount;

    Entry(CoprocessorRuntime runtime, Runnable onStop) {
      this.runtime = runtime;
      this.onStop = onStop;
    }
  }

  public static final class Handle implements AutoCloseable {

    private final String key;
    private final CoprocessorRuntime runtime;
    private volatile boolean released;

    private Handle(String key, CoprocessorRuntime runtime) {
      this.key = key;
      this.runtime = runtime;
    }

    public RegionObserver getRegionObserver() {
      return released ? null : runtime.getRegionObserver();
    }

    public MasterObserver getMasterObserver() {
      return released ? null : runtime.getMasterObserver();
    }

    public RegionServerObserver getRegionServerObserver() {
      return released ? null : runtime.getRegionServerObserver();
    }

    public WALObserver getWALObserver() {
      return released ? null : runtime.getWALObserver();
    }

    public BulkLoadObserver getBulkLoadObserver() {
      return released ? null : runtime.getBulkLoadObserver();
    }

    public byte[] invokeEndpoint(com.virogg.hbasecop.bridge.wire.pb.EndpointInvoke invoke)
        throws java.io.IOException {
      if (released) {
        throw new java.io.IOException("hbasecop: endpoint invoked on a released runtime handle");
      }
      return runtime.invokeEndpoint(invoke);
    }

    public byte[] invokeEndpoint(
        com.virogg.hbasecop.bridge.wire.pb.EndpointInvoke invoke, int regionId)
        throws java.io.IOException {
      if (released) {
        throw new java.io.IOException("hbasecop: endpoint invoked on a released runtime handle");
      }
      return runtime.invokeEndpoint(invoke, regionId);
    }

    public int regionId(String encodedRegionName) {
      return released ? 0 : runtime.regionIdFor(encodedRegionName);
    }

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

    CoprocessorRuntime runtimeForTesting() {
      return runtime;
    }
  }
}
