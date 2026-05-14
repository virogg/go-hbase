// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable spawn-time configuration for a {@link GoProcess}. Use {@link #builder()} to construct;
 * cross-field validation happens in {@code GoProcess.start()}.
 */
public final class GoProcessConfig {

  private final String binaryResourcePath;
  private final Path javaToGoFile;
  private final Path goToJavaFile;
  private final int capacity;
  private final int maxObjectSize;
  private final long heartbeatPeriodMs;
  private final Duration gracefulShutdownTimeout;
  private final Map<String, String> extraEnv;

  private GoProcessConfig(Builder b) {
    this.binaryResourcePath = b.binaryResourcePath;
    this.javaToGoFile = b.javaToGoFile;
    this.goToJavaFile = b.goToJavaFile;
    this.capacity = b.capacity;
    this.maxObjectSize = b.maxObjectSize;
    this.heartbeatPeriodMs = b.heartbeatPeriodMs;
    this.gracefulShutdownTimeout = b.gracefulShutdownTimeout;
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

  public int capacity() {
    return capacity;
  }

  public int maxObjectSize() {
    return maxObjectSize;
  }

  public long heartbeatPeriodMs() {
    return heartbeatPeriodMs;
  }

  public Duration gracefulShutdownTimeout() {
    return gracefulShutdownTimeout;
  }

  /**
   * Extra environment variables injected into the spawned process on top of the parent's
   * environment. Used by example coprocessors (e.g. fault-observer in T36) to forward
   * coprocessor-specific tokens — like {@code HBASECOP_FAULT_MODE} — without having to bake them
   * into the RegionServer container.
   *
   * @return an unmodifiable view; never {@code null}
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
    private int capacity;
    private int maxObjectSize;
    private long heartbeatPeriodMs;
    private Duration gracefulShutdownTimeout = Duration.ofSeconds(1);
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

    public Builder capacity(int n) {
      this.capacity = n;
      return this;
    }

    public Builder maxObjectSize(int n) {
      this.maxObjectSize = n;
      return this;
    }

    /**
     * Period in milliseconds for outbound heartbeats. {@code 0} → cpruntime default (500ms);
     * negative → disabled.
     */
    public Builder heartbeatPeriodMs(long ms) {
      this.heartbeatPeriodMs = ms;
      return this;
    }

    public Builder gracefulShutdownTimeout(Duration d) {
      this.gracefulShutdownTimeout = d;
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

    public GoProcessConfig build() {
      return new GoProcessConfig(this);
    }
  }
}
