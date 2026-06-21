// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.shmem;

public final class Config {

  public static final String BACKEND_MMAP = "mmap";

  public static final String BACKEND_POSIX_SHM = "posix_shm";

  private final String backend;
  private final String filename;
  private final String shmName;
  private final int capacity;
  private final int maxObjectSize;
  private final Role role;

  private Config(Builder b) {
    this.backend = b.backend;
    this.filename = b.filename;
    this.shmName = b.shmName;
    this.capacity = b.capacity;
    this.maxObjectSize = b.maxObjectSize;
    this.role = b.role;
  }

  public String backend() {
    return backend;
  }

  public String filename() {
    return filename;
  }

  public String shmName() {
    return shmName;
  }

  public int capacity() {
    return capacity;
  }

  public int maxObjectSize() {
    return maxObjectSize;
  }

  public Role role() {
    return role;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String backend = BACKEND_MMAP;
    private String filename;
    private String shmName;
    private int capacity;
    private int maxObjectSize;
    private Role role;

    public Builder backend(String backend) {
      this.backend = backend;
      return this;
    }

    public Builder filename(String filename) {
      this.filename = filename;
      return this;
    }

    public Builder shmName(String shmName) {
      this.shmName = shmName;
      return this;
    }

    public Builder capacity(int capacity) {
      this.capacity = capacity;
      return this;
    }

    public Builder maxObjectSize(int maxObjectSize) {
      this.maxObjectSize = maxObjectSize;
      return this;
    }

    public Builder role(Role role) {
      this.role = role;
      return this;
    }

    public Config build() {
      return new Config(this);
    }
  }
}
