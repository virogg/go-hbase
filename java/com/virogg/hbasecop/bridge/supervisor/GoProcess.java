// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import com.virogg.hbasecop.bridge.shmem.Channel;
import com.virogg.hbasecop.bridge.shmem.ShmemException;
import com.virogg.hbasecop.bridge.wire.Encoder;
import com.virogg.hbasecop.bridge.wire.FrameType;
import com.virogg.hbasecop.bridge.wire.Message;
import com.virogg.hbasecop.bridge.wire.WireException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class GoProcess implements AutoCloseable {

  private static final Logger LOG = System.getLogger(GoProcess.class.getName());

  private static final Set<PosixFilePermission> EXEC_PERMS =
      PosixFilePermissions.fromString("rwx------");

  private final GoProcessConfig cfg;
  private final Channel commandChannel;

  private Process process;
  private Path extractedBinary;
  private Thread stdoutPump;
  private Thread stderrPump;

  public GoProcess(GoProcessConfig cfg, Channel commandChannel) {
    this.cfg = Objects.requireNonNull(cfg, "cfg");
    this.commandChannel = Objects.requireNonNull(commandChannel, "commandChannel");
  }

  public synchronized void start() throws IOException {
    if (process != null) {
      throw new IllegalStateException("GoProcess already started");
    }

    extractedBinary = extractBinary(cfg.binaryResourcePath());
    Files.setPosixFilePermissions(extractedBinary, EXEC_PERMS);

    ProcessBuilder pb = new ProcessBuilder(extractedBinary.toString());
    pb.environment().put("HBASECOP_SHMEM_IN_PATH", cfg.javaToGoFile().toString());
    pb.environment().put("HBASECOP_SHMEM_OUT_PATH", cfg.goToJavaFile().toString());
    pb.environment().put("HBASECOP_RING_CAPACITY", Integer.toString(cfg.capacity()));
    pb.environment().put("HBASECOP_RING_MAX_OBJECT_SIZE", Integer.toString(cfg.maxObjectSize()));
    pb.environment().put("HBASECOP_HEARTBEAT_MS", Long.toString(cfg.heartbeatPeriodMs()));
    pb.environment().putAll(cfg.extraEnv());
    pb.redirectErrorStream(false);

    process = pb.start();
    stdoutPump = startPump(process.getInputStream(), Level.INFO, "go-stdout");
    stderrPump = startPump(process.getErrorStream(), Level.INFO, "go-stderr");

    LOG.log(Level.INFO, "GoProcess started: pid={0} binary={1}", process.pid(), extractedBinary);
  }

  public boolean isAlive() {
    return process != null && process.isAlive();
  }

  public long pid() {
    return process == null ? -1L : process.pid();
  }

  public int exitCode() {
    if (process == null) {
      throw new IllegalStateException("GoProcess never started");
    }
    return process.exitValue();
  }

  public synchronized void stop() throws IOException, InterruptedException {
    if (process == null) {
      return;
    }

    if (process.isAlive()) {
      try {
        commandChannel.send(buildShutdownFrame());
      } catch (ShmemException | WireException e) {
        LOG.log(Level.WARNING, "GoProcess.stop: SHUTDOWN send failed, will force-kill", e);
      }

      long timeoutMs = cfg.gracefulShutdownTimeout().toMillis();
      if (!process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
        LOG.log(
            Level.WARNING,
            "GoProcess pid={0} did not exit within {1}ms; destroying forcibly",
            process.pid(),
            timeoutMs);
        process.destroyForcibly();
        process.waitFor();
      }
    }

    joinQuietly(stdoutPump);
    joinQuietly(stderrPump);
    stdoutPump = null;
    stderrPump = null;

    if (extractedBinary != null) {
      try {
        Files.deleteIfExists(extractedBinary);
      } catch (IOException e) {
        LOG.log(Level.WARNING, "GoProcess: failed to delete {0}", extractedBinary, e);
      }
      extractedBinary = null;
    }
  }

  public synchronized void destroyForcibly() {
    if (process == null || !process.isAlive()) {
      return;
    }
    long pid = process.pid();
    process.destroyForcibly();
    try {
      process.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    LOG.log(Level.WARNING, "GoProcess pid={0}: SIGKILL delivered, process reaped", pid);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    stop();
  }

  private Path extractBinary(String resourcePath) throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = GoProcess.class.getClassLoader();
    }
    try (InputStream in = cl.getResourceAsStream(resourcePath)) {
      if (in == null) {
        throw new IOException(
            "GoProcess: classpath resource not found: "
                + resourcePath
                + " (run `make go-build-runtime`)");
      }
      Path tmp = Files.createTempFile("hbasecop-runtime-", "");
      Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
      String expected = cfg.expectedBinarySha256();
      if (expected != null) {
        try {
          verifyChecksum(tmp, expected);
        } catch (IOException e) {
          try {
            Files.deleteIfExists(tmp);
          } catch (IOException suppressed) {
            e.addSuppressed(suppressed);
          }
          throw e;
        }
      }
      return tmp;
    }
  }

  static String computeSha256Hex(Path file) throws IOException {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("SHA-256 not available on this JRE", e);
    }
    byte[] buf = new byte[8192];
    try (InputStream in = Files.newInputStream(file)) {
      int n;
      while ((n = in.read(buf)) > 0) {
        md.update(buf, 0, n);
      }
    }
    return toLowerHex(md.digest());
  }

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private static String toLowerHex(byte[] bytes) {
    char[] out = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      int b = bytes[i] & 0xFF;
      out[i * 2] = HEX[b >>> 4];
      out[i * 2 + 1] = HEX[b & 0x0F];
    }
    return new String(out);
  }

  static void verifyChecksum(Path file, String expectedHex) throws IOException {
    String expected = expectedHex.toLowerCase(Locale.ROOT);
    String actual = computeSha256Hex(file);
    if (!expected.equals(actual)) {
      throw new IOException(
          "GoProcess: ELF SHA-256 mismatch for "
              + file
              + " - expected="
              + expected
              + " actual="
              + actual);
    }
  }

  private static byte[] buildShutdownFrame() throws WireException {
    Message m = new Message(FrameType.SHUTDOWN, 0L, 0, (byte) 0, new byte[0]);
    ByteBuffer bb = new Encoder().encode(m);
    byte[] out = new byte[bb.remaining()];
    bb.get(out);
    return out;
  }

  private static Thread startPump(InputStream in, Level level, String name) {
    Thread t =
        new Thread(
            () -> {
              try (BufferedReader r =
                  new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                  LOG.log(level, "[{0}] {1}", name, line);
                }
              } catch (IOException e) {
              }
            },
            "GoProcess-" + name);
    t.setDaemon(true);
    t.start();
    return t;
  }

  private static void joinQuietly(Thread t) throws InterruptedException {
    if (t == null) {
      return;
    }
    t.join(1000);
  }
}
