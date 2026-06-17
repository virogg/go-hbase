// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.entrypoint;

import com.google.protobuf.Service;
import com.virogg.hbasecop.bridge.CoprocessorRuntime;
import com.virogg.hbasecop.bridge.SharedRuntime;
import com.virogg.hbasecop.bridge.config.PolicyConfig;
import com.virogg.hbasecop.bridge.endpoint.EndpointInvoker;
import com.virogg.hbasecop.bridge.endpoint.GoEndpointServiceImpl;
import com.virogg.hbasecop.bridge.supervisor.ManifestBinaryDescriptor;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.jar.Manifest;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;

/**
 * Shared wiring for the stock {@code Generic*Observer} entrypoints: it builds a {@link
 * CoprocessorRuntime.Config} from the host {@link Configuration} (ring sizes and timeouts default
 * to the documented values) and acquires a {@link SharedRuntime} keyed on the coproc-jar's
 * coproc-id, so authors deploy a Go observer without writing any Java.
 */
final class GenericCoprocessor {

  static final String KEY_RING_CAPACITY = "hbasecop.ring.capacity";
  static final String KEY_RING_MAX_OBJECT_SIZE = "hbasecop.ring.max-object-size";
  static final String KEY_GRACEFUL_SHUTDOWN = "hbasecop.shutdown.graceful-timeout";

  static final int DEFAULT_RING_CAPACITY = 16;
  static final int DEFAULT_RING_MAX_OBJECT_SIZE = 1 << 20; // 1 MiB
  static final Duration DEFAULT_HOOK_TIMEOUT = Duration.ofSeconds(5);
  static final Duration DEFAULT_GRACEFUL_SHUTDOWN = Duration.ofSeconds(2);

  private static final String ELF_RESOURCE_PATH = "bin/linux-amd64/hbasecop-runtime";

  private GenericCoprocessor() {}

  /**
   * The endpoint services the stock entrypoints expose via {@code getServices()}: a single generic
   * {@link GoEndpointServiceImpl} that forwards each client call onto the shared runtime via {@code
   * handleSupplier}. The supplier is read at invoke time (not registration time), so {@code
   * getServices()} works whether or not {@code start()} has run yet; invoking before start (handle
   * still {@code null}) fails cleanly rather than throwing NPE.
   */
  static Iterable<Service> endpointServices(Supplier<SharedRuntime.Handle> handleSupplier) {
    EndpointInvoker invoker =
        invoke -> {
          SharedRuntime.Handle h = handleSupplier.get();
          if (h == null) {
            throw new IOException("hbasecop: endpoint invoked before coprocessor start");
          }
          return h.invokeEndpoint(invoke);
        };
    return Collections.singletonList(new GoEndpointServiceImpl(invoker));
  }

  /** Acquires the shared runtime for key, spawning the Go process on the first acquire. */
  static SharedRuntime.Handle acquire(String key, CoprocessorEnvironment env) throws IOException {
    return SharedRuntime.acquire(
        key,
        () -> {
          Path tmpDir = Files.createTempDirectory("hbasecop-");
          CoprocessorRuntime.Config cfg = buildConfig(env.getConfiguration(), tmpDir);
          return SharedRuntime.Spec.of(cfg, () -> cleanupTmpDir(tmpDir));
        });
  }

  /** Builds the runtime config, reading ring/timeout overrides from conf when present. */
  static CoprocessorRuntime.Config buildConfig(Configuration conf, Path tmpDir) {
    int capacity =
        conf != null
            ? conf.getInt(KEY_RING_CAPACITY, DEFAULT_RING_CAPACITY)
            : DEFAULT_RING_CAPACITY;
    int maxObject =
        conf != null
            ? conf.getInt(KEY_RING_MAX_OBJECT_SIZE, DEFAULT_RING_MAX_OBJECT_SIZE)
            : DEFAULT_RING_MAX_OBJECT_SIZE;
    return CoprocessorRuntime.Config.builder()
        .javaToGoFile(tmpDir.resolve("in.mmap"))
        .goToJavaFile(tmpDir.resolve("out.mmap"))
        .ringCapacity(capacity)
        .ringMaxObjectSize(maxObject)
        .hookTimeout(duration(conf, PolicyConfig.KEY_TIMEOUT_DEFAULT, DEFAULT_HOOK_TIMEOUT))
        .gracefulShutdownTimeout(duration(conf, KEY_GRACEFUL_SHUTDOWN, DEFAULT_GRACEFUL_SHUTDOWN))
        .configuration(conf)
        .build();
  }

  /**
   * The SharedRuntime key: the coproc-id from the coproc-jar manifest, or fallback (the entrypoint
   * class name) when unset. Keying on coproc-id keeps distinct coproc-jars on one RegionServer from
   * colliding on a single shared Go process.
   */
  static String sharedKey(String fallback) {
    String id = coprocIdFromManifest();
    return id != null ? id : fallback;
  }

  private static String coprocIdFromManifest() {
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) {
        cl = GenericCoprocessor.class.getClassLoader();
      }
      URL res = cl.getResource(ELF_RESOURCE_PATH);
      if (res == null) {
        return null;
      }
      URLConnection conn = res.openConnection();
      if (!(conn instanceof JarURLConnection)) {
        return null;
      }
      Manifest mf = ((JarURLConnection) conn).getManifest();
      if (mf == null) {
        return null;
      }
      ManifestBinaryDescriptor d = ManifestBinaryDescriptor.fromAttributes(mf.getMainAttributes());
      return d == null ? null : d.coprocId();
    } catch (IOException e) {
      return null;
    }
  }

  private static Duration duration(Configuration conf, String key, Duration dflt) {
    if (conf == null || conf.get(key) == null) {
      return dflt;
    }
    return Duration.ofMillis(conf.getTimeDuration(key, dflt.toMillis(), TimeUnit.MILLISECONDS));
  }

  static void cleanupTmpDir(Path tmpDir) {
    try (Stream<Path> walk = Files.walk(tmpDir)) {
      walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException ignored) {
                  // best effort
                }
              });
    } catch (IOException ignored) {
      // best effort
    }
  }
}
