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

final class GenericCoprocessor {

  static final String KEY_RING_CAPACITY = "hbasecop.ring.capacity";
  static final String KEY_RING_MAX_OBJECT_SIZE = "hbasecop.ring.max-object-size";
  static final String KEY_GRACEFUL_SHUTDOWN = "hbasecop.shutdown.graceful-timeout";
  static final String KEY_ENDPOINT_TIMEOUT = "hbasecop.endpoint.timeout";
  static final String KEY_SERVICING_POOL_SIZE = "hbasecop.endpoint.servicing-pool-size";
  static final String KEY_SERVICING_QUEUE_DEPTH = "hbasecop.endpoint.servicing-queue-depth";
  static final String KEY_SERVICING_TIMEOUT = "hbasecop.endpoint.servicing-timeout";
  static final String KEY_BULK_RING_CAPACITY = "hbasecop.endpoint.bulk-ring.capacity";
  static final String KEY_BULK_RING_MAX_OBJECT_SIZE = "hbasecop.endpoint.bulk-ring.max-object-size";
  static final String KEY_ALLOW_MUTATE = "hbasecop.endpoint.allow-mutate";
  static final String KEY_MAX_CONCURRENT_CALLS = "hbasecop.endpoint.max-concurrent-calls";
  static final String KEY_MAX_SCANNERS_PER_CALL = "hbasecop.endpoint.max-scanners-per-call";
  static final String KEY_MAX_BYTES_PER_RESP = "hbasecop.endpoint.max-bytes-per-resp";
  static final String KEY_MAX_ROWS_PER_NEXT = "hbasecop.endpoint.max-rows-per-next";
  static final String KEY_SCANNER_IDLE_LEASE = "hbasecop.endpoint.scanner-idle-lease";

  static final int DEFAULT_RING_CAPACITY = 16;
  static final int DEFAULT_RING_MAX_OBJECT_SIZE = 1 << 20; // 1 MiB
  static final Duration DEFAULT_HOOK_TIMEOUT = Duration.ofSeconds(5);
  static final Duration DEFAULT_ENDPOINT_TIMEOUT = Duration.ofSeconds(30);
  static final Duration DEFAULT_GRACEFUL_SHUTDOWN = Duration.ofSeconds(2);
  static final int DEFAULT_SERVICING_POOL_SIZE = 8;
  static final int DEFAULT_SERVICING_QUEUE_DEPTH = 64;
  static final Duration DEFAULT_SERVICING_TIMEOUT = Duration.ofSeconds(30);
  static final boolean DEFAULT_ALLOW_MUTATE = false;
  static final int DEFAULT_MAX_CONCURRENT_CALLS = 8;
  static final int DEFAULT_MAX_SCANNERS_PER_CALL = 16;
  static final int DEFAULT_MAX_BYTES_PER_RESP = 1 << 20; // 1 MiB
  static final int DEFAULT_MAX_ROWS_PER_NEXT = 1000;
  static final Duration DEFAULT_SCANNER_IDLE_LEASE = Duration.ofMinutes(2);

  private static final String ELF_RESOURCE_PATH = "bin/linux-amd64/hbasecop-runtime";

  private GenericCoprocessor() {}

  static Iterable<Service> endpointServices(Supplier<SharedRuntime.Handle> handleSupplier) {
    return endpointServices(handleSupplier, () -> 0);
  }

  static Iterable<Service> endpointServices(
      Supplier<SharedRuntime.Handle> handleSupplier,
      java.util.function.IntSupplier regionIdSupplier) {
    EndpointInvoker invoker =
        invoke -> {
          SharedRuntime.Handle h = handleSupplier.get();
          if (h == null) {
            throw new IOException("hbasecop: endpoint invoked before coprocessor start");
          }
          return h.invokeEndpoint(invoke, regionIdSupplier.getAsInt());
        };
    return Collections.singletonList(new GoEndpointServiceImpl(invoker));
  }

  static SharedRuntime.Handle acquire(String key, CoprocessorEnvironment env) throws IOException {
    return SharedRuntime.acquire(
        key,
        () -> {
          Path tmpDir = Files.createTempDirectory("hbasecop-");
          CoprocessorRuntime.Config cfg = buildConfig(env.getConfiguration(), tmpDir);
          return SharedRuntime.Spec.of(cfg, () -> cleanupTmpDir(tmpDir));
        });
  }

  static CoprocessorRuntime.Config buildConfig(Configuration conf, Path tmpDir) {
    int capacity =
        conf != null
            ? conf.getInt(KEY_RING_CAPACITY, DEFAULT_RING_CAPACITY)
            : DEFAULT_RING_CAPACITY;
    int maxObject =
        conf != null
            ? conf.getInt(KEY_RING_MAX_OBJECT_SIZE, DEFAULT_RING_MAX_OBJECT_SIZE)
            : DEFAULT_RING_MAX_OBJECT_SIZE;
    int bulkCapacity = conf != null ? conf.getInt(KEY_BULK_RING_CAPACITY, capacity) : capacity;
    int bulkMaxObject =
        conf != null ? conf.getInt(KEY_BULK_RING_MAX_OBJECT_SIZE, maxObject) : maxObject;
    int poolSize =
        conf != null
            ? conf.getInt(KEY_SERVICING_POOL_SIZE, DEFAULT_SERVICING_POOL_SIZE)
            : DEFAULT_SERVICING_POOL_SIZE;
    int queueDepth =
        conf != null
            ? conf.getInt(KEY_SERVICING_QUEUE_DEPTH, DEFAULT_SERVICING_QUEUE_DEPTH)
            : DEFAULT_SERVICING_QUEUE_DEPTH;
    boolean allowMutate =
        conf != null
            ? conf.getBoolean(KEY_ALLOW_MUTATE, DEFAULT_ALLOW_MUTATE)
            : DEFAULT_ALLOW_MUTATE;
    int maxConcurrentCalls =
        conf != null
            ? conf.getInt(KEY_MAX_CONCURRENT_CALLS, DEFAULT_MAX_CONCURRENT_CALLS)
            : DEFAULT_MAX_CONCURRENT_CALLS;
    int maxScannersPerCall =
        conf != null
            ? conf.getInt(KEY_MAX_SCANNERS_PER_CALL, DEFAULT_MAX_SCANNERS_PER_CALL)
            : DEFAULT_MAX_SCANNERS_PER_CALL;
    int maxBytesPerResp =
        conf != null
            ? conf.getInt(KEY_MAX_BYTES_PER_RESP, DEFAULT_MAX_BYTES_PER_RESP)
            : DEFAULT_MAX_BYTES_PER_RESP;
    int maxRowsPerNext =
        conf != null
            ? conf.getInt(KEY_MAX_ROWS_PER_NEXT, DEFAULT_MAX_ROWS_PER_NEXT)
            : DEFAULT_MAX_ROWS_PER_NEXT;
    return CoprocessorRuntime.Config.builder()
        .javaToGoFile(tmpDir.resolve("in.mmap"))
        .goToJavaFile(tmpDir.resolve("out.mmap"))
        .ringCapacity(capacity)
        .ringMaxObjectSize(maxObject)
        .hookTimeout(duration(conf, PolicyConfig.KEY_TIMEOUT_DEFAULT, DEFAULT_HOOK_TIMEOUT))
        .endpointTimeout(duration(conf, KEY_ENDPOINT_TIMEOUT, DEFAULT_ENDPOINT_TIMEOUT))
        .gracefulShutdownTimeout(duration(conf, KEY_GRACEFUL_SHUTDOWN, DEFAULT_GRACEFUL_SHUTDOWN))
        .servicingPoolSize(poolSize)
        .servicingQueueDepth(queueDepth)
        .servicingTimeout(duration(conf, KEY_SERVICING_TIMEOUT, DEFAULT_SERVICING_TIMEOUT))
        .bulkRingCapacity(bulkCapacity)
        .bulkRingMaxObjectSize(bulkMaxObject)
        .allowMutate(allowMutate)
        .maxConcurrentCalls(maxConcurrentCalls)
        .maxScannersPerCall(maxScannersPerCall)
        .maxBytesPerResp(maxBytesPerResp)
        .maxRowsPerNext(maxRowsPerNext)
        .scannerIdleLease(duration(conf, KEY_SCANNER_IDLE_LEASE, DEFAULT_SCANNER_IDLE_LEASE))
        .configuration(conf)
        .build();
  }

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
