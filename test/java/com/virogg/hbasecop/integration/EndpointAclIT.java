// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointRequest;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointResponse;
import com.virogg.hbasecop.bridge.endpoint.pb.GoEndpointService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

final class EndpointAclIT {

  private static final String ZK_QUORUM = "localhost";
  private static final String ZK_PORT = "2181";
  private static final String COPROC_JAR_HOST_RELATIVE =
      "test/integration/coproc-jars/endpoint-observer.jar";
  private static final String COPROC_JAR_IN_CONTAINER = "file:///coproc-jars/endpoint-observer.jar";
  private static final String COPROC_CLASSNAME =
      "com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver";
  private static final byte[] CF = "cf".getBytes(StandardCharsets.UTF_8);

  private static final String ADMIN = "hbasecop_admin";
  private static final String GRANTED_USER = "alice";
  private static final String DENIED_USER = "bob";

  @Test
  void endpointInvokeIsGatedByExecPermission() throws Throwable {
    requireStagedJar();
    TableName tn = TableName.valueOf("hbasecop_endpoint_acl_it");

    User adminUser = User.create(UserGroupInformation.createRemoteUser(ADMIN));
    User grantedUser = User.create(UserGroupInformation.createRemoteUser(GRANTED_USER));
    User deniedUser = User.create(UserGroupInformation.createRemoteUser(DENIED_USER));

    try (Connection adminConn = ConnectionFactory.createConnection(clientConfig(), adminUser);
        Admin admin = adminConn.getAdmin()) {

      waitForClusterReady(admin, Duration.ofSeconds(300));
      waitForAclTable(admin, Duration.ofSeconds(120));
      dropTable(admin, tn);
      createTableWithCoproc(admin, tn);
      grantExecWithRetry(adminConn, tn, GRANTED_USER, Duration.ofSeconds(60));

      try {
        try (Connection grantedConn =
                ConnectionFactory.createConnection(clientConfig(), grantedUser);
            Table table = grantedConn.getTable(tn)) {
          byte[] result = callEndpoint(table, "upper", ByteString.copyFromUtf8("hi"));
          assertEquals(
              "HI",
              new String(result, StandardCharsets.UTF_8),
              "a client with EXEC must be able to invoke the endpoint");
        }

        try (Connection deniedConn =
                ConnectionFactory.createConnection(clientConfig(), deniedUser);
            Table table = deniedConn.getTable(tn)) {
          Throwable err =
              assertThrows(
                  Throwable.class,
                  () -> callEndpoint(table, "upper", ByteString.copyFromUtf8("hi")),
                  "a client without EXEC must be denied");
          assertTrue(
              chainContains(err, "AccessDenied") || chainContains(err, "permission"),
              () -> "expected an EXEC-permission denial, got: " + err);
        }
      } finally {
        dropTable(admin, tn);
      }
    }
  }

  private static void grantExecWithRetry(
      Connection conn, TableName tn, String user, Duration deadline) throws Exception {
    Instant cutoff = Instant.now().plus(deadline);
    Exception last = null;
    while (Instant.now().isBefore(cutoff)) {
      try {
        AccessControlClient.grant(conn, tn, user, null, null, Permission.Action.EXEC);
        return;
      } catch (Throwable t) {
        last = (t instanceof Exception) ? (Exception) t : new Exception(t);
        Thread.sleep(1_000);
      }
    }
    throw new IllegalStateException(
        "could not grant EXEC to " + user + " within " + deadline, last);
  }

  private static void waitForAclTable(Admin admin, Duration deadline) throws Exception {
    TableName acl = TableName.valueOf("hbase:acl");
    Instant cutoff = Instant.now().plus(deadline);
    while (Instant.now().isBefore(cutoff)) {
      if (admin.tableExists(acl) && admin.isTableEnabled(acl)) {
        return;
      }
      Thread.sleep(1_000);
    }
    throw new IllegalStateException(
        "AccessController hbase:acl table not available within " + deadline);
  }

  private static boolean chainContains(Throwable t, String needle) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c.getMessage() != null && c.getMessage().contains(needle)) {
        return true;
      }
    }
    return false;
  }

  private static void requireStagedJar() {
    Path jarOnHost = resolveJarOnHost();
    assertTrue(
        Files.isReadable(jarOnHost),
        "coproc-jar not staged on host bind-mount: "
            + jarOnHost
            + " (run `make endpoint-observer-jar` and copy into test/integration/coproc-jars/)");
  }

  private static Configuration clientConfig() {
    Configuration cfg = HBaseConfiguration.create();
    cfg.set("hbase.zookeeper.quorum", ZK_QUORUM);
    cfg.set("hbase.zookeeper.property.clientPort", ZK_PORT);
    cfg.set("zookeeper.recovery.retry", "2");
    cfg.set("hbase.client.retries.number", "10");
    cfg.set("hbase.client.pause", "1000");
    cfg.set("hbase.rpc.timeout", "30000");
    cfg.set("hbase.client.operation.timeout", "60000");
    cfg.set("hbase.client.meta.operation.timeout", "60000");
    return cfg;
  }

  private static byte[] callEndpoint(Table table, String method, ByteString payload)
      throws Throwable {
    GoEndpointRequest request =
        GoEndpointRequest.newBuilder().setMethod(method).setPayload(payload).build();

    Map<byte[], byte[]> perRegion =
        table.coprocessorService(
            GoEndpointService.class,
            null,
            null,
            instance -> {
              ServerRpcController controller = new ServerRpcController();
              AtomicReference<GoEndpointResponse> out = new AtomicReference<>();
              RpcCallback<GoEndpointResponse> done = out::set;
              instance.call(controller, request, done);
              if (controller.failed()) {
                throw new IOException("endpoint controller failed: " + controller.errorText());
              }
              GoEndpointResponse resp = out.get();
              if (resp == null) {
                throw new IOException("endpoint returned no response");
              }
              if (!resp.getError().isEmpty()) {
                throw new IOException("endpoint error: " + resp.getError());
              }
              return resp.getPayload().toByteArray();
            });

    return perRegion.values().iterator().next();
  }

  private static Path resolveJarOnHost() {
    Path here = Paths.get("").toAbsolutePath();
    while (here != null) {
      Path candidate = here.resolve(COPROC_JAR_HOST_RELATIVE);
      if (Files.exists(candidate)) {
        return candidate;
      }
      here = here.getParent();
    }
    return Paths.get(COPROC_JAR_HOST_RELATIVE).toAbsolutePath();
  }

  private static void waitForClusterReady(Admin admin, Duration deadline) throws Exception {
    Instant cutoff = Instant.now().plus(deadline);
    Exception lastFailure = null;
    while (Instant.now().isBefore(cutoff)) {
      try {
        admin.listTableNames();
        return;
      } catch (Exception e) {
        lastFailure = e;
        Thread.sleep(1_000);
      }
    }
    throw new IllegalStateException(
        "HBase cluster not ready within " + deadline + ": " + lastFailure, lastFailure);
  }

  private static void createTableWithCoproc(Admin admin, TableName tn) throws IOException {
    CoprocessorDescriptorBuilder coproc =
        CoprocessorDescriptorBuilder.newBuilder(COPROC_CLASSNAME)
            .setJarPath(COPROC_JAR_IN_CONTAINER)
            .setPriority(0);
    TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tn)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .setCoprocessor(coproc.build())
            .build();
    admin.createTable(desc);
  }

  private static void dropTable(Admin admin, TableName tn) throws IOException {
    if (!admin.tableExists(tn)) {
      return;
    }
    if (admin.isTableEnabled(tn)) {
      admin.disableTable(tn);
    }
    admin.deleteTable(tn);
  }
}
