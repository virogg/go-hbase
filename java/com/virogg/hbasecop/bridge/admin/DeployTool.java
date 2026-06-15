// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.admin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * Registers, lists and removes hbasecop coprocessors on existing tables via the HBase Admin API -
 * the one-command "submit" path. Run it where HBase client jars are on the classpath, e.g. {@code
 * HBASE_CLASSPATH=hbasecop-bridge-all.jar hbase com.virogg.hbasecop.bridge.admin.DeployTool deploy
 * ...}; the hbasecop-build CLI assembles that invocation.
 *
 * <pre>
 *   deploy --table T --jar file:///coproc-jars/x.jar --class FQCN [--priority N] [--zk host] [--zk-port P]
 *   remove --table T --class FQCN [--zk host] [--zk-port P]
 *   list   [--table T] [--zk host] [--zk-port P]
 * </pre>
 */
public final class DeployTool {

  private DeployTool() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("usage: DeployTool <deploy|remove|list> [--flags]");
      System.exit(2);
    }
    String cmd = args[0];
    Map<String, String> f = parseArgs(args);
    Configuration conf = connectionConfig(f);
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin()) {
      switch (cmd) {
        case "deploy":
          deploy(admin, f);
          break;
        case "remove":
          remove(admin, f);
          break;
        case "list":
          list(admin, f);
          break;
        default:
          System.err.println("unknown command: " + cmd);
          System.exit(2);
      }
    }
  }

  private static void deploy(Admin admin, Map<String, String> f) throws IOException {
    TableName tn = TableName.valueOf(require(f, "table"));
    String className = require(f, "class");
    String jar = require(f, "jar");
    int priority = Integer.parseInt(f.getOrDefault("priority", "0"));
    if (!admin.tableExists(tn)) {
      throw new IOException("table " + tn + " does not exist; create it first");
    }
    TableDescriptor next = withCoprocessor(admin.getDescriptor(tn), className, jar, priority);
    modifyInPlace(admin, tn, next);
    System.out.printf("deployed %s on %s (jar=%s)%n", className, tn, jar);
  }

  private static void remove(Admin admin, Map<String, String> f) throws IOException {
    TableName tn = TableName.valueOf(require(f, "table"));
    String className = require(f, "class");
    TableDescriptor cur = admin.getDescriptor(tn);
    if (!hasCoprocessor(cur, className)) {
      System.out.printf("%s not present on %s; nothing to do%n", className, tn);
      return;
    }
    modifyInPlace(admin, tn, withoutCoprocessor(cur, className));
    System.out.printf("removed %s from %s%n", className, tn);
  }

  private static void list(Admin admin, Map<String, String> f) throws IOException {
    List<TableDescriptor> descs;
    if (f.containsKey("table")) {
      descs = List.of(admin.getDescriptor(TableName.valueOf(f.get("table"))));
    } else {
      descs = admin.listTableDescriptors();
    }
    for (TableDescriptor d : descs) {
      for (CoprocessorDescriptor cp : d.getCoprocessorDescriptors()) {
        System.out.printf(
            "%s\t%s\tjar=%s\tpriority=%d%n",
            d.getTableName(),
            cp.getClassName(),
            cp.getJarPath().orElse("(classpath)"),
            cp.getPriority());
      }
    }
  }

  /** Disable, swap the descriptor, re-enable - the registration cycle HBase requires. */
  private static void modifyInPlace(Admin admin, TableName tn, TableDescriptor next)
      throws IOException {
    boolean wasEnabled = admin.isTableEnabled(tn);
    if (wasEnabled) {
      admin.disableTable(tn);
    }
    admin.modifyTable(next);
    if (wasEnabled) {
      admin.enableTable(tn);
    }
  }

  // ---- Pure descriptor helpers (unit-testable without a cluster) ----

  static TableDescriptor withCoprocessor(
      TableDescriptor base, String className, String jarPath, int priority) throws IOException {
    CoprocessorDescriptor cp =
        CoprocessorDescriptorBuilder.newBuilder(className)
            .setJarPath(jarPath)
            .setPriority(priority)
            .build();
    return TableDescriptorBuilder.newBuilder(base).setCoprocessor(cp).build();
  }

  static TableDescriptor withoutCoprocessor(TableDescriptor base, String className) {
    return TableDescriptorBuilder.newBuilder(base).removeCoprocessor(className).build();
  }

  static boolean hasCoprocessor(TableDescriptor desc, String className) {
    return desc.getCoprocessorDescriptors().stream()
        .anyMatch(cp -> cp.getClassName().equals(className));
  }

  // ---- Arg parsing + connection config ----

  static Map<String, String> parseArgs(String[] args) {
    Map<String, String> f = new HashMap<>();
    for (int i = 1; i < args.length; i++) {
      if (!args[i].startsWith("--")) {
        throw new IllegalArgumentException("expected --flag, got " + args[i]);
      }
      String key = args[i].substring(2);
      if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
        throw new IllegalArgumentException("flag --" + key + " needs a value");
      }
      f.put(key, args[++i]);
    }
    return f;
  }

  private static Configuration connectionConfig(Map<String, String> f) {
    Configuration conf = HBaseConfiguration.create();
    if (f.containsKey("zk")) {
      conf.set("hbase.zookeeper.quorum", f.get("zk"));
    }
    if (f.containsKey("zk-port")) {
      conf.set("hbase.zookeeper.property.clientPort", f.get("zk-port"));
    }
    return conf;
  }

  private static String require(Map<String, String> f, String key) {
    String v = f.get(key);
    if (v == null || v.isEmpty()) {
      throw new IllegalArgumentException("missing required flag --" + key);
    }
    return v;
  }
}
