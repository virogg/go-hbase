// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.jupiter.api.Test;

final class DeployToolTest {

  private static final String CLASS = "com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver";

  private static TableDescriptor bareTable() {
    return TableDescriptorBuilder.newBuilder(TableName.valueOf("t"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f"))
        .build();
  }

  @Test
  void withCoprocessorAddsItAndRoundTrips() throws Exception {
    TableDescriptor d = DeployTool.withCoprocessor(bareTable(), CLASS, "file:///x.jar", 0);
    assertTrue(DeployTool.hasCoprocessor(d, CLASS));
    assertEquals(
        "file:///x.jar",
        d.getCoprocessorDescriptors().iterator().next().getJarPath().orElseThrow());
  }

  @Test
  void withoutCoprocessorRemovesIt() throws Exception {
    TableDescriptor with = DeployTool.withCoprocessor(bareTable(), CLASS, "file:///x.jar", 0);
    TableDescriptor without = DeployTool.withoutCoprocessor(with, CLASS);
    assertFalse(DeployTool.hasCoprocessor(without, CLASS));
  }

  @Test
  void hasCoprocessorFalseOnBareTable() {
    assertFalse(DeployTool.hasCoprocessor(bareTable(), CLASS));
  }

  @Test
  void parseArgsCollectsFlagPairs() {
    Map<String, String> f =
        DeployTool.parseArgs(new String[] {"deploy", "--table", "t", "--class", CLASS});
    assertEquals("t", f.get("table"));
    assertEquals(CLASS, f.get("class"));
  }

  @Test
  void parseArgsRejectsDanglingFlag() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DeployTool.parseArgs(new String[] {"deploy", "--table"}));
  }

  @Test
  void parseArgsRejectsNonFlagToken() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DeployTool.parseArgs(new String[] {"deploy", "table", "t"}));
  }
}
