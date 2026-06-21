// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.junit.jupiter.api.Test;

class RegionObserverAdapterFullCoverageTest {

  @Test
  void adapterOverridesEveryRegionObserverMethodName() {
    Set<String> declaredOnAdapter = new HashSet<>();
    for (Method m : RegionObserverAdapter.class.getDeclaredMethods()) {
      declaredOnAdapter.add(m.getName());
    }

    Set<String> baseMethodNames =
        Arrays.stream(RegionObserver.class.getDeclaredMethods())
            .filter(Method::isDefault)
            .map(Method::getName)
            .collect(Collectors.toCollection(TreeSet::new));

    Set<String> missing = new TreeSet<>();
    for (String name : baseMethodNames) {
      if (!declaredOnAdapter.contains(name)) {
        missing.add(name);
      }
    }
    if (!missing.isEmpty()) {
      fail(
          "RegionObserverAdapter missing override(s) for "
              + missing.size()
              + " RegionObserver method(s): "
              + missing);
    }
  }

  @Test
  void hookIdEnumIsBijection() {
    Set<Byte> values = new HashSet<>();
    Set<String> methodNames = new HashSet<>();
    for (HookId id : HookId.values()) {
      assertNotNull(id.methodName(), "HookId " + id.name() + " has null methodName");
      assertTrue(
          (id.value() & 0xFF) > 0,
          "HookId " + id.name() + " has non-positive wire value " + (id.value() & 0xFF));
      assertTrue(
          values.add(id.value()), "HookId " + id.name() + " duplicates wire value " + id.value());
      assertTrue(
          methodNames.add(id.methodName()),
          "HookId " + id.name() + " duplicates methodName " + id.methodName());
    }
  }

  @Test
  void hookIdEnumCoversEveryRegionObserverMethod() {
    Set<String> hookIdNames =
        Arrays.stream(HookId.values()).map(HookId::methodName).collect(Collectors.toSet());

    Set<String> missing = new TreeSet<>();
    for (Method m : RegionObserver.class.getDeclaredMethods()) {
      if (!m.isDefault()) {
        continue;
      }
      if (!hookIdNames.contains(m.getName())) {
        missing.add(m.getName());
      }
    }
    if (!missing.isEmpty()) {
      fail(
          "HookId enum missing entry for "
              + missing.size()
              + " RegionObserver method(s): "
              + missing);
    }
  }

  @Test
  void hookIdEnumByteValuesMatchProtoEnum() {
    for (HookId id : HookId.values()) {
      int protoValue =
          com.virogg.hbasecop.bridge.wire.pb.HookId.valueOf("HOOK_ID_" + id.name()).getNumber();
      assertEquals(
          id.value() & 0xFF, protoValue, "HookId." + id.name() + " value drift vs proto enum");
    }
  }
}
