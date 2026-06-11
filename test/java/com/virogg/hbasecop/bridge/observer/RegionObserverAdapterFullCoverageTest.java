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

/**
 * Pins the T41 surface on the Java side: every declared method on the HBase {@link RegionObserver}
 * interface must have an override on {@link RegionObserverAdapter}, and every override must wire
 * one of the {@link HookId} enum values. The Go side enforces the mirror via reflection on the SDK
 * {@code RegionObserver} interface (see {@code pkg/hbasecop/hooktable_test.go}); together the two
 * tests block a hook from being added to the dispatch table without a matching override on either
 * side.
 *
 * <p>The check is name-based: HBase 2.5's RegionObserver has overloads (e.g. two {@code prePut}
 * signatures, two {@code preCheckAndPut}); the bridge picks the canonical overload per hook for T41
 * stubbing. T42's per-hook serialization work refines overload coverage on a hook-by-hook basis.
 */
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
      // The wire byte is unsigned (0-255); hook ids >= 128 (e.g. the T52
      // region-server surface at 200+) are negative as a signed Java byte,
      // so compare the 0-255 value rather than the signed byte.
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
    // The Java HookId enum is the source of truth on the adapter side; the
    // proto-generated enum mirrors the same numeric values so the wire byte
    // is interchangeable. If the proto enum drifts, regen plus this check
    // fails fast.
    for (HookId id : HookId.values()) {
      int protoValue =
          com.virogg.hbasecop.bridge.wire.pb.HookId.valueOf("HOOK_ID_" + id.name()).getNumber();
      assertEquals(
          id.value() & 0xFF, protoValue, "HookId." + id.name() + " value drift vs proto enum");
    }
  }
}
