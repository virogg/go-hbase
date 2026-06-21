// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;

final class ObserverBypass {

  private static final Logger LOG = System.getLogger(ObserverBypass.class.getName());

  private ObserverBypass() {}

  static void tryBypass(ObserverContext<?> c) {
    try {
      c.bypass();
    } catch (UnsupportedOperationException e) {
      LOG.log(
          Level.WARNING,
          "hbasecop: observer requested bypass on a hook that does not support it - ignored",
          e);
    }
  }
}
