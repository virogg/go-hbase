// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.observer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;

/**
 * Shared helper for honoring a Go-side {@code bypass=true} response defensively.
 *
 * <p>HBase 2.5 only makes the {@link ObserverContext} bypassable for a <em>subset</em> of hooks;
 * calling {@link ObserverContext#bypass()} on a non-bypassable hook throws {@link
 * UnsupportedOperationException}. If that escaped from a coprocessor it would abort the host
 * operation (and, on Master/RegionServer surfaces, can destabilize the server). An over-eager
 * observer must never be able to cause that, so a rejected bypass is downgraded to a WARN and the
 * host operation proceeds unbypassed.
 *
 * <p>{@link RegionObserverAdapter} carries its own private equivalent; this class is used by the
 * Master/RegionServer/WAL/BulkLoad adapters, which previously called {@code c.bypass()} unguarded.
 */
final class ObserverBypass {

  private static final Logger LOG = System.getLogger(ObserverBypass.class.getName());

  private ObserverBypass() {}

  /** Invoke {@link ObserverContext#bypass()} defensively; a rejected bypass becomes a WARN. */
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
