#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0
#
# T26: container entrypoint for the standalone HBase dev cluster.
# Runs `hbase master start` in the foreground so PID 1 dies with HBase and
# `docker logs` sees its stdout.
#
# T51: master coprocessors cannot be attached per-table — they must be
# registered cluster-wide via `hbase.coprocessor.master.classes` with the
# jar on the master classpath. When HBASECOP_MASTER_COPROC_CLASS is set,
# this entrypoint patches hbase-site.xml + HBASE_CLASSPATH before launch;
# when unset (every other IT) it is a no-op, so the shared image stays
# generic.

set -euo pipefail

mkdir -p /hbase-data /hbase-zk /coproc-jars

SITE="${HBASE_HOME}/conf/hbase-site.xml"

if [ -n "${HBASECOP_MASTER_COPROC_CLASS:-}" ]; then
  jar="${HBASECOP_MASTER_COPROC_JAR:-/coproc-jars/master-policy-observer.jar}"
  if [ ! -r "${jar}" ]; then
    echo "entrypoint: master coproc jar not readable: ${jar}" >&2
    exit 1
  fi
  export HBASE_CLASSPATH="${jar}${HBASE_CLASSPATH:+:${HBASE_CLASSPATH}}"

  # Inject the master-coprocessor registration (and optional policy prefix)
  # immediately before the closing </configuration> tag.
  inject="  <property><name>hbase.coprocessor.master.classes</name><value>${HBASECOP_MASTER_COPROC_CLASS}</value></property>"
  if [ -n "${HBASECOP_POLICY_BLOCKED_PREFIX:-}" ]; then
    inject="${inject}\n  <property><name>hbasecop.policy.blocked_prefix</name><value>${HBASECOP_POLICY_BLOCKED_PREFIX}</value></property>"
  fi
  sed -i "s#</configuration>#${inject}\n</configuration>#" "${SITE}"
  echo "entrypoint: registered master coprocessor ${HBASECOP_MASTER_COPROC_CLASS} (jar ${jar})" >&2
fi

# Standalone mode: master process spawns RS + embedded ZK in the same JVM.
exec "${HBASE_HOME}/bin/hbase" master start
