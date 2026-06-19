#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0
#
# T26: container entrypoint for the standalone HBase dev cluster.
# Runs `hbase master start` in the foreground so PID 1 dies with HBase and
# `docker logs` sees its stdout.
#
# T51: master coprocessors cannot be attached per-table - they must be
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

# T52: region-server coprocessors are registered cluster-wide via
# `hbase.coprocessor.regionserver.classes`. When HBASECOP_RS_COPROC_CLASS is
# set, this entrypoint patches hbase-site.xml + HBASE_CLASSPATH before launch;
# when unset it is a no-op, so the shared image stays generic.
if [ -n "${HBASECOP_RS_COPROC_CLASS:-}" ]; then
  jar="${HBASECOP_RS_COPROC_JAR:-/coproc-jars/rs-policy-observer.jar}"
  if [ ! -r "${jar}" ]; then
    echo "entrypoint: region-server coproc jar not readable: ${jar}" >&2
    exit 1
  fi
  export HBASE_CLASSPATH="${jar}${HBASE_CLASSPATH:+:${HBASE_CLASSPATH}}"

  inject="  <property><name>hbase.coprocessor.regionserver.classes</name><value>${HBASECOP_RS_COPROC_CLASS}</value></property>"
  if [ -n "${HBASECOP_RS_POLICY_VETO_WAL_ROLL:-}" ]; then
    inject="${inject}\n  <property><name>hbasecop.policy.veto_wal_roll</name><value>${HBASECOP_RS_POLICY_VETO_WAL_ROLL}</value></property>"
  fi
  sed -i "s#</configuration>#${inject}\n</configuration>#" "${SITE}"
  echo "entrypoint: registered region-server coprocessor ${HBASECOP_RS_COPROC_CLASS} (jar ${jar})" >&2
fi

# T82 - WAL coprocessors are cluster-wide via `hbase.coprocessor.wal.classes`.
# When HBASECOP_WAL_COPROC_CLASS is set, this entrypoint patches
# hbase-site.xml + HBASE_CLASSPATH before launch; when unset it is a no-op,
# so the shared image stays generic.
if [ -n "${HBASECOP_WAL_COPROC_CLASS:-}" ]; then
  jar="${HBASECOP_WAL_COPROC_JAR:-/coproc-jars/wal-observer.jar}"
  if [ ! -r "${jar}" ]; then
    echo "entrypoint: WAL coproc jar not readable: ${jar}" >&2
    exit 1
  fi
  export HBASE_CLASSPATH="${jar}${HBASE_CLASSPATH:+:${HBASE_CLASSPATH}}"

  inject="  <property><name>hbase.coprocessor.wal.classes</name><value>${HBASECOP_WAL_COPROC_CLASS}</value></property>"
  sed -i "s#</configuration>#${inject}\n</configuration>#" "${SITE}"
  echo "entrypoint: registered WAL coprocessor ${HBASECOP_WAL_COPROC_CLASS} (jar ${jar})" >&2
fi

# M3 (review T1): enable HBase's AccessController so EndpointAclIT can prove the
# endpoint EXEC boundary. When HBASECOP_ENABLE_ACL=true, register the
# AccessController on master/region/regionserver, turn on authorization and the
# exec-permission checks that gate CoprocessorService.Call, and add a fixed
# superuser the test impersonates to grant. Unset on every other IT (no-op), so
# the shared image stays generic. NB: this path does not set
# HBASECOP_MASTER_COPROC_CLASS, so it owns hbase.coprocessor.master.classes
# (the endpoint region coproc is attached per-table by the IT, not here).
if [ "${HBASECOP_ENABLE_ACL:-}" = "true" ]; then
  acl="org.apache.hadoop.hbase.security.access.AccessController"
  inject="  <property><name>hbase.security.authorization</name><value>true</value></property>"
  inject="${inject}\n  <property><name>hbase.security.exec.permission.checks</name><value>true</value></property>"
  inject="${inject}\n  <property><name>hbase.coprocessor.master.classes</name><value>${acl}</value></property>"
  inject="${inject}\n  <property><name>hbase.coprocessor.region.classes</name><value>${acl}</value></property>"
  inject="${inject}\n  <property><name>hbase.coprocessor.regionserver.classes</name><value>${acl}</value></property>"
  inject="${inject}\n  <property><name>hbase.superuser</name><value>hbasecop_admin</value></property>"
  sed -i "s#</configuration>#${inject}\n</configuration>#" "${SITE}"
  echo "entrypoint: enabled AccessController (authorization + exec-permission checks)" >&2
fi

# Relay the embedded ZooKeeper client port onto a host-reachable address.
# HBase 2.5.0's MiniZooKeeperCluster binds ZK to 127.0.0.1:2181 (ignoring
# hbase.zookeeper.property.clientPortAddress), which docker's port forward
# cannot reach, so a host-side HBase client's ZK session dies with
# ConnectionLoss for /hbase/master. socat listens on 0.0.0.0:2182 and forwards
# to ZK's loopback; docker-compose maps host 2181 -> container 2182. Harmless
# on 2.5.11 (its ZK already binds 0.0.0.0). Backgrounded; HBase stays PID 1.
socat TCP-LISTEN:2182,fork,reuseaddr TCP:127.0.0.1:2181 &

# Standalone mode: master process spawns RS + embedded ZK in the same JVM.
exec "${HBASE_HOME}/bin/hbase" master start
