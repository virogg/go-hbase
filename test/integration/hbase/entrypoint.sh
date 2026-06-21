#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0

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

  inject="  <property><name>hbase.coprocessor.master.classes</name><value>${HBASECOP_MASTER_COPROC_CLASS}</value></property>"
  if [ -n "${HBASECOP_POLICY_BLOCKED_PREFIX:-}" ]; then
    inject="${inject}\n  <property><name>hbasecop.policy.blocked_prefix</name><value>${HBASECOP_POLICY_BLOCKED_PREFIX}</value></property>"
  fi
  sed -i "s#</configuration>#${inject}\n</configuration>#" "${SITE}"
  echo "entrypoint: registered master coprocessor ${HBASECOP_MASTER_COPROC_CLASS} (jar ${jar})" >&2
fi

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

socat TCP-LISTEN:2182,fork,reuseaddr TCP:127.0.0.1:2181 &

exec "${HBASE_HOME}/bin/hbase" master start
