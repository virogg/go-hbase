# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0
#
# T26: dev-cluster JVM env. eclipse-temurin already exports JAVA_HOME; HBase's
# bin/hbase respects it, so we only override knobs that matter for a tiny
# standalone JVM.

export HBASE_HEAPSIZE=1G
# Standalone mode runs ZK in-process; disabling the managed-ZK fork avoids
# the "starting zookeeper" log line being misleading in our healthcheck.
export HBASE_MANAGES_ZK=true
# Keep logs on stdout so `docker logs` sees them.
export HBASE_ROOT_LOGGER=INFO,console
