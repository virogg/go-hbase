#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0
#
# T26: container entrypoint for the standalone HBase dev cluster.
# Runs `hbase master start` in the foreground so PID 1 dies with HBase and
# `docker logs` sees its stdout.

set -euo pipefail

mkdir -p /hbase-data /hbase-zk /coproc-jars

# Standalone mode: master process spawns RS + embedded ZK in the same JVM.
exec "${HBASE_HOME}/bin/hbase" master start
