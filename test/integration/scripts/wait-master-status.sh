#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0
#
# T26: gate the integration suite on the HBase standalone cluster being truly
# ready, in two phases:
#   1. Master web UI returns HTTP 200 (Jetty up). Cheap liveness.
#   2. The cluster serves an RPC `list` via `hbase shell` inside the container
#      (meta online, RegionServer assigned). This is the real readiness signal.
#
# Phase 1 alone is insufficient: the master UI binds well before the master
# registers in ZooKeeper and the RegionServer hosts meta, so a client RPC made
# right after the 200 races into `KeeperException$ConnectionLoss for /hbase/
# master`. The per-IT Java `waitForClusterReady` probe uses a 60s operation
# timeout equal to its 60s deadline, so a single cold-start RPC consumes the
# whole budget and the test fails before retrying. Waiting for RPC readiness
# here closes that race for every IT.
#
# Invoked by `make hbase-up` / the test-integration-* targets once
# `docker compose up -d` has started the cluster. CWD is the repo root.

set -euo pipefail

URL="${HBASE_MASTER_URL:-http://localhost:16010/master-status}"
TIMEOUT="${HBASE_WAIT_TIMEOUT:-180}"
INTERVAL="${HBASE_WAIT_INTERVAL:-3}"
RPC_TIMEOUT="${HBASE_RPC_WAIT_TIMEOUT:-180}"
RPC_INTERVAL="${HBASE_RPC_WAIT_INTERVAL:-5}"
COMPOSE_FILE="${HBASE_COMPOSE_FILE:-test/integration/docker-compose.yml}"
COMPOSE_SERVICE="${HBASE_COMPOSE_SERVICE:-hbase}"

# ---------------------------------------------------------------------------
# Phase 1: master web UI HTTP 200.
# ---------------------------------------------------------------------------
deadline=$(( $(date +%s) + TIMEOUT ))
printf 'waiting for HBase master web UI at %s (timeout %ss)\n' "$URL" "$TIMEOUT"

attempt=0
ui_ready=0
while true; do
  attempt=$(( attempt + 1 ))
  code=$(curl -fsS -o /dev/null -w '%{http_code}' --max-time 5 "$URL" 2>/dev/null || true)
  if [[ "$code" == "200" ]]; then
    printf 'hbase master web UI ready after %d attempt(s): %s -> 200\n' "$attempt" "$URL"
    ui_ready=1
    break
  fi
  if (( $(date +%s) >= deadline )); then
    printf 'ERROR: HBase master web UI did not become ready within %ss (last http_code=%s)\n' \
      "$TIMEOUT" "${code:-none}" >&2
    exit 1
  fi
  sleep "$INTERVAL"
done

# ---------------------------------------------------------------------------
# Phase 2: RPC readiness - the cluster can actually serve a meta operation.
# ---------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  printf 'WARNING: docker CLI not found; skipping RPC-readiness probe (phase 1 only)\n' >&2
  exit 0
fi

rpc_deadline=$(( $(date +%s) + RPC_TIMEOUT ))
printf 'waiting for HBase RPC readiness via `hbase shell list` in service %s (timeout %ss)\n' \
  "$COMPOSE_SERVICE" "$RPC_TIMEOUT"

rpc_attempt=0
while true; do
  rpc_attempt=$(( rpc_attempt + 1 ))
  # `hbase shell -n` exits non-zero if the embedded command raises; `list`
  # succeeds only once meta is online. Suppress its chatty output.
  if docker compose -f "$COMPOSE_FILE" exec -T "$COMPOSE_SERVICE" \
       bash -c 'echo "list" | hbase shell -n' >/dev/null 2>&1; then
    printf 'hbase RPC ready after %d attempt(s): `list` served\n' "$rpc_attempt"
    exit 0
  fi
  if (( $(date +%s) >= rpc_deadline )); then
    printf 'ERROR: HBase did not serve an RPC `list` within %ss after the web UI came up\n' \
      "$RPC_TIMEOUT" >&2
    exit 1
  fi
  sleep "$RPC_INTERVAL"
done
