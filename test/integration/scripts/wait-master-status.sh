#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0
#
# T26: poll the HBase master web UI until it returns HTTP 200, or exit 1 on
# timeout. Invoked by `make hbase-up` once `docker compose up -d` has started
# the standalone cluster. Standalone HBase needs ~20-40s before the master UI
# binds, so the default deadline is generous.

set -euo pipefail

URL="${HBASE_MASTER_URL:-http://localhost:16010/master-status}"
TIMEOUT="${HBASE_WAIT_TIMEOUT:-180}"
INTERVAL="${HBASE_WAIT_INTERVAL:-3}"

deadline=$(( $(date +%s) + TIMEOUT ))
printf 'waiting for HBase master at %s (timeout %ss)\n' "$URL" "$TIMEOUT"

attempt=0
while true; do
  attempt=$(( attempt + 1 ))
  code=$(curl -fsS -o /dev/null -w '%{http_code}' --max-time 5 "$URL" 2>/dev/null || true)
  if [[ "$code" == "200" ]]; then
    printf 'hbase master ready after %d attempt(s): %s -> 200\n' "$attempt" "$URL"
    exit 0
  fi
  if (( $(date +%s) >= deadline )); then
    printf 'ERROR: HBase master did not become ready within %ss (last http_code=%s)\n' \
      "$TIMEOUT" "${code:-none}" >&2
    exit 1
  fi
  sleep "$INTERVAL"
done
