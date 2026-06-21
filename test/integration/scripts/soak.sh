#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0
#
# soak orchestrator - runs SoakIT (the paced load driver with the data-loss ledger) while injecting
# kill-9 chaos into the Go runtime and sampling RSS / process state inside the container,
# then evaluates the release gates:
#
#   1. dataloss   - SoakIT exit code (every client-acked Put present in scan)
#   2. rss-flat   - median Go RSS of the last 20% of samples ≤ 1.15× first 20%
#   3. zombies    - never more than one runtime proc in any sample, zero
#                   runtime procs after SoakIT drops the table (the release
#                   path stops the Go process; any survivor is leaked), and
#                   zero Z-state procs at the end
#   4. restarts   - 'GoProcess started:' count == 1 + kills, no 'UNHEALTHY'
#
# Assumes the cluster is ALREADY UP (wait-master-status.sh has passed) and
# counter-observer.jar is staged in test/integration/coproc-jars/ - the
# `make soak` target does compose up/down and jar staging around this script.
#
# Env knobs:
#   SOAK_DURATION_S  load duration in seconds          (default 3600)
#   SOAK_RATE        target combined ops/sec           (default 1000)
#   SOAK_KILL_MIN_S  min seconds between kill-9s       (default 120)
#   SOAK_KILL_MAX_S  max seconds between kill-9s       (default 300)
#   SOAK_SAMPLE_S    RSS sampling period in seconds    (default 10)
#   OUT_DIR          artifact dir                      (default test/integration/coproc-jars)

set -euo pipefail

SOAK_DURATION_S="${SOAK_DURATION_S:-3600}"
SOAK_RATE="${SOAK_RATE:-1000}"
SOAK_KILL_MIN_S="${SOAK_KILL_MIN_S:-120}"
SOAK_KILL_MAX_S="${SOAK_KILL_MAX_S:-300}"
SOAK_SAMPLE_S="${SOAK_SAMPLE_S:-10}"
OUT_DIR="${OUT_DIR:-test/integration/coproc-jars}"

CONTAINER="go-hbase-dev"
MVN="${MVN:-mvn}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"
mkdir -p "$OUT_DIR"

MVN_LOG="$OUT_DIR/soak-mvn.log"
EVENTS_LOG="$OUT_DIR/soak-events.log"
RSS_CSV="$OUT_DIR/soak-rss.csv"
KILL_COUNT_FILE="$OUT_DIR/soak-kill-count"
SUMMARY="$OUT_DIR/soak-summary.txt"

: > "$EVENTS_LOG"
: > "$SUMMARY"
echo "0" > "$KILL_COUNT_FILE"
echo "ts,go_pid,go_rss_kb,rs_rss_kb,runtime_procs,zombies" > "$RSS_CSV"

note() {
  printf '%s\n' "$1" | tee -a "$SUMMARY"
}

median() {
  sort -n | awk '
    { a[NR] = $1 }
    END {
      if (NR == 0) { print "" }
      else if (NR % 2) { print a[(NR + 1) / 2] }
      else { printf "%d\n", (a[NR / 2] + a[NR / 2 + 1]) / 2 }
    }'
}

# Load driver

start_ts="$(date +%s)"
printf 'soak: starting SoakIT (duration=%ss rate=%s ops/s), log: %s\n' \
  "$SOAK_DURATION_S" "$SOAK_RATE" "$MVN_LOG"

"$MVN" -B -ntp test -Dtest=SoakIT -DfailIfNoTests=false \
  -Dsoak.duration.s="$SOAK_DURATION_S" -Dsoak.rate="$SOAK_RATE" \
  > "$MVN_LOG" 2>&1 &
MVN_PID=$!

# Chaos loop

RUNTIME_PATTERN='[h]basecop-runtime'

runtime_pid() {
  docker exec "$CONTAINER" sh -c "pgrep -f '$RUNTIME_PATTERN' | head -1" 2>/dev/null || true
}

chaos_loop() {
  local span delay slept n victim
  span=$(( SOAK_KILL_MAX_S - SOAK_KILL_MIN_S + 1 ))
  while kill -0 "$MVN_PID" 2>/dev/null && [[ -z "$(runtime_pid)" ]]; do
    sleep 1
  done
  kill -0 "$MVN_PID" 2>/dev/null || return 0
  local chaos_deadline=$(( $(date +%s) + SOAK_DURATION_S - 30 ))
  printf '%s CHAOS_ARMED\n' "$(date +%s)" >> "$EVENTS_LOG"
  while kill -0 "$MVN_PID" 2>/dev/null && (( $(date +%s) < chaos_deadline )); do
    delay=$(( SOAK_KILL_MIN_S + RANDOM % span ))
    slept=0
    while (( slept < delay )) && kill -0 "$MVN_PID" 2>/dev/null; do
      sleep 1
      slept=$(( slept + 1 ))
    done
    kill -0 "$MVN_PID" 2>/dev/null || break
    (( $(date +%s) < chaos_deadline )) || break
    victim="$(runtime_pid)"
    [[ -n "$victim" ]] || continue
    docker exec "$CONTAINER" sh -c "pkill -9 -f '$RUNTIME_PATTERN'" >/dev/null 2>&1 || true
    printf '%s KILL pid=%s\n' "$(date +%s)" "$victim" >> "$EVENTS_LOG"
    n="$(cat "$KILL_COUNT_FILE" 2>/dev/null || echo 0)"
    echo "$(( n + 1 ))" > "$KILL_COUNT_FILE"
  done
}

# Sampler loop

sampler_loop() {
  local ts go_pid go_rss rs_pid rs_rss runtime_procs zombies
  while kill -0 "$MVN_PID" 2>/dev/null; do
    ts="$(date +%s)"
    go_pid="$(runtime_pid)"
    go_rss=""
    if [[ -n "$go_pid" ]]; then
      go_rss="$(docker exec "$CONTAINER" ps -o rss= -p "$go_pid" 2>/dev/null | tr -d '[:space:]' || true)"
    fi
    rs_pid="$(docker exec "$CONTAINER" sh -c "pgrep -f '[p]roc_master' | head -1" 2>/dev/null || true)"
    if [[ -z "$rs_pid" ]]; then
      rs_pid="$(docker exec "$CONTAINER" sh -c 'pgrep java | head -1' 2>/dev/null || true)"
    fi
    rs_rss=""
    if [[ -n "$rs_pid" ]]; then
      rs_rss="$(docker exec "$CONTAINER" ps -o rss= -p "$rs_pid" 2>/dev/null | tr -d '[:space:]' || true)"
    fi
    runtime_procs="$(docker exec "$CONTAINER" sh -c "pgrep -f '$RUNTIME_PATTERN' | wc -l" 2>/dev/null | tr -d '[:space:]' || true)"
    zombies="$(docker exec "$CONTAINER" sh -c 'ps -eo stat | grep -c "^Z"' 2>/dev/null | tr -d '[:space:]' || true)"
    printf '%s,%s,%s,%s,%s,%s\n' \
      "$ts" "$go_pid" "$go_rss" "$rs_rss" "${runtime_procs:-}" "${zombies:-}" >> "$RSS_CSV"
    sleep "$SOAK_SAMPLE_S"
  done
}

chaos_loop &
CHAOS_PID=$!
sampler_loop &
SAMPLER_PID=$!

cleanup() {
  kill "${CHAOS_PID:-}" "${SAMPLER_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the driver, stop background loops

mvn_status=0
wait "$MVN_PID" || mvn_status=$?
end_ts="$(date +%s)"

cleanup
wait "$CHAOS_PID" "$SAMPLER_PID" 2>/dev/null || true

kill_count="$(cat "$KILL_COUNT_FILE" 2>/dev/null || echo 0)"
printf 'soak: SoakIT finished (exit=%s) after %ss with %s kill(s)\n' \
  "$mvn_status" "$(( end_ts - start_ts ))" "$kill_count"

# Gates

overall=0
note "=== SOAK SUMMARY $(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
note "duration_s=$(( end_ts - start_ts )) configured_s=$SOAK_DURATION_S rate=$SOAK_RATE"
note "kills=$kill_count"
soak_result="$(grep 'SOAK_RESULT' "$MVN_LOG" | tail -1 || true)"
note "${soak_result:-SOAK_RESULT <missing from $MVN_LOG>}"

if [[ "$mvn_status" -eq 0 ]]; then
  note "GATE dataloss: PASS (mvn exit 0)"
else
  note "GATE dataloss: FAIL (mvn exit $mvn_status, see $MVN_LOG)"
  overall=1
fi

rss_gate() {
  local label="$1" col="$2" tolerance_pct="$3" base_window="$4"
  local -a samples
  mapfile -t samples < <(awk -F, -v c="$col" 'NR > 1 && $c != "" { print $c }' "$RSS_CSV")
  local n="${#samples[@]}"
  if (( n < 20 )); then
    note "GATE rss-flat[$label]: SKIPPED (only $n samples, need >=20) - WARNING"
    return 0
  fi
  local k=$(( n / 5 ))
  local base_med last_med base_desc
  case "$base_window" in
    first)
      base_med="$(printf '%s\n' "${samples[@]:0:k}" | median)"
      base_desc="first20pct"
      ;;
    midlate)
      base_med="$(printf '%s\n' "${samples[@]:$(( n * 3 / 5 )):k}" | median)"
      base_desc="60-80pct"
      ;;
  esac
  last_med="$(printf '%s\n' "${samples[@]: -k}" | median)"
  note "${label}_rss_kb medians: ${base_desc}=$base_med last20pct=$last_med (samples=$n)"
  if (( last_med * 100 > base_med * (100 + tolerance_pct) )); then
    note "GATE rss-flat[$label]: FAIL (last median $last_med > ${tolerance_pct}% over $base_desc median $base_med)"
    return 1
  fi
  note "GATE rss-flat[$label]: PASS (growth within ${tolerance_pct}%)"
  return 0
}
rss_gate go 3 15 first || overall=1
rss_gate rs 4 10 midlate || overall=1

sleep 5
max_procs="$(awk -F, 'NR > 1 && $5 != "" && $5 > m { m = $5 } END { print m + 0 }' "$RSS_CSV")"
final_procs="$(docker exec "$CONTAINER" sh -c "pgrep -f '$RUNTIME_PATTERN' | wc -l" 2>/dev/null | tr -d '[:space:]' || true)"
final_procs="${final_procs:-0}"
final_zombies="$(docker exec "$CONTAINER" sh -c 'ps -eo stat | grep -c "^Z"' 2>/dev/null | tr -d '[:space:]' || true)"
final_zombies="${final_zombies:-0}"
note "max_runtime_procs_sampled=$max_procs final_runtime_procs=$final_procs final_zombies=$final_zombies"
if (( max_procs <= 1 )); then
  note "GATE single-runtime: PASS (never more than 1 sampled)"
else
  note "GATE single-runtime: FAIL (sampled $max_procs concurrent hbasecop-runtime procs)"
  overall=1
fi
if [[ "$final_procs" == "0" ]]; then
  note "GATE no-leaked-runtime: PASS (0 procs after table drop)"
else
  note "GATE no-leaked-runtime: FAIL (table dropped but $final_procs hbasecop-runtime proc(s) survive)"
  overall=1
fi
if [[ "$final_zombies" == "0" ]]; then
  note "GATE zombies: PASS"
else
  note "GATE zombies: FAIL (expected 0 Z-state procs, got $final_zombies)"
  overall=1
fi

restart_count="$(docker logs "$CONTAINER" 2>&1 | grep -c 'GoProcess started:' || true)"
restart_count="${restart_count:-0}"
unhealthy_count="$(docker logs "$CONTAINER" 2>&1 | grep -c 'UNHEALTHY' || true)"
unhealthy_count="${unhealthy_count:-0}"
expected_restarts=$(( 1 + kill_count ))
note "restarts=$restart_count expected=$expected_restarts unhealthy_lines=$unhealthy_count"
if [[ "$restart_count" == "$expected_restarts" ]]; then
  note "GATE restart-accounting: PASS"
else
  note "GATE restart-accounting: FAIL (GoProcess started: $restart_count, expected $expected_restarts)"
  overall=1
fi
if [[ "$unhealthy_count" == "0" ]]; then
  note "GATE no-unhealthy: PASS"
else
  note "GATE no-unhealthy: FAIL ($unhealthy_count UNHEALTHY line(s) in docker logs)"
  overall=1
fi

if [[ "$overall" -eq 0 ]]; then
  note "OVERALL: PASS"
else
  note "OVERALL: FAIL"
fi
note "artifacts: $MVN_LOG $RSS_CSV $EVENTS_LOG $SUMMARY"

exit "$overall"
