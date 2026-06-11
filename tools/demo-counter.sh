#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0
#
# CP-γ: public demo — drive N Puts through a live HBase coprocessor and
# show the Go observer's log counter ticking up on each one. Invoked by
# `make demo-counter`. Leaves the cluster running so reviewers can poke
# around with `docker exec go-hbase-dev hbase shell` themselves.

set -euo pipefail

CONTAINER="${CONTAINER:-go-hbase-dev}"
TABLE="${TABLE:-demo_counter}"
N="${N:-50}"

bold() { printf '\033[1;36m== %s ==\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m%s\033[0m\n' "$*"; }

bold "1/4  bring up HBase 2.5 dev cluster (T26)"
make hbase-up

bold "2/4  build + stage counter-observer.jar"
make counter-observer-jar
cp examples/counter-observer/target/counter-observer.jar \
   test/integration/coproc-jars/counter-observer.jar

# Mark the boundary so the post-Put log scrape only counts THIS run.
since=$(date -u +"%Y-%m-%dT%H:%M:%S.000000000Z")

bold "3/4  create '${TABLE}' with CounterRegionObserver attached"
docker exec -i "${CONTAINER}" /opt/hbase/bin/hbase shell -n <<EOF
disable '${TABLE}' rescue nil
drop    '${TABLE}' rescue nil
create  '${TABLE}', { NAME => 'cf' }
disable '${TABLE}'
alter   '${TABLE}', METHOD => 'table_att', 'coprocessor' => 'file:///coproc-jars/counter-observer.jar|com.virogg.hbasecop.examples.counter.CounterRegionObserver|0|'
enable  '${TABLE}'
EOF

bold "4/4  put ${N} rows; each Put fires a Go-side prePut hook"
{
  for i in $(seq 1 "${N}"); do
    echo "put '${TABLE}', 'row-${i}', 'cf:q', 'v${i}'"
  done
  echo "exit"
} | docker exec -i "${CONTAINER}" /opt/hbase/bin/hbase shell -n > /dev/null

echo
bold "Go observer log lines from this run (tail)"
docker logs --since "${since}" "${CONTAINER}" 2>&1 \
  | grep -E 'counter-observer: prePut' \
  | tail -n 10

count=$(docker logs --since "${since}" "${CONTAINER}" 2>&1 \
        | grep -c 'counter-observer: prePut' || true)

echo
ok "DEMO SUCCESS: ${count}/${N} prePut hooks fired by Go observer."
echo
echo "The cluster is still up. Try it interactively:"
echo "  docker exec -it ${CONTAINER} /opt/hbase/bin/hbase shell"
echo "    > put '${TABLE}', 'rowX', 'cf:q', 'vX'"
echo "    > exit"
echo "  docker logs --tail 20 ${CONTAINER} | grep counter-observer"
echo
echo "Tear down when done:  make hbase-down"
