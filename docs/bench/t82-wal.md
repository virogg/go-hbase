# T82 - WAL write throughput: WALObserver on vs off

**Verifies:** plan task T82 "throughput WAL writes с WALObserver vs без,
regression < 50%".

A/B comparison on a live dockerized HBase 2.5 standalone cluster. Cycle A
boots the cluster bare; cycle B boots it with the `wal-observer` coproc-jar
registered cluster-wide (`hbase.coprocessor.wal.classes`; WAL coprocessors
cannot attach per-table), whose Go side is a **no-op WALObserver**
(`examples/wal-observer`), so the delta is pure bridge dispatch cost on the
WAL append path (preWALWrite/postWALWrite, hooks 220/221). Both cycles run
the same `WalThroughputBenchIT`: 20 000 puts in batches of 100 against one
table, wall-clocked from the client. The IT verifies coproc presence
(`pgrep -f hbasecop-runtime` in the container) matches the leg, so a stale
cluster can't contaminate the baseline.

## How to reproduce

```
make bench-wal                       # both cycles + gate
# knobs:
make bench-wal BENCH_WAL_OPS=20000 BENCH_WAL_MAX_REGRESSION_PCT=50
```

## Result (2026-06-10)

Hardware: AMD Ryzen 7 5800H, WSL2 + Docker, HBase 2.5.11 standalone.

| Leg | Throughput |
|-----|-----------:|
| A: no WAL coprocessor | 16 170 ops/s |
| B: go-hbase no-op WALObserver | 13 685 ops/s |

**Regression: 15.4%, PASS (< 50% gate).**

## Read

- The WAL path amortizes well: one batch of 100 puts produces roughly one
  WAL append per region, so the per-append hook cost (~2× dispatch round
  trip at ~70µs each) spreads across the batch. Workloads of unbatched
  single puts would see a higher relative hit; the plan's risk table flags
  sampling-only WAL hooks as the fallback if a real workload needs it.
- The measurement is end-to-end client put throughput, not isolated WAL
  append rate: it is the regression a user actually experiences.

## Caveats

- Standalone single-JVM HBase under WSL2/Docker; absolute numbers are not
  representative of a production cluster, but the A/B ratio is the metric
  and both legs share the environment.
- Run-to-run variance ±10%; the 50% gate has ample margin.
