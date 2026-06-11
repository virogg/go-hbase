# T81 — Per-hook latency overhead vs a Java-only observer

**Verifies:** plan task T81 "latency overhead per hook (Java-only baseline vs
through-shmem), p50/p95/p99 на prePut/postPut, batch 1/100/1000" and the
SPEC §7.6 MVP target **<100µs p50 overhead on prePut**.

The bench drives the production Java path end to end: the real
`RegionObserverAdapter` (proto conversion included) → `Multiplexer` → wire
encode → shmem ring → real spawned Go process running a **silent no-op
observer** (`test/bench/noop-observer`) → response path back to the caller.
The baseline leg makes the identical calls against a default-method no-op
Java `RegionObserver`; overhead = p50(bridge) − p50(java-only).

## How to reproduce

```
make bench-latency            # builds the no-op ELF, runs the gate
# knobs:
make bench-latency BENCH_P50_MAX_US=100
mvn test -Dtest=LatencyBenchIT -Djacoco.skip=true -Dbench.ops=10000
```

JaCoCo is skipped for the bench run — latency is measured uninstrumented.

## Methodology

- **batch=1 (gated)**: continuous serial round trips, same methodology as
  the T19 ping-pong baseline. This is the regime the SPEC target describes:
  under load the rings stay warm.
- **batch=100/1000**: bursts of N back-to-back calls separated by a 1ms idle
  gap — how a client batch of N mutations reaches prePut on one handler
  thread.
- **sparse (reported, not gated)**: single calls each preceded by 1ms idle —
  the cold-resume cost after the spin-wait reader threads have been
  descheduled. Scheduler-dominated and noisy on WSL2/shared CI runners.
- 10 000 timed ops per leg after a 2 000-op warmup; percentiles by
  sort-and-index.

## Result (2026-06-10)

Hardware: AMD Ryzen 7 5800H, WSL2, Linux x86-64. Production-default ring
config (capacity 16, maxObjectSize 1MiB, heartbeats on).

| Leg                      | p50    | p95    | p99    |
|--------------------------|-------:|-------:|-------:|
| prePut bridge, batch=1   | ~75µs  | ~190µs | ~300µs |
| prePut java-only, batch=1| ~0.9µs | ~2µs   | ~4µs   |
| prePut bridge, batch=100 | ~64µs  | ~150µs | ~280µs |
| prePut bridge, batch=1000| ~62µs  | ~200µs | ~400µs |
| postPut bridge, batch=1  | ~75µs  | ~190µs | ~300µs |
| sparse (not gated)       | ~90–135µs | ~250µs | ~390µs |

**Gate: prePut p50 overhead = 72–78µs across three runs — PASS (<100µs).**

## The iteration that got it under the target

The first harness run measured 128–135µs p50 — over target. Two fixes:

1. **Measure uninstrumented.** The JaCoCo agent (on by default under
   `mvn test`) instruments the bridge hot path; skipping it saved ~7µs.
2. **Spin-before-park in `MuxHookDispatcher`** (production change). The
   calling thread used to park immediately in `CompletableFuture.get()`,
   paying two context switches per hook — more than the remaining wait,
   since the whole round trip completes in ~100µs. The dispatcher now
   spin-polls the future for up to 150µs (`Thread.onSpinWait`) before
   falling back to the blocking wait. Steady-state p50 dropped 128µs → ~75µs
   (batch=1000: 95µs → ~62µs). Slow or failing hooks fall through to the
   blocking path after at most the spin budget, so timeout semantics are
   unchanged; the cost is ≤150µs of busy CPU on an RPC handler thread for
   hooks that are already in flight.

## Caveats

- The remaining ~60µs floor is split across proto build/parse, two ring
  hops, and the Go-side reader→handler→writer goroutine handoffs; the T62
  report's analysis of the single-writer/single-reader shims applies.
- The sparse leg shows that a workload idling >1ms between hooks pays
  roughly double the p50 — that's OS scheduling of the spin-wait readers,
  not protocol cost, and it straddles the 100µs line on WSL2. On bare-metal
  Linux expect the gap to shrink.
- WSL2 numbers vary ±10% run to run; shared CI runners more. The CI gate
  uses the absolute SPEC target; a CI miss warrants a local re-run before
  action.
