# Concurrency model: reader → router → per-region actor

> ⚠️ **FUTURE DESIGN — NOT IMPLEMENTED.** The router / per-region actor /
> lifecycle-barrier model below was planned for T41 but **was not built**.
> The shipped runtime is a flat reader + writer + heartbeat with one
> goroutine per request and **no per-region ordering or lifecycle barrier**
> — observer state must be safe for fully concurrent invocation. For the
> as-built model see [`docs/architecture.md`](../architecture.md)
> §"Concurrency model (as built)". Keep this document only as a design
> sketch for a possible post-v0.1.0 ordering layer.

> **Status.** Foundation in place via T17 (`internal/cpruntime.Loop`).
> Full enforcement — including the lifecycle barrier — lands in T41 when
> the RegionObserver hook surface fills in.
> **Scope.** This document covers the Go-side runtime
> (`internal/cpruntime`). Java-side mux (T24) mirrors the same shape on
> the producer half but has a simpler intra-region story because each
> HBase handler thread is naturally one-call-at-a-time.

## Goals

1. **Concurrency across regions.** N regions on one RegionServer must
   process hooks in parallel; one slow region must not stall others.
2. **Concurrency within a region.** Multiple data hooks for the same
   region (e.g. `prePut` rows A/B from a `preBatchMutate`) run on
   independent goroutines so handlers can do real work.
3. **Lifecycle ordering within a region.** When HBase emits
   `preClose` / `postClose` / `preSplit` / `postSplit` / `preOpen` /
   `postOpen` for a region, the handler runs **after** all earlier
   in-flight data-hook handlers for that region have returned, and
   **before** any subsequent data hook for that region starts.
4. **Bounded resource use.** No unbounded backlog; backpressure must
   propagate without livelock.

## Topology

```
            ┌───────────────────────────────────────────────────────┐
            │                       Go runtime                      │
            │                                                       │
shmem in ──►│  reader gr ──► routing chan(buf=1024) ──► router gr ──┼─► actor[regionID].inbox
            │                                                       │      │       │
            │                          actor[1] ◄───────────────────┘      │       │
            │                          actor[2] ◄───────────────────┼──────┘       │
            │                          actor[N] ◄───────────────────┼──────────────┘
            │                                                       │
            │   actor.run():                                        │
            │     for msg := range inbox {                          │
            │       if isLifecycle(msg.HookID) {                    │
            │         inflight.Wait()         // drain THIS region  │
            │         handler(msg)            // monopolistic, sync │
            │       } else {                                        │
            │         inflight.Add(1)                               │
            │         go handler-runs-here(msg)  // concurrent      │
            │       }                                               │
            │     }                                                 │
            │                                                       │
            │   handlers emit → outbound chan(buf=256) → writer gr ─┼─► shmem out
            └───────────────────────────────────────────────────────┘
```

Three goroutines own I/O:

| Goroutine     | Responsibility                                       | Why dedicated                                                      |
|---------------|------------------------------------------------------|--------------------------------------------------------------------|
| `reader`      | drain shmem inbound → push to routing chan           | Must never block in user code; pure I/O loop.                      |
| `router`      | dequeue routing chan → push to actor's inbox         | Decouples shmem reads from any per-region backpressure.            |
| `writer`      | dequeue outbound chan → encode → shmem outbound      | shmem ring is SPSC; single writer enforces that.                   |

`N` ephemeral goroutines own per-region work:

| Goroutine          | Responsibility                                              |
|--------------------|-------------------------------------------------------------|
| `actor[regionID]`  | Serializes lifecycle vs. data for one region; spawns handler goroutines. |
| `handler` (data)   | Runs one user-code hook invocation; lives until response is queued. |
| `handler` (lifecycle) | Same, but runs synchronously on the actor goroutine so the barrier holds. |

Actors are **lazy-allocated** on the first frame with a new `region_id`.
`region_id` is **monotonic** (T61 — never recycled). When the actor's
`postClose` handler completes and the matching `unregisterRegion` Java
event ships through, the actor's inbox is closed and the goroutine
returns. State that referred to the region is collectable.

## Why this shape

### Why a separate `router` goroutine

If the reader pushed directly into `actor.inbox`, a full actor inbox
would block the reader and stop draining shmem — head-of-line block
across **all** regions, caused by one slow region. The router is the
indirection that lets the reader stay live: it pulls from a large
shared buffer (routing chan), absorbs spikes, and only blocks on the
*slow region's* inbox when pushing there.

### Why a `regionBarrier` cannot sit on the reader path

Equivalent design: hold a `sync.RWMutex` per region — `RLock` for data,
`Lock` for lifecycle — and acquire it on the reader goroutine. **Wrong**:
acquiring `Lock` waits for all current `RLock` holders to release.
While waiting, the reader is parked, which means no other region's
frames are being drained either. One slow `prePut` handler delays
`preClose` for region A *and* every hook for regions B…Z. The actor
model confines that wait to one goroutine that nobody else depends on.

### Why concurrency *inside* a region is allowed

`preBatchMutate` ships up to `hbase.client.write.buffer` worth of
mutations in one RPC. Serializing those across the actor would make
per-region throughput a single-handler cliff. HBase itself doesn't
guarantee cross-row ordering inside a batch, so handlers can run in
parallel without changing observed semantics — provided lifecycle still
joins.

### Why `region_id` is monotonic

A recycled id reuses an actor that may still be cleaning up. Monotonic
ids — `AtomicInt64` on the Java mux — guarantee a fresh actor for every
`preOpen`, and let stale responses arriving after a cancel be dropped
in O(1) (`live` set lookup).

## Buffer sizing — chosen for T41

- **routing chan:** 1024 frames. Absorbs ~50ms of 20k-msg/s sustained
  inbound throughput across all regions on a moderate RS.
- **actor.inbox:** 64 frames. Enough to ride out an 8-cell
  `preBatchMutate` plus the lifecycle hooks that delimit it without
  the router ever waiting.
- **outbound chan:** 256 frames (already in T17). Drains as fast as
  the single writer-thread can push to shmem (~µs/frame).

These are starting points; T62 (multi-region stress) is the gate that
either confirms them or forces a re-tune.

## Head-of-line trade-off (open)

The router will block on `actor.inbox <- msg` if the target region's
inbox is full. **This is intentional** — the alternative is to drop or
error-spike under load, which we want under explicit policy, not as a
silent regression.

For T41 the chosen mitigation is **mitigation (1): generous buffers**.
At the buffer sizes above, "actor inbox full" requires a user handler
that genuinely cannot keep up — i.e. a real backpressure event the
operator must see, not a startup transient.

### Two further mitigations exist; one is structural, one is not

**(2) Multi-router fan-out** — replace the single router goroutine with
a pool of M routers concurrently draining the routing chan. Reduces the
probability that one full inbox stalls the whole router stage when other
inboxes have room.

> Multi-router does **not** structurally close the HoL window. If every
> actor's inbox is full simultaneously (genuine system-wide overload),
> all M routers block. This mitigation only shifts the threshold; under
> sustained overload the symptom returns. Adopt only as a tuning knob
> after T62 measurements, never as the load-shedding mechanism.

**(3) Non-blocking offer + overflow policy** — router uses
`select { case a.inbox <- msg: default: overflowPolicy(msg) }`.
Overflow paths: respond with `Error{code=Overloaded}` (strict-mode →
IOException to client), or drop+WARN (best-effort).

> This is the only **structural** closure of router-level HoL: the
> router never waits, ever. If the runtime is ever required to bound
> tail latency under overload (P8 SLOs), this is the lever; (2) cannot
> substitute. Adoption is gated on agreeing on a default policy and
> wiring it through `hbasecop.policy` config (T31).

## Lifecycle barrier semantics

| Hook class | Examples                                | Dispatch                                                       |
|------------|------------------------------------------|----------------------------------------------------------------|
| data       | `prePut`, `postPut`, `preGet`, `postScannerNext`, `preBatchMutate`, … | `inflight.Add(1)`; runs in fresh `go` routine; `Done()` after response queued. |
| lifecycle  | `preOpen`, `postOpen`, `preClose`, `postClose`, `preSplit`, `postSplit`, `preMerge`, `postMerge`, `preFlush`, `postFlush`, `preCompact`, `postCompact` | Actor goroutine first `inflight.Wait()`, then runs handler synchronously. New data hooks for the region wait behind it. |

Decision rules:

1. The barrier guarantees: when a lifecycle handler runs, no data
   handler for **the same region** is concurrently running.
2. The barrier does **not** order hooks across regions. Region A's
   `postOpen` may run while Region B's `preClose` is running.
3. The barrier does not unwind on lifecycle handler failure — error is
   reported, actor continues, next hook resumes normally.
4. A data handler that ignores `ctx.Done()` can indefinitely delay a
   lifecycle hook for its region. The SDK contract (T22) requires
   handlers to honour `ctx`; T35 enforces a hard deadline by forcing
   an `Error` response when the deadline passes.

## What this implies for the Java side (T24, T61)

- One **MPSC writer-thread** per shmem direction; handler-threads
  `enqueue + future.get` only. No `synchronized` per send.
- `Map<RegionInfo, RegionID>` allocates monotonic ids; never recycles.
- `unregisterRegion` is bookkeeping-only; runs **after** the `postClose`
  future resolves. There is no separate "drain region" call — the
  HBase RPC contract guarantees in-flight RPCs are drained before
  `preClose` reaches the adapter (see `HRegion.doClose`).
- Cancellation of a future on RPC timeout immediately removes it from
  the inflight map. Late Go responses are dropped in O(1).

## What this implies for the SDK contract (T22, T41)

- `RegionObserver` handler signatures take `context.Context` first.
  Long-running handlers must respect `ctx.Done()`.
- Lifecycle hooks (`PreOpen` … `PostMerge`) execute serially within a
  region; per-region state mutation inside a lifecycle handler is
  race-free without user-level locks.
- Data hooks within the same region run concurrently. User-level state
  shared across data hooks requires the usual `sync` primitives.
- Per-region state lives in user code, indexed by `env.RegionID`.
  Bridge does not migrate state across regions on split/merge — the
  observer's `PostSplit`/`PostMerge` handlers do that.

## What this leaves open

- T62 (multi-region stress) will publish concrete latency numbers under
  `docs/bench/` and either ratify the buffers above or trigger the
  multi-router tuning.
- The non-blocking-offer policy (mitigation 3) is unimplemented and
  unconfigured. It is the structural closure of router-level HoL and
  the only viable lever for hard tail-latency SLOs in P8.
- Heartbeats currently flow through the same outbound chan as
  responses (T17). Under sustained outbound saturation a heartbeat can
  be dropped — already logged, but T33 (watchdog) is the consumer of
  that signal and must tolerate sporadic misses without false-killing
  a healthy process.
