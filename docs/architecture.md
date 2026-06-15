# Architecture

How a single HBase operation flows through go-hbase, what runs where, and
what happens when things fail. Describes the system **as
implemented**; the failure-mode contract it realizes is SPEC.md §3.

## Components

```
RegionServer JVM                                Go process (one per coproc-jar per RS)
┌─────────────────────────────────────────┐    ┌──────────────────────────────────┐
│ HBase core                              │    │ cmd/<your-observer> (ELF,        │
│   │ hook invocation                     │    │  embedded in the coproc-jar)     │
│   ▼                                     │    │                                  │
│ *ObserverAdapter        bridge.observer │    │ pkg/hbasecop.Run                 │
│   │ converters: HBase types → protobuf  │    │   │                              │
│   ▼                                     │    │ internal/cpruntime.Loop          │
│ MuxHookDispatcher ──▶ Multiplexer       │    │   reader gr ─▶ per-request gr    │
│   req_id ↔ CompletableFuture            │    │   │             │ dispatcher     │
│   ▼ Encoder (wire frame)                │    │   │             ▼ hook table     │
│ shmem.Channel ══ java-go-shmem ring ════╪════╪═▶ wire.Decoder  user Observer    │
│         (mmap files, lock-free)         │    │                 │ recover()      │
│ ChannelReader ◀══ Go→Java ring ◀════════╪════╪── writer gr ◀── HookResponse     │
│   │ Multiplexer.deliver(req_id)         │    │                                  │
│   ▼ HookResponse → bypass/error/Result  │    │ heartbeat gr (500ms)             │
│                                         │    └──────────────────────────────────┘
│ GoProcess supervisor: extract ELF       │
│  (SHA-256 verify) → exec → watchdog     │
│  → RestartController                    │
└─────────────────────────────────────────┘
```

| Layer | Java | Go |
|---|---|---|
| Observer surface | `bridge.observer.*ObserverAdapter` (Region/Master/RegionServer/WAL/BulkLoad) | `pkg/hbasecop` interfaces + `Unimplemented*` embeddings |
| Dispatch | `MuxHookDispatcher` → `multiplex.Multiplexer` | `dispatch.go` + per-surface hook tables |
| Wire codec | `bridge.wire.Encoder/Decoder` | `internal/wire` |
| Transport | `bridge.shmem.Channel` | `internal/shmem.Channel` |
| Lifecycle | `CoprocessorRuntime`, `SharedRuntime`, `supervisor.*` | `internal/cpruntime.Loop` |

## Request flow (a strict pre-hook, e.g. `prePut`)

1. **Hook fires.** HBase invokes `RegionObserverAdapter.prePut(...)` on an
   RPC handler thread.
2. **Serialize.** The adapter converts HBase types to the vendored protobuf
   messages (`MutationConverter`, `CellConverter`, ...), wraps them in the
   per-hook request (`PrePutRequest`) plus `HookContext` (table, region).
3. **Dispatch.** `MuxHookDispatcher` resolves the per-hook policy + timeout
   (`PolicyConfig`); `Multiplexer.callTracked` allocates a monotonic
   `req_id`, registers a `CompletableFuture`, encodes a `REQUEST` frame and
   sends it on the Java→Go ring (sends serialized: the ring is
   single-producer).
4. **Wire.** One frame:
   `[u32 len][u8 type][u64 req_id][u32 region_id][u8 hook_id][u8 flags][u32 chunk_idx][u32 chunk_total][protobuf]`,
   big-endian, max 64 KiB (`MaxFrameSize`). Decoders bound `chunk_total`
   (`MaxChunks` = 1024) and concurrent reassemblies. Production payloads are
   single-chunk, so size payloads below the frame cap.
5. **Go receives.** The `cpruntime` reader goroutine polls the ring, decodes
   the frame, and spawns one goroutine for the request.
6. **User code.** The dispatcher unmarshals the per-hook request, looks up
   the hook table, and calls your observer method. A returned error (or a
   recovered **panic**) becomes `HookResponse.error`; `Bypass`,
   `BlockedIndices` and `ResultCells` are copied into the `HookResponse`.
7. **Respond.** The response frame goes to the single writer goroutine and
   onto the Go→Java ring.
8. **Java completes.** `ChannelReader` decodes it, `Multiplexer.deliver`
   completes the future for that `req_id`. On timeout the dispatcher cancels
   the call and drops it from the pending map.
9. **Apply.** The adapter maps the `HookResponse`: `bypass=true` →
   `ObserverContext.bypass()` (guarded: a WARN where HBase doesn't allow
   it); `error` → policy (`strict` → `IOException`, the client's operation
   aborts; `best-effort` → WARN + proceed); `PreAppend`/`PreIncrement`
   bypass returns a `Result` built from `ResultCells`.

Post-hooks follow the same path; under the default best-effort policy their
failures never abort the host operation. They currently still *wait* for
the Go response synchronously, bounded by the hook timeout.

## Process model

**One Go process per coproc-jar per RegionServer**, shared across all
regions and tables using that jar: the first `start()` spawns it via
`SharedRuntime.acquire(key, ...)` (refcounted), later starts attach, the last
`stop()` shuts it down (`SHUTDOWN` frame → graceful wait → force-kill).
Each region gets a `region_id` from `RegionIdAllocator` at observer start;
it rides the frame header so the Go side scopes `ObserverEnv` per region.

**Startup:** `CoprocessorRuntime.start()` opens the two mmap ring files,
reads the jar manifest's `HbaseCop-Go-Bin-SHA256`, extracts the embedded
ELF, **verifies the digest** (mismatch → fail-fast; protects against
corruption/wrong-arch, not a signature scheme), exec's it with the
`HBASECOP_*` environment, and starts the reader thread + supervisor
scheduler.

## Concurrency model (as built)

- **Go:** one reader goroutine, one writer goroutine, one heartbeat
  goroutine, and **one goroutine per in-flight request**. No global mutex on
  the hot path; backpressure via a bounded outbound queue (256).
- **Java:** one reader thread per runtime; hook callers block on their
  future; ring sends serialized by a lock (SPSC ring).
- **Ordering: none beyond HBase's own.** Requests from different regions,
  and concurrent requests from the same region, execute concurrently on the
  Go side. There is **no per-region serialization and no lifecycle barrier**:
  observer state must be safe for concurrent use (use atomics/locks).
  `docs/architecture/concurrency.md` describes a *future* per-region actor
  design that is **not implemented**.

Throughput scales with cores and shows no head-of-line blocking across
regions (T62 bench: `docs/bench/t62-region-concurrency.md`).

## Failure modes (SPEC §3)

| Failure | Detection | Consequence |
|---|---|---|
| Go returns an error / panics | response carries `HookResponse.error` | per-hook policy: strict → client `IOException`; best-effort → WARN + proceed |
| Go slow / unresponsive | per-hook timeout (default 5s) | policy, as above; the pending call is cancelled |
| Go process exits | supervisor tick (`detectExitedGoProcess`, runs even with heartbeats disabled) | in-flight futures fail (`GoSideCrashed`); restart with backoff 200 ms to 5 s (×2, jitter ±20%) |
| Go process hung | 3 missed 500 ms heartbeats | SIGKILL, then the restart path above |
| Restarts keep failing | `max-fails` (5) consecutive failures | marked unhealthy; hooks fail by policy immediately; probe every 30 s |
| Calls during a restart window | n/a | parked up to `hbasecop.restart.deadline` (3 s), then fail by policy |
| Corrupted/wrong ELF in jar | SHA-256 vs manifest at extract | coprocessor fails to start, clear log message |
| Malformed/oversized wire frames | decoder bounds (length, type, `MaxChunks`, pending cap) | frame rejected with an error; never an unbounded allocation |

Validated end-to-end by the fault-injection matrix (`make test-fault`,
10 cases: {strict, best-effort} × {kill -9, hang, exit-1, OOM, error}),
asserting correct semantics, no data loss, no double-apply, and recovery.

## Key limits & defaults

| Limit | Value | Where |
|---|---|---|
| Max wire frame | 64 KiB | `internal/wire.MaxFrameSize` / `WireFormat.MAX_FRAME_SIZE` |
| Max chunks / pending reassemblies | 1024 / 4096 | both decoders |
| Ring | capacity 16 slots × 1 MiB | `CoprocessorRuntime.Config` defaults |
| Hook timeout | 5 s | `hbasecop.timeout.default` |
| Heartbeat / miss threshold | 500 ms / 3 | `hbasecop.heartbeat.*` |
| Restart backoff / unhealthy / probe | 200 ms→5 s ±20% / 5 fails / 30 s | `hbasecop.restart.*` |

The full configuration reference lives in the top-level
[README](../README.md#configuration-reference).
