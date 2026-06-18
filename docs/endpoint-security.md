# Endpoint security & the ACL-bypass boundary (Tier 2)

This note documents the authorization model of go-hbase **endpoint** coprocessors
(Tier 2: client-initiated, server-side Go RPCs) and, in particular, the
boundary that reverse reads/writes bypass. It is the gate for **CP-E4**.

> TL;DR — An endpoint is server-side code that runs with **RegionServer
> authority**. Invoking it is gated by HBase EXEC permission; once inside, its
> reverse reads/writes (`env.Get`/`OpenScanner`/`Put`/`Delete`) act with the
> RegionServer's identity, **not the calling client's**, and writes are
> additionally gated by `hbasecop.endpoint.allow-mutate` (off by default). This
> is the same trust model as HBase's own `AggregateImplementation` and
> `MultiRowMutationEndpoint`.

## Trust model

An endpoint is **not** a client request that happens to run on the server — it
is server-side coprocessor code executing inside the RegionServer (or Master)
JVM. Like every HBase `CoprocessorService` endpoint, it operates with the
**daemon's authority**, because the reverse operations it issues
(`Region.get`/`getScanner`/`put`/`delete`) are server-side calls with no
client RPC `User` attached.

Deploying an endpoint therefore means trusting its code with the data the
RegionServer can reach. Restricting *who may invoke it* is the control point.

## What IS enforced

- **EXEC permission to invoke.** When HBase's `AccessController` is deployed, a
  client must hold `EXEC` permission on the table/endpoint to call it
  (`preEndpointInvocation`), checked on the RPC handler thread with the client's
  identity. A client lacking EXEC cannot invoke the endpoint at all.
- **`hbasecop.endpoint.allow-mutate` (off by default).** Reverse `MUTATE` is
  rejected unless the invoking region's table (or the cluster) opts in. Read-only
  endpoints need no opt-in; write-capable endpoints are explicit. See the
  per-table gate in `ReverseRpcServicer.allowMutateFor`.
- **Admission & resource caps** (`hbasecop.endpoint.{max-concurrent-calls,
  max-scanners-per-call, max-bytes-per-resp, max-rows-per-next,
  scanner-idle-lease}`) bound the blast radius (DoS protection of the RS handler
  pool and memory), independent of authorization.
- **The RegionObserver pipeline still fires.** A reverse `MUTATE` goes through
  `Region.put`/`delete`, so deployed observers' `prePut`/`postPut`/`preBatchMutate`
  hooks run (audit, secondary indexing, etc.) — exactly as for
  `MultiRowMutationEndpoint`. WAL/durability apply normally.

## What is BYPASSED (the boundary)

- **The invoking client's data ACL.** Reverse `Get`/`Scan`/`Put`/`Delete` run on
  the bridge's servicing-pool thread with **no client `User` context**, so
  `AccessController`'s per-cell/per-table READ/WRITE checks evaluate against the
  RegionServer's (superuser) identity, not the caller's. A client with only
  `EXEC` on the endpoint can thus cause reads/writes it could not perform
  directly. **Authorization collapses to "who may invoke the endpoint."**
- **The client RPC stack for the reverse op.** Reverse operations do not traverse
  `RSRpcServices`, so RPC-level quotas, throttling, and request-level
  interceptors do not apply to them (they are bounded instead by the endpoint
  admission caps above).

This is inherent to server-side coprocessor endpoints in HBase; go-hbase does not
widen it. What go-hbase adds is the **default-off `allow-mutate` gate** and the
**admission caps**, so the write path is opt-in and bounded.

## Master endpoints (TE43)

Master endpoints (`Admin.coprocessorService`) carry `region_id 0` and have **no
region**: region-local reverse reads/writes are unavailable there (A-12). Their
scope is master/meta state. EXEC permission still gates invocation.

## Reentry & deadlock-safety (CP-reentry)

An endpoint may read **and write** the very region it was invoked on (TE41). This
does not self-deadlock by construction: the reverse op runs on the bridge's
bounded **servicing-pool** thread, never the RegionServer handler thread that is
blocked awaiting the endpoint's `EndpointResult`. On the Go side each call is a
fresh goroutine, and the servicing pool is **fail-closed** — saturation surfaces
as an error, not an unbounded block. The reentry-stress IT
(`EndpointRoundTripIT.clientReverseMutateReentryStress`: 20 concurrent
read-then-write calls on one region) confirms it.

Because the RegionObserver pipeline fires on reverse `MUTATE`, an endpoint whose
own `postPut` issues another reverse `MUTATE` can recurse. go-hbase adds **no
guard** against this — it is the endpoint author's responsibility, exactly as for
hand-written HBase coprocessors. Keep reverse mutations free of unbounded
self-recursion.

## Handler-pinning & the heartbeat watchdog (long endpoints)

A long-running endpoint does **not** trip the supervisor's heartbeat watchdog into
a false restart. The Go heartbeat is emitted from a **dedicated goroutine**,
independent of the per-invoke handler goroutine, and the single out-writer funnel
is non-blocking; on the Java side the watchdog records liveness from the reader
thread and ticks on its own scheduler, while a long endpoint blocks only the RS
handler thread it pinned (on `future.get`). So an endpoint may run for as long as
`hbasecop.endpoint.timeout` permits (default 30s) without being mistaken for a
hung process. The fault matrix confirms this
(`EndpointFaultIT.longEndpointDoesNotTripWatchdog`: a call far longer than the
heartbeat-miss deadline returns normally, with no restart). Bound endpoint
duration with `hbasecop.endpoint.timeout` and concurrency with the admission caps.

## Operator guidance

- **Grant endpoint `EXEC` permission deliberately.** It is the only authorization
  boundary; a caller with EXEC inherits the endpoint's data reach.
- **Keep `allow-mutate` off** unless the endpoint must write, and set it
  per-table (coprocessor property) so only the intended tables permit writes. It
  is read once per shared runtime (per coproc-id), so set it consistently for all
  tables sharing one coproc-jar on a RegionServer.
- **Review endpoint Go code as privileged.** It runs with RegionServer authority.
- **Reserve writes for trusted coprocessors**; do not deploy untrusted
  endpoint-jars with `allow-mutate` enabled.
