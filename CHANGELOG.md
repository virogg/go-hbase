# Changelog

All notable changes to go-hbase are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org/) with the wire protocol versioned independently
(`virogg.hbasecop.v1`).

## [0.1.0] — unreleased

First release. HBase Observer coprocessors written in Go, executing in a
long-lived Go process per RegionServer, bridged over a shared-memory ring
buffer — no fork-per-call, no RPC hop.

### Highlights

- **All five Observer surfaces**: Region, RegionServer, Master, WAL and
  BulkLoad observers — 103 Observer hooks (143 request/response wire
  messages) dispatched over a protobuf wire protocol
  (`virogg.hbasecop.v1`). The Master surface ships a curated subset (20 of
  HBase 2.5's master hooks); the other four surfaces are complete. Wire
  framing has a cross-language golden corpus and a Go↔proto hook-id parity
  guard; hook payload messages are round-tripped per language (full
  cross-language byte-parity for payloads is tracked as follow-up).
- **Go SDK** (`pkg/hbasecop`): implement `RegionObserver` (or any other
  observer interface), call `hbasecop.Run(...)`; `Unimplemented*` embeddings
  keep observers forward-compatible. Panics in user hooks are recovered and
  surfaced as hook errors, never process crashes.
- **One Go process per RegionServer**, shared across regions and coprocessor
  instances (refcounted `SharedRuntime`), multiplexed by region/hook/req_id.
- **Supervision**: heartbeat watchdog, crash detection and auto-restart with
  exponential backoff; in-flight hooks during a crash window fail by policy.
  Per-hook `strict` / `best-effort` failure policies via HBase configuration.
- **Packaging**: `hbasecop-build` CLI assembles a deployable coproc-jar from
  a user observer class + Go ELF; the embedded binary is integrity-checked
  (SHA-256 manifest digest) at spawn.
- **Performance** (T81/T82 benches, gated in CI): prePut p50 dispatch
  overhead ~70–80µs vs a no-op Java observer (<100µs SPEC target) after the
  spin-before-park dispatch optimization; WAL-write throughput regression
  with a registered WALObserver inside the <50% gate.
- **Hardening**: wire decoders on both sides bound chunk counts and pending
  reassemblies (OOM-DoS resistant), fuzzed continuously (Go native fuzzing +
  Java jazzer) and nightly in CI; 1h kill -9 soak with data-loss, RSS-growth
  and zombie-supervisor gates.

### Security

- Wire decoder allocation bounds (`MAX_CHUNKS`, `MAX_PENDING_REASSEMBLIES`,
  and a cumulative retained-byte cap `MAX_PENDING_BYTES`) enforced in both
  the Go and Java decoders before any peer-controlled allocation.
- Embedded Go ELF extracted to a 0700 temp file and verified against the
  coproc-jar manifest SHA-256 before exec.

### Known limitations

- Linux x86-64 only; HBase 2.5.x; Java 11+.
- Post-hooks dispatch synchronously (SPEC §3 fire-and-forget is future work).
- `MutationConverter` drops mutation-level attributes (cellVisibility, ACL,
  TTL); Get/Scan conversions drop per-CF time ranges.
- Endpoint coprocessors are out of scope.
- Single-RS soak topology; multi-RS chaos is untested.
