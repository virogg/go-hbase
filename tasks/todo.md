# go-hbase - Todo

> Источник: `tasks/plan.md`. Чекаем по мере готовности. Подробности (AC/verify): в плане.

> 🟡 **v0.1.0 RELEASE: CONDITIONAL** (review → fixes, branch `fix/v0.1.0-blockers`).
> A spec-driven review found 2 Critical + multiple High defects in shipped code
> and CI gates that enforced nothing, all now **resolved** (9 must-fix items
> fixed with regression tests; CI gates real; SPEC §7 coverage met: Go 82.5%,
> Java 87.8%; integration matrix + fuzz green). Tasks whose AC had been
> overstated are corrected: **T71** (SHA-256 now wired into the runtime), **T05**
> (lint action v7, coverage gates enforced, integration + bench + nightly jobs),
> **T12/T13** (chunk_total bounded; chunking limits documented), **T17/T22**
> (reader cancels ctx; user-panic recovered). Remaining before the T85 tag is
> Phase 8 + documented non-blocker findings: [`tasks/RELEASE-BLOCKERS.md`](RELEASE-BLOCKERS.md).

## Phase 0: Foundation
- [x] T01 Repo skeleton (dirs, LICENSE Apache-2.0, .gitignore)
- [x] T02 Go build (go.mod, golangci-lint, Makefile go-*)
- [x] T03 Java build (pom.xml, JUnit5, Spotless, JaCoCo)
- [x] T04 Top-level Makefile (deps/proto/build/test/lint/clean)
- [x] T05 GitHub Actions CI (lint+go-test+java-test+contract-stub)
- [x] T06 License header tool + check
- [x] **CP-α:** `make all` + CI зелёные

## Phase 1: Java↔Go IPC validated (no HBase)
- [x] T11 wire.proto v1 + hooks.proto skeleton (PB round-trip golden)
- [x] T12 Go wire framing + chunking (`internal/wire`)
- [x] T13 Java wire framing + chunking
- [x] T14 java-go-shmem dependency wiring (Go+Java)
- [x] T15 Go shmem wrapper (`internal/shmem`)
- [x] T16 Java shmem wrapper (`bridge.shmem`)
- [x] T17 Go runtime event loop + heartbeat sender (`internal/cpruntime`)
- [x] T18 Java supervisor: spawn Go from jar resource
- [x] T19 E2E ping/pong 10k roundtrip
- [x] **CP-β (CRITICAL):** demo-ping artifact, latency report

## Phase 2: One hook E2E on real HBase 2.5
- [x] T21 Vendored HBase .proto + PrePut/PostPut messages
- [x] T22 Go SDK skeleton: `RegionObserver{PrePut,PostPut}` + `Run(...)`
- [x] T23 Java RegionObserverAdapter (Put hooks only)
- [x] T24 Mux v0 (single region, req_id matching)
- [x] T25 Coproc-jar packaging via Maven shade (counter-observer example)
- [x] T26 HBase 2.5 docker-compose dev cluster + `make hbase-up`
- [x] T27 Integration test: Put → Go observer counter
- [x] **CP-γ (DEMO READY):** `make demo-counter` works end-to-end on live HBase

## Phase 3: Failure semantics & supervisor production-grade
- [x] T31 Per-hook policy parsing (strict / best-effort)
- [x] T32 Strict-mode wiring → IOException; best-effort → log+continue
- [x] T33 Heartbeat watchdog (kill -9 on miss)
- [x] T34 Auto-restart with exponential backoff
- [x] T35 Inflight handling on crash + restart-deadline
- [x] T36 Fault-injection test suite (≥10 matrix cases)
- [x] **CP-δ:** strict/best-effort semantics validated; Open Q #1 (multi-tenant) → deferred post-MVP

## Phase 4: Full RegionObserver
- [x] T41 Hook dispatch table generated from .proto (68 hooks)
- [x] T42 Per-hook serialization mappers (HBase native ↔ proto)
- [x] T43 Read-path hooks (preGet, scanner)
- [x] T44 Batch hooks (preBatchMutate)
- [x] T45 Storage hooks (flush/compact)
- [x] T46 Coverage matrix doc + CI gate
- [x] **CP-ε1:** RegionObserver complete

## Phase 5: Other Observer types
- [x] T51 MasterObserver adapter + tests
- [x] T52 RegionServerObserver adapter
- [x] T53 WALObserver adapter (+ WAL throughput bench)
- [x] T54 BulkLoadObserver adapter
- [x] **CP-ε2:** all SPEC §2 observer types covered

## Phase 6: Multi-region multiplexing
- [x] T61 Region-scoped routing in mux (region_id in header)
- [x] T62 Concurrent inflight from N regions stress test
- [x] T63 Lifecycle refcount on Observer start/stop
- [x] **CP-ε3:** N regions per RS sharing one Go process; Open Q #2 (hot reload) → defer post-MVP; Open Q #3 → SHA-256 checksum in manifest (T71)

## Phase 7: DX: CLI, examples, docs
- [x] T71 `hbasecop-build` CLI (Java ManifestBinaryDescriptor + GoProcess SHA-256 validation + Go CLI emitting coproc-jar)
- [x] T72 examples/audit-observer (post-hook audit, row-digest privacy, AuditObserverIT 50 ops → 50 records)
- [x] T73 examples/ttl-validator (strict PrePut validation, TtlValidatorIT invalid → IOException)
- [x] T74 Top-level README + getting started (quick start, config-defaults reference, FAQ)
- [x] T75 Architecture doc (docs/architecture.md: as-built flow; concurrency.md marked future-design)
- [x] **CP-ε4:** release candidate ready: all 9 review must-fix items fixed (regression-tested), CI gates real, SPEC §7 coverage met (Go 82.5%, Java 87.8%), integration matrix + fuzz green. Remaining work is Phase 8 + documented non-blocker findings; see tasks/RELEASE-BLOCKERS.md (status: CONDITIONAL).

## Phase 8: Bench, harden, release
- [x] T81 Bench harness latency overhead (gate PASS: 72-78µs p50 < 100µs after spin-before-park fix; docs/bench/t81-latency.md)
- [x] T82 Bench WAL/flush throughput (gate PASS: 15.4% regression < 50%; docs/bench/t82-wal.md)
- [x] T83 Fuzz wire codec (Go 20m + Java jazzer 10m, clean; found+fixed missing Java decoder H2/H4 bounds; docs/bench/t83-fuzz.md)
- [x] T84 Soak 1h kill-9 chaos (all gates PASS: 2.84M acked puts / 0 lost across 12 kill -9s, Go RSS flat, RS RSS +6.5% post-ramp, 13 restarts = 1+12 exact, 0 zombies; docs/bench/t84-soak.md)
- [ ] T85 v0.1.0 release (tag, changelog, artifacts): machinery ready: CHANGELOG.md, `make release` validated (hbasecop-bridge-0.1.0.jar + hbasecop-build-linux-amd64), .github/workflows/release.yml on v* tag, README install-from-release. Remaining: user decision on non-blocker findings + push tag.
- [ ] **CP-ζ:** v0.1.0 released
