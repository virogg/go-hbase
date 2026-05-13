# go-hbase — Todo

> Источник: `tasks/plan.md`. Чекаем по мере готовности. Подробности (AC/verify) — в плане.

## Phase 0 — Foundation
- [x] T01 Repo skeleton (dirs, LICENSE Apache-2.0, .gitignore)
- [x] T02 Go build (go.mod, golangci-lint, Makefile go-*)
- [x] T03 Java build (pom.xml, JUnit5, Spotless, JaCoCo)
- [x] T04 Top-level Makefile (deps/proto/build/test/lint/clean)
- [x] T05 GitHub Actions CI (lint+go-test+java-test+contract-stub)
- [x] T06 License header tool + check
- [x] **CP-α:** `make all` + CI зелёные

## Phase 1 — Java↔Go IPC validated (no HBase)
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

## Phase 2 — One hook E2E on real HBase 2.5
- [x] T21 Vendored HBase .proto + PrePut/PostPut messages
- [x] T22 Go SDK skeleton: `RegionObserver{PrePut,PostPut}` + `Run(...)`
- [x] T23 Java RegionObserverAdapter (Put hooks only)
- [ ] T24 Mux v0 (single region, req_id matching)
- [ ] T25 Coproc-jar packaging via Maven shade (counter-observer example)
- [ ] T26 HBase 2.5 docker-compose dev cluster + `make hbase-up`
- [ ] T27 Integration test: Put → Go observer counter
- [ ] **CP-γ (DEMO READY):** `make demo-counter` works end-to-end on live HBase

## Phase 3 — Failure semantics & supervisor production-grade
- [ ] T31 Per-hook policy parsing (strict / best-effort)
- [ ] T32 Strict-mode wiring → IOException; best-effort → log+continue
- [ ] T33 Heartbeat watchdog (kill -9 on miss)
- [ ] T34 Auto-restart with exponential backoff
- [ ] T35 Inflight handling on crash + restart-deadline
- [ ] T36 Fault-injection test suite (≥10 matrix cases)
- [ ] **CP-δ:** strict/best-effort semantics validated; **gate:** decide Open Q #1 (multi-tenant)

## Phase 4 — Full RegionObserver
- [ ] T41 Hook dispatch table generated from .proto (~30 hooks)
- [ ] T42 Per-hook serialization mappers (HBase native ↔ proto)
- [ ] T43 Read-path hooks (preGet, scanner)
- [ ] T44 Batch hooks (preBatchMutate)
- [ ] T45 Storage hooks (flush/compact)
- [ ] T46 Coverage matrix doc + CI gate
- [ ] **CP-ε1:** RegionObserver complete

## Phase 5 — Other Observer types
- [ ] T51 MasterObserver adapter + tests
- [ ] T52 RegionServerObserver adapter
- [ ] T53 WALObserver adapter (+ WAL throughput bench)
- [ ] T54 BulkLoadObserver adapter
- [ ] **CP-ε2:** all SPEC §2 observer types covered

## Phase 6 — Multi-region multiplexing
- [ ] T61 Region-scoped routing in mux (region_id in header)
- [ ] T62 Concurrent inflight from N regions stress test
- [ ] T63 Lifecycle refcount on Observer start/stop
- [ ] **CP-ε3:** N regions per RS sharing one Go process; **gate:** Open Q #2 (hot reload), #3 (signing)

## Phase 7 — DX: CLI, examples, docs
- [ ] T71 `hbasecop-build` CLI
- [ ] T72 examples/audit-observer
- [ ] T73 examples/ttl-validator
- [ ] T74 Top-level README + getting started
- [ ] T75 Architecture doc
- [ ] **CP-ε4:** release candidate ready

## Phase 8 — Bench, harden, release
- [ ] T81 Bench harness latency overhead (target: <100µs p50 prePut)
- [ ] T82 Bench WAL/flush throughput
- [ ] T83 Fuzz wire codec (Go + Java)
- [ ] T84 Soak 1h kill-9 chaos
- [ ] T85 v0.1.0 release (tag, changelog, artifacts)
- [ ] **CP-ζ:** v0.1.0 released
