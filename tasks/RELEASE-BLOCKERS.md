# v0.1.0 — RELEASE: CONDITIONAL

> Status: **CONDITIONAL.** The blockers a specification-driven review found —
> two Critical and a set of High defects in shipped code, plus CI quality gates
> that enforced nothing — are all **resolved**: the 9 must-fix items are fixed
> with regression tests, the CI gates are real, both SPEC §7 coverage gates are
> met (Go 82.5% ≥ 80%, Java 87.8% ≥ 0.75) and verified, and the full
> integration matrix + the wire fuzzer run green. CP-ε4 (release candidate) is
> closed.
>
> **Remaining before T85 (tag) — not blockers, tracked as Phase 8 + follow-ups:**
> - Phase 8: T81 latency bench asserting the <100µs p50 prePut target, T82 WAL
>   throughput regression gate, T83 extended (nightly) fuzz, T84 1h soak/chaos.
> - Non-blocker review findings (enhancements, not correctness blockers):
>   post-hooks dispatch synchronously rather than fire-and-forget (SPEC §3);
>   `MutationConverter` drops mutation-level attributes (cellVisibility/ACL/TTL);
>   no cross-language *byte-parity* contract test for the 143 hook messages.
>
> These do not gate correctness or safety; they gate the performance claims and
> polish that a 0.1.0 tag should carry. Decide per item whether each blocks the
> tag or ships as a documented known-limitation.

This branch (`fix/v0.1.0-blockers`) addresses the must-fix set. Status legend:
**FIXED** (done + regression test), **CONFIG** (gate/CI made real), **PARTIAL**
(scoped fix + tracked follow-up).

## Must-fix

| ID | Severity | Issue | Status |
|----|----------|-------|--------|
| C1 | Critical | User-observer panic was never `recover()`ed → crashed the shared Go process for **all regions** on the RegionServer (SPEC §6). | **FIXED** — `recoverInvoke` in `pkg/hbasecop/dispatch.go` turns a panic into `HookResponse.error`; backstop recover in `internal/cpruntime/loop.go`. Regression: `TestDispatchRecoversObserverPanic`. |
| C2 | Critical | Disabling heartbeats silently disabled **all crash detection + auto-restart** (the supervisor scheduler was only started when a watchdog existed). | **FIXED** — `CoprocessorRuntime` now always starts the supervisor scheduler (crash-probe cadence when heartbeats off); watchdog tick is optional. |
| H1 | High | T71/CP-ε3 SHA-256 ELF checksum was **dead code** — `CoprocessorRuntime.buildGoProcess()` never passed the digest, so it was never verified at runtime. | **FIXED** — runtime resolves `HbaseCop-Go-Bin-SHA256` from the coproc-jar manifest and passes it to `GoProcessConfig`; `GoProcess` fails closed on mismatch. |
| H2 | High | Wire-decoder OOM DoS: unbounded peer-controlled `chunk_total` pre-allocated a multi-GiB chunk slice (Go + Java). | **FIXED** — `MaxChunks` bound in `internal/wire/decoder.go` and `bridge/wire/Decoder.java`. Regression: `TestDecodeRejectsHugeChunkTotal` + `FuzzDecode`. |
| H4 | High | Decoder reassembly map unbounded / never evicted (abandoned req_ids). | **FIXED** — `MaxPendingReassemblies` cap, both decoders. Regression: `TestDecodeCapsPendingReassemblies`. |
| H3 | High | `Multiplexer` pending future/map leaked on every hook timeout. | **FIXED** — `Multiplexer.cancel(reqId)` + `callTracked`; `MuxHookDispatcher` cancels on `TimeoutException`. Pending-count cap added. |
| H5 | High | `hbasecop-build` emitted structurally broken coproc-jars (`path.Clean` stripped directory markers). | **FIXED** — entry names preserved verbatim in `cmd/hbasecop-build/build.go`. Regression: `TestBuild_PreservesDirectoryEntries`. |
| H7 | High | cpruntime reader hard-error returned without cancelling the run context → writer/heartbeat hang, `Run()` never returns. | **FIXED** — `runReader` cancels ctx on transport error (`internal/cpruntime/loop.go`). |
| H10 | High | `c.bypass()` called unguarded in Master/RegionServer/WAL/BulkLoad adapters → a bypass on a non-bypassable hook throws and aborts the host op. | **FIXED** — shared guarded `ObserverBypass.tryBypass` used by all four adapters. |
| H12 | High | Bypassed `preAppend`/`preIncrement` returned `null` Result (HookResponse carries no value), silently dropping a value-substituting bypass. | **FIXED** — `HookResponse.result` (repeated Cell, proto field 4, additive); Go `HookResult.ResultCells`; `CellConverter.fromProto`; the adapter returns a substitute `Result` built from those cells on bypass. Regression: `TestDispatchPreAppendResultBypass` (Go) + `preAppendBypassReturnsSubstituteResultFromHookResponseCells` (Java). |
| H6 | High | CI enforced none of SPEC §7's gates. | **CONFIG** — see below. |

## CI gates made real (H6)

- golangci-lint-action bumped `v6 → v7` (v6 cannot run the v2 config / `v2.10.1` binary). `.golangci.yml` `go: 1.22 → 1.24`.
- **Go coverage gate** (`make go-cover`, ≥80% line, excludes generated `.pb.go`) wired into the `go` CI job.
- **JaCoCo** line gate `0.00 → 0.75` (SPEC §7) in `pom.xml`.
- **Integration job** added (`docker-compose` HBase + the 8 `*IT` make targets) on push-to-main / PR-to-main / nightly / dispatch, across an HBase-version matrix (2.5.0, 2.5.11).
- **Fuzz**: `make fuzz` (`FuzzDecode`) smoke on every CI; 10m nightly.
- **Bench** job nightly (region-concurrency); `schedule:` trigger added.

## Coverage — SPEC §7 targets MET ✅

Both coverage gates are now set to the SPEC §7 values and pass (generated
protobuf excluded from measurement):

- **Go**: **82.5%** hand-written line coverage ≥ 80% gate (`make go-cover`).
  `pkg/hbasecop` 34% → 86.6% via the `Unimplemented*` no-op reflection test,
  `loadShmemConfigFromEnv`/`Run*` guard + config-error tests, the `RunMaster/
  RunRegionServer/RunWAL/RunBulkLoad` end-to-end shmem-loop test, and
  `parseFlags` tests. `internal/*` already strong (wire 95.8%, multiplex 95.3%,
  shmem 83.1%).
- **Java**: **87.8%** line coverage ≥ 0.75 JaCoCo gate (`pom.xml`).
  `bridge.observer` 33.9% → 89.3% via unit tests covering every
  RegionObserverAdapter hook (stub + payload), the Master/RegionServer/WAL/
  BulkLoad adapters, and MuxHookDispatcher (response/error/timeout-cancel
  paths). Other packages 80–91%.

**Earlier ratchet history (resolved).** Setting the JaCoCo gate to 0.75 before
the tests existed broke the build (the bridge jar is produced via `mvn install`,
whose `verify` phase runs `jacoco-check`, so a 0.75 gate failed
`counter-observer-jar` and made the integration tests un-runnable). It was
therefore first ratcheted to 0.55, and now raised to the SPEC 0.75 once the
coverage was actually there. Both gates are real, enforced, and green.

## Still open (from the review, not in the must-fix nine)

- Cross-language *byte-parity* contract test for the 143 hook messages (today each side only self-round-trips).
- Missing `TestHookIDMatchesProtoEnum` Go↔proto parity guard; no full-coverage parity test for Master/RegionServer/WAL/BulkLoad surfaces.
- post-hooks block synchronously (SPEC §3 fire-and-forget) — design change.
- `MutationConverter` drops mutation-level attributes (cellVisibility/ACL/TTL); Get/Scan drop per-CF time ranges.
- ~~Docs: README still "bootstrap"; `docs/architecture.md` missing; `concurrency.md` documents an unbuilt actor model; examples mismatch (T72–T75).~~ **Resolved:** T72 (audit-observer) + T73 (ttl-validator) shipped with ITs; T74 README rewritten with the config-defaults reference (SPEC §8); T75 `docs/architecture.md` describes the as-built flow and `concurrency.md` is banner-marked as future design.
- Busy-spin in the Go reader/writer; latency/WAL bench targets (T81/T82) not asserted; soak (T84).
