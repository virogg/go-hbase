# v0.1.0 — RELEASE BLOCKED

> Status: **BLOCKED.** A specification-driven review found two Critical and a
> set of High defects in shipped code, plus CI quality gates that enforced
> nothing. T85 (release) must not proceed until the items below are resolved
> and re-verified with integration + fuzz actually running.

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
| H12 | High | Bypassed `preAppend`/`preIncrement` returned `null` Result (HookResponse carries no value), silently dropping a value-substituting bypass. | **PARTIAL** — made explicit (WARN) and documented via `applyValueReturningHookResponse`. Follow-up: add a result payload to `HookResponse` to actually support value-substituting bypass. |
| H6 | High | CI enforced none of SPEC §7's gates. | **CONFIG** — see below. |

## CI gates made real (H6)

- golangci-lint-action bumped `v6 → v7` (v6 cannot run the v2 config / `v2.10.1` binary). `.golangci.yml` `go: 1.22 → 1.24`.
- **Go coverage gate** (`make go-cover`, ≥80% line, excludes generated `.pb.go`) wired into the `go` CI job.
- **JaCoCo** line gate `0.00 → 0.75` (SPEC §7) in `pom.xml`.
- **Integration job** added (`docker-compose` HBase + the 8 `*IT` make targets) on push-to-main / PR-to-main / nightly / dispatch, across an HBase-version matrix (2.5.0, 2.5.11).
- **Fuzz**: `make fuzz` (`FuzzDecode`) smoke on every CI; 10m nightly.
- **Bench** job nightly (region-concurrency); `schedule:` trigger added.

## Coverage reality (must raise before release)

Enforcing the gates surfaced a genuine pre-existing gap:

- **Go**: hand-written line coverage is **57.6%** (excl. generated code) — below the 80% gate. Strong where it counts (`internal/wire` 95.8%, `internal/multiplex` 95.3%, `internal/shmem` 83.1%, `internal/cpruntime` 76.9%) but `pkg/hbasecop` is ~34% (the `RunX` env-bootstrap entrypoints + several observer/hooktable files are untested).
- **Java**: **57.3%** (JaCoCo, generated protobuf excluded; 215 unit tests pass). Strong hand-written packages (supervisor 91%, wire 91%, shmem 91%, multiplex 82%, bridge 80%) but `bridge.observer` is **33.9%** — the big adapters are exercised by the docker **integration ITs**, which `mvn verify` (unit phase) does not run. So the JaCoCo unit gate understates real coverage of the adapters.

Both coverage gates are set to the SPEC values and currently FAIL (~57%). Options to resolve (team decision): (a) add adapter / `RunX` unit tests to reach the gate; (b) measure coverage over the integration job too (the adapters are IT-covered); (c) ratchet the gate at the current level and raise it over time. Until then the coverage gates are RED — correct for a blocked release; the rest of CI (build, unit tests, race, lint, fuzz) is green.

Raising coverage to the gates is a separate, substantial task (not part of the
blocker fixes). Until it lands, the coverage gates are RED — which is correct
for a blocked release.

## Still open (from the review, not in the must-fix nine)

- Cross-language *byte-parity* contract test for the 143 hook messages (today each side only self-round-trips).
- Missing `TestHookIDMatchesProtoEnum` Go↔proto parity guard; no full-coverage parity test for Master/RegionServer/WAL/BulkLoad surfaces.
- post-hooks block synchronously (SPEC §3 fire-and-forget) — design change.
- `MutationConverter` drops mutation-level attributes (cellVisibility/ACL/TTL); Get/Scan drop per-CF time ranges.
- Docs: README still "bootstrap"; `docs/architecture.md` missing; `concurrency.md` documents an unbuilt actor model; examples mismatch (T72–T75).
- Busy-spin in the Go reader/writer; latency/WAL bench targets (T81/T82) not asserted; soak (T84).
