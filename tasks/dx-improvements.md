# go-hbase: DX / UDF-ergonomics improvement plan

> Workstream **DX** (post-Phase-8). Goal: close the gap between today's
> coprocessor SDK and the MapReduce/Hive **UDF ergonomic** — "write one
> function against public typed args; the framework owns packaging,
> distribution, registration, and local testing."
>
> Grounding: a 6-dimension DX review of the tree. Findings on the two
> build/API-critical axes (authoring-API, build & packaging) were
> adversarially verified `grounded:true, feasible:true` against the code.
> Key structural fact: the architecture is already UDF-ready — the
> dispatcher holds all five observer surfaces and routes by `HookID`
> range (`dispatch.go:33-120`), the wire is a language-neutral
> proto-over-shmem ABI, and the coproc-jar manifest already carries
> `observer-class` + `coproc-id`. Most work below is API/packaging sugar,
> **not** a transport rewrite.

## Priority order

| ID | Title | Pri | Effort | Verified |
|----|-------|-----|--------|----------|
| DX1 | Re-export wire types from `pkg/hbasecop` (unblock public SDK) | P0 | S | ✅ high |
| DX2 | Stock generic Java delegate in bridge jar (zero hand-written Java) | P1 | M | ✅ med |
| DX3 | Single `hbasecop package` command (source → coproc-jar) | P1 | L | ✅ high |
| DX4 | Exported in-process test harness `hbasecop/hbasecoptest` (no Docker) | P1 | M | ✅ high |
| DX5 | Func/handler registration façade (`NewRegion().OnPrePut(...)`) | P2 | M | ✅ high |
| DX6 | `Run(...any)` + observer chaining (drop 1-observer/1-surface cap) | P2 | M | ✅ high |
| DX7 | `hbasecop deploy/list/remove` admin CLI + startup config preflight | P2 | L | (unverified) |
| DX8 | Observability: `ObserverEnv.Logger`, per-hook metrics, error taxonomy | P2 | M | (unverified) |
| DX9 | `hbasecop init` scaffolding + per-example READMEs + setup FAQ | P3 | M | (unverified) |

UDF analogy per item: DX1 = public typed args (Hive `Text`); DX2 = framework
owns the task-runner wrapper; DX3 = `hadoop jar … submit`; DX4 = MRUnit /
local mode; DX5 = implement only `map()`; DX6 = `ChainMapper`; DX7 = one
submit/stage command; DX8 = framework counters + `Context`; DX9 =
`mvn archetype:generate`.

---

## DX1 — Re-export wire types (P0, unblocks everything) — DONE

> Caveat: DX1 fixes the type-*naming* blocker. External `go get` of the SDK is
> still blocked separately by the `github.com/viroge/go-shmem` local `replace`
> (unpublished submodule dep) — tracked in RELEASE-BLOCKERS, not DX1.


**Problem.** 66 of 68 region hooks (and all master/RS/WAL/bulk-load hooks —
101 `*Request` types total) are typed as `*hookpb.<Hook>Request`, and
`hookpb`/`hbasepb` live under `internal/` (`observer.go:9-10,91`; module
`github.com/virogg/go-hbase`). Go forbids importing another module's
`internal/`, so an out-of-tree `go get .../pkg/hbasecop` consumer cannot
name the argument type for 66/68 hooks and cannot override them. Only
`MutationProto` is re-exported today (`observer.go:19`), which is why the
README `PrePut` quickstart compiles and hides the gap. In-tree examples
compile only because they live inside the module.

**Approach.** Type-alias re-export (NOT relocating generated packages —
that would break the 18 in-tree importers and touch `go_package`/protoc).
Aliases are the same type as their target, so an external author writing
`func (o) PreGetOp(_ context.Context, _ hbasecop.ObserverEnv, req *hbasecop.PreGetOpRequest)`
satisfies the existing interface method whose signature is `*hookpb.PreGetOpRequest`.

- Add a committed generator `tools/gen-wiretypes` (AST-parses the two
  generated packages) + a `//go:generate` directive.
- Emit `pkg/hbasecop/wiretypes.go` aliasing: every exported `*Request`
  type and `HookContext` from `hookpb`; every exported message type from
  `hbasepb` (Cell/Get/Scan/MutationProto/TableName/…). Exclude pure
  transport types (`HookResponse`, `HookError`, `HookId` — SDK-internal;
  `HookID`/`HookResult` already public).

**AC.** (1) `pkg/hbasecop` exports a usable alias for every `*Request` type
named in any public Observer interface. (2) Generation is reproducible:
`go generate ./pkg/hbasecop` produces no diff on a clean tree. (3) A drift
guard fails if a new `*Request` type appears in `hookpb` without an alias.
**Verify.** `go build ./...`, `go vet ./...`; a parity test enumerating
`hookpb` `*Request` types vs exported aliases; an out-of-module smoke
(doc-example / scratch module) overriding a non-Put hook compiles.

## DX2 — Stock generic Java delegate — DONE (4 surfaces)

> `com.virogg.hbasecop.bridge.entrypoint.Generic{Region,Master,RegionServer,WAL}Observer`
> ship in the bridge jar: name one in `setCoprocessor` and write zero Java. The
> shared `GenericCoprocessor` reads ring sizes / hook+graceful timeouts from the
> host Configuration (documented defaults; `hbasecop.ring.capacity`,
> `hbasecop.ring.max-object-size`, `hbasecop.timeout.default`,
> `hbasecop.shutdown.graceful-timeout`) and keys the SharedRuntime on the
> coproc-jar's **coproc-id** (the previously-dead manifest field), falling back
> to the class name — so two coproc-jars on one RegionServer don't collide on a
> single Go process. Unit-tested (config defaults/overrides, key fallback);
> compiles vs HBase 2.5; spotless-clean.
>
> Deferred: BulkLoad (exposed via RegionCoprocessor.getBulkLoadObserver — couples
> to the one-process-one-surface split, revisit with DX6). End-to-end IT with the
> generic delegate runs in CI (Docker); not exercised locally.

**Problem.** Every observer author hand-writes a ~30–86-line Java
`RegionCoprocessor` delegate (`CounterRegionObserver.java`) that is pure
boilerplate (examples byte-identical after name-normalization); the bridge
jar ships adapters but no concrete instantiable entrypoint. Ring sizes /
timeouts are hand-coded magic numbers that can diverge from the
`HBASECOP_RING_*` env contract and `hbase-site.xml`.

**Approach.** Ship five parameterless entrypoints in the bridge jar
(`com.virogg.hbasecop.GenericRegionObserver` + Master/RegionServer/WAL/
BulkLoad) that derive `SHARED_KEY` from `getClass().getName()` and read all
`CoprocessorRuntime.Config` from `env.getConfiguration()`. Author names the
stock class in `setCoprocessor` and writes **zero** Java.
**AC.** counter example runs end-to-end with no example-specific Java class.
**Verify.** `make test-integration` against the generic delegate.

## DX3 — Single `hbasecop package` command

**Problem.** 5–7 manual, order-dependent, cross-language steps from "Go
func" to deployable jar; `hbasecop-build` needs 5 flags incl. a
hand-pasted `~/.m2` SNAPSHOT bridge path; following the README verbatim
produces a jar naming a class it never bundled (`build.go:46-107` has no
field for the user delegate). Two of five required flags
(`--observer-class`, `--coproc-id`) are dead manifest metadata
(`ManifestBinaryDescriptor` getters unused).

**Approach.** `hbasecop package ./path/to/observer --surface region --out x.jar`:
cross-compile the ELF (correct GOOS/GOARCH; fail fast on wrong arch by
reading the ELF header), resolve the bridge jar from the SDK module
version (no `~/.m2` path), select the DX2 stock delegate per `--surface`,
shade+embed+SHA-256 in one pass. Demote `--observer-class`/`--coproc-id` to
optional/inferred.
**AC.** one command, one jar, no Maven/Java step touched by the user.
**Verify.** packaged jar deploys and the counter IT passes.

## DX4 — Exported test harness (no cluster) — DONE

> `hbasecop.InvokeRegion(obs, hookID, req)` drives the production dispatcher in
> process (env decode + panic recovery + result mapping) and returns the
> decoded HookResult/error. `pkg/hbasecop/hbasecoptest` wraps it with an Env →
> HookContext builder and typed PrePut/PostPut/Invoke helpers, using only public
> aliases (no internal imports), so external authors `go test` with zero Docker.
> Other surfaces (Master/RS/WAL/BulkLoad) + a `make test-local` demo are a
> follow-up.



**Problem.** The only "see it work" path is `make test-integration*`
(Docker + HBase). A full in-process rig exists (`loopHarness`,
`newDispatcher`, `buildRequestFrame` in `*_test.go`) but is package-private;
example tests call `obs.PrePut(...)` directly, bypassing dispatch (env
decode, panic recovery, bypass/blocked-indices, result mapping untestable
from outside).

**Approach.** Export `pkg/hbasecop/hbasecoptest`: input builders for
`MutationProto`/`Get`/`Scan` (no `internal/`) + an in-process `Invoker`
running the real dispatcher and returning the decoded `HookResult` + the
`HookError` code/message the Java side would see. Add a `make test-local`
example target.
**AC.** an observer is unit-testable through dispatch with `go test`, zero
Docker. **Verify.** harness round-trips a PrePut bypass + a panic.

## DX5 — Func/handler registration façade — DONE (Region)

> Done for RegionObserver: `tools/gen-builder` emits `region_builder.go`
> (`NewRegion().OnPrePut(fn)`, 68 hooks, unset = no-op) from the interface, with
> a drift guard + dispatch test + godoc example. The generator is parameterized
> (`-iface`/`-src`), so Master/RS/WAL/BulkLoad builders are a small follow-up
> (parameterize the emitted type/ctor/assertion names).

**Problem.** To handle one hook the author embeds a 68-method interface
(`unimplemented.go`). No `OnPrePut(func...)` registration exists.

**Approach.** Thin builder over `hooktable.go` + `Unimplemented`:
`h := hbasecop.NewRegion(); h.OnPrePut(func(ctx, env, m) (HookResult, error){…}); hbasecop.Run(h)`.
Registry-backed observer; unset slots stay no-op. Keep the interface for
power users; make registration the documented happy path. No transport
change. **AC.** counter rewritten via registration. **Verify.** existing
dispatch tests pass against the registry-backed observer.

## DX6 — Unified `Run` + chaining

**Problem.** `Run(observers ...RegionObserver)` advertises multi-observer
but errors at runtime on `len>1` (`run.go:43-45`, same in all five `Run*`);
one process serves one surface; five near-identical `Run*` bodies (~250
dup lines). The dispatcher already holds all five surfaces.

**Approach.** (a) `hbasecop.Chain(obs...)` folding per-hook results (first
`Bypass` wins; documented `BlockedIndices` merge). (b) collapse to one
`Run(...any)` type-switching each arg onto the matching dispatcher field,
letting one process serve Region+Master+WAL together. Wire/shmem untouched.
**AC.** two region observers chain in one process; one `Run` serves two
surfaces. **Verify.** new chain + multi-surface dispatch tests.

## DX7 — Admin CLI + config preflight (P2)

`hbasecop deploy --jar --table` (disable/alter/enable, FQ class read from
manifest), `hbasecop list`, `hbasecop remove`; `hbasecop config --list` /
`config-check`. Startup preflight in `CoprocessorRuntime.start()` validating
every `hbasecop.*` key for the loaded hook bitmap (unknown → WARN, malformed
→ fail fast) instead of lazy per-hook failure on a live write path.

## DX8 — Observability (P2)

`Logger` on `ObserverEnv` (or `FromContext(ctx)`) pre-tagged hook+req_id+
region; opt-in per-hook metrics sink (count/latency/panic via the
`recoverInvoke` seam, `dispatch.go:150`); stable `HookError` code enum
(ObserverRejected/Panic/DeadlineExceeded/InternalMarshal) replacing the
flat `Code=1` (`dispatch.go:262`); redact panic value before it crosses the
bridge to the client.

## DX9 — Onboarding (P3)

`hbasecop init <name> --surface region` scaffolding (Go main + DX2 wiring +
build target); per-example READMEs; add the missing `wal-observer` row and
reconcile the example count; setup-failure FAQ entries (`--recursive`,
`make deps`, JDK 11, "test without a cluster"). Decide English-canonical
docs + `README.ru.md`.

---

## Open decisions

1. DX1 scope: alias only `*Request`+payloads (curated) vs all exported wire
   types (firehose). Plan picks curated-by-policy in the generator.
2. PrePut/PostPut: keep frozen `*MutationProto`, or unfreeze to
   `*PrePutRequest` so all 68 hooks share one shape (pre-release window).
3. DX2 SharedRuntime key: one generic class keyed by class-name (refcount
   ok?) vs codegen a per-observer subclass.
4. Multi-observer/chain (DX6): land in v0.1.0 or stay post-MVP deferred.
5. Docs language (DX9): English canonical + RU translation, or RU primary.
