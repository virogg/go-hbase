# go-hbase

Write HBase Observer-coprocessors in **Go**.

A long-lived Go process runs next to each RegionServer and talks to a thin
Java bridge over a lock-free **shared-memory ring buffer** (protobuf frames,
no sockets, no fork-per-call as in Hadoop Streaming). Your domain logic runs
*inside the database*, on every write/read path, in Go.

```
HBase client ──RPC──▶ RegionServer
                        │  Java bridge (this repo): serialize hook → shmem ring
                        ▼
                      Go process (your observer, pkg/hbasecop SDK)
```

> **Status: pre-release.** Implemented through Phase 7 of
> [`tasks/plan.md`](tasks/plan.md); v0.1.0 is gated on
> [`tasks/RELEASE-BLOCKERS.md`](tasks/RELEASE-BLOCKERS.md).
> Targets HBase **2.5.x**, Java 11, Linux x86-64 only.

- Full spec: [`SPEC.md`](SPEC.md) · architecture: [`docs/architecture.md`](docs/architecture.md)
- IPC primitive: [`virogg/java-go-shmem`](https://github.com/virogg/java-go-shmem)

## Install from a release

Each [GitHub release](https://github.com/virogg/go-hbase/releases) ships two
artifacts:

- `hbasecop-bridge-<version>.jar`: the Java bridge your coproc-jar shades in
  (install into your Maven repo: `mvn install:install-file
  -Dfile=hbasecop-bridge-<version>.jar -DgroupId=com.virogg
  -DartifactId=hbasecop-bridge -Dversion=<version> -Dpackaging=jar`).
- `hbasecop-build-linux-amd64`: the packaging CLI; `chmod +x` and use it as
  `hbasecop-build` in step 3 below.

The Go SDK is fetched as a normal module:
`go get github.com/virogg/go-hbase/pkg/hbasecop@v<version>`. To build
everything from source instead, follow the quick start.

## Quick start: your first observer in ~5 minutes

Prereqs: Go >= 1.24, JDK 11, Maven, Docker (for the dev cluster), Linux x86-64.

**1. Clone + bootstrap** (the shmem dependency is a git submodule):

```bash
git clone --recursive https://github.com/virogg/go-hbase
cd go-hbase
make deps          # go mod download + installs java-go-shmem into ~/.m2
make all           # lint + build + test, both languages
```

**2. Write the Go observer.** Embed `UnimplementedRegionObserver`, override
only the hooks you need:

```go
package main

import (
    "context"
    "log/slog"
    "os"

    "github.com/virogg/go-hbase/pkg/hbasecop"
)

type myObserver struct {
    hbasecop.UnimplementedRegionObserver
}

func (myObserver) PrePut(
    _ context.Context,
    env hbasecop.ObserverEnv,
    mut *hbasecop.MutationProto,
) (hbasecop.HookResult, error) {
    slog.Info("prePut", "table", env.TableName)
    // return an error → strict policy aborts the client's write;
    // return HookResult{Bypass: true} → HBase skips its own implementation.
    return hbasecop.HookResult{}, nil
}

func main() {
    if err := hbasecop.Run(myObserver{}); err != nil {
        slog.Error("fatal", "err", err)
        os.Exit(1)
    }
}
```

`hbasecop.Run` blocks for the life of the coprocessor; configuration comes
from environment variables set by the Java supervisor. Master / RegionServer
/ WAL / BulkLoad surfaces use `RunMaster` / `RunRegionServer` / `RunWAL` /
`RunBulkLoad`.

**3. Build the ELF and pack the coproc-jar** with the `hbasecop-build` CLI:

```bash
GOOS=linux GOARCH=amd64 go build -o myobserver ./path/to/your/observer
mvn install -DskipTests   # publishes the bridge jar into ~/.m2

go run ./cmd/hbasecop-build \
  --go-bin         ./myobserver \
  --bridge-jar     ~/.m2/repository/com/virogg/hbasecop-bridge/0.0.1-SNAPSHOT/hbasecop-bridge-0.0.1-SNAPSHOT.jar \
  --observer-class com.virogg.hbasecop.examples.counter.CounterRegionObserver \
  --coproc-id      my-observer \
  --out            my-observer.jar
```

The CLI shades the bridge, embeds your ELF at
`bin/linux-amd64/hbasecop-runtime`, and writes its SHA-256 into the manifest
(`HbaseCop-Go-Bin-SHA256`); the supervisor verifies the digest before exec
and refuses a corrupted or wrong-arch binary.

`--observer-class` is the Java `RegionCoprocessor` HBase instantiates. The
examples each ship a small delegating class (~30 lines of boilerplate; see
[`examples/counter-observer`](examples/counter-observer)); reuse one or copy
it under your own package name into the jar.

**4. Deploy on a table:**

```bash
make hbase-up      # dockerized HBase 2.5 standalone with a /coproc-jars bind-mount
cp my-observer.jar test/integration/coproc-jars/
```

```java
admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf))
    .setCoprocessor(CoprocessorDescriptorBuilder
        .newBuilder("com.your.pkg.MyObserver")
        .setJarPath("file:///coproc-jars/my-observer.jar")
        .build())
    .build());
```

Every Put on that table now flows through your Go code. To see it working
end-to-end right away: `make test-integration` (counter example, 100 Puts →
100 Go-side hook invocations, asserted).

## Examples

| Example | Hooks | Demonstrates | IT |
|---|---|---|---|
| [`counter-observer`](examples/counter-observer) | PrePut | minimal observer, log-based assertion | `make test-integration` |
| [`audit-observer`](examples/audit-observer) | PostPut/PostDelete | best-effort post-hook audit, payload privacy | `make test-integration-audit` |
| [`ttl-validator`](examples/ttl-validator) | PrePut | strict validation → client IOException | `make test-integration-ttl` |
| [`filter-observer`](examples/filter-observer) | PreGetOp/Scan/Batch/Flush/Compact | read-path bypass, storage hooks | `make test-integration-read` |
| [`fault-observer`](examples/fault-observer) | PrePut/PostPut | crash/hang/OOM injection (fault matrix) | `make test-fault` |
| [`master-policy-observer`](examples/master-policy-observer) | PreCreateTable | MasterObserver policy veto | `make test-integration-master` |
| [`rs-policy-observer`](examples/rs-policy-observer) | PreRollWALWriterRequest | RegionServerObserver | `make test-integration-rs` |

## Configuration reference

All keys are read from the HBase `Configuration` (`hbase-site.xml` or table
descriptor). Defaults are what ships; every timeout/buffer is configurable
(SPEC §8).

### Failure policy (per hook)

| Key | Values / default |
|---|---|
| `hbasecop.policy.<hook>` (e.g. `hbasecop.policy.prePut`) | `strict` \| `best-effort`. Default: `pre*` → **strict**, `post*` → **best-effort**, anything else → strict |
| `hbasecop.timeout.<hook>` | Hadoop duration (**include the unit**: `500ms`, `2s`). Per-hook wait for the Go response |
| `hbasecop.timeout.default` | Fallback timeout. Default **5s** |

**strict**: Go error / timeout / process-down → `IOException` to the client,
operation aborts. **best-effort**: WARN in the RegionServer log, hook is a
no-op, operation proceeds. Note: hooks whose HBase signature cannot throw
(`postOpen`, `postClose`, `postCompactSelection`) are effectively
best-effort regardless of configuration.

### Supervisor: heartbeat, restart

| Key | Default | Meaning |
|---|---|---|
| `hbasecop.heartbeat.period` | `500ms` | Go→Java heartbeat interval |
| `hbasecop.heartbeat.miss-threshold` | `3` | consecutive misses → SIGKILL + restart |
| `hbasecop.restart.initial-delay` | `200ms` | first restart backoff (doubles each failure) |
| `hbasecop.restart.max-delay` | `5s` | backoff cap (jitter ±20%) |
| `hbasecop.restart.max-fails` | `5` | consecutive failures → mark unhealthy |
| `hbasecop.restart.probe-interval` | `30s` | restart probe cadence while unhealthy |
| `hbasecop.restart.deadline` | `3s` | how long calls issued during a crash wait for the restart before failing by policy |

Crash detection and auto-restart run even when heartbeats are disabled.

### Go process environment (set by the supervisor)

`HBASECOP_SHMEM_IN_PATH`, `HBASECOP_SHMEM_OUT_PATH` (mmap ring files),
`HBASECOP_RING_CAPACITY` (slots, default 16), `HBASECOP_RING_MAX_OBJECT_SIZE`
(bytes/slot, default 1 MiB), `HBASECOP_HEARTBEAT_MS`. The Go SDK logs JSON
via `slog` to stderr; the bridge forwards each line into the RegionServer log.

## FAQ / troubleshooting

**What happens when my Go observer crashes or hangs?**
The supervisor detects exit immediately (and hang via missed heartbeats →
SIGKILL), restarts with exponential backoff, and fails the in-flight hooks by
policy: strict callers get an `IOException`; best-effort callers proceed. New
calls during the restart window wait up to `hbasecop.restart.deadline`. After
`max-fails` consecutive failed restarts the coprocessor is marked unhealthy
and probed every `probe-interval`. The fault-injection matrix
(`make test-fault`) asserts no data loss and no double-apply across kill -9 /
hang / exit / OOM in both policies.

**A panic in my Go callback?**
Recovered by the SDK and returned as a hook error (policy applies). It never
kills the shared Go process.

**`ELF SHA-256 mismatch` at startup?**
The jar's embedded Go binary doesn't match its manifest digest: corrupted
jar or a stale/mixed build. Rebuild with `hbasecop-build` (it writes the
digest) and redeploy. This check is corruption/wrong-arch protection, not a
signature scheme.

**`classpath resource not found: bin/linux-amd64/hbasecop-runtime`?**
The coproc-jar has no embedded ELF: pack with `hbasecop-build` (or the
example Maven setup), and build the ELF with `GOOS=linux GOARCH=amd64`.

**My Put fails with `RetriesExhaustedWithDetailsException`.**
That's strict policy working: a pre-hook returned an error (see the
RegionServer log for the Go-side reason). Validation failures are
deterministic; the client retries won't change the outcome.

**Which hooks can `Bypass` / substitute a result?**
`HookResult{Bypass: true}` maps to `ObserverContext.bypass()` where HBase 2.5
allows it (a WARN is logged where it doesn't). `PreAppend`/`PreIncrement`
(and `*AfterRowLock`) substitute the client-visible `Result` from
`HookResult.ResultCells` instead. `PreScannerOpen` emulates bypass by
constraining the scan to an empty range. Batch hooks use
`HookResult.BlockedIndices` to fail individual mutations.

**Sensitive data in logs?**
The framework never logs row keys or cell values at default level (SPEC §8),
and forwards your observer's stdout/stderr into the RegionServer log at INFO;
don't print payloads. See `examples/audit-observer` for the
digest-instead-of-key pattern.

**How many Go processes per RegionServer?**
One per coproc-jar (`SharedRuntime` refcounts it across all regions/tables
using that jar on the RS). Multiple distinct jars each get their own process
and ring pair.

## Repository layout

```
pkg/hbasecop/        public Go SDK (the only public Go package)
internal/            wire codec, shmem wrapper, multiplexer, event loop
java/com/virogg/     Java bridge: adapters, supervisor, mux, shmem
cmd/hbasecop-build/  coproc-jar packer CLI
proto/               canonical .proto (wire, hooks, vendored HBase)
examples/            seven runnable observers (see table above)
test/integration/    dockerized HBase 2.5 + *IT tests
docs/                architecture, benches, coverage matrix
```

## Development

```bash
make help                # every target, annotated
make all                 # lint + build + test (both languages)
make go-cover            # Go coverage gate
make fuzz FUZZTIME=30s   # wire-codec fuzzer
make hbase-up            # dev cluster on localhost:16010
make test-integration    # counter example end-to-end
```

CI runs structure/license checks, Go (lint+race+coverage+fuzz), Java
(spotless+tests+JaCoCo), the cross-language golden-corpus contract job, and
(on main/nightly) the full integration matrix on HBase 2.5.0 and 2.5.11.

## License

Apache 2.0. See [`LICENSE`](LICENSE).
