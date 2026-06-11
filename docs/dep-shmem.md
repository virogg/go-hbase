# Dependency: `java-go-shmem`

This document records the integration of [virogg/java-go-shmem][upstream] —
the lock-free shared-memory ring used for Go↔Java IPC in this project — and
the procedure for pinning and updating it.

[upstream]: https://github.com/virogg/java-go-shmem

## Decision: git submodule + local install

The upstream library is not published to Maven Central (its Java artifact is
`com.jgshmem:java-go-shmem:1.0.0-SNAPSHOT`) and the Go module declares
`github.com/viroge/go-shmem` from a subdirectory layout
(`/go/`) — so neither side resolves cleanly with stock `go get` / Maven
Central.

We carry the upstream as a **git submodule** under
`third_party/java-go-shmem/` and wire each language side against the local
copy:

- **Go**: `go.mod` declares `require github.com/viroge/go-shmem` and
  `replace github.com/viroge/go-shmem => ./third_party/java-go-shmem/go`.
  Imports use the upstream module path (`github.com/viroge/go-shmem/pkg/ring`).
  No publish or vendoring step is required.
- **Java**: `make deps-shmem` runs `mvn install` on the submodule's pom and
  drops `com.jgshmem:java-go-shmem:1.0.0-SNAPSHOT` into `~/.m2`. The bridge
  pom then depends on it like any other Maven artifact.

### Why not …

- **Vendoring** (copying source into `third_party/` without git history):
  loses the upstream SHA pin and the easy update path. Submodules are the
  smallest tool that records exactly which commit we built against.
- **Maven Central**: not available; upstream is `*-SNAPSHOT`, no GA release.
- **GitHub Packages Maven repo**: would require credentials at every clone
  and CI run for a single SNAPSHOT artifact. Disproportionate.

## Pin

Current submodule commit: see
[`.gitmodules`](../.gitmodules) and
[`git submodule status third_party/java-go-shmem`](#) — the pinned SHA is
the source of truth, not anything written here.

The upstream `viroge` vs `virogg` mismatch in the Go module path (and the
fact that the Go module lives at `/go/`, not the repo root) means a direct
`go get github.com/virogg/java-go-shmem/...` will not work; the replace
directive is load-bearing.

## How to bump

```sh
git submodule update --remote third_party/java-go-shmem
# inspect the diff in third_party/java-go-shmem to confirm the change set
git -C third_party/java-go-shmem log --oneline -5

make deps-shmem          # reinstall the new SNAPSHOT into ~/.m2
make all                 # confirm both languages still build and test

git add third_party/java-go-shmem
git commit -m "deps: bump java-go-shmem to <sha>"
```

If the upstream's Go module path changes from `github.com/viroge/go-shmem`,
update `go.mod`'s `require`/`replace` lines accordingly.

## How a fresh checkout works

```sh
git clone --recurse-submodules https://github.com/virogg/go-hbase.git
# or, if you already cloned without submodules:
git submodule update --init --recursive

make deps-shmem    # one-time per ~/.m2; CI runs this on every job
make all
```

## CI integration

Both the `go` and `java` jobs in `.github/workflows/ci.yml` use
`actions/checkout@v4` with `submodules: recursive`. The `java` job runs
`make deps-shmem` before `make java-test` so the local SNAPSHOT is in the
Maven cache when the bridge build runs.
