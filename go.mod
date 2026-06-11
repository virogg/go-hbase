module github.com/virogg/go-hbase

go 1.24.3

require (
	github.com/viroge/go-shmem v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.36.11
)

// The upstream module declares `github.com/viroge/go-shmem` even though it
// lives at github.com/virogg/java-go-shmem (note the upstream typo and
// /go/ subdir). We carry it as a git submodule under third_party/ and
// resolve via a local replace; see docs/dep-shmem.md (T14) for the
// version pin and update procedure.
replace github.com/viroge/go-shmem => ./third_party/java-go-shmem/go
