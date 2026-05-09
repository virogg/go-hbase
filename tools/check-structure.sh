#!/usr/bin/env bash
# Verifies that the repository skeleton matches SPEC.md §4.
# Used by T01 acceptance and CI.
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail=0

# Required files (top-level + placeholders).
REQUIRED_FILES=(
  "SPEC.md"
  "README.md"
  "LICENSE"
  "CONTRIBUTING.md"
  "CODE_OF_CONDUCT.md"
  ".gitignore"
  "proto/wire.proto"
  "proto/hooks.proto"
  "pkg/hbasecop/observer.go"
  "pkg/hbasecop/env.go"
  "pkg/hbasecop/run.go"
  "pkg/hbasecop/policy.go"
  "internal/wire/doc.go"
  "internal/multiplex/doc.go"
  "internal/cpruntime/doc.go"
  "internal/shmem/doc.go"
  "cmd/hbasecop-build/main.go"
  "tools/proto-gen.sh"
  "tools/check-structure.sh"
  "pom.xml"
)

# Required directories (must exist even if empty -- enforced via .gitkeep).
REQUIRED_DIRS=(
  "proto/hbase"
  "java/com/virogg/hbasecop/bridge"
  "java/com/virogg/hbasecop/supervisor"
  "java/com/virogg/hbasecop/multiplex"
  "examples/audit-observer"
  "examples/ttl-validator"
  "test/java/com/virogg/hbasecop"
  "test/integration"
  "test/e2e"
)

for f in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "$ROOT/$f" ]]; then
    echo "MISSING FILE: $f"
    fail=1
  fi
done

for d in "${REQUIRED_DIRS[@]}"; do
  if [[ ! -d "$ROOT/$d" ]]; then
    echo "MISSING DIR:  $d"
    fail=1
  fi
done

if [[ $fail -ne 0 ]]; then
  echo "structure check FAILED"
  exit 1
fi

echo "structure check OK"
