#!/usr/bin/env bash
# Copyright 2026 The go-hbase Authors
# SPDX-License-Identifier: Apache-2.0

# Apache 2.0 license-header check / inserter.
#
# Modes:
#   --check (default)  — exit 1 if any tracked source file is missing the
#                         SPDX header. Used by CI and `make license-check`.
#   --fix              — prepend the appropriate per-language header to every
#                         source file that does not yet have it.
#
# Header template lives in tools/license-header.txt (single line of text per
# line, no comment markers). Markers are added per file extension below.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_FILE="$ROOT/tools/license-header.txt"

mode="check"
case "${1:-}" in
  ""|--check) mode="check" ;;
  --fix)      mode="fix" ;;
  *) echo "usage: $0 [--check|--fix]" >&2; exit 2 ;;
esac

if [[ ! -f "$TEMPLATE_FILE" ]]; then
  echo "missing template: $TEMPLATE_FILE" >&2
  exit 2
fi

# Marker line we look for to decide presence.
MARKER="SPDX-License-Identifier: Apache-2.0"

# Files to inspect. find ... -prune to skip generated/vendored trees fast.
mapfile -t TARGETS < <(
  cd "$ROOT" && find . \
      \( -path ./.git -o -path ./target -o -path ./bin -o -path ./.idea \
         -o -path ./.vscode -o -path ./.serena -o -path ./.claude \
         -o -path ./memory -o -path ./tasks -o -path ./node_modules \
         -o -path ./third_party \) -prune \
      -o -type f \( \
            -name '*.go' -not -name '*.pb.go' \
         -o -name '*.java' -not -name '*Pb.java' \
         -o -name '*.proto' \
         -o -name '*.sh' \
         -o -name 'pom.xml' \
         -o -path './.github/workflows/*.yml' \
      \) -print | sort
)

# Build header bodies for each comment style.
make_block() {
  local prefix="$1" suffix="${2:-}"
  while IFS= read -r line; do
    if [[ -z "$line" ]]; then
      printf '%s\n' "${prefix%% }"
    else
      printf '%s%s%s\n' "$prefix" "$line" "$suffix"
    fi
  done < "$TEMPLATE_FILE"
}

style_for() {
  local f="$1"
  case "$f" in
    *.go|*.java|*.proto) echo "slash" ;;
    *.sh|*.yml)          echo "hash" ;;
    pom.xml)             echo "xml"  ;;
    *)                   echo ""     ;;
  esac
}

block_for_style() {
  case "$1" in
    slash) make_block "// " "" ;;
    hash)  make_block "# "  "" ;;
    xml)   echo "<!--"; while IFS= read -r line; do printf '  %s\n' "$line"; done < "$TEMPLATE_FILE"; echo "-->" ;;
  esac
}

missing=()
for rel in "${TARGETS[@]}"; do
  [[ -z "$rel" ]] && continue
  rel="${rel#./}"
  abs="$ROOT/$rel"
  # Skip files that are intentionally header-less.
  case "$rel" in
    LICENSE|*/.gitkeep) continue ;;
  esac

  # Already has the marker anywhere in the first 20 lines? OK.
  if head -20 "$abs" | grep -Fq "$MARKER"; then
    continue
  fi

  if [[ "$mode" == "check" ]]; then
    missing+=("$rel")
    continue
  fi

  style="$(style_for "$rel")"
  [[ -z "$style" ]] && continue

  tmp="$(mktemp)"
  block_for_style "$style" > "$tmp"

  # For shell scripts and YAML keep the shebang/yaml-doc-start as the first line.
  first_line="$(head -1 "$abs")"
  if [[ "$first_line" == '#!'* ]] || [[ "$first_line" == '---' ]]; then
    {
      echo "$first_line"
      cat "$tmp"
      echo
      tail -n +2 "$abs"
    } > "$abs.new"
  elif [[ "$rel" == "pom.xml" ]] && [[ "$first_line" == '<?xml'* ]]; then
    {
      echo "$first_line"
      cat "$tmp"
      tail -n +2 "$abs"
    } > "$abs.new"
  else
    {
      cat "$tmp"
      echo
      cat "$abs"
    } > "$abs.new"
  fi
  mv "$abs.new" "$abs"
  rm -f "$tmp"
  echo "fixed: $rel"
done

if [[ "$mode" == "check" && ${#missing[@]} -gt 0 ]]; then
  echo "license header MISSING in ${#missing[@]} file(s):" >&2
  printf '  %s\n' "${missing[@]}" >&2
  echo "run: ./tools/license-header.sh --fix" >&2
  exit 1
fi

if [[ "$mode" == "check" ]]; then
  echo "license header OK (${#TARGETS[@]} files inspected)"
fi
