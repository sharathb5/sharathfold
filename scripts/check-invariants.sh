#!/usr/bin/env bash
# Enforce AGENTS.md bans that agents can otherwise violate with plausible code.
# Exit non-zero on any violation. Invoked by CI; safe to run locally.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

module="$(go list -m)"
failed=0

say_fail() {
  echo "invariant FAIL: $*" >&2
  failed=1
}

# --- Rule 5: store backends must not import the root package ----------------
# Cheapest and most mechanical. Walk the import graph with go list -deps.
for pkg in store/memory store/postgres; do
  if [[ ! -d "$pkg" ]]; then
    continue
  fi
  if ! go list "./$pkg" >/dev/null 2>&1; then
    continue
  fi
  if go list -deps -f '{{.ImportPath}}' "./$pkg" | grep -Fxq "$module"; then
    say_fail "./$pkg depends on root package $module (store backends must stay optional)"
  else
    echo "invariant OK: ./$pkg does not depend on $module"
  fi
done

# --- Rule 1: no serialization in the worker path ----------------------------
# Payload is encoded once in Dispatch; worker.go / deliver.go must not re-encode.
serialize_files=()
for f in worker.go deliver.go; do
  if [[ ! -f "$f" ]]; then
    say_fail "missing $f"
    continue
  fi
  serialize_files+=("$f")
done
if ((${#serialize_files[@]} > 0)); then
  if go run ./scripts/checkserialize "${serialize_files[@]}"; then
    echo "invariant OK: no json serialization in ${serialize_files[*]}"
  else
    failed=1
  fi
fi

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
echo "invariant OK: all mechanical checks passed"
