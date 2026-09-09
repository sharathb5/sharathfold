#!/usr/bin/env bash
# Smallest Manifold↔fold e2e: Elixir orchestrator + two Go nodes + shared Postgres.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
DSN="${FOLD_PG_DSN:-postgres://localhost/fold_test?sslmode=disable}"
COOKIE=fold

epmd -daemon 2>/dev/null || true

echo "==> ensure DB reachable"
psql "$DSN" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null

echo "==> build Go nodes"
(cd "$ROOT/go" && go mod tidy && go build -o fold-node .)

PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT

echo "==> migrate schema via go0, then truncate"
"$ROOT/go/fold-node" -name go0@localhost -cookie "$COOKIE" -node 0 -nodes 2 -dsn "$DSN" -record >/tmp/fold-go0-migrate.log 2>&1 &
MIG=$!
sleep 0.6
kill "$MIG" 2>/dev/null || true
wait "$MIG" 2>/dev/null || true
psql "$DSN" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE fold_deliveries, fold_subscribers, fold_subscriber_seq CASCADE;
SQL

echo "==> start go0 + go1"
"$ROOT/go/fold-node" -name go0@localhost -cookie "$COOKIE" -node 0 -nodes 2 -dsn "$DSN" -record -http :18080 &
PIDS+=($!)
"$ROOT/go/fold-node" -name go1@localhost -cookie "$COOKIE" -node 1 -nodes 2 -dsn "$DSN" -record -http :18081 &
PIDS+=($!)
sleep 0.8

echo "==> elixir deps + Manifold fan-out"
(cd "$ROOT/elixir" && mix deps.get)
(cd "$ROOT/elixir" && elixir --sname orch --cookie "$COOKIE" -S mix run -e 'FoldOrch.run(events: 5)')

echo "==> node delivery counts"
python3 - <<'PY'
import json, urllib.request
from collections import defaultdict
for name, port in [("go0", 18080), ("go1", 18081)]:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/deliveries") as r:
        d = json.load(r)
    by_sub = defaultdict(list)
    for x in d:
        by_sub[x["subscriber_id"]].append(x["sequence"])
    inversions = 0
    for sid, seqs in by_sub.items():
        for a, b in zip(seqs, seqs[1:]):
            if b <= a:
                inversions += 1
    print(f"{name}: deliveries={len(d)} subscribers={sorted(by_sub)} inversions={inversions}")
    if inversions:
        raise SystemExit(f"ordering broken on {name}")
PY

echo "==> e2e OK"
