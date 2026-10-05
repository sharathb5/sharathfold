#!/usr/bin/env bash
# Graceful cross-process handoff e2e over real Manifold + Erlang distribution.
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

echo "==> truncate fold tables (incl. ownership)"
# Migrate via a short-lived node, then wipe so EnsureOwnerAssignments re-seeds.
"$ROOT/go/fold-node" -name go0@localhost -cookie "$COOKIE" -node 0 -nodes 2 -dsn "$DSN" -record >/tmp/fold-go0-migrate.log 2>&1 &
MIG=$!
sleep 0.8
kill "$MIG" 2>/dev/null || true
wait "$MIG" 2>/dev/null || true
psql "$DSN" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE fold_deliveries, fold_subscribers, fold_subscriber_seq, fold_partition_owners CASCADE;
SQL

echo "==> start go0 + go1 (seed ownership + NodeID)"
"$ROOT/go/fold-node" -name go0@localhost -cookie "$COOKIE" -node 0 -nodes 2 -dsn "$DSN" -record -http :18080 &
PIDS+=($!)
"$ROOT/go/fold-node" -name go1@localhost -cookie "$COOKIE" -node 1 -nodes 2 -dsn "$DSN" -record -http :18081 &
PIDS+=($!)
sleep 1.0

echo "==> ownership seed check"
psql "$DSN" -v ON_ERROR_STOP=1 -c \
  "SELECT owner_node, count(*) FROM fold_partition_owners GROUP BY 1 ORDER BY 1;"

echo "==> elixir handoff e2e (Manifold route A → handoff → route B)"
(cd "$ROOT/elixir" && mix deps.get)
(cd "$ROOT/elixir" && elixir --sname orch --cookie "$COOKIE" -S mix run -e 'FoldOrch.run_handoff()')

echo "==> handoff e2e OK"
