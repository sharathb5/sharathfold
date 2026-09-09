#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
epmd -daemon 2>/dev/null || true
(cd "$ROOT/go" && go build -o go-node .)
"$ROOT/go/go-node" &
GO_PID=$!
trap 'kill $GO_PID 2>/dev/null || true' EXIT
sleep 0.5
(cd "$ROOT/elixir" && elixir --sname elixir --cookie spike -S mix run -e 'ManifoldBridge.run()')
