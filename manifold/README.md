# manifold

Optional scale-out path: real [Manifold](https://github.com/discord/manifold) (Elixir)
routes dispatch messages to Go nodes that run `fold` against shared Postgres.

**Division of labor**

- **fold / Postgres `fold_partition_owners`** — authoritative for which logical
  `NodeID` (`go0`, `go1`, …) may Claim and deliver a partition.
- **Manifold** — unmodified Hex package; performs cross-node Erlang distribution
  sends to the Go `fold_dispatch` PID chosen by the orchestrator.
- **Live handoff** — `BeginHandoff` / `CompleteHandoff` on Postgres; the
  orchestrator re-reads ownership so subsequent Manifold routes follow the new
  owner. No auto-rebalance.

```bash
# Requires: epmd, Elixir, Go, Postgres (FOLD_PG_DSN or default fold_test)
./run.sh            # fan-out proof across both nodes
./run_handoff.sh    # graceful partition handoff go0 → go1 over Manifold
```

See the root README for architecture notes (JSON wire format; untested Manifold
pack/offload modes).
