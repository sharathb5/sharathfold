# manifold

Optional scale-out path: real [Manifold](https://github.com/discord/manifold) (Elixir)
routes subscriber work to Go nodes that run `fold` against shared Postgres.

Manifold decides **where**; fold decides **when** and handles failure (HOL, retries,
suspend/resume). Go joins the Erlang cluster via Ergo and registers as
`Elixir.Manifold.Partitioner` — no Manifold fork.

```bash
# Requires: epmd, Elixir, Go, Postgres (FOLD_PG_DSN or default fold_test)
./run.sh
```

See the root README for architecture and caveats (static partition slices; JSON
wire format; untested Manifold pack/offload modes).
