# cursor.md

## What this is

`fold` — a Go library for webhook delivery. A host app imports it, hands it an event and a list of subscriber URLs, and it handles delivery: in parallel across subscribers, in order per subscriber, with retries and durable storage.

The one thing that makes it different: subscribers hash onto a fixed set of partitions, and each worker owns a slice of those partitions. A given subscriber always lands on the same worker, so their events go out in sequence, while other subscribers are delivered in parallel. Ordering and throughput at the same time, instead of one or the other.

Everything else is table stakes. The ordering guarantee is the product.

Design decisions and their rationale live in `DECISIONS.md`. Read it before proposing anything that contradicts it.

## Layout

```
fold.go              New, Dispatcher, Dispatch, Start, Close, Resize, Config
types.go             Event, Subscriber, DeliveryStatus
options.go           Config fields / functional options
worker.go            pool, claim loop, head-of-line interaction
partition.go         hash, ownership map, resize/drain
deliver.go           HTTP POST, timeouts, signing
doc.go               package docs + ordering claim
store/
  store.go           Store interface + Delivery record
  memory/            map + mutex default
  postgres/          SKIP LOCKED implementation + schema.sql
internal/
  backoff/
  hash/
bench/
  naive/             unordered baseline for comparison
  fifo/              strict single-threaded FIFO baseline
spike/               throwaway experiments; never imported by library code
manifold/            optional Manifold scale-out (Elixir orch + Go nodes); separate go.mod
```

Organized by responsibility, not by feature. Keep the root package small and host-facing.

## Rules

1. **Never serialize in the worker path.** The payload is encoded once in `Dispatch` and the bytes are read-only afterward. No `json.Marshal` in `worker.go` or `deliver.go`.
2. **Never make HTTP calls on the `Dispatch` path.** The caller returns as soon as rows are written to the store. Delivery is the workers' job.
3. **Never claim outside owned partitions, and never bypass the head-of-line gate.** Exclusive partition ownership is the ordering guarantee across workers: one subscriber → one partition → one owner. The HOL gate is also load-bearing in ordinary operation — it covers two cases (see DECISIONS D7): (1) same-owner skip-ahead during retry backoff, when sequence N is pending on backoff and N+1 becomes due; (2) resize handoff, when the old owner may still have in-flight work while the new owner begins claiming. It is not resize-only and must not be treated as skippable outside a resize window. Skipping ownership or the gate silently breaks the premise.
4. **Never mark a delivery terminal without checking the generation stamp.** A worker drained during a resize must not complete work under a stale ownership map.
5. **`store/postgres` and `store/memory` must not import the root package.** Storage backends stay optional so core stays light.
6. **Commit incrementally as work lands.** Prefer many small, coherent commits over one huge commit at the end of a session. Each commit should be one feature, fix, or logical step — scoped and atomic — so history reads as a sequence of meaningful changes. Commit when a unit of work is done and makes sense on its own (e.g. store interface before the memory impl, memory before workers, workers before retries), not when the whole task is finished. No drive-by refactors bundled with functional changes.

Rules 1, 2, 4, and 5 are enforced by CI (`scripts/check-invariants.sh` + tests), not by review. A green build means they held; do not weaken the checks to make a change pass.

## Definition of done

The ordering test must run with at least two workers and a transport that can hold a delivery open long enough for a peer to claim a later sequence for the same subscriber. It must assert zero inversions with head-of-line enforcement on, and nonzero with it off. Run it after any change to `worker.go`, `partition.go`, or either store implementation. That test uses `OverlapPartitions` (not a production path). The gate's production jobs are same-owner retry skip-ahead and resize handoff (DECISIONS D7); the resize test covers handoff, and disabling HOL under exclusive ownership with failure injection covers retry skip-ahead.

A test for an invariant must be shown to fail when the invariant is removed. Before trusting a new invariant test, disable the mechanism it guards, confirm the test fails, then restore it and confirm it passes. Report both outputs. A test that passes either way proves nothing and is worse than no test, because it looks like coverage.

Do not report a task as finished based on the code looking correct. Run the test, and say what it output.

If a change makes the ordering test fail, that is the change being wrong — not the test needing adjustment. Do not relax the assertion.

## Current phase

v1 complete (single process). Phase two: optional Manifold scale-out under `manifold/` (DECISIONS D12). Elixir orchestrator routes with real Manifold; Go nodes run fold against shared Postgres with disjoint `PartitionsClaim`. v1 public Dispatch API unchanged.

The Go↔Erlang distribution bridge is proven (Ergo + `erlang23` on OTP 29). Ownership identity stays an opaque string; multi-node uses `IDPrefix` + `PartitionsClaim`.

## Working notes

- `DECISIONS.md` records product decisions and rejected alternatives. Append to it when a real decision gets made; don't rewrite history.
- `FRICTION-LOG.md` records observations about porting Manifold's design. Add to it when something surprising happens, while it's fresh.