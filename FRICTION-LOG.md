# Friction log — Manifold

Notes for a possible conversation with Discord engineers. Kept separate from the project pitch on purpose: the project stands on its own, and this is feedback, not a sales document.

Two kinds of entries, tracked separately because they carry different weight:

- **Porting notes** — what happened translating Manifold's ideas into Go. Available now, since v1 reimplements the mechanisms rather than calling the library.
- **Library notes** — issues hitting the actual Elixir library and the Erlang distribution bridge. Only starts once the scale-out phase begins.

Rules for entries: record it when it happens, not from memory afterward. Include what was expected, what happened, and what it cost. Speculation is fine if labeled as such. An entry that says "this was fine, and here's why I expected it not to be" is worth as much as a complaint.

---

## Porting notes

### P1 — Hashing directly onto worker count doesn't survive resizing
**What happened:** the obvious translation of "hash each subscriber to a worker" is `hash(subscriber) % workerCount`. That remaps nearly every subscriber whenever the worker count changes, which destroys the ordering guarantee at exactly the moment you're trying to scale.

**The fix:** a fixed partition count (256), hash subscribers onto partitions, assign partitions to workers. Rebalancing moves partitions, not subscribers.

**Worth asking Discord:** Manifold partitions PIDs across a fixed pool per node, so this problem may not surface the same way in their topology — the number of partitions is a configured constant rather than something that shifts under load. Curious whether they hit this during development or whether the BEAM's process model sidestepped it.

---

### P2 — Erlang gives Manifold a cluster for free; Go gives nothing
**What happened:** the largest single gap. Manifold gets to be a pure library because Discord's app is already many BEAM nodes with built-in distribution, membership, and messaging. Go has no equivalent, so any multi-machine version requires building coordination from scratch — and that coordination is infrastructure, which conflicts with shipping a library.

**The resolution:** v1 is single-process. Cross-machine is a later, optional path.

**Worth raising:** this is the honest answer to "why doesn't this exist in Go already." The mechanism is portable; the runtime assumptions underneath it are not.

---

### P3 — Destination grouping doesn't transfer, workload partitioning does
**What happened:** initially read Manifold's node-grouping as one idea. It's two. Grouping recipients by destination saves network sends for Discord because many recipients genuinely share a machine. Webhook subscribers are independent external hosts, so that saving evaporates — every subscriber costs one POST regardless.

What does transfer is partitioning the *sending workload* across workers with stable assignment. Same mechanism, different benefit.

**Why it's logged:** worth checking this framing against how Discord thinks about it. If they'd describe the two benefits as one thing, that's interesting; if they'd separate them the same way, that's a useful confirmation.

---

### P4 — Ordering implies head-of-line blocking, and the docs don't dwell on it
**What happened:** strict per-subscriber ordering means a subscriber with a down endpoint blocks their own queue while retries back off. v1 accepts this and uses dead-lettering as the escape valve.

**The question for Discord:** Manifold's ordering guarantee comes from `send/2` semantics through a consistent worker, but Discord's recipients are sessions they control, not third-party endpoints that can be down for an hour. Whether they ever had to reason about head-of-line blocking, or whether the failure model made it moot, would be genuinely informative.

---

### P5 — First ordering test stayed green with the guarantee disabled
**What happened:** disabled the head-of-line gate in the in-memory Claim path and re-ran the concurrent ordering test. It still passed with zero inversions. Sorting candidates by `(subscriber, sequence)` inside Claim, plus a single worker delivering each batch synchronously through an always-succeeding transport, produced attempt order without the gate. The test looked like coverage and wasn't.

**Expected:** removing the mechanism that enforces the invariant should make the invariant test fail.

**Cost:** false confidence until step 2; would have compounded once generation stamps and partition claiming had the same failure mode — plausible code, silent breakage, green tests.

**Lesson (not Manifold-specific):** an ordering test only proves the gate if it can fail without it — at least two workers and a transport that holds a delivery open long enough for a peer to claim a later sequence for the same subscriber. Assert zero inversions with HOL on, nonzero with it off.

---

### P6 — Manifold never faces subscriber endpoint failure the way webhooks do
**What happened:** deciding what to do when a subscriber's endpoint stays down forced a product choice (suspend-and-resume; see DECISIONS D8) that Manifold's design never had to make. Discord's recipients are sessions they own; those sessions don't fail for hours the way a customer's webhook URL does. Once a down endpoint can block a subscriber's queue indefinitely, "dead-letter one row and continue" vs "suspend the subscriber" becomes a real tradeoff between liveness and the ordering claim.

**Expected:** the hard parts of porting would be fan-out mechanics — hashing, partition ownership, serialize-once, offload.

**What actually diverged:** the failure model. The fan-out mechanism transferred; the assumption that recipients stay reachable did not. That is where the design had to leave Manifold's path.

**Worth asking Discord:** whether they ever modeled long-lived recipient unavailability, or whether session ownership made it a non-issue from day one.

---

## Library notes

### L1 — Ergo can impersonate Manifold.Partitioner on OTP 29
**What happened:** Spike under `spike/manifold-bridge/`. Elixir app depending on real `manifold` 1.7.0 called `Manifold.send/2` with a PID living on an Ergo Go node. Manifold grouped by `node(pid)` and `GenServer.cast` to `{Manifold.Partitioner, :"go@localhost"}`. Go had a process registered as `Elixir.Manifold.Partitioner` (the atom Elixir uses for that module name), handled `$gen_cast` / `{:send, pids, message}` via `erlang23.GenServer.HandleCast`, forwarded to a local receiver, and the receiver replied. Full path printed `SPIKE PROOF OK` on both sides.

**Expected:** This was the hard unknown — Manifold does not send to recipient PIDs directly across the network; it addresses a named partitioner on the target node. Ergo is not a full BEAM, so name registration + GenServer cast semantics might not hold up.

**What was not missing:** Registered-name delivery (`REG_SEND`), `$gen_cast` dispatch into `HandleCast`, and local `Send` to Ergo PIDs whose `node(pid)` Elixir sees as the Go node. No Manifold fork required; the Go node does not run the Manifold application — only something answering to that registered name.

**Still untested:** `pack_mode: :binary` (`{:manifold_binary, bin}`), multiple partitioners (`Manifold.Partitioner_N`), `:send_mode :offload` (Sender stays on the Elixir side), and whether a fuller worker-pool impersonation is worth it vs. delivering from the partitioner itself.

**Dismissed from the pre-list:** The old `goerlang/node` concern — this path uses Ergo + `erlang23` (same stack as `dist-handshake`) and completed against OTP 29 without handshake patching.

### L2 — Nested ETF maps from Elixir did not round-trip; JSON binary did
**What happened:** First multi-node e2e under `manifold/` sent `{:dispatch, event_map, subscribers, from}` with nested Elixir maps. `give_pid` worked; the dispatch tuple never produced a reply. Direct `send/2` of the same shape also timed out — so it was not Manifold-specific. Switching the payload to a Jason JSON binary (`{:dispatch, json, from}`) delivered immediately and `fold.Dispatch` ran.

**Expected:** ETF maps with string keys would decode into `etf.Map` on Ergo the way atoms and binaries already do.

**Cost:** An hour chasing Manifold routing when the failure was on the wire format. Workaround is fine for the orchestrator boundary; a richer ETF map story is still open if we want native term payloads later.

### L3 — Cross-node HOL tooth needs overlapped claims, not production assignment
**What happened:** `TestMultiNodeHOLEnforcement` with two Dispatchers on shared Postgres: disjoint `PartitionsClaim` (production Manifold assignment) produced **zero** peer inversions even with HOL off — exclusivity already prevents the race. Intentionally overlapping claims restored the tooth: HOL on → 0 inversions, HOL off → nonzero. Same lesson as P5, now at node boundary.

**Expected:** "Does the ordering test still fail with HOL disabled across nodes?" — only when claim scopes overlap. Under D12's disjoint assignment the peer race is impossible by construction; HOL's remaining jobs are same-owner retry skip-ahead and any future handoff.

Do not pre-write further entries. Fill them in as they actually happen.

---

## For the conversation itself

Lead with what worked and what was interesting, not with complaints. The strongest entries are the ones where the answer is genuinely unknown from outside — P1 and P4 both ask about design decisions that only someone who built it would know. That's a better opener than a bug report.
