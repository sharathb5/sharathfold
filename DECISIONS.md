# Decision log

Running record of product decisions and tradeoffs. Newest sections appended at the bottom.

---

## What we're building

A Go library for webhook delivery. An app imports it, hands it an event and a list of subscriber URLs, and it handles delivery — in parallel, with retries, and in order per subscriber.

**The gap it targets:** webhook systems today force a choice between throughput and ordering. Fan out across a worker fleet and a given subscriber's events can arrive shuffled. Serialize to preserve order and throughput collapses. Svix ships both modes and makes the user pick. Most others pick throughput and tell customers to reorder on their end.

**The borrowed idea:** Discord's Manifold resolves the same tension by hashing each recipient to a permanent home worker. Same subscriber, same worker, every time — their events queue in sequence while other subscribers are handled in parallel elsewhere.

---

## Decisions

### D1 — Build on Manifold's idea, not lilliput
**Decided:** use Manifold (Discord's Erlang/Elixir fan-out library) as the conceptual basis.

**Rejected alternatives:**
- lilliput (their Go image-resizing library) — explored several angles and rejected all: request coalescing and decode-once-resize-many mirror architecture Discord has already published, so not novel; fuzzing/security was ruled out by preference; comparative benchmarks likewise; the HEIC-gap finding was real but small.

**Why:** Manifold is a technique Discord invented for their own architecture rather than a wrapper around existing C libraries, and it's closer to the messaging/migration work Sharath is moving into at SAS.

---

### D2 — Apply Manifold to webhooks, not to Discord's own use case
**Decided:** target webhook delivery.

**Rejected alternatives:**
- Live market-data broadcast (Kalshi/Polymarket angle) — a synthetic market layer would dilute the pitch and stretch scope.
- Cache invalidation — too solved already, little room to say anything new.
- Config propagation — viable, not chosen.
- Reproducing Discord's guild-broadcast use case — not novel; it's their own problem.

**Why:** webhook delivery has the same fan-out shape, is self-contained (no simulated domain layer needed to make it legible), and has a real published shortfall to attack.

---

### D3 — The differentiator is the mechanism, not the packaging
**Decided:** lead with the Manifold-derived delivery mechanism (hash-pinned ordering + serialize-once + offload).

**What changed:** the original pitch was "embeddable webhook delivery, no separate infra." Found that Postel already occupies that exact position ("Svix is for when webhooks are your product; Postel is for when webhooks are a feature"), and whook is adjacent. So "embeddable" alone is a taken category.

**Why it still works:** nothing found suggests the existing embeddable options do hash-pinned ordering. Their pitch is footprint; ours is behavior under fan-out.

---

### D4 — Which parts of Manifold actually transfer
**Decided:** carry over four mechanisms, with an honest note on what each buys here.

| Manifold mechanism | Transfers? | What it buys for webhooks |
|---|---|---|
| Group recipients by destination | Partially | Not network batching — subscriber URLs are independent external hosts, so every subscriber still costs one HTTP POST. What it does buy is deciding **which machine owns which subscriber**. |
| Consistent hashing to a fixed worker pool | Fully | The core of the project. Ordering per subscriber, parallelism across subscribers. |
| Serialize payload once, reuse bytes | Fully | CPU savings — one JSON encode per event instead of one per subscriber. |
| Offload sending off the calling process | Fully | The app's request path isn't blocked while fan-out happens. Matters especially for an embedded library. |

**Correction worth recording:** an earlier read of this claimed distribution across nodes doesn't help webhooks. That conflated two separate benefits. Destination-batching genuinely doesn't transfer, but **partitioning the sending workload across nodes does** — it's how the system scales horizontally while each subscriber keeps a stable home. Both are Manifold's ideas; only the first fails to carry over.

---

### D5 — Ship as a library first, distributed as an optional path
**Decided:** default is a pure Go library, single process, nothing new to deploy. The multi-machine setup (Elixir orchestrator running real Manifold, coordinating Go worker nodes over the Erlang distribution protocol) is an optional scale-out path.

**The tension being resolved:** "no new service to run" and "distributed across machines" pull against each other. Manifold gets to be a pure library because Erlang hands it a cluster for free — Discord's app is already many BEAM nodes that know about each other. Go has no equivalent, so cross-machine coordination has to be built, and that coordination is infrastructure.

**Options considered:**
- **A — embed, single process.** True library. Keeps hashing/ordering, serialize-once, and offload. Gives up cross-machine distribution; scale by running more app copies.
- **B — embed, app copies coordinate with each other.** Ordering holds fleet-wide with nothing extra to deploy, but instances need to agree on membership — that agreement is the hard part. Most novel engineering.
- **C — keep the Elixir orchestrator.** Most faithful to Manifold, best "I got Discord's real code talking to Go" story, but abandons the no-infra premise.

**Chosen:** A as the default, C as the optional scale-out. Keeps the headline claim honest (it's a library) while the Erlang bridge still gets built as the showpiece.

**Note on packaging C:** it can be shipped as containers (Compose file or Helm chart), but "easy to install" is not "no infrastructure." Svix is easy to install too. So C can't carry the no-infra claim — only the mechanism can.

---

### D6 — What we're actually competing with
**Decided:** treat the queue-plus-worker-fleet pattern as the reference design and the competition.

Three patterns companies use today:
1. **Roll your own inline** — loop and POST, bolt on retries/backoff/signing as each becomes a problem. Where most teams' half-finished webhook code lives.
2. **Queue plus worker fleet** (Kafka/SQS/Rabbit + consumers) — the standard grown-up answer. Durability, retries, horizontal scale. Cost: operating the queue and the fleet.
3. **Outsource** (Svix, Hookdeck, EventBridge) — least work, third party in your event stream, per-delivery cost.

**The claim:** deliver #2's core benefits — durability, retries, parallel delivery — plus ordering, which it doesn't offer, without requiring its infrastructure.

**Testable:** benchmark directly against a naive queue-and-workers setup and show ordering holding where theirs doesn't.

---

### D7 — Ordering comes from exclusive partition ownership, not the HOL gate
**Decided:** per-subscriber ordering is a consequence of exclusive partition ownership. Each subscriber hashes to one partition; each partition has one owner; that owner is the only claimant for that subscriber's deliveries. Serialization across *workers* is by construction.

**What the HOL gate actually does:** two load-bearing jobs, not one.

1. **Same-owner skip-ahead during retry backoff (ordinary operation).** Exclusive ownership stops a *peer* from claiming a later sequence, but it does not stop the owning worker from claiming sequence N+1 while N is pending on backoff after a failed attempt. Without the gate, that worker delivers ahead and inversions appear with no resize involved. The uniform-latency attribution run (`fold-no-HOL` under exclusive ownership) produced 85 inversions this way. So the gate is live whenever retries occur — not dead code in steady state.

2. **Resize handoff.** When a partition changes hands, the old owner may still have work in flight while the new owner begins claiming. HOL prevents the new owner from racing ahead of that in-flight earlier sequence.

**Corrected earlier claim:** an earlier revision of this entry said the gate "never fires" under exclusive ownership with no resize, and that its real purpose was only the resize window. That was wrong. Peer races under exclusive ownership are indeed impossible (proving a *peer* race still needs `OverlapPartitions`), but same-owner skip-ahead during retry is a production path the gate must cover.

**Consequence for tests:** both cases need proof. The resize test covers handoff. Same-owner retry skip-ahead is covered by disabling HOL under exclusive ownership with failure injection (see bench attribution). The synthetic `OverlapPartitions` ordering test remains useful for the peer-race shape but is not the product's only HOL production path.

---

### D8 — Failure handling is suspend-and-resume
**Decided:** a subscriber is either healthy (receiving in order) or suspended (receiving nothing). Never receiving out of order. When deliveries to a subscriber exhaust their retries, that subscriber is suspended: queueing stops, backlog is dead-lettered, and they resume explicitly once their endpoint recovers. A subscriber's partition never changes; only their status does.

**Rejected alternatives:**
- Dead-letter the failed delivery and continue with the next one — trades the ordering guarantee (the product) for liveness.
- Reassign a recovered subscriber to a different worker pool — breaks hash-determined ownership and reintroduces the two-claimant window the design exists to prevent.

**Open (record, do not resolve here):**
- How a subscriber learns they have been suspended.
- Whether resume replays stored backlog or hands them a cursor.

*(Resolved in D9 and D10.)*

---

### D9 — Recovery is pull-based, not push-based
**Decided:** when a subscriber is suspended, fold sends them nothing — including no notification that they have been suspended. The host app exposes suspension state; the subscriber calls a resume endpoint when their endpoint has recovered. The recovery signal comes from the subscriber. Resolves D8's suspension-notification open question.

**Rejected alternatives:**
- Push a suspension notification to the webhook URL — circular: the notification fails for the same reason the webhook did, and it forces a second delivery path with its own retry semantics.
- Active probing / health checks from fold — the library would be guessing recovery; the subscriber is the only party who reliably knows.

**Why:** circuit-breaker logic. Stop calling a failing dependency and let it prove itself healthy rather than probing it. Fold's job is to stop delivery and retain (bounded) backlog; the host's job is to surface that state and accept the resume call.

---

### D10 — Catch-up is a bounded backlog, not an event API
**Decided:** events for a suspended subscriber are retained up to a configurable bound (count and age). Resume clears suspension and replays what is still retained, in sequence order. If the subscriber was down longer than the window, resume reports that a gap occurred and returns a marker so the host app can reconcile as it sees fit. Resolves D8's resume-semantics open question.

**Rejected alternatives:**
- Require the host app to expose an event-history API that subscribers fetch from — a library cannot assume that shape exists, and requiring it would hurt adoption.
- Unbounded retention — grows without limit for subscribers who never return.

**Why:** fold can guarantee ordered replay of what it still holds. It cannot invent the host's historical event API. The bound makes the failure mode explicit (gap + marker) instead of silent memory growth.

**Note on the suspend-resume boundary (D9/D10):** the boundary is currently safe by construction, not by test. Suspension is only triggered by `ExhaustAndSuspend` after `Deliver` returns, which dead-letters the in-flight row and flips the suspended flag under one lock, so no delivery can still be running when replay begins. The suspend-resume test asserts zero inversions on the quiet path only and would not catch a violation of this. Any future change that introduces a host-initiated Suspend while a delivery is in flight, or a drain path where an old owner completes work after a new owner starts claiming, reopens this window and must either preserve the post-delivery-under-lock ordering or add a test that synthesizes the overlap.

---

### D11 — Crash recovery uses claim age, not owner liveness
**Decided:** on `Start`, `RecoverStale` resets `in_flight` rows whose `claimed_at` is older than `StaleClaimAge` (default `2 × DeliveryTimeout`) back to `pending`, clearing owner and generation. Sequence is unchanged, so HOL re-applies and deliveries keep accept order after recovery.

**How abandoned is distinguished from live:** age of `claimed_at`, not process heartbeats. A live worker’s claim is fresher than the delivery timeout; an abandoned claim from a dead process sits past that window.

**Why safe with shared Postgres:** a live worker still inside `Deliver` has `claimed_at` within the timeout window, so another process’s `RecoverStale` will not reset that row and will not create a second claimant. After the age threshold, either the live attempt has finished (`Mark*` with matching generation) or the owning process is gone — resetting is then the same as reclaiming stranded work. Stealing a still-live attempt would require the HTTP call (or the worker) to outlive `StaleClaimAge`; that is why the default exceeds `DeliveryTimeout`.

**Rejected alternatives:**
- Reset every `in_flight` on Start — breaks exclusivity when multiple processes share a store.
- Heartbeat / lease table — correct for multi-node ownership, heavier than v1 needs; age-gated visibility timeout matches queue practice and reuses the existing `claimed_at` column.

---

## Open questions

- **Durability.** A queue means events survive a crash; an in-process library holding events in memory doesn't. Needs an answer — possibly "persist to whatever database you already run," which is Postel's approach.
- **Rebalancing.** When a worker or node joins or leaves, some subscribers get reassigned. Consistent hashing bounds how many, but any reassignment is a window where ordering can break. This is the most interesting design question in the project.
- **Erlang handshake risk.** The Go-side distribution-protocol library found (goerlang/node) is old and admits missing pieces. May need patching to talk to a modern OTP release. This is the highest-uncertainty part of the scale-out path.

---

## Separate from the pitch

Any issues or feedback found in Discord's own code along the way get logged separately, not mixed into the project's main story.
