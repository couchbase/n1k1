# Design: CEP — n1k1 as a complex-event-processing / standing-query engine

> A research + positioning note, not a build plan. It argues that n1k1's **MQO / `.multi`**
> substrate (see [DESIGN-prepare.md](DESIGN-prepare.md), "PREPARE++") is already the *hard half*
> of a complex-event-processing engine — many standing rules evaluated cheaply over one data flow
> — and sketches the (smaller, well-seeded) other half: an **unbounded source + continuous emit +
> incremental state**. "CEP" is used deliberately instead of "streaming" (which is overloaded with
> ETL/transport meanings); the interesting thing here is *standing queries reacting to events*, not
> byte transport.

## Status & remaining TODOs

_Research note; no code committed for the CEP direction._

**The claim:** the expensive, differentiated part of a CEP engine — *fan one flow out to N
standing rules and evaluate them at sub-linear cost in N* — is **built and benchmarked** as the
shared-scan MQO stack. What's missing is continuous operation. The gap is real engine work but
each piece grows from something that already exists.

**The API insight** (see [Control surface & APIs](#control-surface--apis-design-brainstorm)): the
first thing to build is **not** a `tail -f` daemon — it's the **durable cursor + `poll` mode**, so a
cron/harness **AI agent** can ask *"what changed since I last looked?"*, get a token-sized delta, and
exit. Stateless process, stateful cursor; crash-safe (at-least-once); no daemon. `follow`/daemon is
a later escalation. Query stays pure SQL++; **liveness rides the binding, cadence rides a run
modifier** — the grammar never changes (no `FROM tail(…)` TVF).

**Remaining, roughly in build order (none started):**
- [ ] **Cursor + `poll` mode + the `monitor` object** — the low-lift, high-value first ship: a
  persisted per-source cursor, a since-filter at the scan, `append`-offset **and** `snapshot`/`diff`
  delta strategies (Debezium `{op,id,before,after}` envelope), and `.monitor` CRUD (create/list/show/
  poll/pause/reset/delete). No daemon required.
- [ ] **Unbounded source** (for `follow`) — a `records.Source` that blocks/yields as data arrives
  instead of returning `io.EOF` (tail-follow a file, watch a directory, a socket, a Kafka/Redpanda
  consumer, a Debezium CDC topic).
- [ ] **Continuous emit / triggers** — emit-on-watermark for windowed/aggregate detectors (fused
  filter+project detectors already emit per row, so they're free once the source is unbounded).
- [ ] **Incremental standing state** — generalize the O(N) incremental window fold to long-running
  aggregates; hang state on the spill-backed rhmap.
- [ ] **Daemon + wire API** — `n1k1 serve`, with an **MCP** transport (monitors as resources +
  `resources/subscribe`; the AI-native surface) and a CouchDB-style long-poll HTTP `_changes`.
- [ ] **Event-time + watermarks + out-of-order**, **retention/TTL on unbounded state**, and
  **restart-from-source-offset** (at-least-once; *not* distributed exactly-once — see Positioning).

## Thesis: the MQO substrate is a CEP fabric in batch clothing

PREPARE++ was built for support bundles: compile a **git corpus of SQL++ "detectors"** once, run
it against each GB-scale bundle, `UNION ALL` the findings. Structurally that is *"N standing
queries over one pass of data"* — the defining shape of CEP, evaluated in **batch-over-a-finite-
scan** mode. Re-point the scan at an **unbounded** source and the same machinery is a continuous
standing-query engine. The push-based execution model (`base.Val = []byte` flowing through
`yield`) is the same dataflow model a stream processor uses; n1k1 just happens to hit EOF today.

## What already exists — the MQO ↔ CEP mapping

Every shared-scan lever (all DONE + benchmarked; [DESIGN-prepare.md](DESIGN-prepare.md) §"the four
levers") has a direct CEP twin:

| n1k1 MQO lever | CEP / standing-query equivalent |
|---|---|
| `engine.OpBroadcast` — scan once, tee each `[]byte` row to K inlined filter+project detectors, zero-boxing (decode once) | The canonical "one event → N standing queries" fan-out (tee) |
| `engine.OpBroadcastIndexed` + `base.AhoCorasick` — index each detector by a **necessary** discriminating literal; one pass over raw row bytes wakes only detectors whose literal is present. O(K×rows) → ~O(hits×rows), ~60× at K=1000, **flat in K** | The **rule-discrimination network** (Rete / literal pre-filter) — the single hardest problem in high-fan-in CEP, and n1k1 has a *sound* one (necessary ⇒ over-wake safe, under-wake impossible; guarded by a byte-identical differential test) |
| `BroadcastCSE` — sub-predicates shared across detectors computed once/row (`^cseN` precompute column; expr-identity via canonical marshal) | Multi-query optimization / shared operators (the NiagaraCQ → TelegraphCQ streaming-DB research lineage) |
| `BroadcastRoute` — a source fans out only to detectors that `FROM` it | Per-stream routing of a multiplexed event bus |
| Window functions `OVER (ROWS/RANGE/GROUPS …)` + the **incremental fold** (left-anchored O(N) carry; invertible add-enter/remove-leave for COUNT) | Sliding / tumbling / session windows with incremental aggregation — the seed of incremental view maintenance |
| ASOF / nearest-preceding merge ([DESIGN-merging.md](DESIGN-merging.md)) — stock correlated-argmax subquery lowered to an O(n) merge | Temporal / as-of stream join (correlate an event with the latest prior state) |
| Late binding: logical→physical per bundle at EXECUTE, fail-loud on empty | Connector/source binding; "rebind is data, not code" — the compiled MQO structure is bind-invariant |
| `CorpusCompile` fuse / standalone / reject classification + `gate:` preconditions | A planner deciding which rules share the scan vs run their own operator, and cheap guards to skip idle rules |
| `.multi run/lint/test/list`, git corpus, golden-fixture CI, the **lint report card** | **Detections-as-code** with a correctness + efficiency oracle (see Agentic angle) |

The load-bearing point: the **predicate index is the crown jewel for CEP.** "N standing rules × M
events/sec" is precisely the problem it already solves, and its cost is ~flat in N. Most of a CEP
engine's value is *making N large cheaply*; that part is done.

## The gap to continuous operation (ranked by lift; each with its seed)

1. **Unbounded source — biggest lift, cleanest shape.** Execution is already **push-based**, and
   the JS streaming-extract (`records.Recipe.ExtractStream`) already proved a **backpressured
   emit** across a goroutine boundary (unbuffered channel, race-clean; [DESIGN-data.md](DESIGN-data.md)).
   A follow-source just never returns EOF — it parks until the next append/message. Everything
   downstream (broadcast, index, filter, project) is unchanged.
2. **Continuous emit / triggers.** Fused filter+project detectors *already emit per row*, so under
   an unbounded source they are continuous with zero new code. Only windowed/aggregate/standalone
   detectors need emit-on-**watermark** (punctuation) instead of the current emit-at-EOF.
3. **Incremental standing state.** Long-running aggregates need incremental update, not batch
   re-fold. n1k1 already did the **O(N) incremental window fold** and the **invertible COUNT
   slide**; generalize that and hang the accumulator on the **spill-backed rhmap** (which already
   survives working sets > RAM), with retention/TTL to bound it.
4. **Event-time + watermarks + out-of-order.** The ASOF merge already assumes a sortable time key
   ([DESIGN-sorting.md](DESIGN-sorting.md) proposes a shared sorted-stream substrate); a watermark
   model extends it to bound how long to wait for late events.
5. **Retention / bounded state** for genuinely unbounded queries (spill helps; needs a TTL story).
6. **Fault tolerance.** Deliberately *shallow*: at-least-once with **restart-from-source-offset**,
   not distributed exactly-once. See Positioning — this is where the heavyweights win and n1k1
   should not compete.

⚠ **The `_meta.pos`-per-file gotcha becomes a `_meta.offset`-per-partition gotcha.** The context
(`grep -A/-B/-C`) window idiom partitions by `_meta.`path`` because positions restart per file
([DESIGN-prepare.md](DESIGN-prepare.md)); the streaming analog is partitioning window state by
source/partition key so state and ordering don't leak across event partitions.

## The agentic angle — "Jarvis for your business"

PREPARE++ already assumes the corpus is *"authored by an AI agent and run by support teams."*
Generalize from support bundles to live business data and the agentic-monitoring loop falls out:

> An LLM turns *"tell me when a tier-gold customer shows a churn signal within 24h of a payment
> failure"* → a SQL++ detector committed to the **git corpus** → n1k1 runs it **continuously** over
> the CDC / event stream → a findings row → an alert or action.

The differentiator is the **feedback loop the AI author gets**, which almost no CEP/rule system
offers: `.multi lint` already reports, per detector, *does it fuse? is it index-pruned or
always-wake? native byte path or boxed? does it match its golden fixture?* — a cheap **correctness
+ efficiency oracle** an agent can iterate against. That is what makes an LLM-authored rule corpus
*trustworthy and fast* rather than a pile of unvetted queries, and it would be a moat: competitors
would have to build the oracle, not just accept the rules.

Two natural first beachheads — the user's own two examples:
- **CDC monitoring.** Debezium → (Kafka/topic source) → n1k1 corpus → findings. n1k1 already reads
  the on-ramp shapes (JSON/jsonl(.gz)/CSV/Parquet); a blocking Kafka/CDC source closes it.
- **Dataset monitoring.** Freshness/volume/schema-drift/distribution monitors *are* detectors —
  `COUNT(*) per window < threshold`, a schema-change predicate, a distribution-shift check — so
  **the corpus IS the monitor library** (the Monte-Carlo/Anomalo job, but embeddable and in SQL).

## Control surface & APIs (design brainstorm)

This is the meat of the CEP direction: *what new API surface does n1k1 grow, and what do we call
it?* The design below is a proposal, not settled — but it rests on one load-bearing realization.

### The core realization: the CURSOR is the primitive, the daemon is optional

The obvious first instinct is a `tail -f` flag (`n1k1 -f`) or a long-running daemon. That's the
*push/follow* half. But the more important half for the stated primary user — **an AI agent waking
on a cron/harness tick** — is not "stay running forever," it's *"what changed since I last
looked?"* That is a **durable cursor** question, and it needs **no daemon at all**: persist a
position, and on each wake scan only what's new since it, emit the delta, advance the position,
exit. Stateless process, stateful cursor. This is cheaper, crash-safe by construction (crash before
advancing ⇒ re-poll re-delivers ⇒ at-least-once), and a far smaller lift than a fault-tolerant
daemon. So the primitive to design first is the cursor; `follow`/daemon is a later escalation.

A `tail -f` CLI flag is also **too limiting** for the reason the user suspected: it hard-codes one
source, one query, one mode. The right factoring is three **orthogonal axes**, none of which touch
the SQL++ grammar (the dialect stays frozen — [DESIGN-prepare.md](DESIGN-prepare.md)):

1. **The query** stays *pure stock SQL++* — a detector, unchanged whether run once, polled, or
   followed.
2. **Source liveness** is a property of the **binding** (data, not code), via the existing
   logical→physical late-binding manifest: `orders → glob("*.json")` (static) vs `orders →
   tail("app.log")` / `kafka://…` / `cdc://…` / `poll("https://api.github.com/…", every="5m")`
   (live). Same detector; the manifest decides where bytes come from.
3. **Run cadence** is an *execution modifier* around the query — `once` | `poll` | `follow` —
   supplied by a CLI flag / dot-command / monitor policy, exactly like `-mode` or a `LIMIT` flag.

⚠ **Why not a `FROM tail(…)` table-valued function or a streaming `UDF()`** (the user's second
idea): n1k1's extensions are **scalar-only**; a TVF-in-`FROM` needs fork grammar changes (called
out as "the one gap" in [DESIGN-prepare.md](DESIGN-prepare.md)), which the grammar-freeze forbids.
And a scalar UDF can't change a query's execution mode. So liveness/cadence must live *outside* the
SQL text — in the binding and the run modifier — which is also cleaner (the same rule is reusable
across static replay, catch-up poll, and live follow).

### The three run modes

| mode | behavior | who it's for | lift |
|---|---|---|---|
| **`once`** (today's default) | scan to EOF, emit, exit | ad-hoc humans, batch | done |
| **`poll`** (a.k.a. `catch-up`, `tick`, `since`) | scan only what's new since the cursor, emit the delta, advance + persist the cursor, exit | **the cron/harness AI agent** — "what changed since I last looked" | small — just the cursor + a source-level since-filter |
| **`follow`** (a.k.a. `tail`, `watch`) | block on the source, emit continuously as data arrives; process stays alive | live dashboards, alerting daemons | large — unbounded source + watermark emit ([the gap](#the-gap-to-continuous-operation-ranked-by-lift-each-with-its-seed)) |

Poll is the sweet spot: high value, low lift, no daemon. Ship it first.

### The monitor object + its lifecycle (CRUD)

A **monitor** is the manageable, persisted unit: a *detector* (or a whole corpus) + a *source
binding* + a *cursor* + a *run policy* (`once`/`poll`/`follow`, optional schedule) + an optional
*action* + *status*. It reuses the existing recipe/corpus format for the "what to look for" and adds
the live-operation state. Proposed dot-command family (a sibling of the existing `.multi` family;
verbs deliberately borrowed from tools devs & agents already know — see Prior art):

```
.monitor create <name> [from a recipe/corpus + binding + policy]   # arm / add / define
.monitor list                       # ls / ps  — table: name, source, mode, cursor, last-tick,
                                     #            #findings, status(armed|active|paused|errored|lagging), sha
.monitor show <name>                # describe / inspect / status — definition + cursor + last-N findings
.monitor poll <name>                # tick / check / catch-up — run from cursor, emit delta, advance
.monitor follow <name>              # tail / watch — long-running
.monitor pause | resume <name>      # suspend/resume (Snowflake TASK) / disable/enable (systemd)
.monitor edit <name>                # update / set
.monitor reset <name> [--since …]   # seek / rewind the cursor (Kafka seek, --from-beginning)
.monitor snooze <name> <dur>        # silence (Alertmanager) — an agent muting a noisy monitor
.monitor test <name>                # dry-run vs a golden fixture (reuse .multi test)
.monitor log <name>                 # history — past ticks & findings
.monitor delete <name>              # rm / drop / disarm / cancel
```

Same verbs over the wire (daemon mode): `GET/POST /monitors`, `POST /monitors/{name}/poll`,
`DELETE /monitors/{name}`, `GET /monitors/{name}/findings?since=…`.

### Monitor metadata — a NAME is the handle, not the whole story

A bare NAME is enough to *address* a monitor (it is the cursor key and the gitops reconcile key),
but not enough for the primary use case: if the monitor store is an episodic agent's **externalized
standing memory**, it has to carry the *why*, not just the *what*. `new-github-issues` tells a
future-me nothing about the intent that spawned it. So a monitor carries an open metadata bag,
borrowing the well-worn **Kubernetes labels-vs-annotations** split (also Prometheus rules'
`labels:`/`annotations:`, Datadog's `tags` + `message`):

- **`name`** — the stable identity/handle, and *only* that. It is the cursor key + the reconcile
  key, so it must stay **stable**: renaming = delete+recreate = lose the cursor. Keep human-friendly,
  changeable meaning *out* of the name.
- **`description`** — a first-class free-text line (one paragraph: what it watches and why).
- **`labels`** — small string `key=value` pairs, **indexed / selectable**: `team=support`,
  `env=prod`, `severity=high`. Drive `monitor list --selector team=support`, bulk ops, and routing.
- **`annotations`** — an arbitrary, client-owned JSON blob n1k1 **stores and returns verbatim, never
  interprets** (size-bounded, à la k8s' 256 KB cap). This is where the agent puts "whatever it
  wants": the natural-language ask that generated the detector, reference URLs (the issue / ticket /
  PR that motivated it), a runbook link, notes-to-future-self, arbitrary tags.

The field that most earns its keep for agents is **provenance** — e.g.
`annotations.provenance = {authored_by, model, at, prompt, source_ref}` — so a *future* me (or a
human) can reconstruct why a monitor exists and decide to trust, edit, or prune it. That is exactly
what turns the store from a list of opaque names into usable standing memory.

⚠ **Metadata edits must not reset the cursor.** Split "what it computes" (the SQL++ + binding —
changing *that* may invalidate the high-water) from "metadata about it" (description / labels /
annotations — free to edit, cursor untouched). The delta-identity stays `(query+binding SHA,
source-fingerprint)`, so a retag or a reworded description is a no-op for state — otherwise a gitops
`apply` would silently rewind a monitor just because someone relabeled it.

All of this rides the **existing recipe front-matter** (`-- key: value` + SQL++), so it is
git-committed, PR-reviewable, and reconciled by `apply` for free — no new storage, no new format.

### Where the cursor lives — and why the agent holds only a *name*, not a cookie

The sharpest question: for a polling client like an AI agent, where does the high-water mark live,
and does the caller get an opaque idempotency token? The reality that decides it: **AI agents are
bad at durably holding state** — my context resets between cron wakes; "put this cookie somewhere
and give it back next time" has no good home. So the design must not require the agent to persist a
cursor across wakes. Resolution, in three layers:

1. **The durable cursor lives server-/store-side, keyed by the monitor NAME.** The agent's only
   cross-wake handle is a stable string it keeps in its own config/memory: `monitor poll
   new-issues`. n1k1 holds the high-water mark. This is the Snowflake `STREAM` model (the stream
   advances on consumption), the Kafka consumer-**group** model (the coordinator holds the committed
   offset keyed by `group.id`), and the Dagster-sensor model (the framework persists the cursor). The
   agent stores *nothing* but a name.
2. **An opaque `cursor` token is returned in the poll response, but the agent only needs it
   *within* a single tick — never across wakes.** Default is auto-advance (n1k1 commits the new
   high-water when `poll` returns). For must-not-miss agents, a two-phase mode: `poll` returns
   findings + a *candidate* `next_cursor` **without** advancing; the agent processes, then calls
   `ack(name, next_cursor)` to commit. If it crashes before `ack`, n1k1 hasn't advanced, so re-poll
   re-delivers (at-least-once). The token thus lives in the agent's *working context for the
   duration of one tick* and is discarded — which is the one place an agent *can* reliably hold it.
   So: **cookie, yes — but a within-tick cookie, not a persist-across-wakes cookie.**
3. **Embedded / no-server: the cursor is a `tfstate`-style local state dir.** `n1k1 monitor poll
   --state ./.n1k1-state/ …` makes n1k1 a pure function of `(definitions, state-dir, source)` — the
   state dir is the only mutable thing, gitignored, on a volume the harness persists. Same opaque-
   cursor abstraction as the served store; only the *backend* differs (local dir → served KV →
   distributed object store). Designing the cursor as an **opaque, comparable, serializable value
   from day one** is the discipline that keeps embedded and server modes one codebase.

⚠ **Keep the git-committed definition split from the per-deployment cursor** (Terraform config vs
`tfstate`; dbt models vs `target/`): the recipe + binding + policy are shareable/versioned; the
cursor + tick-log are local + gitignored (they mean nothing on another machine / data root).
Monitors live under `<dir>/.n1k1/monitors/<name>/`. A monitor's identity for caching/delta is
`(recipe SHA, binding, source-fingerprint)`. For the rare handoff/backfill (resume *exactly here* on
another box), a cursor is **exportable** as a token (`monitor export/import`) — but that's opt-in,
not the default.

**Separate concern — action idempotency.** The cursor stops n1k1 *re-emitting* a finding; it does
not stop the *agent* from acting twice (creating the same Jira ticket) if it retries. So each
finding carries a stable **fingerprint** = hash of `(detector-sha, source-id, matched-key)` — the
Alertmanager/PagerDuty `dedup_key` pattern — so the agent (or an action sink) can dedupe side
effects independently of cursor advance.

### Two delta strategies (this is what makes "what changed" real)

Sources differ in how "new" is even defined; a monitor declares one:

- **`append`** (logs, new files, new issues/tickets/emails) — cursor is a high-water **offset**
  (byte/line for a file; a set of seen file-ids+mtime for a directory; a Kafka offset; a CDC LSN).
  "New rows since the offset." Cheap, stateless beyond the offset. Covers the user's *"new GitHub
  issues, new support tickets, new emails."*
- **`snapshot`/`diff`** (a customer record was updated, an issue moved to *closed*) — no natural
  offset, so n1k1 keeps a prior **snapshot keyed by doc-id**, and on poll diffs current-vs-prior to
  emit change events in the **Debezium envelope** shape: `{op: insert|update|delete, id, before,
  after}`. This is `git diff` / `kubectl diff` / `terraform plan` / dbt-snapshot for arbitrary
  keyspaces, and it's what lets an agent polling a plain REST API (which returns *current state*,
  no change feed) still learn *"issue #42 → closed, #57 is new, #12 deleted."* The stored snapshot
  can spill (reuse the rhmap store) so it scales past RAM.

### Cursor semantics: at-least-once + optional ack

Default: **auto-advance** the cursor when `poll` returns (simple; at-most-once loss only if the
agent crashes mid-consume). For agents that must not miss a finding, a **two-phase** option: `poll`
returns findings + a *candidate* cursor; a later `ack`/`commit` advances it — Kafka manual commit /
SQS delete-after-process / a `_changes` checkpoint. `reset --since` re-seeks (replay/backfill).
Expose **lag** (cursor distance behind head) in `.monitor list` — the universal "am I keeping up?"
signal.

### Daemon mode & the AI-native wire API

When following many monitors or serving multiple clients, n1k1 becomes long-running:
`n1k1 serve <dir>` (a.k.a. `daemon`). Two transports, both worth having:
- **MCP** (the important one — the primary users are AI agents whose harnesses already speak it):
  expose each monitor as an **MCP resource**; `resources/subscribe` + `notifications/
  resources/updated` = `follow`; MCP **tools** = `monitor.poll` / `list` / `create` / `delete`. A
  cron agent calls the `monitor.poll` tool; a long-running agent subscribes. This makes n1k1 a
  drop-in **standing-query server for agents**, no bespoke client.
- **Long-poll HTTP** (CouchDB `_changes?feed=longpoll|continuous|normal`) for everything else, so a
  plain `curl` / shell agent works too.

### "What changed since I last looked" — the exact cron-agent loop

```
# agent wakes on its cron tick; no daemon anywhere
$ n1k1 monitor poll new-github-issues        # or MCP tool call monitor.poll{name}
→ { "since": "cursor@2026-07-27T09:00Z", "now": "cursor@2026-07-27T14:00Z",
    "findings": [ {op:insert, id:"#57", title:"…", detector:"triage@a1b2"} , … ],
    "count": 3, "lagging": false }
# cursor advanced + persisted in .n1k1/monitors/new-github-issues/; agent acts on 3 items, exits
```

The agent supplies no cursor and manages no state — it names a monitor and gets a **compact,
structured, token-sized delta** (change-type + provenance: `source_file`, `line_range`, `sha`).
Re-polling after a crash is safe. This is the whole ask, and it needs only the cursor + `append`/
`diff` delta — *not* the daemon.

### GitOps: declarative monitors, the Terraform model (the preferred agent workflow)

Yes — the API should be **git-first and declarative**, because that is exactly how an AI agent (and
a human team) works best: *edit a file, commit it, tell the tool to reconcile*. Agents are excellent
at authoring/regenerating declarative files and terrible at long imperative CRUD sequences they must
remember they already ran. The Terraform mental model maps almost 1:1:

| Terraform | n1k1 monitors |
|---|---|
| `.tf` config (git-committed) | **monitor definitions** — a dir of recipe files (the existing `.multi` recipe format: front-matter + SQL++), each = detector + binding + policy |
| `tfstate` (a backend, not the app repo) | **cursors + tick logs** — local/served state, gitignored |
| `terraform plan` | **`.monitor plan <dir>`** — diff declared-vs-live: which monitors would be created / updated / destroyed, no changes applied (fold in `.multi lint` so the plan also reports fuse/index/cost) |
| `terraform apply` | **`.monitor apply <dir>` [`--prune`]** — reconcile: create new, update changed, (optionally) delete removed; **cursors of unchanged monitors are preserved** |

So the blessed workflow is: agent maintains `monitors/*.sql` in git → `n1k1 monitor apply monitors/`
→ n1k1 reconciles the live set to match. It's **idempotent** (re-applying the same dir is a no-op,
so the agent needn't remember what it already created), **PR-reviewable**, and CI/Flux/Argo-style
auto-apply-on-merge falls out for free. This also cleanly resolves the state question above: the
**definition is the agent's durable memory (the committed file)**; the **cursor is n1k1's job**
(keyed by the name in the file) — the agent still never holds a persistent cookie.

⚠ **Declarative and imperative can drift** (`kubectl edit` vs GitOps). Rule: `apply` is the blessed
path; `.monitor create/delete` are for interactive/exploratory use and are subordinate — `apply`
can detect drift and optionally self-heal (Argo-style). Don't let an agent mix both on the same
monitor set.

### Deployment modes & the future "n1k1 server" — same API, three backends

Correct that the monitor API is designed to *grow into* a server — and the discipline that makes
that free is the opaque, serializable cursor + the name-keyed store. The **same** `.monitor` /
MCP / HTTP surface runs in three modes, differing only in the cursor-store backend:

1. **Embedded, no server** (ship first) — cursors in a local `.n1k1-state/` dir; poll from cron; no
   daemon. The whole "what changed since I last looked" loop works here.
2. **Single `n1k1 serve`** — same store, now also `follow` (long-running) + the MCP/HTTP wire API +
   multiple concurrent clients. One box.
3. **Distributed n1k1 server** (future) — the cursor store moves to a shared/replicated backend
   (object store or KV). Name-keyed cursors + client-carryable tokens make polls horizontally
   scalable (any replica serves any poll). No API change — only the backend swaps.

The monitor abstraction is the seam; keep the cursor opaque and the store pluggable and mode 3 is a
backend, not a rewrite.

### MVP — the first slice to attack

Smallest genuinely-useful cut that validates the cursor abstraction and delivers the cron-agent
loop, in order:

1. **`append`-mode cursor + `.monitor poll` + minimal CRUD (`list` / `show` / `delete`)**, cursor in
   a local state dir, over a growing file / directory source (n1k1 already reads jsonl/dir — add a
   since-offset filter at the scan). No daemon, no MCP, no diff, no follow. *This alone is the whole
   append use case: new issues / tickets / emails / log lines.*
2. **`snapshot`/`diff` delta** (the Debezium envelope) — unlocks "what *changed*" on mutable / REST
   sources.
3. **GitOps `plan` / `apply`** over a recipe dir (small, on top of the existing corpus loader).
4. **`n1k1 serve` + MCP** (follow + subscribe).
5. Later: unbounded `follow` source, two-phase `ack`, distributed cursor store.

Steps 1–3 need **no** daemon, **no** unbounded-source engine work, and **no** grammar change — they
are cursor + delta + reconcile plumbing over the existing scan + corpus machinery. That is the
high-leverage beachhead.

### The AI-agent's-eye view (why this matters for "future me")

Speaking as a likely primary user: the single most valuable property is that **the monitor store
becomes my externalized standing memory.** An agent on a cron schedule has *no memory between
wakes* — my context resets. Today I'd rediscover "what am I even watching, and where did I leave
off?" every tick. If n1k1 holds `(what I watch, where I left off, what already fired)` durably and
keyed by a stable name, then `monitor list` + `monitor poll <name>` *is* my working memory of the
watch — I don't have to reconstruct it. That reframes n1k1 from "a query tool I call" to "the
persistent standing-query memory for episodic agents."

Corollaries I'd want, in priority order:
1. **Deltas, not re-scans** — return only what changed, token-sized, with a `--summary` (counts +
   drill-down) so a big change set doesn't blow my context.
2. **Idempotent + at-least-once** — I will crash and retry; re-`poll` must be safe.
3. **Author → test → arm in one loop** — I write the detector in SQL++ (from a natural-language
   ask), `monitor test` it against a fixture, then `create` it; `.multi lint` tells me if it's
   sound/cheap *before* it goes live.
4. **Introspectable across wakes** — `list`/`show` let a *fresh* me reason about the watches a
   *previous* me armed (and hand them off to a human or another agent).
5. **Guard rails** — a monitor declares bounds (max findings/tick, cost ceiling, rate limit) so a
   runaway source can't flood or bankrupt me; `.multi lint`'s cost signals feed this.

## Vocabulary — nouns & verbs (a proposal to react to)

Naming is half the design; the words leak into every command and API. Proposed core lexicon,
with the runners-up and where each is borrowed from:

| concept | **recommended** | alternatives considered | borrowed from |
|---|---|---|---|
| the saved standing query + binding + cursor + policy | **monitor** | watch, subscription, continuous-query/CQ, standing-query, sensor, task, trigger | Datadog *monitors*; Snowflake TASK; Dagster *sensor* |
| the SQL++ rule itself (already n1k1's term) | **detector** | rule, detection, check, probe | Sigma/Panther/Elastic *detections*; DESIGN-prepare |
| the durable position per source | **cursor** | watermark, offset, checkpoint, bookmark, since-token, high-water-mark, replication-slot | CouchDB `since`; Kafka *offset*; Dagster sensor *cursor*; Datomic `since` |
| one wake/execution of a monitor | **tick** | poll, check, run, fire, catch-up, pass | Dagster *tick*; cron |
| an emitted result row (already n1k1's term) | **finding** | hit, alert, event, change, match | Sigma/SIEM; DESIGN-prepare |
| a detected change on a mutable doc | **change** `{op,id,before,after}` | delta, diff, mutation, revision | Debezium *change event*; CouchDB `_changes` |
| long-running process | **serve** / **daemon** | watch, listen, run | `n1k1 serve`; systemd |
| run cadence | **once / poll / follow** | batch / catch-up / tail | `tail -f`; CouchDB `feed=`; `kubectl --watch` |

Recommendation: lead with **monitor** (CRUD-friendly, observability-native, unambiguous), keep
**detector**/**finding** (already in the corpus), and use **cursor** + **tick** for the
poll machinery. Reserve *"task"* for a monitor whose policy includes an **action** (Snowflake's
STREAM-vs-TASK split: the cursor-bearing thing vs the scheduled thing) rather than as a second
noun — i.e. a task is "a monitor that also *does* something."

## Prior art we (and agents) already use — steal the nouns & verbs

- **CouchDB/Couchbase `_changes` feed** (on-brand — Couchbase heritage): `?since=<seq>` +
  `feed=normal|longpoll|continuous` + `limit` + `include_docs`. This is *the* canonical
  "what changed since seq N" API and maps almost 1:1 onto `poll`/`follow` + cursor.
- **Snowflake Streams + Tasks**: a `STREAM` is a table's change-cursor that **advances on
  consumption**; a `TASK` is scheduled SQL (`SHOW TASKS`, `ALTER TASK RESUME/SUSPEND`, `EXECUTE
  TASK`). The clean STREAM-vs-TASK (cursor vs action) split is worth copying.
- **Watchman** (file-watch): `watch` / `subscribe` / `trigger` + a `since` clockspec + named
  subscriptions — a near-exact shape for named monitors with cursors and actions.
- **Dagster sensors**: a `@sensor` wakes on a tick and persists a **cursor** string between ticks to
  track "what's new" — *the* precedent for the cron-agent poll model.
- **Kafka consumer groups**: durable **offset**, `commit`, `seek`, `--from-beginning`/`latest`,
  consumer **lag** — the append-cursor vocabulary and at-least-once/manual-commit semantics.
- **Datomic** `as-of` / `since` DB filters — elegant temporal-cursor naming (`since` = only what was
  added after T).
- **MCP** resources + `resources/subscribe` + `notifications/resources/updated` — the AI-native
  transport; monitors-as-resources is the natural binding for agent harnesses.
- **cron / systemd / `crontab -l`**, **`docker ps`**, **`kubectl get --watch`** (+ resourceVersion
  cursor), **Prometheus/Alertmanager** (`ALERT`/`expr`/`for:` duration, *silence*/snooze) — the
  management-CRUD and alerting verbs (`list/ls/ps`, `enable/disable`, `pause/resume`, `silence`).

## Positioning — the open niche: "the DuckDB of CEP"

Every established player is either a **distributed cluster** you provision (Flink, Spark
Structured Streaming, Materialize, RisingWave) or a **domain-locked rule language** (Sigma/SPL/KQL,
security only). The gap n1k1 could own:

> **A single pure-Go, CGO-free, cross-compiled binary** running a **git corpus of plain-SQL++
> standing rules** with **MQO scale** (the AhoCorasick predicate index), **authored + maintained by
> an AI agent**, deployable **embedded / at the edge / inside the bundle** with **zero
> infrastructure**.

The wedge is the *combination*, not any single axis: **embedded + SQL++ + AI-authored git corpus +
MQO**. DuckDB proved "serious SQL, no cluster, one binary" is a huge market for OLAP; the CEP
equivalent (standing detection rules, no cluster, one binary, AI-authored) is comparatively empty.

## Landscape (as of 2026) — who plays here

Four adjacent clusters, all converging on "standing SQL over a change stream, increasingly
AI-flavored":

- **Stream processors (cluster-scale).** Apache **Flink** (incumbent; pushed hard as **Flink SQL by
  Confluent** post-Immerok), **Spark Structured Streaming** (Databricks), **Kafka Streams / ksqlDB**,
  and **Arroyo** (Rust, SQL) — **acquired by Cloudflare (2025)**, now powering Cloudflare Pipelines.
- **Streaming databases / IVM (closest conceptual match).** **Materialize** (differential dataflow,
  Postgres-wire), **RisingWave** (Rust, Postgres-wire, OSS) — now explicitly branding itself
  *"Streaming Infrastructure for Agentic AI,"* **Feldera** (DBSP incremental theory), **Timeplus /
  Proton** (ClickHouse-based; markets *"CEP made easy with streaming SQL + UDF"* — the nearest pitch
  to n1k1's), **Epsio** / **Readyset** (IVM bolted onto existing Postgres/MySQL via CDC), plus
  warehouse-native incremental (Snowflake Dynamic Tables, Databricks DLT).
- **Classic CEP (StreamBase heritage).** TIBCO **Streaming** (the StreamBase lineage), **Esper**
  (OSS EPL), Flink CEP.
- **Detections-as-code / SIEM (the "corpus of detectors" domain).** **Sigma** (vendor-neutral OSS
  detection-rule format — spiritually identical to n1k1's SQL++ corpus, different rule language),
  **Panther** (detections-as-code, streaming), **Elastic Security** (ES|QL rules), **Splunk** (SPL),
  **Falco** (runtime rules). Real 2025 convergence: Rust "Flink-alternatives for detection
  engineering" pairing Sigma + MITRE ATT&CK with stream processing.
- **Data observability + agentic (monitoring *datasets*).** **Monte Carlo** and **Anomalo**
  (freshness/volume/schema-drift/distribution) — *both added "agentic observability" layers in 2025*
  trying to bridge detection→resolution; Bigeye / Metaplane nearby.
- **AI-SRE agents (frothy, 2025–26).** A wave of autonomous monitor-and-diagnose agents (OpenObserve,
  IBM Instana GenAI, and many startups). Consistent analyst read: many single-purpose AI-SRE
  startups now, **consolidating** toward "one platform orchestrating many agents" by ~2027.
- **CDC on-ramp.** **Debezium** (OSS standard), **Estuary Flow**, **Decodable** (managed Flink),
  **Striim**.

Sources: [RisingWave — streaming DB landscape 2026](https://risingwave.com/blog/streaming-database-landscape-2026-complete-guide/) ·
[RisingWave home ("Streaming Infra for Agentic AI")](https://risingwave.com/) ·
[Epsio / IVM / differential dataflow](https://materializedview.io/p/epsio-ivms-differential-dataflow) ·
[Materialize vs RisingWave](https://materialize.com/guides/materialize-vs-risingwave/) ·
[Timeplus — CEP with streaming SQL + UDF](https://www.timeplus.com/post/cep-with-streaming-sql-udf) ·
[detection-as-code (GitHub topic)](https://github.com/topics/detection-as-code) ·
[Sigma — Complex Event Processing](https://www.sigmacomputing.com/blog/complex-event-processing-cep) ·
[Monte Carlo vs Anomalo](https://www.anomalo.com/blog/monte-carlo-vs-anomalo/) ·
[Estuary — CDC reality](https://estuary.dev/blog/cdc-magic/) ·
[Mezmo — 5 startups defining AI SRE](https://www.mezmo.com/newsroom/5-startups-defining-ai-sre) ·
[Fabrix.ai — 2026: agentic AI disrupts observability](https://fabrix.ai/blog/2026-the-year-agentic-ai-disrupts-observability-security-and-enterprise-saas/)

## Risks & open questions

- **Exactly-once + distributed fault tolerance is the heavyweights' moat.** Don't chase it; own the
  embedded / at-least-once / restart-from-offset / edge lane. Be explicit that n1k1 is not Flink.
- **Continuous operation is real engine work** (weeks, not days) even with good seeds — the
  unbounded source + watermark/emit model + incremental-state generalization are each non-trivial.
- **Crowded, consolidating market.** The wedge must be the combination nobody else has (embedded +
  SQL++ + AI-authored git corpus + MQO), not a me-too stream processor.
- **Grammar constraint still holds** ([DESIGN-prepare.md](DESIGN-prepare.md)): no dialect changes —
  windows/rate/burst/streak/gap via stock `OVER (…)`, temporal via the ASOF idiom, no `EMIT`/`STREAM`
  keywords. Continuous semantics must ride config/pragmas/CLI, not new SQL++ syntax. A `.macro.js`
  could sugar common CEP patterns (rate/burst/absence) into stock SQL++ without touching the parser.
- **State durability model unsettled** — where does long-running aggregate/window state live across
  restarts (spill files? an offset+snapshot?), and how is it reconciled with source replay.
- **Delta-report synergy** — the PREPARE++ re-run **delta report** (keyed by fingerprint + corpus
  SHA) is the batch analog of "emit only on change"; the two should share a design.
