# Design: n1k1 CEP — named cursors, standing queries, and monitors

> **Spec + build plan.** Turn n1k1's MQO / `.multi` substrate (see
> [DESIGN-prepare.md](DESIGN-prepare.md), "PREPARE++") into a **poll-first, cursor-based
> standing-query engine** for AI agents and humans: *"what changed since I last looked?"* answered
> by a named cursor with no daemon, graduating later to a long-running `n1k1 serve` with monitors.
> Background — why this is a natural fit, prior art, and the market — is condensed at the end.
> "CEP" (complex event processing) is used over "streaming" (overloaded with ETL/transport); the
> subject is *standing queries reacting to events*.

## What you get, if we build this

Concrete capabilities, from the user/agent point of view — in the order they'd ship:

- **Poll "what changed since last time", by name, no daemon.** A cron/harness agent (or a human,
  or CI) runs one command, gets a compact delta of only what's new since its last visit, and exits.
  It holds **only a name** — n1k1 holds the high-water mark.

  ```
  $ n1k1 .multi run triage.sql --cursor=new-github-issues   # or MCP tool call
  → { "since":"@2026-07-27T09:00Z", "now":"@2026-07-27T14:00Z", "count":3,
      "findings":[ {op:insert, id:"#57", detector:"triage@a1b2", …}, … ] }
  # the named cursor advanced + persisted; agent acts on 3 items and exits. Re-running is safe.
  ```

- **Two flavors of "changed":** new **appended** rows (new issues / tickets / emails / log lines),
  *and* **diffs of mutable records** (issue #42 → closed, #57 new, #12 deleted) even when the source
  only exposes current state (a plain REST API), via a stored snapshot.

- **GitOps for standing queries.** Keep your detectors as recipe files in git; `n1k1 .multi apply
  monitors/` reconciles the live set to match — idempotent, PR-reviewable, Flux/Argo-style. The
  committed files are the durable definition; cursors are local state (like Terraform's `tfstate`).

- **An authoring loop an agent can trust.** Write a detector in stock SQL++ from a natural-language
  ask, `.multi test` it against a fixture, `.multi lint` tells you *before* it goes live whether
  it's sound and cheap (fuses? index-pruned? boxed?). This is what makes an LLM-authored corpus
  trustworthy rather than a pile of unvetted queries.

- **Later: `n1k1 serve` + MCP.** Long-running monitors that run themselves on a schedule (or truly
  `follow` a live source), exposed as MCP resources an agent harness auto-discovers and subscribes
  to. Same names, same recipes — the CLI cursors *become* server monitors.

The through-line: **the store of named cursors/monitors is an episodic agent's externalized standing
memory** — a fresh wake asks `list` / `poll <name>` instead of reconstructing "what am I watching
and where did I leave off?" from nothing.

## Model & vocabulary

The key distinction (and the answer to "does *monitor* fit the run-and-done CLI?" — it doesn't):

- **cursor** — a small **named, durable high-water position** (`NAME → {source → offset}`). This is
  the *only* thing that persists in lightweight poll mode; nothing is "monitoring." A `.multi run
  --cursor=NAME` reads-since it and advances it on success, then the process exits. **The poll-mode
  primitive.**
- **detector** — the SQL++ rule (existing n1k1 term); **finding** — an emitted result row (existing).
- **monitor** — the **serve-mode** live entity: *a cursor that also carries its query + schedule and
  runs itself* — a cursor with a heartbeat. Only exists inside `n1k1 serve`.
- **Graduation:** named cursors (CLI, run-and-done) **→** monitors (server, self-running). Same
  cursor underneath; a monitor is "a cursor that hangs around." So "monitor" is reserved for the
  server, where it's the right word, and never imposed on the run-and-done CLI.

Every cursor/monitor also carries metadata (a NAME alone can't be an agent's memory — it loses the
*why*), using the Kubernetes labels-vs-annotations split:

- **`name`** — stable identity/handle *only* (it's the cursor key + the gitops reconcile key;
  renaming = delete+recreate = lose the cursor). Keep changeable meaning out of it.
- **`description`** — a first-class free-text line.
- **`labels`** — small, indexed, *selectable* `key=value` (`team=support`, `severity=high`) → drive
  `list --selector`, routing, bulk ops.
- **`annotations`** — an arbitrary client-owned JSON blob, **stored/returned verbatim, never
  interpreted** (size-bounded). Where an agent stuffs the NL ask, reference URLs, a runbook link,
  and especially **`provenance` = {authored_by, model, at, prompt, source_ref}** — the field that
  lets a future agent/human reconstruct why a monitor exists and trust/prune it.

⚠ **Metadata edits must not reset the cursor.** Delta-identity is `(query+binding SHA,
source-fingerprint)`; a retag/reword is a no-op for state, so a gitops `apply` never silently
rewinds a monitor just because someone relabeled it. All of it rides the existing recipe
front-matter (`-- key: value` + SQL++), so it's git-committed and PR-reviewable for free.

## Spec: the surface

### Run modes — one query, three cadences

The query is *pure stock SQL++*, unchanged across all three; cadence is an execution modifier:

| mode | behavior | who | lift |
|---|---|---|---|
| **`once`** (today) | scan to EOF, emit, exit | ad-hoc / batch | done |
| **`poll`** | scan only what's new since the named cursor, emit the delta, advance+persist, exit | **the cron/harness agent** | small |
| **`follow`** | block on a live source, emit continuously; process stays alive | dashboards, alerting daemons | large (engine work — see the gap) |

### Poll: the `--cursor=NAME` flag (+ optional `SINCE(NAME)`)

The canonical mechanism is a **run modifier**, not a monitor object and not SQL syntax:

- **`--cursor=NAME`** on `.multi run <recipe|dir>` (or on a plain query): (1) inject a since-filter
  at the scan so only rows past NAME's stored high-water are processed; (2) on *successful*
  completion, atomically advance + persist the new high-water. Run-and-done. This drives **both**
  delta strategies (append offset and snapshot diff) transparently, and — crucially — owns the
  read-*and*-advance as one committed-iff-success step (an at-least-once guarantee a `WHERE`
  predicate can't express).
- **Optional read-only escape hatch: a scalar `SINCE(NAME)` UDF** for authors who want the mark
  visible inside the query (`WHERE log.ts > SINCE('daily')`). It's grammar-legal (scalar UDFs
  exist; a `FROM tvf(…)` would not be). It **reads** the mark but never advances it (the flag owns
  advance) and only fits the append case with a monotonic column. Prefer the flag; keep `SINCE()`
  as sugar. (Why not a `FROM tail(…)`/streaming TVF: n1k1's extensions are scalar-only and a TVF
  needs the forbidden fork-grammar change — [DESIGN-prepare.md](DESIGN-prepare.md), "the one gap".)

### Named-cursor management (the run-and-done CRUD)

Because named cursors are the only durable thing in poll mode, they get a small management surface
(no "monitor" needed yet):

```
.multi cursors                 # list — NAME, sources, offset, last-run, count, lag
.cursor show   <NAME>          # the stored high-water + last-run summary
.cursor reset  <NAME> [--to …] # seek/rewind (Kafka seek / --from-beginning) — replay/backfill
.cursor rm     <NAME>          # forget it (next run starts fresh)
.cursor export/import <NAME>   # opaque token for handoff/backfill to another box (opt-in)
```

### Delta strategies — what "new" means

A recipe declares one (front-matter `mode:`):

- **`append`** — cursor is a high-water **offset** (byte/line for a file; seen-set + mtime for a
  dir; a Kafka offset; a CDC LSN). "New rows since the offset." Cheap, stateless beyond the offset.
  Covers new issues / tickets / emails / log lines.
- **`diff`/`snapshot`** — no natural offset, so keep a prior **snapshot keyed by doc-id** under the
  cursor NAME and diff current-vs-prior into the **Debezium envelope** `{op:insert|update|delete,
  id, before, after}`. This is `git diff` / `terraform plan` for arbitrary keyspaces — it's what
  lets an agent polling a current-state REST API still learn *what changed*. The snapshot spills
  (reuse the rhmap store) so it scales past RAM.

### State & idempotency — the agent holds a name, not a cookie

AI agents are bad at durably holding state (context resets between wakes), so the design never
requires an agent to persist a cursor across wakes:

- **The durable high-water lives store-side, keyed by NAME** — the agent's only cross-wake handle is
  a stable string. (Snowflake `STREAM` advances-on-consume; Kafka consumer-**group** offset by
  `group.id`; Dagster-sensor cursor.)
- **Embedded backend = a `tfstate`-style local dir** (`--cursor-store ./.n1k1-state/`, gitignored);
  n1k1 is then a pure function of `(recipe, cursor-store, source)`. Keep the cursor an **opaque,
  serializable, comparable value** so the *same* code works when the backend later becomes a served
  KV or a distributed object store.
- **Within-tick ack** (see Delivery & crash-safety below): `poll` returns findings + a *candidate*
  next-cursor **without** advancing; an `ack(NAME, tick-id)` commits. The only "cookie" is a
  *within-tick* one the agent holds in working context and discards — never across wakes. Because
  agents crash, **ack-required is the safe default**; `--ack=auto` (advance-on-emit) is the
  fire-and-forget opt-in.
- **Action idempotency is separate:** the cursor stops n1k1 *re-emitting*; it doesn't stop the agent
  *acting twice*. Each finding carries a **fingerprint** = hash of `(detector-sha, source-id,
  matched-key)` (the Alertmanager/PagerDuty `dedup_key`) so the agent dedupes side effects
  independently of cursor advance.

### Delivery & crash-safety — the tick journal (n1k1 owns durability, not the agent)

The failure auto-advance invites: the agent runs `--cursor=NAME`, n1k1 emits to stdout and advances,
the agent crashes *before* durably acting → those findings are lost, never re-delivered (Kafka's
`enable.auto.commit` foot-gun). The commit point decides the guarantee: advance-before-emit =
at-most-once; advance-on-emit-success (stdout flushed, exit 0) catches a *broken pipe* (dead reader)
but **not** "agent read it all, then crashed while acting" — still at-most-once from the agent's
side; **advance-only-on-ack** = at-least-once.

Since the agent is the unreliable party, durability belongs on the reliable side — n1k1 — so the
agent needn't `tee` its own `.out`. Make a **tick** a durable, replayable unit n1k1 owns:

- A `poll`/`run --cursor` **opens a tick**: n1k1 journals `{tick-id, from→to cursor, findings,
  started-at}` under `.n1k1/ticks/<NAME>/` and returns it, but leaves the *committed* cursor at
  `from`.
- The agent processes, then `.cursor ack <NAME> <tick-id>` → commits `to`, closes the tick.
- **Crash before ack → the next `poll` re-delivers the open tick verbatim from the journal** (exact
  same rows, no re-scan) instead of advancing. At-least-once with exact-content replay. Finding
  fingerprints make re-delivery safe (idempotent consumer) → exactly-once *effect*.

This one journal unifies the obvious remedies: it *is* "n1k1 records the `.out` in `.n1k1/`", its
open-tick re-delivery *is* the crash safety net, and keeping the last K closed ticks *is* "a few
rounds of previous cursor states" — a **reflog**: `.cursor log <NAME>` (history) + `.cursor reset
<NAME> --back N` (rewind/replay). Bounded ring (GC old ticks); journal full findings when small,
else positions + fingerprints + count and replay by re-running from `from` (idempotent via
fingerprints). Prior art: git `reflog`, ZFS snapshots, Kafka seek-back, the transactional outbox.

### Composition — a DAG of packs (one pack's findings feed the next)

Findings are just rows, so a pack's findings are themselves a keyspace another pack can `FROM` —
which makes a **hierarchy of `.multi` packs a DAG**: primitive detections feeding
correlation/aggregation packs. Prior art: **Prometheus recording rules → alerting rules**, SIEM
base-detections → correlation-rules, **dbt** models `ref()`-ing models into a topologically-ordered
DAG, cascading materialized views.

- Pack A `FROM indexer_log` → findings `pack:A`. Pack B `FROM pack:A GROUP BY … HAVING count>N` →
  higher-level "incident" findings. n1k1 topologically orders the DAG (reject cycles, like dbt).
- **Synergy with the tick journal:** A's journaled tick output *is* the materialized intermediate B
  reads — one mechanism (the durable findings outbox) serves both crash-safety *and* inter-pack
  dataflow. Each pack keeps its **own cursor** over its (derived) source, so incremental poll
  composes down the DAG: A's fresh findings this tick are B's new input rows.
- **Lineage composes:** a B-finding carries the `detector@sha` chain + the fingerprints of the
  A-findings (and their source rows) that produced it, so an agent can answer *"why did this
  incident fire?"* by walking the lineage.
- ⚠ **Fully-incremental-across-layers is the hard part** (the Materialize/DBSP problem). MVP keeps
  it simple: materialize A's findings as a keyspace and let B re-poll it with its own cursor, rather
  than true cross-layer delta propagation.

### GitOps — declarative reconcile (the preferred agent workflow)

Agents are great at authoring/regenerating declarative files and bad at long imperative CRUD they
must remember they ran. The Terraform model maps 1:1:

| Terraform | n1k1 |
|---|---|
| `.tf` config (git) | recipe files in a dir — each = detector + binding + policy + metadata |
| `tfstate` (a backend, gitignored) | cursors + run logs — local/served state |
| `plan` | **`.multi plan <dir>`** — diff declared-vs-live (create/update/destroy), folds in `.multi lint`; no changes |
| `apply` | **`.multi apply <dir>` [`--prune`]** — reconcile; **cursors of unchanged entries preserved** |

Blessed workflow: agent commits `monitors/*.sql` → `n1k1 .multi apply monitors/`. Idempotent
(re-apply = no-op), PR-reviewable, auto-apply-on-merge for free. The **definition is the agent's
durable memory (the file); the cursor is n1k1's job** (keyed by the file's name). ⚠ Declarative
`apply` is blessed; imperative `.cursor`/`create` are subordinate exploratory tools (they can drift
like `kubectl edit` vs GitOps).

### Monitors & the wire API (serve mode)

`n1k1 serve <dir>` promotes named cursors into **monitors** — each a cursor + its query + schedule
+ status, run by the server. Two transports:

- **MCP** (primary — agent harnesses already speak it): each monitor is an **MCP resource**;
  `resources/subscribe` + `notifications/resources/updated` = `follow`; tools `monitor.poll` /
  `list` / `create` / `delete`. A cron agent calls the poll tool; a long-running agent subscribes.
- **Long-poll HTTP** (`_changes?feed=longpoll|continuous|normal`, CouchDB-shaped) for plain `curl` /
  shell clients.

Monitor CRUD (`create/list/show/pause/resume/reset/snooze/delete`) mirrors the CLI + these verbs.
**Insight:** `serve` can begin as *scheduled-poll* (the server runs Phase-1 `poll` on a timer — no
unbounded-source engine work), and only later add true `follow`. So monitors are useful well before
the continuous-operation engine work lands.

### Three axes, one frozen grammar

None of the above touches the SQL++ dialect: **query = pure SQL++**, **source liveness = the
late-binding manifest** (`orders → glob(*.json)` static vs `tail(app.log)` / `kafka://` / `cdc://`
/ `poll(url, every=5m)` live), **cadence = a run modifier** (`--cursor` / mode / monitor policy).
The same detector replays statically, catches up via poll, and follows live — liveness is *data*
(the binding), not *code* (the query).

## Build plan

Each phase is independently shippable and useful. Phases 1–3 need **no daemon, no unbounded-source
engine work, and no grammar change** — pure cursor + delta + reconcile plumbing over the existing
scan + corpus machinery.

**Phase 1 — Named cursors + `poll` + the tick journal (MVP).** *Build on:* push-based scan,
`records.Source` over jsonl/dir, `.multi run`, `_meta.pos/offset`, the recipe loader. *New:* a
cursor store (`.n1k1-state/cursors/<NAME>`, atomic write-temp-rename); the `--cursor=NAME` modifier
(since-filter + advance); **append** delta only; the **tick journal** (`.n1k1/ticks/<NAME>/`,
bounded ring) giving at-least-once open-tick re-delivery, `ack`, and a reflog; and `.multi cursors`
/ `.cursor show|reset|rm|log|ack`. → The whole crash-safe run-and-done "what's new" loop for append
sources.

**Phase 2 — `diff`/snapshot delta.** *Build on:* the spillable rhmap store, doc-id extraction.
*New:* `mode: diff` — persist a prior snapshot keyed by id under the cursor, diff on run, emit the
Debezium envelope, replace snapshot. → "what changed" on mutable / current-state-only sources.

**Phase 3 — GitOps `plan` / `apply`.** *Build on:* the corpus loader (already reads a dir of
recipes), `.multi lint`. *New:* treat a recipe dir as desired-state; `plan` (diff + lint) and
`apply --prune` (reconcile, preserve unchanged cursors); labels/annotations in front-matter.

**Phase 4 — Composition (pack DAG).** *Build on:* temp-tables / CTEs / sequence op (exist), and the
Phase-1 tick journal as the materialized intermediate. *New:* a `pack:<name>` findings keyspace a
downstream pack can `FROM`; topological ordering (reject cycles); per-pack cursors so incremental
poll composes; lineage on findings. MVP re-polls A's materialized findings from B (not true
cross-layer delta). → correlation/incident packs over primitive detections.

**Phase 5 — `n1k1 serve` + MCP (scheduled monitors).** *New:* a long-running process holding the
cursor store; the `monitor` object (cursor + query + schedule + status); server-driven scheduled
`poll`; the MCP resource/tool/subscribe surface + long-poll HTTP. No unbounded source yet
(scheduled-poll reuses Phase 1). → self-running monitors, agent-subscribable.

**Phase 6 — true `follow` / continuous.** The heavy engine work (see next section): unbounded
source, continuous/watermark emit, incremental standing state, event-time, and a distributed
cursor-store backend.

## The engine gap for `follow` (Phase 6)

Continuous operation is real work, but each piece has a seed in the codebase:

1. **Unbounded source** — a `records.Source` that parks instead of returning `io.EOF`
   (tail/dir-watch/socket/Kafka/CDC). Execution is already push-based, and the JS streaming-extract
   already proved a **backpressured cross-goroutine emit** ([DESIGN-data.md](DESIGN-data.md)).
2. **Continuous emit** — fused filter+project detectors already emit per row (free once unbounded);
   only windowed/aggregate detectors need emit-on-**watermark** instead of emit-at-EOF.
3. **Incremental standing state** — generalize the existing **O(N) incremental window fold** +
   invertible COUNT slide; hang the accumulator on the spill-backed rhmap, with retention/TTL.
4. **Event-time + watermarks + out-of-order** — the ASOF merge ([DESIGN-merging.md](DESIGN-merging.md))
   already assumes a sortable time key; a watermark bounds how long to wait for late events.
5. **Fault tolerance** — deliberately shallow: at-least-once + restart-from-source-offset, **not**
   distributed exactly-once (that's the heavyweights' moat; don't compete there).

⚠ **Partition the window/context state by source key** — the `_meta.pos`-per-file gotcha (positions
restart per file; the `grep -A/-B/-C` idiom partitions by `_meta.`path``) becomes an
offset-per-partition gotcha, so state/ordering don't leak across event partitions.

---

## Background: why this is a natural fit (condensed)

**The MQO substrate is the *hard half* of a CEP engine, already built and benchmarked.** PREPARE++
compiles a git corpus of SQL++ detectors once and runs it over each bundle — structurally "N
standing queries over one pass of data," i.e. CEP in batch clothing. Point the scan at an unbounded
source and it's continuous. The shared-scan levers map directly:

| n1k1 MQO lever (all DONE) | CEP equivalent |
|---|---|
| `OpBroadcast` — scan once, tee each `[]byte` row to K inlined detectors, zero-boxing | "one event → N standing queries" fan-out |
| `OpBroadcastIndexed` + `AhoCorasick` — wake only detectors whose *necessary* literal is present; O(K×rows) → ~O(hits×rows), **flat in K**, ~60× at K=1000 | the **rule-discrimination network** (Rete/pre-filter) — the single hardest high-fan-in CEP problem, and n1k1 has a *sound* one |
| `BroadcastCSE` — shared sub-predicates computed once/row | multi-query optimization / shared operators (NiagaraCQ → TelegraphCQ) |
| Window `OVER(…)` + incremental fold | sliding/tumbling/session windows, incremental aggregation (IVM seed) |
| ASOF nearest-preceding merge | temporal / as-of join |
| late binding (logical→physical) | connector/source binding; rebind is data not code |
| `.multi lint` report card | detections-as-code with a correctness+efficiency oracle |

The predicate index is the crown jewel: "N rules × M events/sec" is exactly what it already solves,
flat in N — and *making N large cheaply* is most of a CEP engine's value.

**The agentic angle:** an LLM turns "tell me when a gold customer churn-signals within 24h of a
payment failure" → a SQL++ detector → n1k1 runs it continuously → a finding → an action. The
`.multi lint` oracle (does it fuse/index-prune/box? match its fixture?) is the feedback loop that
makes an LLM-authored corpus trustworthy — a moat competitors would have to build, not just accept
the rules. First beachheads: **CDC monitoring** (Debezium → corpus → findings) and **dataset
monitoring** (freshness/volume/schema-drift monitors *are* detectors — the corpus is the
Monte-Carlo/Anomalo job, embeddable and in SQL).

**Positioning — "the DuckDB of CEP".** Every established player is either a distributed cluster you
provision (Flink, Materialize, RisingWave) or a domain-locked rule language (Sigma/SPL/KQL). The
open niche: *a single pure-Go, CGO-free binary running a git corpus of plain-SQL++ standing rules
with MQO scale, AI-authored, deployable embedded / at the edge, zero infra.* The wedge is the
combination — embedded + SQL++ + AI-authored git corpus + MQO — not any single axis.

## Prior art — nouns & verbs we're borrowing (the a-ha takeaways)

- **CouchDB/Couchbase `_changes`** (on-brand): `?since=<seq>` + `feed=normal|longpoll|continuous`.
  *The* canonical "what changed since seq N" API — maps 1:1 onto poll/follow + cursor.
- **Snowflake Streams + Tasks:** a `STREAM` is a change-cursor that **advances on consumption**; a
  `TASK` is scheduled SQL. The STREAM-vs-TASK (cursor vs scheduled-action) split is exactly our
  cursor-vs-monitor split — and why we reserve "task"/"monitor" for the thing that *does* something.
- **Watchman:** `watch`/`subscribe`/`trigger` + a `since` clockspec + named subscriptions.
- **Dagster sensors:** a `@sensor` persists a **cursor** string between ticks — the precedent for
  the cron-agent poll model.
- **Kafka consumer groups:** durable **offset**, `commit`, `seek`, `--from-beginning`, consumer
  **lag** — the append-cursor + at-least-once/manual-commit vocabulary.
- **Datomic `since`/`as-of`** — elegant temporal-cursor naming.
- **Kubernetes labels vs annotations** (+ Prometheus rules' `labels`/`annotations`, Datadog
  `tags`+`message`) — the selectable-vs-freeform metadata split.
- **Terraform `plan`/`apply` + `tfstate`** — declarative reconcile with state split out.
- **MCP** resources + `resources/subscribe` — the AI-native transport; monitors-as-resources.
- **cron/systemd/`crontab -l`, `docker ps`, `kubectl get --watch`, Alertmanager silence** — the
  management-CRUD + alerting verbs (`list/ls/ps`, `pause/resume`, `snooze`, `reset`).

## Landscape (as of 2026)

Four adjacent clusters, all converging on "standing SQL over a change stream, increasingly
AI-flavored":

- **Stream processors (cluster-scale):** Apache **Flink** (pushed as **Flink SQL by Confluent**),
  **Spark Structured Streaming**, **Kafka Streams/ksqlDB**, **Arroyo** (Rust, SQL; **acquired by
  Cloudflare, 2025**).
- **Streaming DBs / IVM (closest match):** **Materialize** (differential dataflow), **RisingWave**
  (now branding *"Streaming Infrastructure for Agentic AI"*), **Feldera** (DBSP), **Timeplus/Proton**
  (markets *"CEP made easy with streaming SQL + UDF"* — nearest pitch to n1k1's), **Epsio**/**Readyset**
  (IVM over Postgres/MySQL CDC), warehouse-native (Snowflake Dynamic Tables, Databricks DLT).
- **Classic CEP (StreamBase heritage):** TIBCO **Streaming**, **Esper**, Flink CEP.
- **Detections-as-code / SIEM:** **Sigma** (vendor-neutral OSS rule format — spiritually identical
  to the SQL++ corpus), **Panther**, **Elastic Security** (ES|QL), **Splunk** (SPL), **Falco**.
- **Data observability + agentic:** **Monte Carlo** and **Anomalo** (both added "agentic
  observability" layers in 2025); Bigeye/Metaplane.
- **AI-SRE agents (frothy):** OpenObserve, IBM Instana GenAI, many startups — consolidating toward
  "one platform orchestrating many agents" by ~2027.
- **CDC on-ramp:** **Debezium** (OSS standard), **Estuary Flow**, **Decodable**, **Striim**.

Sources: [RisingWave — streaming DB landscape 2026](https://risingwave.com/blog/streaming-database-landscape-2026-complete-guide/) ·
[RisingWave home](https://risingwave.com/) ·
[Epsio/IVM/differential dataflow](https://materializedview.io/p/epsio-ivms-differential-dataflow) ·
[Materialize vs RisingWave](https://materialize.com/guides/materialize-vs-risingwave/) ·
[Timeplus — CEP with streaming SQL + UDF](https://www.timeplus.com/post/cep-with-streaming-sql-udf) ·
[detection-as-code (GitHub topic)](https://github.com/topics/detection-as-code) ·
[Sigma — CEP](https://www.sigmacomputing.com/blog/complex-event-processing-cep) ·
[Monte Carlo vs Anomalo](https://www.anomalo.com/blog/monte-carlo-vs-anomalo/) ·
[Estuary — CDC reality](https://estuary.dev/blog/cdc-magic/) ·
[Mezmo — 5 startups defining AI SRE](https://www.mezmo.com/newsroom/5-startups-defining-ai-sre) ·
[Fabrix.ai — 2026 agentic AI disrupts observability](https://fabrix.ai/blog/2026-the-year-agentic-ai-disrupts-observability-security-and-enterprise-saas/)

## Risks & open questions

- **Don't chase distributed exactly-once** — own the embedded / at-least-once / restart-from-offset
  edge lane; n1k1 is not Flink.
- **Continuous operation (Phase 6) is real engine work** — weeks, not days — even with good seeds.
- **Crowded, consolidating market** — the wedge must be the combination nobody else has, not a
  me-too processor.
- **Grammar stays frozen** — no `EMIT`/`STREAM`/TVF syntax; cadence/liveness ride config + the
  binding; a `.macro.js` can sugar rate/burst/absence patterns into stock SQL++.
- **State durability across restarts** unsettled for Phase 6 windows/aggregates (spill files? an
  offset+snapshot?), and how it reconciles with source replay.
- **Delta-report synergy** — PREPARE++'s re-run delta report (keyed by fingerprint + corpus SHA) is
  the batch analog of "emit only on change"; the two should share a design.
