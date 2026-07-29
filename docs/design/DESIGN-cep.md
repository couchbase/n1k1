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
  $ n1k1 .multi cursor peek new-github-issues   # a safe look; or the MCP tool call
  → { "cursor":"new-github-issues", "status":"pending", "count":3,
      "from":"gh:issues@321", "to":"gh:issues@324",
      "labelResults":[ {"op":"insert","id":"#322","fingerprint":"f5e1a2", "…":"…"}, "…" ] }
  # peek does NOT move the cursor (safe). Agent acts, then
  # `.multi cursor advance new-github-issues --to gh:issues@324` commits. Crash before that?
  # re-peek still returns everything pending (a superset if more arrived) — never misses.
  # (bare `advance` = get + move in one; fail-safe — it echoes what it passed.)
  # (One-time setup earlier: `.multi cursor create new-github-issues --pack triage.sql`.)
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
memory** — a fresh wake asks `cursor list` / `cursor peek <name>` instead of reconstructing "what
am I watching and where did I leave off?" from nothing.

## Model & vocabulary

- **cursor** — a small **named, durable high-water position** (`NAME → {source → offset}`), **bound
  to the query-pack it polls** (its record also holds the `(pack, binding)` identity — see Command
  taxonomy). The *only* thing that persists in poll mode (nothing is "monitoring"): `.multi run
  cursor create NAME --pack <pack>` binds it, then `peek`/`advance` drive it. **The poll-mode
  primitive.**
- **detector** — the SQL++ rule (existing n1k1 term). **labelResult** — an emitted output row: the
  struct `glue.LabelResult` pairs a `label` (which detector fired) with its `result` value (the
  row's `SELECT` projection). Named distinctly (not plain "result") so it's greppable and unmistakable.
- **monitor** — the **serve-mode** live entity: *a cursor that also carries its query + schedule and
  runs itself* — a cursor with a heartbeat. Only exists inside `n1k1 serve`.
- **Graduation:** named cursors (CLI, run-and-done) **→** monitors (server, self-running) — same
  cursor underneath; a monitor is "a cursor that hangs around," so the word is reserved for `serve`.

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
| **`peek`/`advance`** (poll cadence) | one caller-driven step: run the bound pack from the named cursor, emit the delta. `peek` does *not* move the cursor; `advance` moves it (and echoes what it passed) | **the cron/harness agent** | small |
| **`follow`** | block on a live source, emit continuously; process stays alive | dashboards, alerting daemons | large (engine work — see the gap) |

### Verbs: `peek` / `advance` (+ `--quiet`)

n1k1 here is a **durable consumer reading forward over a change stream** — a cursor with a committed
position, lag, and at-least-once redelivery (the queue/stream-consumer model). Two verbs, split by
whether they move the cursor — **`peek` never moves, `advance` always moves**:

- **`.multi cursor peek NAME`** — look: the pending delta (everything since the committed position);
  the cursor does **not** move. It's **non-advancing**, not idempotent — re-`peek` reports whatever
  is pending *as of now* (a **superset** if the source grew), so it **never misses** anything
  (at-least-once). Inviolably non-mutating (queue/stack *peek* = look at the head without consuming).
- **`.multi cursor advance NAME [--to <pos>]`** — move the cursor forward **and return the delta it
  passed**, so a one-call `advance` (get + move, the fire-and-forget) still *shows you what it
  skipped* — **fail-safe** (echoing by default prevents silently moving past unseen data). `--to`
  pins the target from a prior `peek` (errors if the committed position moved under you); bare
  `advance` goes to the current head. This is Kafka's *commit-offset*, said plainly.
- **`.multi cursor advance --quiet NAME --to <pos>`** — same move, ack only (`from`/`to`/`count`, no
  labelResults): the lightweight commit for the safe two-step `peek` → act → `advance --quiet` where you
  already hold the delta (token economy). The flag trims *output*, never the mutation.
- **`.multi cursor create NAME --pack <pack> [--bind <manifest>] [--from now|beginning]`** — the
  one-time **bind**: register NAME → (pack, binding, start position). It **validates, not just
  registers** — compiles the pack (`.multi lint`: fuse/standalone/reject), resolves the binding
  **fail-loud** (the source must exist), and cheaply probes it — returning a structured ok/error
  report, *no result rows*. `--from now` (default) means the first `peek` returns only what arrives
  after creation; `--from beginning` replays all history (Kafka `latest`/`earliest`). So binding is
  cleanly separate from getting rows: **`peek`/`advance` are the *only* result-getters, one shape** —
  the first peek looks exactly like every later one.

You commit a *position* (`advance --to <pos>`), as a stream consumer commits an offset — no batch id
to track. Other stream-consumer concepts carry over: **lag** (how far behind), **`reset`/`seek`**
(jump to an arbitrary position, distinct from forward `advance`), **at-least-once** +
**fingerprint-dedup**.

Optional read-only escape hatch inside SQL: the scalar **`SINCE(NAME)`** UDF for authors who want the
mark visible in a predicate (`WHERE log.ts > SINCE('daily')`). Grammar-legal (scalar UDFs exist; a
`FROM tvf(…)` would not be — [DESIGN-prepare.md](DESIGN-prepare.md), "the one gap"). It **reads** the
mark, never advances it (the verbs own advance), and only fits append with a monotonic column.
Prefer the verbs; keep `SINCE()` as sugar.

### Command taxonomy — cursors live under `.multi` (a cursor is meaningless without a query)

A cursor is the persisted state of *polling a particular query-pack*, so it has no meaning on its
own — hence it lives **under `.multi`**, not as a top-level `.cursor`. `.multi cursor create NAME
--pack <pack>` **binds + validates** NAME → that pack's identity (`query+binding SHA`) + a start
position; thereafter the cursor knows its own query (`.multi cursor show NAME` reveals the bound
pack), so `.multi cursor peek NAME` re-runs it without re-naming the pack — and `create` on an
existing NAME with a *different* pack is an **error**, not a silent meaningless reuse.

The pack verbs stay flat (`run`/`lint`/`explain`/`test`/`list`); ancillary state gets a sub-noun,
`.multi cursor <verb>` (later `.multi monitor <verb>`) — the git model (`git commit`/`git log` flat,
`git remote`/`git stash` grouped).

```
.multi cursor create  <NAME> --pack <pack> [--from now|beginning]   # BIND + VALIDATE; ok/error report, no rows
.multi cursor peek    <NAME>            # look: pending delta; cursor NOT moved (safe, non-advancing)
.multi cursor advance <NAME> [--to <pos>]   # move forward + RETURN the delta passed (fail-safe get+move); bare = fire-and-forget
.multi cursor advance --quiet <NAME> --to <pos>   # move + ack only (no labelResults) — the two-step commit after a peek
.multi cursor list                      # NAME, bound pack, sources, committed position, last-run, count, lag
.multi cursor show    <NAME>            # committed position + bound pack/binding + pending? + last-run summary
.multi cursor log     <NAME>            # advance history (reflog)
.multi cursor reset   <NAME> [--to … | --back N]   # seek/rewind — replay/backfill
.multi cursor rm      <NAME>            # forget it (next bind starts fresh)
.multi cursor export/import <NAME>      # opaque position token for handoff/backfill (opt-in)
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
- **Two-step advance** (see Delivery & crash-safety below): `peek` returns the pending delta
  **without** moving the cursor; a later `advance(NAME, --to <pos>)` commits it. The only "cookie" is
  a within-*step* one (the `to` position) the agent holds in working context and discards — never
  across wakes. Because agents crash, the safe pattern is **`peek` (look, don't move) → act →
  `advance --quiet`**; bare `advance` (get + move in one) is the fire-and-forget opt-in.
- **Action idempotency is separate:** the cursor stops n1k1 *re-emitting*; it doesn't stop the agent
  *acting twice*. Each labelResult carries a **fingerprint** = hash of `(detector-sha, source-id,
  matched-key)` (the Alertmanager/PagerDuty `dedup_key`) so the agent dedupes side effects
  independently of cursor advance.

### Delivery & crash-safety — `peek` never misses (it's non-advancing)

The failure a get-and-advance-in-one invites: the agent gets the delta, the cursor advances, the
agent crashes *before* durably acting → lost, never redelivered (Kafka's `enable.auto.commit`
foot-gun). The fix is the two-step, and its whole content is: **getting the delta must not move the
cursor.**

- **`peek` returns the pending delta without moving the cursor**, so a crash any time before
  `advance` **never misses** anything — the next `peek` still returns it (a superset if the source
  grew) = at-least-once. (Byte-identical replay is what the optional journal adds.)
- **`advance --to <pos> --quiet`** commits once you've durably acted (the two-step); bare **`advance`**
  (get + move, echoing the delta) is the one-call fire-and-forget (at-most-once).
- **LabelResult fingerprints** (the `dedup_key`) make redelivery safe: since re-`peek` can hand back
  rows already acted on, the agent dedupes by fingerprint → at-least-once + idempotent consumer =
  exactly-once *effect*.

So the agent needn't `tee` its own `.out` and there's **no batch id** to track — durability *is* the
committed position (n1k1's); the pending delta is recomputable on demand. *(Optional: journal each
pending delta under `.n1k1/journal/<NAME>/` for exact/re-scan-free replay + a `log`/`reset --back`
history — an internal optimization. Prior art: Kafka seek-back, the transactional outbox.)*

### Worked example — the JSON an agent sees

One response envelope across every case. `status` tells the agent what happened; `advanced` says
whether the *committed* cursor moved. `from`/`to` are opaque position tokens (shown here in a
human-legible form: `gh:issues@324`, `file:app.log#20480`, `snap:9`) — the agent passes `to` back to
`advance`; there is no separate batch id.

**0 — `create` (one-time bind + validate).** Returns a validation report, *no rows*: pack compiled,
binding resolved (fail-loud), source probed, start position set. On failure the cursor is not
created:

```jsonc
$ n1k1 .multi cursor create new-github-issues --pack triage.sql --from now
{ "created":"new-github-issues", "ok":true, "pack":"triage@a1b2c3", "compiles":"fused",
  "bound":"gh_issues → poll(api.github.com/repos/…/issues, every=15m)", "source":"reachable",
  "from":"gh:issues@321" }

# validation failure — nothing is created:
{ "created":"new-github-issues", "ok":false,
  "error":{"kind":"source-unbound","message":"logical keyspace 'gh_issues' resolved to nothing (fail-loud)"} }
```

**1 — `peek`, new appended rows.** The pending delta since the committed position; the cursor does
**not** move (`advanced:false`, `status:"pending"`):

```jsonc
$ n1k1 .multi cursor peek new-github-issues
{ "cursor":"new-github-issues", "pack":"triage@a1b2c3", "status":"pending",
  "from":"gh:issues@321", "to":"gh:issues@324", "advanced":false, "count":3,
  "labelResults":[
    {"op":"insert","id":"#322","fingerprint":"f5e1a2","detector":"triage@a1b2c3",
     "doc":{"number":322,"title":"crash on startup","labels":["bug"]}},
    {"op":"insert","id":"#323","fingerprint":"9c0b7d","detector":"triage@a1b2c3","doc":{"…":"…"}},
    {"op":"insert","id":"#324","fingerprint":"2a4f11","detector":"triage@a1b2c3","doc":{"…":"…"}} ] }
```

**2 — `advance --quiet` (two-step commit).** Agent already peeked + acted, so it commits to that
`to` and asks for the ack only (no labelResults echoed back):

```jsonc
$ n1k1 .multi cursor advance --quiet new-github-issues --to gh:issues@324
{ "cursor":"new-github-issues", "status":"advanced",
  "from":"gh:issues@321", "to":"gh:issues@324", "advanced":true, "count":3 }
```

**3 — nothing new (the "nothing happened" case).** No rows past the cursor ⇒ `from==to`,
`advanced:false`, empty — the agent's cheap "nothing to do" signal:

```jsonc
$ n1k1 .multi cursor peek new-github-issues
{ "cursor":"new-github-issues", "pack":"triage@a1b2c3", "status":"empty",
  "from":"gh:issues@324", "to":"gh:issues@324", "advanced":false, "count":0, "labelResults":[] }
```

**4 — crash before `advance` → just re-`peek`.** No special path: `peek` is non-advancing, so it
still returns everything pending — and if two more issues arrived meanwhile, a *superset* (note `to`
moved 324→326, `count` 3→5). Nothing is missed; the agent dedupes #322–#324 by `fingerprint`:

```jsonc
$ n1k1 .multi cursor peek new-github-issues        # never advanced last time
{ "cursor":"new-github-issues", "pack":"triage@a1b2c3", "status":"pending",
  "from":"gh:issues@321", "to":"gh:issues@326", "advanced":false, "count":5,
  "labelResults":[ "… #322,#323,#324 (as before) + #325,#326 (arrived since) …" ] }
```

**5 — bare `advance` (get + move, fire-and-forget).** Moves to the current head *and echoes the
delta it passed* (fail-safe) — `advanced:true`, at-most-once, fine for "I don't mind missing some":

```jsonc
$ n1k1 .multi cursor advance log-errors
{ "cursor":"log-errors", "pack":"errsev@99", "status":"advanced",
  "from":"file:app.log#10240", "to":"file:app.log#20480", "advanced":true, "count":2, "labelResults":[ "…" ] }
```

**6 — `diff`-mode `peek` (mutable source).** Change events in the Debezium envelope:

```jsonc
$ n1k1 .multi cursor peek open-incidents
{ "cursor":"open-incidents", "pack":"incidents@7f", "status":"pending",
  "from":"snap:8", "to":"snap:9", "advanced":false, "count":3,
  "labelResults":[
    {"op":"update","id":"#42","before":{"status":"open"},"after":{"status":"closed"},"fingerprint":"…"},
    {"op":"insert","id":"#57","after":{"status":"open","sev":"high"},"fingerprint":"…"},
    {"op":"delete","id":"#12","before":{"status":"open"},"fingerprint":"…"} ] }
```

**7 — error (fail-loud).** A binding that resolves to nothing errors; the cursor never moves:

```jsonc
$ n1k1 .multi cursor peek new-github-issues
{ "cursor":"new-github-issues", "pack":"triage@a1b2c3", "status":"error",
  "from":"gh:issues@324", "to":"gh:issues@324", "advanced":false, "count":0,
  "error":{"kind":"source-unbound","message":"logical keyspace 'gh_issues' resolved to nothing (fail-loud)"} }
```

**8 — introspection, `.multi cursor show`:**

```jsonc
$ n1k1 .multi cursor show new-github-issues
{ "cursor":"new-github-issues", "pack":"triage@a1b2c3",
  "binding":"gh_issues → poll(api.github.com/repos/…/issues, every=15m)", "mode":"append",
  "committed":"gh:issues@324", "pending":false, "lag":"0 (current)",
  "last_run":"2026-07-27T14:03:11Z", "last_count":3, "total_advances":8,
  "labels":{"team":"support","severity":"normal"},
  "annotations":{"provenance":{"authored_by":"agent-x","model":"…","at":"2026-07-20",
                               "prompt":"tell me about new bug issues"}} }
```

The `status` enum an agent switches on: **`pending`** (a delta exists — from `peek`; act, then
`advance --quiet`, or use bare `advance`), **`advanced`** (position moved — from `advance`),
**`empty`** (nothing to do), and **`error`** (fail-loud; cursor unmoved). Only `advanced` implies
`advanced:true`; re-`peek` after a crash is `pending` again (non-advancing — may include newer rows,
never misses).

### Composition — a DAG of packs (one pack's labelResults feed the next)

LabelResults are just rows, so a pack's labelResults are themselves a keyspace another pack can `FROM` —
which makes a **hierarchy of `.multi` packs a DAG**: primitive detections feeding
correlation/aggregation packs. Prior art: **Prometheus recording rules → alerting rules**, SIEM
base-detections → correlation-rules, **dbt** models `ref()`-ing models into a topologically-ordered
DAG, cascading materialized views.

- Pack A `FROM indexer_log` → labelResults `pack:A`. Pack B `FROM pack:A GROUP BY … HAVING count>N` →
  higher-level "incident" labelResults. n1k1 topologically orders the DAG (reject cycles, like dbt).
- **Synergy with the journal:** A's materialized labelResults (the same durable outbox that makes
  replay exact) *are* the intermediate B reads — one mechanism serves both crash-safety *and*
  inter-pack dataflow. Each pack keeps its **own cursor** over its (derived) source, so incremental
  polling composes down the DAG: A's fresh labelResults this step are B's new input rows.
- **Lineage composes:** a B-labelResult carries the `detector@sha` chain + the fingerprints of the
  A-labelResults (and their source rows) that produced it, so an agent can answer *"why did this
  incident fire?"* by walking the lineage.
- ⚠ **Fully-incremental-across-layers is the hard part** (the Materialize/DBSP problem). MVP keeps
  it simple: materialize A's labelResults as a keyspace and let B re-poll it with its own cursor, rather
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
`apply` is blessed; imperative `.multi cursor`/`create` are subordinate exploratory tools (they can
drift like `kubectl edit` vs GitOps).

### Monitors & the wire API (serve mode)

`n1k1 serve <dir>` promotes named cursors into **monitors** — each a cursor + its query + schedule
+ status, run by the server. Two transports:

- **MCP** (primary — agent harnesses already speak it): each monitor is an **MCP resource**;
  `resources/subscribe` + `notifications/resources/updated` = `follow`; tools `monitor.poll` /
  `list` / `create` / `delete`. A cron agent calls the poll tool; a long-running agent subscribes.
- **Long-poll HTTP** (`_changes?feed=longpoll|continuous|normal`, CouchDB-shaped) for plain `curl` /
  shell clients.

Monitor CRUD (`create/list/show/pause/resume/reset/snooze/delete`) mirrors the CLI + these verbs.
**Insight:** `serve` can begin as *scheduled-poll* (the server runs Phase-1 `peek`+`advance` on a timer — no
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

**Phase 1 — Named cursors + `peek`/`advance` (poll cadence) (MVP).** *Build on:* push-based scan,
`records.Source` over jsonl/dir, `.multi run`, `_meta.pos/offset`, the recipe loader. *New:* a
cursor store (`.n1k1-state/cursors/<NAME>`, atomic write-temp-rename); the cursor verbs
`.multi cursor {create,peek,advance,list,show,log,reset,rm}` (`create` binds + validates;
`advance` echoes by default, `--quiet` trims); **append** delta only; at-least-once (peek is non-advancing).
Optional: journal each pending delta (`.n1k1/journal/<NAME>/`) for exact/re-scan-free replay. → The
whole crash-safe run-and-done "what's new" loop for append sources.

**Phase 2 — `diff`/snapshot delta.** *Build on:* the spillable rhmap store, doc-id extraction.
*New:* `mode: diff` — persist a prior snapshot keyed by id under the cursor, diff on run, emit the
Debezium envelope, replace snapshot. → "what changed" on mutable / current-state-only sources.

**Phase 3 — GitOps `plan` / `apply`.** *Build on:* the corpus loader (already reads a dir of
recipes), `.multi lint`. *New:* treat a recipe dir as desired-state; `plan` (diff + lint) and
`apply --prune` (reconcile, preserve unchanged cursors); labels/annotations in front-matter.

**Phase 4 — Composition (pack DAG).** *Build on:* temp-tables / CTEs / sequence op (exist), and the
Phase-1 labelResults journal as the materialized intermediate. *New:* a `pack:<name>` labelResults keyspace a
downstream pack can `FROM`; topological ordering (reject cycles); per-pack cursors so incremental
poll composes; lineage on labelResults. MVP re-polls A's materialized labelResults from B (not true
cross-layer delta). → correlation/incident packs over primitive detections.

**Phase 5 — `n1k1 serve` + MCP (scheduled monitors).** *New:* a long-running process holding the
cursor store; the `monitor` object (cursor + query + schedule + status); server-driven scheduled
`peek`+`advance`; the MCP resource/tool/subscribe surface + long-poll HTTP. No unbounded source yet
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
payment failure" → a SQL++ detector → n1k1 runs it continuously → a labelResult → an action. The
`.multi lint` oracle (does it fuse/index-prune/box? match its fixture?) is the feedback loop that
makes an LLM-authored corpus trustworthy — a moat competitors would have to build, not just accept
the rules. First beachheads: **CDC monitoring** (Debezium → corpus → labelResults) and **dataset
monitoring** (freshness/volume/schema-drift monitors *are* detectors — the corpus is the
Monte-Carlo/Anomalo job, embeddable and in SQL).

**Positioning — "the DuckDB of CEP".** Every established player is either a distributed cluster you
provision (Flink, Materialize, RisingWave) or a domain-locked rule language (Sigma/SPL/KQL). The
open niche: *a single pure-Go, CGO-free binary running a git corpus of plain-SQL++ standing rules
with MQO scale, AI-authored, deployable embedded / at the edge, zero infra.* The wedge is the
combination — embedded + SQL++ + AI-authored git corpus + MQO — not any single axis.

## Prior art — nouns & verbs we're borrowing (the a-ha takeaways)

- **Kafka / queue-consumer model** — what we are: a durable consumer with a committed **offset**,
  **lag**, **seek**, at-least-once + redelivery, and *commit-offset* (= `advance`). The fitting
  mental model, and the source of `peek`/`advance`/`lag`. (`git remote`/`git stash` lend the
  flat-verbs-+-sub-noun command taxonomy.)
- **CouchDB/Couchbase `_changes`** (on-brand): `?since=<seq>` + `feed=normal|longpoll|continuous`.
  *The* canonical "what changed since seq N" API — maps 1:1 onto peek/follow + cursor.
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
