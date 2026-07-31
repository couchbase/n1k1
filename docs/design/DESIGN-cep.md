# Design: n1k1 CEP — named cursors, standing queries, and monitors

> **⚠ Naming updated (2026-07 overhaul; this doc predates it).** Command examples below use
> pre-rename names — the current CLI is: `--pack` → **`--queries`**; a compose node reads an upstream
> node with **`FROM node('<name>')`** (not `FROM pack_<name>`; `pack_` is now an internal
> keyspace name); and the GitOps reconcile **`.multi cursor plan`/`apply` was removed** (returns with
> a `serve`/monitor runtime). Cursor verbs are `create`/`peek`/`advance`/`list`/`show`/`rm`. See
> `tmp/naming.md` for the full spec.

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
  taxonomy). The *only* thing that persists in poll mode (nothing is "monitoring"): `.multi cursor
  create NAME --pack <pack>` binds it, then `peek`/`advance` drive it. **The poll-mode primitive.**
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
- **`.multi cursor create NAME --pack <pack> [--bind <manifest>] [--from now|start]`** — the
  one-time **bind**: register NAME → (pack, binding, start position). It **validates, not just
  registers** — compiles the pack (`.multi lint`: fuse/standalone/reject), resolves the binding
  **fail-loud** (the source must exist), and cheaply probes it — returning a structured ok/error
  report, *no result rows*. `--from now` (default) means the first `peek` returns only what arrives
  after creation; `--from start` replays all history (Kafka `latest`/`earliest`; `beginning` is an
  accepted alias for `start`). So binding is
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
.multi cursor create  <NAME> --pack <pack> [--from now|start]   # BIND + VALIDATE; ok/error report, no rows
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

### Dynamic sub-stream sources (directory trees, git) — the non-monotonic case

Two grounding use cases — **follow `~/.claude/`** (a growing tree of jsonl files) and **follow git
commits across sibling worktrees** (agents spinning up branches) — both reduce to the *same* shape
the plain `append` model under-specifies: **a dynamic set of append-mostly sub-streams (files / git
refs) that each grow, appear, and eventually disappear.** They fit the design in shape but pin down
four things:

- **Position is a per-sub-stream map**, not a scalar: `{file → offset}` / `{ref → commit-sha}`.
  Sub-streams appear over time (new files/subdirs; new worktree branches) and the recursive glob /
  ref-set picks them up on the next `peek`.
- **Set-membership (create / DELETE) is a `diff` concern, not `append`** — and it's the *primary*
  cleanup signal, not front-truncation. The `~/.claude/` layout is **date-named** files/dirs
  (`YYYYMMDD…`), so a file only ever **grows or is deleted whole** — no shrink, no front-prune. Day-31
  cleanup is therefore a **whole-file/dir delete**, detected precisely because the cursor tracks the
  **path set**: a tracked path vanished. `append` surfaces new rows and silently ignores that
  vanishing; a `diff` cursor over the path set emits `{op:delete}` if you want to *observe* cleanup.
  So a tree monitor is `append` over contents + (optionally) `diff` over membership.
- ⚠ **The genuinely non-monotonic case is git, not files.** Date-named files don't rewind; but a git
  ref **can** — rebase/reset/force leaves the cursor SHA **no longer an ancestor** of the ref. Detect
  (`merge-base --is-ancestor` fails) and re-baseline (see the git provider below). (A *few* file
  sources do rewind — `logrotate copytruncate` shrinks in place; handle the same way, size-backwards
  → reset — but the common date-named layout sidesteps it.) Either way the **fingerprint must be
  content/identity-based** (a commit's SHA; a row's content) so any re-read dedupes rather than
  re-emitting as new.
- **Cursor-map retention** — the position map grows with every sub-stream ever seen; cleanup / dead
  worktrees leave stale entries, so prune them, or cursor state grows unbounded.

New source providers this implies: a **recursive jsonl-dir** source (per-file append offsets + path-set
membership; n1k1 nearly has it), and a **`git://` source** (next), both wired through the
late-binding manifest (`sessions → dir("~/.claude/**/*.jsonl")`, `commits → git(worktrees)`).

### "Append-mostly, with whole-file rotation" — field evidence, and what the cursor owes the census

Production confirmation of the sub-stream model's deletion case, from the n1k1-for-ai corpus
(2026-07): two full census passes twenty minutes apart read 198,589 then 194,527 records (458 → 457
files) — a session file **rotated away, taking the only two records carrying `attachment.path` with
it**. `COUNT(*)` for that field is now 0 corpus-wide. Their coinage names the source property
precisely: **append-mostly, with whole-file rotation.** Three consequences the design must own:

**1. The accumulated census is strictly more informative than any fresh scan — by design, so say so.**
A fresh scan proves *currently present*; only the committed, incrementally-folded census proves
*ever existed*. Corollaries: a census **replay** (`rm` + re-create `--from start`, or a consumer's
`--reset`) is **not idempotent against a rotating corpus** — it silently loses every field whose
evidence rotated away, so replay should be a last resort and its docs should say it forfeits
rotated history. And **`doctor` / any "field died" alarm must consult the accumulated census, not
a rescan**: `last_seen` stopped advancing = *behavior*; the carrying records left the corpus =
*retention*. Reporting the second as the first is a false finding — the consumer's rule ("check
committed history before reporting a change in behaviour rather than retention") is the right
default and Phase-4/doctor work should bake it in.

**2. What the shipped `append` watermark actually does under rotation/truncation (verified in
`RecordScanFilter`).** `NewWater` max-merges committed and observed offsets, and `admit` skips
at-or-below the watermark. So today:
- **Rotated file** → its watermark entry is carried forever (max-merge). Safe — a same-named
  reappearing file cannot re-deliver from 0 — but **silent**: nothing surfaces "a container in the
  committed position no longer exists", and the position map never shrinks (the retention bullet
  above).
- **Truncated / rewritten-in-place below the watermark** → the watermark **never rewinds**, so
  nothing double-delivers; instead every record of the new content below the old byte offset is
  **silently skipped, forever**. No error, no event. The `unsafe-position` guard does not apply —
  it validates `--to` position *tokens*, not the source changing beneath a committed position.
- **Appended-then-rotated between peeks** → those records are never delivered. The honest delivery
  contract is therefore: *at-least-once for records that survive until the next scan; never
  re-delivered; rotation and truncation losses are currently silent.*

**3. The response ladder** (cheap first):
- **Surface, don't guess — ✅ SHIPPED**: every `peek`/`advance` (pack AND census cursors) reports
  `rotated: [containers]` (committed water key, nothing observed this scan — deleted or now empty)
  and `truncated: [containers]` (observed extent below the committed offset) in the envelope —
  same disclosure pattern as `dropped/rewound/unknown` on `advance --to`. Evidence loss is an
  event a census/doctor can correlate. `advance --prune-rotated` drops the rotated entries from
  the committed position (acked as `pruned_rotated`; disclosed cost: a same-named file reappearing
  later replays from byte 0). `RecordScanFilter.SourceAnomalies`; guard:
  `TestMultiCursorRotationDisclosure`.
- **Fail-loud on truncation — ✅ SHIPPED**: a truncated container REFUSES `advance` (error kind
  `source-truncated`, position untouched) — the source violated `mode: append`'s contract, and
  committing past it would entrench the loss (under the never-rewinding max-merge the container
  even stays *dead* until the file regrows past its old offset: future appends below it are
  skipped too). `--accept-truncation` acknowledges the discontinuity (the `--allow-drift` shape)
  and re-baselines each truncated container at the position this scan observed
  (`RecordScanFilter.ObservedWater`): rewritten content below the old offset is not re-delivered,
  future appends deliver again. With an explicit `--to`, the token wins — the flag only waives the
  refusal. `peek` stays read-only-and-disclosing; a census cursor stays disclosure-only (its fold
  is additive and never rewinds, so blocking it would only lose more). Guard: the truncation act
  of `TestMultiCursorRotationDisclosure`.
- **Prefix fingerprints — ✅ SHIPPED (tier-1 boundary-record)**: `water_fp` stores, beside each
  committed offset, the hash (FNV-1a 64) of the record that STARTS there — the last record the
  cursor advanced past. Each scan verifies it (the filter already reads every byte, so the check
  is a per-container hash of one record; the scan keeps a running COPY of the max record rather
  than hashing every record, ~10× cheaper); a mismatch — or no record at that offset — is a
  REWRITE-IN-PLACE that preserved length, the one violation size cannot see. Disclosed as
  `rewritten:`, refused on advance (kind `source-rewritten`), acknowledged by the same
  `--accept-truncation`; fingerprints re-stamp on accept and BACKFILL opportunistically on any
  advance for legacy sidecars (no flag day). Chosen over a full-prefix rolling hash deliberately:
  the boundary-record check stays cheap under a future seek-to-watermark scan, where full-prefix
  hashing would force re-reading everything seeking skips. This is also the `git://` stepping
  stone: a committed position carrying content identity (`water_fp` ≙ SHA-as-position), and
  `SourceAnomalies`' rotated/truncated/rewritten trio ≙ git's ref-deleted/rewound/history-rewritten,
  riding the same disclose → refuse → acknowledge cycle. Guards:
  `TestMultiCursorRewriteFingerprint` (same-length rewrite caught, identical bytes not, legacy
  backfill), plus the rotation/truncation act unchanged.

### The `git://` source provider (design)

A git repo exposed as **queryable, cursor-followable keyspaces** — so `SELECT … FROM commits`, a
detector, or a monitor runs over the commit log. It's the cleanest possible fit for the cursor
model because git's object store *is* a content-addressed append log with built-in ancestry: the
**commit SHA is the opaque, comparable, serializable position** the design already asks for, and the
SHA doubles as the row **fingerprint** (identity-stable across re-read and rewrite).

**Keyspaces (record shapes).** One binding surfaces several:
- **`reflog`** (the primary *followable* stream, `append`) — git's own append-only journal of ref
  movements (`.git/logs/**`), one record per entry:
  ```jsonc
  { "ref":"refs/heads/master", "old_sha","new_sha", "op":"commit|merge|rebase|reset|checkout|branch|pull",
    "actor":{name,email,time}, "message":"rebase (finish): returning to refs/heads/master" }
  ```
  **Why follow the reflog, not ref-state:** it's append-only *even through rewrites* — a
  rebase/force/reset doesn't rewind it, it *appends* `{old→new, "rebase"}`. So the non-monotonic
  problem (below) largely **disappears**: a rewrite is just another event, the cursor is plain
  `append` (offset = last entry per ref), and force-pushes/rebases become first-class rows an agent
  can watch. Caveats: the reflog is **local & unshared** (per clone; local worktrees share the
  common `.git` so they're followable together, but a teammate's work only appears post-`fetch`),
  **expiring** (`gc.reflogExpire` 90d/30d — fine for near-real-time, not deep backfill), and
  **event-thin** (SHAs + op, no commit content — see Reconstruction).
- **`commits`** (the *content* keyspace, `append`) — one record per commit:
  ```jsonc
  { "sha","short","parents":[…], "author":{name,email,time}, "committer":{…},
    "subject","body", "refs":["refs/heads/master",…],           // decoration: who points here
    "files":[{path,added,deleted,status}], "insertions","deletions",
    "_meta":{ "ref":"refs/heads/worktree-x" } }                  // which ref this arrived through
  ```
  `files`/diff-stat is **lazy** (computed only if the query projects/filters on it — reuse the
  `ColumnsProjector` pushdown). So detectors do `WHERE author.email=… AND subject LIKE 'fix%'`,
  `WHERE ANY f IN files SATISFIES f.path LIKE 'glue/%' END`, `GROUP BY author.name`, commits/hour
  rate-windows, etc.
- **`refs`** (`diff`) — the current `{ref → sha}` set; a `diff` cursor emits `{op:insert}` (new
  branch), `{op:update, before, after}` (ref moved), `{op:delete}` (branch gone). The
  "did someone create/delete/force-push a branch?" monitor.
- **`worktrees`** (`diff`) — `{path, branch, head, locked, prunable}` per `git worktree list`; new
  worktree appears / torn-down worktree disappears.

**Binding & selectors** (late-binding manifest): `commits → git(<repo>, refs=<selector>)`:
- `repo`: a local path (`.`); worktrees share one object DB, so `git(".")` sees every sibling
  worktree's branch — exactly use-case-2 (`this repo + .claude/worktrees/*`).
- `refs`: `master` | glob `refs/heads/worktree-*` | `--all` | **`worktrees`** (every worktree HEAD).
- **pushdown**: `WHERE` on author/time/path lowers to `git log --author= --since= -- <path>` — a
  cheap source-level predicate prune (like the Iceberg/Parquet pushdown seam).

**Position & the "since" query.** Cursor position = `{ref → sha}` (a per-ref map, per the sub-stream
model). "What's new" for each ref is literally `git log <cursor-sha>..<ref>` (commits reachable from
the ref but not the old position) — git's own since-query. New ref → start per the `create --from`
policy (`now` = its current HEAD, empty until new commits; `beginning` = all reachable, bounded/warned
on huge history). Cross-ref dedup is free: **dedupe by SHA** (a commit merged into two followed refs
emits once). A unified time-ordered timeline across refs uses the merge/sorted-stream substrate; per-ref
"what's new" needs no merge.

**Reconstruction — the `.multi` hierarchy (why the reflog isn't enough alone).** The `reflog` is the
*event* half (ref moved old→new, via op X, when) and `commits` is the *content/structure* half
(parents, author, files); higher-level views are a composition DAG ([Composition](#composition--a-dag-of-packs-one-packs-labelresults-feed-the-next)) that joins them:
- **L0** — follow `reflog` (append, rewrite-proof).
- **L1 — enrich**: join each reflog entry to the commits it *introduced* (`rev-list old_sha..new_sha`)
  → per-commit rows with content, tagged by the triggering op. (`reflog` ⋈ `commits`.)
- **L2 — reconstruct/correlate**: branch lifecycle (create/reset/delete, from the op events), rates,
  "rebase dropped N commits", "a merge introduced a detector-match", etc.

What's derivable from where: **merges** are a *graph* fact — a commit with ≥2 parents
(`WHERE ARRAY_LENGTH(parents) >= 2`); the reflog's `merge X:` names the merged-in ref. **Branch
lifecycle** (and *when*) is the reflog event stream. **Branch membership / ancestry / merge-base** is
a DAG walk — expressible in pure SQL++ via `WITH RECURSIVE` over `{sha, parents[]}` (n1k1 has
recursive CTEs), but far cheaper as **provider helpers** (`rev-list` / `merge-base` / `is-ancestor`)
than a brute recursive scan. So a fancy-enough `.multi` hierarchy *can* rebuild branches/merges/
rebases — but only by composing the reflog (events) with the commit graph (structure); neither
alone suffices.

**Ref-state fallback (if you follow `refs` instead of `reflog`).** Where the reflog is unavailable
(remote/shared history, expired) you follow ref-*state* with position `{ref → sha}` and
`git log <cursor>..<ref>` as the since-query — but then rewrites *are* non-monotonic: on each `peek`,
if `merge-base --is-ancestor <cursor-sha> <ref>` is false, reconcile via the symmetric difference
(`dropped = <ref>..<cursor>`, `added = <cursor>..<ref>`) and reset to HEAD — **silent** (re-emit
`merge-base..HEAD`, SHA-dedup absorbing survivors) or **surfaced** as `{op:rewind, ref, dropped, added}`.
Following the `reflog` instead makes this the exception, not the rule.

**Implementation (pure-Go, CGO-free).** A `records.Source` (like `records/parquet.go`,
`//go:build !js`) backed by **go-git** (`github.com/go-git/go-git`, pure-Go — fits the arrow-go /
bleve provider pattern): reflog iteration (`.git/logs/**`, incl. per-worktree `HEAD` logs),
structured commit objects (no fragile `git log` text parsing), and `CommitObject.IsAncestor` /
merge-base for `rev-list old..new` + the fallback rewrite check. Shelling out to the `git` binary is
the alternative (trivial, exact, but needs `git` on PATH + subprocess + parsing). `create`-time
validation: repo opens as a git dir, the `refs` selector resolves to ≥1 ref, the pack compiles.

**Field evidence — which join key survives (n1k1-for-ai, 2026-07).** The flagship consumer join —
agent transcripts (`agent-<id>.jsonl`) to the commits they shipped — was planned around the
`worktree-agent-<id>` branch name, and that key **loses**: only 13 of 380 agent transcripts ever
produced a worktree branch, and most branches are merged-and-deleted (46 `gitBranch` values in the
corpus, 30 still exist). The key that survives rewrite/squash/branch-deletion is a **commit
trailer** (`Co-Authored-By: …` — 85% of their windowed commits carry it), which lives on the commit
object itself. Design consequence: the `commits` record should parse **trailers** into a queryable
field (`trailers: {key: [values]}` — `git interpret-trailers` semantics, cheap at read time), so the
transcript⋈commits join is `ON t.agent_id = c.trailers.…` rather than a branch-name reconstruction.
Keep the branch join for the exact per-agent case; the trailer is the coverage path.

**Open edges:** remote refs need a periodic `git fetch` (a `fetch=every 5m` bind option; local
worktrees need none — the use case is all-local); shallow clones/grafts have incomplete ancestry;
merge commits' diff-stat is first-parent by default; `--from beginning` on a large repo must be
bounded.

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

### Parameterized packs (design — ✅ SHIPPED for file packs: --param k=v on run/lint/test/show/compose + cursor create; CursorState.Params replayed on peek/advance/check; guards TestMultiRunParams + TestMultiCursorParams)

File-based packs (`--queries <dir>`) will want parameters — a threshold, a date window, a field
name — without editing files or duplicating them per variant. The embedded builtins shipped the
contract (`-- param: <name> <type> [= <default>]` front-matter + typed `$name` substitution —
SQL++ named-parameter SYNTAX, ONE grammar, bound early; `cmd/n1k1/builtins`); this section
designs its extension to user packs, and above all the **identity story**: what a parameter
does to `spec_hash`, drift, and a cursor.

**Axes, not conflated.** Three different questions, three different mechanisms:
- *Which physical data?* → `--bind` (logical keyspace → glob). Already shipped; stays the only
  keyspace mechanism.
- *What values/identifiers does the query take?* → params (this section).
- *When/how is it driven?* → run / cursor / monitor cadence (unchanged).

**Mechanism: ONE syntax (SQL++ `$name`), bound EARLY for packs.** There are not two parameter
grammars: a pack parameter is written exactly as a SQL++ named parameter, and n1k1 binds it at
load time (pre-parse constant folding) instead of engine-runtime. The fork's runtime named
parameters are fully plumbed in glue (`Session.NamedArgs` → planner + eval
`NamedParameter`), and for an *ad-hoc single statement* they are the principled value mechanism
(a future `-args` CLI flag; no rendering, no injection surface). But a PACK's economics are
literal-driven: the Aho-Corasick predicate index prunes on **string literals extracted at plan
time**, and the compiled lane **bakes literals into generated code** — `WHERE s.type = $t` has no
extractable literal, so every parameterized gate degrades to always-wake and drops out of the
compile lane. Early binding happens before the planner ever looks: the engine sees a
literal, and fusion/pruning/compiled all work unchanged (same reason macros are pre-parse) —
same syntax, different binding TIME, chosen per context: packs fold early; a future ad-hoc
`-args` flag can pass the same `$name` as true runtime args. Resolution is quote- and
comment-aware and matches the longest DECLARED name (`$type-field` binds `type-field`;
`$depth - 1` needs the spacing); an undeclared `$word` in a pack errors naming the declared set.
Injection is closed by typing, as in builtins: `ident` renders backticked and rejects backticks,
`int` must parse, `str` renders via a quoted literal, `list` as a JSON string array.

**Supply.** `--param k=v` (repeatable) on run/lint/test/show/compose/`cursor create` — the
`--annotation k=v` shape. Defaults live in front-matter (git-committed, PR-reviewable — the
GitOps home). A `--param` naming nothing any entry declared is a loud error listing the declared
set (a typo'd `depht=1` must never silently run `depth=2`). NOT via `--queries "./dir?k=v"`:
`?` is a glob metacharacter in paths, and the ref grammar stays unambiguous.

**Identity — the crux.** A parameter change IS a meaning change: a cursor's watermark was
advanced under the old predicate, so running new params against the old position silently skips
records the new predicate would have matched behind it — exactly the ISSUE-17 query-drift
hazard. So a cursor's delta identity must cover **template + resolved params**, and the design
gets that almost for free:
- `QueriesID` keeps hashing the statement **as rendered** (substitution happens at load, before
  hashing) — params are automatically inside the delta identity, no new hash input.
- `CursorState` gains `Params map[string]string` — the params **resolved at create** (defaults
  baked in), the census-cursor precedent (it already persists keyspace/type-field/depth/exclude
  and replays them). `peek`/`advance`/`check` re-render with the STORED params; a template edit
  drifts exactly as today, and a later front-matter **default** change cannot silently move a
  live cursor (its params were resolved and stored).
- Changing a cursor's params = a new standing question: `rm` + `create` (or, later, an
  `--allow-drift`-shaped "adopt new params, keep the position" — the drift machinery already
  models "the question changed, the caller acknowledges").
- `show`/`list --long` echo `params`, so the ledger can record them next to `spec_hash`.

**Fixtures & goldens** run under the front-matter **defaults** — a golden is a contract for the
default rendering. (Per-param-set fixtures are possible later via a fixture-section header;
deferred.)

**Versioning tie-in.** `-- version:` is the artifact's compatibility statement; adding a new
param WITH a default leaves the rendered-under-defaults statement byte-identical, so it
re-baselines nothing — the cheap-evolution path. A param whose default *changes* the rendering
is a meaning change: bump the version. (Follow-up, same story for the JS artifact family:
macros/UDF modules/extract recipes have no declared-version convention today — extract recipes
carry only an automatic `name@<source-hash>` fingerprint, an identity, not a compatibility
statement. A `// version:` top-comment surfaced by `.extensions list`/`.macro list` unifies it.)

**Open questions (deferred).** Compose: can a downstream node pass params to an upstream node
(`-- needs: CC-SPEND?threshold=0.9`)? Probably yes-but-later — it makes node identity
per-instantiation and the DAG a template graph. A `--params <file>` for large sets. Engine
named-args exposure (`-args`) for ad-hoc statements, independent of packs.

### Three axes, one frozen grammar

None of the above touches the SQL++ dialect: **query = pure SQL++**, **source liveness = the
late-binding manifest** (`orders → glob(*.json)` static vs `tail(app.log)` / `kafka://` / `cdc://`
/ `poll(url, every=5m)` live), **cadence = how it's driven** (`once` batch / the `peek`+`advance`
verbs / a monitor's schedule). The same detector replays statically, catches up via poll, and
follows live — liveness is *data* (the binding), not *code* (the query).

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

> **Status — SHIPPED** (`.multi cursor {create,peek,advance,list,show,rm}`). The `append` delta
> rides a `records.Source` wrapper installed at the single `KeyspaceRecordsOpen` choke point in
> `DatastoreScanRecords` (nil-by-default `GlueContext.scanFilter`, reached via `getRoot()` so
> UNION-ALL clones share one high-water collector) — no engine/records hot-path change. Position
> comes from the record id (`<path>#<line>@<offset>`, always present on jsonl — no `-meta`). Core:
> `glue/cursor.go` (`CursorStore`, `RecordScanFilter`, `Session.RunCursorPack`, `PackID`); surface:
> `cmd/n1k1/multi_cursor.go`. `from`/`to` are opaque, deterministic `{container→offset}` tokens.
> *MVP simplifications, still open:* `log`/`reset` + the journal not yet built; the fingerprint is a
> content hash (`hash(label,result)`), not yet `hash(detector-sha, source-id, matched-key)`; a
> filtered scan forgoes column/predicate pushdown (only under an active cursor; jsonl has none
> anyway); `--from now` positions by running the pack once and discarding rows.

**Phase 2 — `diff`/snapshot delta.** *Build on:* the spillable rhmap store, doc-id extraction.
*New:* `mode: diff` — persist a prior snapshot keyed by id under the cursor, diff on run, emit the
Debezium envelope, replace snapshot. → "what changed" on mutable / current-state-only sources.

> **Status — SHIPPED** (`.multi cursor create … --mode diff [--id-field <name>]`). A diff cursor
> runs the pack over the **full current state** (no append filter — `Session.RunPackFull`), keys each
> labelResult by its id-field value (default `id`; a result missing it is skipped, not silently
> dropped), and diffs against a prior snapshot persisted as a `<name>.snap.json` sidecar (atomic
> temp-rename; keyed by `(label, id)` so overlapping detectors keep independent streams). `peek`
> recomputes the diff without moving; `advance` replaces the snapshot and bumps the `snap:N` version.
> Output rows are the Debezium envelope: `insert` (after), `update` (before+after), `delete`
> (before), each with a fingerprint. Core: `glue/cursor.go` (`SnapshotEntry`, `SnapshotFromResults`,
> `DiffSnapshot`, `CursorStore.{Load,Save}Snapshot`). *Open (still):* the snapshot is an in-memory
> map + JSON sidecar, not yet spilled through the rhmap store (matters only past RAM); `--to` is
> append-only (diff `advance` always commits the peeked current state); the id-field is a top-level
> field only (no path).

**Phase 3 — GitOps `plan` / `apply`.** *Build on:* the corpus loader (already reads a dir of
recipes), `.multi lint`. *New:* treat a recipe dir as desired-state; `plan` (diff + lint) and
`apply --prune` (reconcile, preserve unchanged cursors); labels/annotations in front-matter.

> **Status — SHIPPED** (`.multi cursor plan <dir>` / `apply <dir> [--prune]`). Each `*.sql++` file
> in `<dir>` is one cursor: **name = its front-matter `label:`, else the file stem**; pack = the file
> itself (new `glue.LoadPack` loads a single file or a dir uniformly, so peek/advance reload either);
> policy from front-matter (`mode`/`bind`/`from`/`id-field`/`labels`/`annotations` + `description`).
> `labels` accepts `k=v, k2=v2` or a JSON object; `annotations` is stored/echoed verbatim (the
> provenance home). Reconcile is driven by a `SpecHash` = the **delta identity** (pack content + mode
> + bind + id-field) — deliberately NOT metadata, so a retag/reword never re-baselines a cursor:
> declared-not-live → **create**; identity differs → **update** (re-validate; position **preserved**
> when the mode is unchanged, else re-baselined per `from`); identity equal but metadata drifted →
> **metadata** (labels/annotations/description refreshed in place, position untouched); fully equal →
> **noop**; live-managed-not-declared → **destroy** (only `apply --prune`). `plan` **warns** on
> unsupported front-matter keys (never silently drops). Apply stamps `Managed` so an imperative
> `.multi cursor create` is **never pruned** (adoptable if later declared). `plan` folds in
> `.multi lint`; `apply` refuses on any invalid file (no partial apply). Core:
> `glue.{LoadPack,SpecHash,PlanReconcile,ReconcilePlan}`; CLI `buildDesired`/`provisionCursor`/
> `cursorReconcile`. The default cursor-store is **CWD-relative `./.n1k1-state/cursors`** (never inside
> the datastore bundle, which may be read-only or owned by another live process). ⚠ A hyphenated
> label isn't a bare identifier, so a downstream `FROM` must backtick the materialized name
> (`` FROM `pack_CC-SPEND` ``). ⚠ A cursor whose pack spans **≥2 keyspaces** inherits a pre-existing
> cbq-fork `expression.Copy` race in the fused UNION-ALL execution (fails `-race`; tracked with the
> other fork-pool races — not cursor logic). Single-keyspace packs (the common monitor shape) are clean.

> **Amended — `plan`/`apply` retired; metadata moved to `create`.** The declarative `plan`/`apply`
> reconcile above was withdrawn in the `.multi` naming overhaul (it returns with a `serve`/monitor
> runtime); `.multi cursor create` (one `*.sql++` = one cursor) is the create path. Annotations/labels
> passthrough — which had shipped only on the retired `apply` path — is now wired onto `create`
> (`--annotation k=v`, `--annotations-file <f>`, `--labels`, `--source-ref`, plus the same front-matter
> keys), so the *"put the SHA in the cursor"* provenance workflow no longer needs an external ledger.
> `--source-ref` **auto-captures** the queries dir's git HEAD (+`-dirty`) when unset (the ISSUE-03 #5
> ask). Metadata stays OUT of `SpecHash`/`PackID` (the delta identity), so a retag never re-baselines —
> asserted by `TestMultiCursorAnnotations` (two cursors, same pack, different metadata → equal
> `spec_hash`).

> **Amended — `spec_hash` is stable across n1k1 versions (hash-scheme versioning).** The queries id
> (`glue.QueriesID`, né `PackID`) hashes raw source text under a NORMALIZATION SCHEME, and the
> conventions evolve (scheme 1 = ends-only `TrimSpace`; scheme 2 = ISSUE-05's blank-line-invariant
> normalizer — whose landing re-hashed every file with interior blank lines and made `advance` refuse
> with a false `query-drift` on every cursor stamped by an older binary). Now: each scheme's
> normalizer is kept frozen in `queriesHashNormalizers` (a change = a NEW scheme, never an edit);
> drift comparison is `QueriesIDMatches` — the stored id matching under ANY known scheme is NOT
> drift; `advance` re-stamps `queries` to the current scheme (`hash_scheme` in the sidecar and in
> `show`/`list --long`), migrating old sidecars forward with no flag day. Only a real content edit
> (no scheme matches) drifts. Guards: `TestQueriesIDSchemes`, `TestMultiCursorHashSchemeUpgrade`.

**Phase 4 — Composition (pack DAG).** *Build on:* temp-tables / CTEs / sequence op (exist), and the
Phase-1 labelResults journal as the materialized intermediate. *New:* a `pack:<name>` labelResults keyspace a
downstream pack can `FROM`; topological ordering (reject cycles); per-pack cursors so incremental
poll composes; lineage on labelResults. MVP re-polls A's materialized labelResults from B (not true
cross-layer delta). → correlation/incident packs over primitive detections.

> **Status — SHIPPED** (`.multi compose <dir>`). Each `*.sql++` in `<dir>` is one DAG node (**name =
> its front-matter `label:`, else the file stem**); a node declares upstream deps via `-- needs: a, b`
> front-matter (an unknown dep is a hard error) and reads them as `FROM pack_a` (backtick the name if
> the label has hyphens). `--only <node,…>` / `--terminal` limit which nodes emit their labelResults
> (each still reports a count), so an upstream 75k-row detector isn't a firehose.
> Nodes run in **topological order** (cycles + unknown deps rejected) on ONE session;
> each node's labelResults materialize into a `pack_<name>` **temp keyspace** (`temp_keyspace.go` — the
> same heap that backs fixtures), so a downstream `FROM pack_<up>` resolves through the session's temp
> overlay. Materialized rows are `{label, result, fingerprint}` — `result` stays nested (navigate
> `x.result.<field>`), `label` enables per-detector `GROUP BY`, `fingerprint` is the lineage handle.
> Core: `glue/compose.go` (`ComposeNode`, `TopoOrder`, `Session.Compose`); CLI `multi_compose.go`.
> *MVP scope (as the design calls for):* **batch** re-run of the whole DAG (materialize + re-poll) —
> per-node cursors for incremental cross-layer composition, and full lineage-graph walk, are deferred
> (the Materialize/DBSP problem). Uses the same single-keyspace-per-node fusion, so it's `-race` clean.

**Phase 5 — `n1k1 serve` + MCP (scheduled monitors).** *New:* a long-running process holding the
cursor store; the `monitor` object (cursor + query + schedule + status); server-driven scheduled
`peek`+`advance`; the MCP resource/tool/subscribe surface + long-poll HTTP. No unbounded source yet
(scheduled-poll reuses Phase 1). → self-running monitors, agent-subscribable.

**Phase 6 — true `follow` / continuous.** The heavy engine work (see next section): unbounded
source, continuous/watermark emit, incremental standing state, event-time, and a distributed
cursor-store backend.

### Adjacent application — incremental SI / FTS indexing

The cursor primitive isn't only for *external* agents; its most natural *internal* consumer is
n1k1's **own index builders** (`glue/idx_si.go`, `glue/idx_fts.go`). Today a secondary or FTS index
over a growing keyspace is a **full rebuild** — re-scan every doc, re-tokenize, re-write the catalog
— because there's no notion of "what's new since the last build." A cursor *is* exactly that notion:
the committed high-water is the index's watermark, and an `append` `peek` yields precisely the
documents added since — the incremental **index-maintenance delta**.

The shape lines up cleanly:

- **The index is the cursor's consumer, not a labelResult sink.** A cursor named after the index
  (`si:orders.by_city`) binds to a trivial "pack" (a `SELECT` of the indexed key expression over the
  keyspace). `peek` returns the new docs; the builder folds them into the existing btree/postings and
  `advance`s the watermark. A crash between fold and advance just re-`peek`s (at-least-once) — the
  fingerprint (doc id) makes the re-index **idempotent** (upsert by key), the same dedup story the
  agent path uses.
- **Deletes/updates want `diff` (Phase 2), not `append`.** An append cursor covers insert-only
  growth (new log lines, new docs); a mutable keyspace needs the `{op:insert|update|delete}` envelope
  to retract/replace postings. So incremental SI/FTS is **Phase 1 for append-only corpora, Phase 2
  for mutable ones** — no new machinery beyond the delta strategies already planned.
- **Reuses the existing catalog + eager/lazy build modes.** The `.n1k1/catalog.json` already records
  index defs; add a per-index watermark alongside it (or a `si:`/`fts:`-prefixed cursor in the store)
  and the eager-build-on-open path becomes an eager-*catch-up* (fold the delta) instead of a rebuild.

This is a distinct workstream from the agent-facing verbs (it needs no new CLI surface — it's an
internal caller of the same `RunCursorPack` + high-water), but it validates the primitive from a
second direction and would turn index maintenance from O(corpus) per open into O(delta). *(Open: FTS
segment merge under incremental adds; whether the watermark lives in the index catalog or the cursor
store; interaction with the non-monotonic rewind case for mutable keyspaces.)*

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
- **Sub-stream rewind policy is unsettled** (see "Dynamic sub-stream sources"): when a file
  rotates/shrinks or a git ref is rebased under a cursor, is the reset **silent** (just re-read +
  fingerprint-dedup) or **surfaced** as an event the agent sees (`{op:reset, reason:rotated|rewound}`)?
  A monitor for *"did someone force-push / did cleanup run?"* wants it surfaced; a plain content
  tail wants it silent. Likely a per-cursor policy.
- **Append vs diff on one source** — the tree/repo cases want `append` over contents *and* `diff`
  over set-membership at once; unclear whether that's two cursors on the same binding or one cursor
  with a combined mode.
