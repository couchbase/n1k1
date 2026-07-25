# Design: K-way sorted merge & merge joins (ASOF) for n1k1

Generic over any time-ordered records (logs, trades/quotes, sensor streams,
observations). When many sources share one comparable key, a **K-way sorted merge**
turns them into one globally ordered stream cheaply (O(N log K), often O(N)), which makes
**ASOF temporal correlation** ("what was the reference stream's value when this event
happened?"), **windowed detection** (rate/burst/streak/gap over `PARTITION BY … ORDER BY
ts`), and **de-duplicated timelines** fall out of one ordered pass instead of the O(n²)
correlated-subquery shape they parse as. Driving use case: **PREPARE++**
(`DESIGN-prepare.md`) detectors over date-sorted `cbcollect_info` bundles.

⚠ **Hard constraint: the cbq grammar/parser is off-limits** (a new `ASOF` keyword /
`merge(...)` TVF means fork divergence). Every capability lands as a **plan-time / operator
optimization over stock SQL++ idioms** — recognized at conversion or post-plan rewrite and
lowered to new `base.Op`s. New runtime, no new syntax.

## Status & remaining TODOs

_Last reviewed: 2026-07-25._

**Done:** the K-way sorted merge SCAN (all three regimes — concatenate, strict min-heap,
watermarked-near with disorder-bound validation) and the sorted merge JOIN (equi, ASOF
nearest-preceding + nearest-following, soft/tolerance-bounded, partitioned) both ship
(`engine/op_merge_scan.go`, `op_merge_join.go`). Grammar-free surfacing landed: `UNION ALL
… ORDER BY <key>` → `merge-scan` (`WireTemporalMergeMeta`, opt-in `EnableMergeRewrite`
fallback) and correlated-argmax subquery → ASOF merge-join (`WireASOFJoin` + the
conservative `MatchArgmaxAsof` recognizer), covering scalar `[0]`/RAW forms, right-only
content residuals, and near-sorted keyspaces. Merge runs streaming (single-child
watermarked + a K-way pull-coordinator), spills the build/reorder buffer past budget,
exposes per-request `base.MergeStats` in the `.multi` run summary, and scans multi-file
keyspaces per-file (the cross-node enabler). Measured ~19× lower RSS vs. the
correlated-subquery shape, speed-neutral.

**Remaining (headline TODOs):**
- [ ] Seek-by-time via sync points + predicate pushdown to the merge (phasing step 5) —
  highest-leverage perf step for time-bounded detectors; still unbuilt.
- [ ] Window-stream sort-sharing: feed a `WindowAggregate` over a sorted source / merge-scan
  directly, skipping its own sort (step 9).
- [ ] Equi merge-join *lowering* from conv (the engine op exists; the plan-shape recognition
  that would choose it over hash-join is unwired — step 8, low priority).
- [ ] Catalog-VIEW carried merges + optional `merge:` keyspace-name convention (step 10) —
  depends on catalog work in `DESIGN-data.md`.
- [ ] A fully general "source advertises its order" contract: extract-layer int64-key
  normalization isn't wired end-to-end, so firing still leans on
  `SortedSourceMetasForKeyspace` per keyspace.
- [ ] Secondary late-record policies (`drop`, `resort`) and `nearest` (either-side) soft ASOF
  — only `error`/`widen` and preceding/following/soft landed.

## Background: what we ride on

Push-based execution makes a merge op a **scan-shaped source**: it owns K child cursors and
calls one downstream `yield` per emitted row. It reuses existing machinery: the spillable
max-heap (`base/heap.go`, `HeapValsProjected` over `store.Heap`) for the small K-entry
frontier heap and the watermarked reorder buffer; and **actor-per-branch fan-in**
(`base.Stage`, `Stage.BatchCh`; `OpUnionAll`'s substrate) — the merge is `OpUnionAll`'s
fan-in with an *ordering discipline* on the consumer (peek each actor's head row, pop the
minimum). Actor-per-cursor also solves stepping: push ops run to completion so you can't
"yield once and pause", but a cursor in its own actor naturally parks at its `BatchCh` send
when the consumer stops crediting it.

Current lowerings (all `glue/conv.go`, executing as `base.Op`s, fork untouched):

| Plan op | Lowered to | Note |
|---|---|---|
| `NLJoin` (ANSI) | `joinNL` | **re-drives the right branch per left row** (O(n·m)) |
| `HashJoin` (1-key equi) | `joinHash-inner` | build probe map one side |
| `Order` | `order-offset-limit` | max-heap, spill, folds OFFSET/LIMIT |
| `UnionAll` | `union-all` | by-name label union |
| `WindowAggregate` | window op | `OVER (PARTITION BY … ORDER BY …)` |
| `Nest`/`NLNest`/`HashNest`/`IndexNest` | **`NA()`** | none convert (verdict below) |

Load-bearing observation: an ANSI join re-drives the inner branch per outer row (`joinNL` =
O(n·m)), and the ASOF idiom parses as a correlated subquery (nested-loop + inner ORDER BY …
LIMIT 1 per outer row) — the quadratic re-drive this doc replaces with one co-advancing pass
when both inputs are time-ordered.

## The sorted-source contract (recap of `DESIGN-data.md`)

The merge ops are *consumers* of metadata `DESIGN-data.md` (§4 extract + §5 manifest)
produces; that's authoritative. In brief:
- **Normalized sort key** — the extract layer normalizes each source's timestamp
  format/tz/precision into ONE comparable **int64 epoch-nanos** key (a merge compares int64s,
  never re-parses strings).
- **Metadata is computed once and cached** — `describe(file) → ExtractSpec` is memoized in the
  `.n1k1/` sidecar once per file; the merge planner consults it **at plan time** (pick regime,
  seed `disorder_bound`, read `min_key`/`max_key` zone maps) and cursors read the cached
  `sort_key`/`sortedness`/sync-points at run time — **so classification + key normalization
  are not on the merge's hot path**.
- **Sortedness class** per source: `strict` (key non-decreasing) / `near` (bounded disorder) /
  `none` (must spill-sort).
- ⚠ **`disorder_bound`** for `near` sources (`{window: Δt}` or `{span: N}`) — **a claim, and a
  wrong claim silently corrupts a merge**, so the op MUST validate it at runtime.
- **Manifest zone maps** (`min_key`/`max_key` per file) + key→offset **sync points** that let a
  cursor seek to a start time (and double as seekable doc-IDs).

## §1 The K-way sorted merge SCAN op (`merge-scan`)

Presents K sorted files as one stream ordered by the normalized int64 key (available as a
labeled register so downstream sorts/compares on the int64). A **new op kind added n1k1-side**
(not a new plan op or scan kind — so it stays compiler-safe like the data-source work).
`Params`: `[0]` keyIdx, `[1]` regime, `[2]` per-child sortedness, `[3]`/`[4]` per-child
min/max zone maps, `[5]` per-child disorder-bound nanos, `[6]` late-record policy; live child
cursors in `Temps`. It picks the cheapest legal regime from the metadata; all three emit into
the same downstream `yield`:

- **(a) Concatenate** — when key ranges are disjoint and ordered (`max_key(fᵢ) ≤ min_key(fᵢ₊₁)`,
  the common dated-log-rotation case): read files back-to-back. O(N), zero comparisons, one
  open cursor at a time; zone maps prove disjointness without opening anything. (A near-source
  can still concatenate at file granularity if its bound is smaller than the inter-file gap —
  regime is chosen per boundary, not globally.)
- **(b) Min-heap merge** — overlapping ranges, all `strict`: classic K-way merge over a
  K-entry min-heap keyed on each cursor's frontier (pop min, yield, advance, re-push). O(N log
  K), K = live (overlapping) cursors, tiny. Row bytes stay in the child's reused buffer until
  popped (borrow: copy on `yield`).
- **(c) Watermarked-near** — a `near` source's head key is *not* a lower bound on its remaining
  keys, so emitting on head-min alone could emit before a smaller-keyed row that hasn't
  surfaced. Fix (the Flink watermark model on a bounded offline stream): frontier = min over
  live cursors' head keys; watermark = frontier − max(disorder_bound); buffer rows in a small
  min-heap, emit only once key ≤ watermark. Buffer bounded by `bound × rate` (or N rows);
  spills via `store.Heap` past budget. O(N log B), B small — vs a full spill-sort (`none`).

**Cursors & seek-by-time.** A cursor = a child scan + head row/key; stepping is the
**actor-per-cursor** shape (each child in a `Stage` actor pushing over a credit-bounded
`BatchCh`; the consumer holds each actor's head, pops the min, credits that actor for one
more — a cursor with no credit parks). **Lazy opening**: a file whose `min_key` is beyond the
frontier isn't opened until the frontier reaches it (zone map gates cursor creation, bounding
open FDs to the overlapping set). **Seek-by-time** (unbuilt, step 5): sync points let a merge
bounded by `WHERE ts >= …` `os.Seek` to the sync-point offset at/before the start key — the
temporal analog of a sargable range scan; **predicate pushdown to the merge is the single
highest-leverage optimization** (shares the "predicate reaches the scan" prerequisite with
zone-map pruning).

Backpressure is the natural push way (advance only when `yield` returns; `BatchCh` credit
bounds in-flight batches for a parallel merge). Concatenate/strict-heap are streaming (K
frontier entries, no spill); watermarked-near + `none` spill through the ORDER BY `store.Heap`
path.

**Soft options (the correctness heart) — Params, defaulted from the manifest, overridable per
query (never new syntax):**
- **Disorder tolerance** = the effective bound enforced (wider = more buffer, tolerates
  jitter; narrower = cheaper, risks late records).
- **Late-record policy** for a record arriving *below the watermark* (bound too small):
  **`widen`** (default exploratory — widen, re-buffer, `Warn`), **`error`** (fail with "source
  X violated disorder_bound at key K" — the safe default for correctness-critical detectors),
  **`drop`** (Flink's default; fine for approximate rate/burst), **`resort`** (full spill-sort
  fallback). ⚠ **Bound-validation is always on**: the op checks its own output monotonicity
  (one int64 compare/row) — the tripwire that catches a wrong `disorder_bound` before it
  corrupts a downstream ASOF join. **A wrong bound is a silent data-corruption bug; the merge
  must be paranoid, because nothing downstream can detect an out-of-order stream promised to be
  ordered.**

## §2 The sorted merge JOIN op (`op_merge_join.go`)

Co-advances two ordered cursors instead of re-driving the inner per outer row.

- **Equi merge-join** — standard sort-merge equijoin (advance the lagging cursor; on equality
  emit the cross-product, buffering the right group for left duplicates). The ordered analog of
  `joinHash-inner`; wins when inputs are *already* sorted (no build/probe, streaming), loses
  when they aren't (a sort is a pipeline breaker — so only choose it when sortedness is free).
- **ASOF nearest-preceding** (the temporal star). For each left row, the right row with the
  **greatest key ≤ the left key** (optionally partitioned by equality keys). Mechanics: keep a
  single `held` = latest right row with key ≤ current left key; advance right while `right.key
  ≤ left.key` updating `held` (the argmax), emit `left ⋈ held` (or NULL), advance left, never
  rewind right. **One linear pass O(N+M)** holding one right row per active partition (a small
  `partition → held` map, evicted as partitions leave the frontier) — the direct replacement
  for the O(n·m) correlated-argmax subquery.
- **Nearest-following** (the forward mirror, `Params[7]` direction) — smallest right key ≥ left
  key; the forward cursor advances to the first `right.key ≥ left.key` but does **not** consume
  it (one right row can be "next" for several ascending left rows); partitioned following keeps
  a per-partition index list + cursor (`mergeJoinStepAsofFollowing`).
- **Soft ASOF** — bounds staleness with a max look-back Δt (plain ASOF plus a
  subtraction-and-compare at emit): **within-tolerance-or-null** (match only if `left.key −
  held.key ≤ Δt`, else NULL), **bounded-staleness drop**, **nearest** (either side within ±Δt,
  one buffered look-ahead — still TODO). Cheaper than plain ASOF in practice: the Δt bound lets
  the right cursor discard held rows out of the window, capping per-partition state.

⚠ **Re-entrancy is the crux.** A merge join consumes the right stream *incrementally
interleaved* with the left (advance right a bit, emit, advance more), so the right side must
be a **resumable cursor** stepped one row at a time. This is *not* what `OpJoinNestedLoop`
does (it re-executes the whole inner branch per left row — the O(n·m) cost being replaced); the
merge uses the same **actor-per-cursor** crediting as merge-scan (no new primitive). State
between left rows is just `held` + the partition map (+ ≤1 look-ahead) — tiny, bounded, so the
join is **streaming, not a pipeline breaker**. Spill only for large equal-key duplicate groups
(equi) or a huge partition map (both via the ORDER BY `store.Heap` path). The ideal shape
`merge-join(merge-scan(left…), merge-scan(right…))` composes without materialization: one
ordered pass over the whole bundle, bounded memory.

## §3 Grammar-free surfacing

Each capability is triggered by recognizing a **stock idiom** and lowering it — recognition
must be *robust* (no false positives that silently change semantics), and its canonical form
is what detector authors are told to write. Both live in `glue/optimize_temporal.go`, a
read-only **post-plan** rewrite downstream of the fork's plan output (never touching
grammar/planner).

**Argmax-subquery → ASOF.** Canonical form:
```sql
SELECT e.*, (SELECT r.<field> FROM <right> r
             WHERE r.<key> <= e.<key>            -- (A) one inequality vs an outer key
               [ AND r.<eqk> = e.<eqk> ]*        -- (B) zero+ equality (partition) preds
               [ AND r.<key> >= e.<key> - <Δt> ] -- (C) optional look-back → SOFT ASOF
             ORDER BY r.<key> DESC LIMIT 1) AS <alias>   -- (D) argmax by the same key
  FROM <left> e
```
Rewrite only if: correlated via exactly the `<key>` inequality (A) + zero-or-more equalities
(B), no other correlation; `ORDER BY r.<key> <dir> LIMIT 1` where `<dir>` and the inequality
**agree** (`<= … DESC` = preceding, `>= … ASC` = following — a mismatch is not an argmax);
the projected value is a plain field of `r`; both sides orderable by `<key>`; optional (C) ⇒
soft ASOF with that Δt. ⚠ **The rewrite is semantics-preserving by construction, so a false
*negative* only costs speed — the danger is a false *positive*** (matching `LIMIT 2`, an extra
correlation, an aggregate projection, or a `<key>` that differs between ORDER BY and the
inequality). The recognizer (`MatchArgmaxAsof`/`recognizeASOFRoot`) is **conservative**:
require the exact shape, bail on anything else. It's non-local (spans outer + subquery), so
it's an independently-testable analysis half feeding the `WireASOFJoin` lowering.

**UNION ALL of sorted streams → merge.** `rewriteTemporal`/`mergeScanFromOrderUnion`
recognizes an `order-offset-limit` whose single child is `union-all` and whose ORDER BY key is
a prefix of the branches' sort keys, and replaces `order(union-all(…))` with a `merge-scan` —
O(N log K) streaming instead of a full spill-sort. Fires metadata-driven
(`WireTemporalMergeMeta` seeds each branch's real sortedness/zone-map Params) with the opt-in
`EnableMergeRewrite` fallback. Safe fallback: if any branch isn't sorted on the key, keep
`order(union-all(…))`.

**Window functions ride the ordered stream.** `… OVER (PARTITION BY … ORDER BY <key> …)` is
already stock + lowered; the win (unbuilt, step 9) is *sharing* the ordered stream — feed a
window op over a sorted source / merge-scan directly, skipping its own sort.

**Verdicts (paths declined):** **no `FROM merge(...)` TVF** — needs parser + algebra + planner
support (the merge-hostile fork divergence); use a **catalog VIEW** whose stored SQL is the
canonical `UNION ALL … ORDER BY` (expanded as an implicit WITH, pure glue) or a backtick-quoted
`` `merge:ns_server.*` `` keyspace-name convention. **NEST stays `NA()`** — the temporal need
("all log lines within ±Δt of each rebalance") is a **band merge-join + `ARRAY_AGG`/GROUP BY**
(a many-within-Δt generalization of soft ASOF producing flat rows stock SQL++ can nest), not
NEST's equijoin/ON-KEYS grouping; add a band variant of the merge-join only on demonstrated
demand.

## Cross-node K-way clusters (the canonical case)

A customer sends **one `cbcollect` per node** unzipped into sibling dirs, same layout, running
concurrently → overlapping time ranges → correlating events *across* nodes by time is the whole
point. This is K sorted streams merged by time — the design's headline, not a stress case. It
sharpens three things:

1. **Per-file child scans are the enabler.** A `**/ns_server.info.log` glob keyspace is a union
   of the K per-node files; concatenating them would trip the monotonicity tripwire on
   overlapping ranges, so the merge scans **each file as its own merge input** (the K cursors
   *are* the K nodes) — `perFileScans` in `optimize_temporal.go`. The same wiring feeds
   cross-node ASOF (the build side is the K-node-merged state stream).
2. ⚠ **Clock skew is a cross-node correctness factor.** The merged stream's effective disorder
   is `max(per-node disorder_bound) + inter-node clock skew`, so a cross-node watermark must be
   widened by the expected skew; unbounded skew → no bounded merge is correct → fall back to
   `none` spill-sort. The validate-or-widen policy is the guardrail (a record below the
   watermark = skew exceeded the bound, caught not silently mis-ordered).
3. **Provenance is a required column** — a merged cross-node row is meaningless without "which
   node"; the extract `describe` stamps the node into `provenance` (or use `_meta.path`'s
   leading bundle-dir segment).

## Phasing

Done ✅: (1) concatenate regime, (2) strict min-heap regime, (3) UNION-ALL→merge recognition,
(4) watermarked-near + soft options (`error`/`widen`; `drop`/`resort` TODO), (6) ASOF
nearest-preceding + argmax→ASOF rewrite + nearest-following, (7 partial) soft ASOF
within-tolerance + bounded look-back (`nearest` TODO). Partial ◐: (8) equi merge-join (engine
op exists, conv-side recognition unwired, low priority). Unbuilt ⬜: (5) seek-by-time + predicate
pushdown (highest-leverage perf), (9) window-stream sort-sharing, (10) catalog-VIEW merges +
`merge:` convention.

Testing rides the existing discipline: every merge op gets an interpreter/compiler differential
case; the recognizer gets golden plan-rewrite tests (canonical argmax → assert ASOF; near-misses
— `LIMIT 2`, mismatched ORDER BY dir, extra correlation — assert *no* rewrite); and a
**disorder-bound-violation fixture** (a source that lies about its bound) exercises each
late-record policy — the correctness tripwire's subtlest failure mode deserves its own test.

## Open questions

- **Measuring `disorder_bound`** — declared vs measured-by-sampling; a measured max is a lower
  bound on the true max, so what conservative padding? Too-tight is the silent-corruption risk.
- **Late-record policy default** — `widen`+`warn` (self-healing) vs `error` (safe) as the
  engine default for ad-hoc use (per-detector metadata otherwise).
- **Recognizer scope** — only the exact scalar-subquery form, or also JOIN-shaped / `GROUP BY …
  HAVING max`? Each shape widens coverage but raises false-positive risk.
- **Key materialization on the hot path** — the int64 key is still *produced per record* from
  the raw timestamp at scan time; for billions of records this may dominate. Fuse the key
  production into the scan/extract that already touches the bytes?
- **ASOF partition-map eviction** — with many equality partitions the `partition → held` map
  grows; frontier-based eviction assumes partitions don't reappear far apart — safe?
- **Cross-node clock-skew budget** — fixed default vs measured vs user-declared, and the
  give-up threshold past which the merge must spill-sort.
- **Compiler / PREPARE++** — the merge ops carry live cursors/actors in `Temps`; `OpUnionAll`'s
  actor fan-in already codegens, but does the ordered-drain consumer (frontier heap + per-actor
  crediting) codegen cleanly, or cap a query at some prepare level?

_(Prior art: DuckDB / ClickHouse / kdb+ / pandas `merge_asof` ASOF joins; Flink bounded
out-of-orderness watermarks + allowed-lateness; Graefe 1993 for external sort-merge + K-way
merge foundations.)_
