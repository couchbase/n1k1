# Design: MQO for stateful detectors — the shared sorted-stream substrate

Status: **proposal** (no code yet). Companion to `DESIGN-prepare.md` (PREPARE++ /
multi-query optimization), `DESIGN-merging.md` (the ASOF K-way merge), and
`DESIGN-exprs.md` (the native window functions this builds on).

## TL;DR

n1k1's multi-query optimization (MQO) fuses **stateless per-row** detectors — the
`broadcast` op tees one scanned row to K `if pred(row) { emit proj(row) }` filters, with an
Aho-Corasick predicate index waking only the few whose literal is present. Two important
detector classes fall **outside** that model and today run **standalone** (their own scan,
no sharing):

- **Window / context detectors** — grep -A/-B/-C evidence gathering: emit a match line ±N
  lines of surrounding context (`… OVER (PARTITION BY … ORDER BY … ROWS BETWEEN B AND A)`).
- **Temporal cross-keyspace correlation** — "XYZ happened in log1, then ABC happened soon
  after in log2" (an ASOF / band correlation across two streams by timestamp).

Both are already **expressible in stock SQL++** and **correct** today; both are **not
shared** across detectors. This note argues they share ONE missing substrate — a **shared,
sorted-by-key stream per keyspace** — and that a modest set of recognizers/directives lets a
whole class of stateful detectors fuse onto it. The unifying levers:

1. **Share the sort**, not just the scan. Stateless MQO shares a row stream; stateful
   detectors need a stream *sorted+partitioned by a key*, and detectors overwhelmingly share
   the SAME key (`(_meta.path, _meta.pos)`, or a normalized `time:` key). That shared key is
   the fusion handle, and the sort is the dominant cost on GB logs — so sharing it is ~K×.
2. **Let content filters ride the sorted stream** via the existing predicate index /
   residual filters — matches are sparse, so most of the stream needs no stateful work.

## Contents

- [Background: what fuses today, and what doesn't](#background)
- [The unifying substrate: a shared sorted-by-key stream](#substrate)
- [Part A — window / context detectors (grep -A/-B/-C)](#part-a)
- [Part B — temporal cross-keyspace correlation (XYZ → ABC)](#part-b)
- [How to make it detectable without changing the grammar](#detect)
- [Phasing (incremental, evidence-gated)](#phasing)
- [Open questions & honest caveats](#open)

## Background: what fuses today, and what doesn't <a name="background"></a>

The fused path (`engine.OpBroadcast` / `OpBroadcastIndexed`, `DESIGN-prepare.md` §MQO) works
because a fused detector is a **pure function of the current row**: scan pushes each row once,
each detector is `if pred(row) { emit proj(row) }`, and the tee fans the shared bytes to K of
them with zero re-decode. The Aho-Corasick index makes it ~O(hits × rows) instead of
O(K × rows). Correctness rests on: no cross-row state, no ordering requirement, output ⊆ the
matching row.

The corpus compiler (`glue.CorpusCompile`) routes anything that isn't that shape —
window / GROUP BY / join / ASOF — to **standalone**: run its own optimized plan, union the
findings. The `gate:` precondition (`DESIGN-prepare.md`) lets a standalone detector be
**skipped** when its keyspace can't match, but it does **not** let two standalone detectors
**share** a scan or a sort. So K context detectors over one `logs` keyspace = K scans + K
sorts + K window passes; that is the gap this note attacks.

## The unifying substrate: a shared sorted-by-key stream <a name="substrate"></a>

Both classes below need the same thing the raw tee can't give them: rows delivered **sorted
by, and grouped by, a key** — `PARTITION BY P ORDER BY O`. The observation that makes fusion
possible is that **detectors cluster on a small set of keys**:

- context detectors almost all use `(_meta.`path`, _meta.pos)` (per-file line order) or a
  common normalized `time:` key;
- temporal-correlation detectors almost all use a `time:` key (± a partition like `node`).

So the fusable **signature** is `(keyspace, PARTITION key, ORDER key)`. Every detector with
the same signature can ride ONE scan + ONE sort. Because sorting a GB of logs (O(N log N),
possibly spilled) dominates a single detector's cost — the per-row window/merge work is
O(N) — sharing the sort across K detectors is the primary win, larger than the shared scan.

A refinement removes even the sort in the common case: `_meta.pos` is already ascending
within a file, and an extract recipe's `order:` / `time:` fields (the same **sorted-source
metadata** the ASOF merge already requires, `DESIGN-merging.md`) let the scan **advertise its
order** so the sort is *elided*. Then "sorted stream" is just "the scan, in order."

Concretely the substrate is a **sorted broadcast**: scan → (sort or ordered-scan) → a
`tee`-like op that fans the sorted, partition-boundaried stream to K stateful consumers, each
carrying its own predicate(s), frame/band parameters, and projection. It is `OpBroadcast`
with (a) an order/partition contract on its input and (b) consumers that keep bounded
per-partition state instead of being pure per-row functions.

## Part A — window / context detectors (grep -A/-B/-C) <a name="part-a"></a>

**Expressible + correct today** (verified end-to-end via `.rules`):

```sql
SELECT p, pos, line FROM (
  SELECT _meta.`path` AS p, _meta.pos AS pos, line,
         MAX(CASE WHEN sev = "ERROR" THEN 1 ELSE 0 END)
           OVER (PARTITION BY _meta.`path` ORDER BY _meta.pos
                 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS near
  FROM logs) sub
WHERE sub.near = 1
```

Runs standalone (it has an `OVER`), and `gate:` can skip it when the keyspace has no ERROR.
But K such detectors don't share the scan+sort of `logs`.

**The sparse-match decomposition.** grep -C is *"emit r iff some match row m (pred(m) true)
is within ±N of r in the sorted partition."* That factors into:

1. **Match detection** — rows where `pred` holds. *Stateless, per-row, index-prunable* — the
   exact thing `broadcast` + the AC index already do best, and matches are rare.
2. **Context expansion** — emit ⋃ of `[m−B, m+A]` intervals around matches (deduped).

So instead of folding a window over *every* row, run the cheap sparse match-detection and
expand context only *around* matches — one streaming pass with a bounded ring buffer of
`max(B,A)+1` rows (buffer the last B, look ahead A). O(N) time, O(B+A) space, no per-row
frame fold, and the match half is index-pruned.

**The shared context operator.** For all context detectors with signature `(logs, P, O)`:
one scan + one ordered stream + one shared ring buffer, fanned to K **context extractors**
`= (matchPred, before, after, projection)`. Cost drops from `O(K × scan × sort)` to
`O(scan + sort + Σ matches × N)`. It is a stateful sibling of `OpBroadcast`, and the AC index
selects which detectors' match-predicates even run per row.

This also cleanly beats the general window machinery *for this idiom* — no O(N) frame folds,
no per-detector sort — while the native window functions (`DESIGN-exprs.md`) remain the
general fallback for arbitrary `OVER` shapes.

## Part B — temporal cross-keyspace correlation (XYZ → ABC) <a name="part-b"></a>

**Is it expressible in stock SQL++?** Yes, several ways:

```sql
-- (i) nearest FOLLOWING ABC within Δt after each XYZ (an argmax subquery -- the ASOF shape):
SELECT a.ts, a.msg,
  (SELECT b.msg FROM log2 b
   WHERE b.ts >= a.ts AND b.ts <= a.ts + 5000 AND b.msg LIKE "%ABC%"
   ORDER BY b.ts ASC LIMIT 1) AS following_abc
FROM log1 a
WHERE a.msg LIKE "%XYZ%"

-- (ii) existence (did ABC follow XYZ within Δt) -- a temporal SEMI-join:
SELECT a.ts, a.msg FROM log1 a
WHERE a.msg LIKE "%XYZ%"
  AND EXISTS (SELECT 1 FROM log2 b
              WHERE b.ts BETWEEN a.ts AND a.ts + 5000 AND b.msg LIKE "%ABC%")

-- (iii) a plain band JOIN (all pairs within the band):
SELECT a.ts AS xyz, b.ts AS abc FROM log1 a JOIN log2 b
  ON b.ts BETWEEN a.ts AND a.ts + 5000
WHERE a.msg LIKE "%XYZ%" AND b.msg LIKE "%ABC%"
```

Add `AND b.node = a.node` for same-entity correlation (a partition equi-key). All parse and
run **correctly** today. Form (iii), naively, is O(n×m) nested-loop.

**What n1k1 optimizes today — and the gap.** The ASOF recognizer (`glue.WireASOFJoin` /
`MatchArgmaxAsof`, `DESIGN-merging.md`) lowers the argmax-subquery shape (i) to a **streaming
K-way merge** — O(n+m) — covering nearest-preceding *and* nearest-following (ASC), soft/
bounded (± Δt), partitioned (`b.node = a.node`), cross-node, and near-sorted. BUT: its WHERE
recognizer accepts **only** partition equi-keys (`field = field`) and key-band inequalities;
**any content predicate on the inner stream bails** (`MatchArgmaxAsof`'s conjunct loop has a
`default: return nil, false`, and `classifyEq` requires *both* operands to be field refs, so
`b.msg LIKE "%ABC%"` and even `b.act = "ABC"` are rejected). So the very predicate that makes
this "*ABC* soon after XYZ" — `b.msg LIKE ABC` — **defeats the merge**, and (i) falls back to
a per-outer-row correlated subquery (effectively O(n × m)).

**The unlocking extension — a residual content filter on the right stream.** A content
predicate on the inner stream doesn't break the merge's monotonicity; it just filters
candidates. Let the ASOF right input be a **filtered** ordered scan (`log2 WHERE msg LIKE
ABC`, pushed down): the merge advances the right cursor as usual but only *considers* rows
that pass the residual. Still O(n + m); now "nearest ABC after XYZ" is a merge, not a
nested loop. This is the single highest-value change for Part B and is local to the
recognizer + merge input.

**Then MQO composes on top**, via the same sorted substrate as Part A:

- **Per-stream content filters fuse.** `a.msg LIKE XYZ` over log1 and `b.msg LIKE ABC` over
  log2 are the stateless fused shape — shared scan + AC index per keyspace. A corpus of
  correlation detectors reading log1/log2 shares each keyspace's scan and wakes only the
  detectors whose literal is present.
- **The sort is shared.** All correlation detectors ordering log1/log2 by `time:` share one
  sorted stream per keyspace — the same shared-sort win as Part A (and elided when the source
  advertises order).
- **(Speculative) the merge is shared.** K correlators doing `log1 ⋈_ts log2` with different
  content-predicate pairs could ride ONE cursor-advance over the two sorted streams, each
  contributing its predicate pair — MQO applied to the join itself, a step past shared scan.

**Multi-step sequences (XYZ → ABC → DEF).** A chain of ASOF joins — each step a merge over
the next stream — expresses bounded event sequences (the common CEP case) in stock SQL++
(nested argmax subqueries / chained joins). The general "match a regex over an event stream"
(SQL:2016 `MATCH_RECOGNIZE`) is out of scope (grammar), but 2–3 step chained-ASOF covers most
support-detector needs and rides the same merge substrate.

## How to make it detectable without changing the grammar <a name="detect"></a>

The hard constraint (`DESIGN-prepare.md` §stock-sqlpp) stands: **no dialect changes**. Three
grammar-free levers, in the spirit of how ASOF is already recognized — usable together:

1. **Recognize the canonical idiom** (like `MatchArgmaxAsof`). The engine spots the context
   match-flag shape (or the correlation argmax) and lowers it to the shared operator. Authors
   write normal SQL. *Con:* recognizers are intricate and polarity-sensitive (the context
   flag must select "present", not "absent"; I hit exactly this fragility auto-deriving the
   `gate:` literal — see `DESIGN-prepare.md`).
2. **A front-matter directive** (recipe *metadata*, not grammar — same category as `source:`
   / `gate:`), e.g.
   `context: { match: 'sev = "ERROR"', before: 2, after: 2, partition: _meta.path, order: _meta.pos }`
   or `correlate: { left: log1, left_match: '…', right: log2, right_match: '…', within: 5000, partition: node }`.
   The corpus compiler builds the shared operator directly — trivially fusable, trivially
   authored (ideal for AI-generated recipes). *Con:* a second representation that can drift
   from the SQL; needs a "which wins / must agree" rule.
3. **A blessed template** — the authoring guide standardizes ONE exact SQL skeleton (the
   CONTEXT idiom already in `.rules help`), and the compiler matches *that skeleton*
   structurally rather than reasoning about arbitrary windows. Middle ground: stock SQL++,
   but the recognizer only handles the blessed shape.

**Recommendation:** lead with **(2)** for both cases (highest leverage, cleanest fusion,
best for agents), sharing one lowering target with **(1)** added later as sugar for
hand-authors. This mirrors ASOF exactly: a declared/recognized idiom → a specialized shared
operator → grammar-free. The directive's `match:` predicate feeds the AC index for free, and
the `(source/left/right, partition, order)` fields ARE the fusion signature.

## Phasing (incremental, evidence-gated) <a name="phasing"></a>

Each step is independently useful and benchmark-gated (like the DESIGN-col roadmap):

1. **Sort elision when the source is already ordered** (single-detector, no MQO). Teach the
   window/merge path to skip the sort when the scan advertises `order:` matching the `ORDER
   BY` key. Benefit even before any sharing; foundation for the shared stream. *Measure:* a
   context detector over a pre-ordered keyspace vs today's forced sort.
2. **ASOF residual content filter on the right stream** (Part B unlock). **DONE (preceding).**
   `MatchArgmaxAsof` now recognizes WHERE conjuncts that reference only the right alias
   (`refsOnlyAlias`) as a residual (`AsofMatch.RightResidual`), and the lowering pushes them
   as a filter onto the build scan (`withRightResidual`) so the merge finds the nearest row
   that ALSO matches — byte-identical to the correlated baseline
   (`TestASOFLoweringRightResidualDifferential`). This turns "the nearest <content-matching>
   row of another stream" from O(n×m) into a merge, for the **nearest-preceding** direction.
   *Surfaced while building this:* nearest-**following** (ASC + `r.key >= e.key`) is
   recognized but the merge-join op is **preceding-only**, so following was lowering to the
   WRONG rows — a latent correctness bug. It now **bails to the correct correlated subquery**
   (`TestASOFFollowingBailsToCorrelated`). So the residual works today on preceding; the
   flagship "XYZ → ABC soon after" is *following*, which needs step 2b.
2b. **Nearest-following in the merge-join op** (the flagship's real unblock). **DONE.**
   `engine/op_merge_join.go` gained a following mode (Params[7] `direction`): a
   non-consuming forward cursor (first right row with key ≥ left key), unpartitioned and
   partitioned (a per-partition ascending index list + cursor, `mergeJoinStepAsofFollowing`);
   the recognizer accepts the look-AHEAD soft bound `r.key <= e.key + Δt` (`splitLookahead`
   → soft following, "within Δt after"); the lowering threads `AsofMatch.Direction` and no
   longer bails on following. Differential suite: `TestASOFLoweringFollowing{,Residual,Soft,
   Partitioned}Differential` — all byte-identical to the correlated baseline. So the flagship
   "XYZ → ABC soon after" now lowers to an O(n+m) merge (with the step-2 content residual
   composing). Still ahead: MQO across correlators (steps 3–5) — this is still a single
   standalone detector's optimization, not yet scan/sort-shared across a corpus.
3. **The shared sorted broadcast** (the substrate). One scan+sort per `(keyspace, P, O)`
   signature, fanned to K stateful consumers. First consumer type: the **context extractor**
   (Part A). **Engine primitive DONE** (`engine/op_broadcast_context.go`, kind
   `broadcast-context`, `OpBroadcastContext`/`BroadcastContextExec`): fans ONE pre-sorted,
   partition-grouped child stream to K grep -B/-A extractors — per extractor a look-behind
   buffer (deep-copied, capped at `beforeMatch`) + a `afterMatch` forward counter emits each
   context row once, tagged; an optional partition-key expr resets the window per partition
   so context never crosses files. Interpreter-oriented + copied verbatim into intermed
   (compiler differential green), exactly like `OpBroadcast`. Tests
   (`op_broadcast_context_test.go`): grep -C1/-B2/-A2 vs a brute-force reference,
   cross-partition no-leak, no-partition. **Glue recognition DONE** (`glue/corpus_context.go`):
   `recognizeContextDetector` paranoidly matches the windowed match-flag idiom in the
   converted plan — `SELECT … FROM (SELECT …, MAX(CASE WHEN <pred> THEN 1 ELSE 0 END) OVER
   (PARTITION BY <P> ORDER BY <O> ROWS …) AS near …) WHERE near = <positive>` — extracting
   the frame→match mapping (`beforeMatch`=FOLLOWING count, `afterMatch`=PRECEDING count), the
   CASE predicate, the (P,O) sort, and the keyspace; it descends through passthrough projects
   and a pure outer ORDER BY (cosmetic for a set of findings) and bails to standalone on any
   deviation (an absence `near = 0` polarity, OFFSET/LIMIT, multi-column PARTITION, non-MAX,
   …). `CorpusCompile` groups recognized detectors by their `(keyspace, P, O)` signature and
   `buildContextBroadcast` emits ONE fresh scan → `order-offset-limit` → `broadcast-context`
   per group (self-rooted exprs via `renameAliasToSelf`), unioned with the fused broadcasts;
   evidence is the whole matched/context row (MVP). Differential-tested against each
   detector's own SQL (`corpus_context_test.go`: same-signature detectors share ONE
   scan+sort+broadcast-context and match the standalone window result; different signatures
   split; absence stays standalone). Verified end-to-end via `.rules run`.
   **Sort-elision DONE (step 1) for the flagship shape:** a group partitioned by
   `_meta.`path`` and ordered by `_meta.pos` needs NO sort — the file datastore already
   yields records grouped per file (filepath.Walk + sort.Strings, one file fully before the
   next) and in ascending `_meta.pos` within each (the in-file ordinal), so the raw scan is
   already in (partition, order) form (the context op needs only per-partition contiguity +
   in-partition order; findings are an unordered set). `buildContextBroadcast` feeds the
   scan straight to `broadcast-context` (no `order-offset-limit`), O(N) streaming instead of
   O(N log N) + a full buffer/spill. Guarded narrowly to those exact `_meta` keys
   (`isDotMetaField`); any other (partition, order) keeps the explicit sort. (Subtlety:
   `renameAliasToSelf` mutates in place, so the elision test runs on the pristine
   alias-rooted exprs before the sort terms are built.)
   **AC-index sparse-match DONE:** `BroadcastContextExec` now builds an Aho-Corasick index
   over the extractors' necessary literals (reusing `PrefilterLiteral` + `base.AhoCorasick`,
   like `OpBroadcastIndexed`); one AC pass per row wakes only the extractors whose literal
   is present, and a non-woken extractor's (boxed CASE / regexp) predicate is NOT evaluated
   (its literal is absent ⇒ necessarily not a match). Every extractor still runs its cheap
   context bookkeeping (a non-match row can be a neighbour's context). For the AC to bite,
   the glue lowers each match predicate to its NATIVE tree (`contextPredTree`, mirroring
   `normalizeCorpusPred`) so a literal is extractable; a non-nativizable pred is always-wake
   (safe). Since log matches are sparse, this skips the great majority of predicate evals.
   Tests: engine `TestOpBroadcastContextAlwaysWake` (+ the grep differentials run through
   the AC path); glue `TestCorpusContextPredNativized` (the pred is native, not boxed).
   *Remaining:* evidence shaped to the SELECT projection (vs whole-row); multi-column
   PARTITION; a general "source advertises its order" contract (beyond the `_meta` keys);
   per-detector hit stats for context ops. *Measure:* K context detectors, shared vs
   standalone (expect ~K× on the sort).
4. **Correlation consumers on the shared substrate** (Part B MQO). Fuse per-stream filters
   (shared scan + index) and share the sorted streams feeding the merges.
5. **(Stretch) shared merge** across correlators with a common `(left, right, key)`.

## Open questions & honest caveats <a name="open"></a>

- **Signature clustering in practice.** The win assumes many detectors share `(keyspace, P,
  O)`. Plausible for context (`_meta.pos`) and time-correlation (`time:`), but unquantified —
  needs a real corpus to confirm the groups are large.
- **New stateful engine surface.** The sorted broadcast + ring-buffer context extractor +
  residual-merge are genuinely new operators, not plumbing over the per-row tee — the biggest
  build here. Worth prototyping one consumer (context) before generalizing.
- **Two ways to say the same thing.** A `context:` / `correlate:` directive alongside raw SQL
  needs a precedence/agreement story (and `.rules lint` should flag a directive that doesn't
  match its SQL body).
- **The normalized time key.** Cross-log correlation needs many timestamp formats/zones
  reduced to one sortable int64 key — the per-source extract/parse-spec layer
  (`DESIGN-data.md`), still the load-bearing prerequisite for Part B on real bundles (same
  open item ASOF already has).
- **Soundness of directive-declared preconditions.** As with `gate:`, a directive asserts
  semantics the compiler trusts; `.rules lint` / golden fixtures are the guardrail.
- **When NOT to fuse.** A lone detector, or one with a unique `(P, O)`, gains nothing from the
  shared substrate and should stay standalone — fusion is a corpus-scale optimization, not a
  single-query one (the crossover analysis in `DESIGN-prepare.md` §worth-it applies).
