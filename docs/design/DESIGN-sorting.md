# Design: Shared sorted-stream substrate

_Last reviewed: 2026-07-25._

Companion to `DESIGN-prepare.md` (PREPARE++ / MQO), `DESIGN-merging.md` (the ASOF K-way merge),
and `DESIGN-exprs.md` (the native window functions this builds on). (Formerly
`DESIGN-mqo-sorted.md`.)

## The idea

n1k1's MQO fuses **stateless per-row** detectors: the `broadcast` op tees one scanned row to K
`if pred(row) { emit proj(row) }` filters, with an Aho-Corasick predicate index waking only the
few whose literal is present (O(hits × rows), not O(K × rows)). Correctness rests on: no
cross-row state, no ordering requirement, output ⊆ the matching row. The corpus compiler routes
anything else (window / GROUP BY / join / ASOF) to **standalone** — its own scan, no sharing. So
K context detectors over one `logs` keyspace = K scans + K sorts + K window passes.

Two detector classes fall outside the per-row model but share ONE missing substrate — a **shared,
sorted-by-key stream per keyspace**:

- **Window / context detectors** — grep -A/-B/-C: emit a match line ±N lines of context
  (`… OVER (PARTITION BY … ORDER BY … ROWS BETWEEN B AND A)`).
- **Temporal cross-keyspace correlation** — "XYZ in log1, then ABC soon after in log2" (an ASOF /
  band correlation by timestamp).

The fusable **signature** is `(keyspace, PARTITION key, ORDER key)`, and detectors cluster on a
small set of keys (`(_meta.path, _meta.pos)` per-file line order, or a normalized `time:` key).
Every detector with the same signature can ride ONE scan + ONE sort. **Sharing the *sort* is the
primary win** (~K×): sorting a GB of logs (O(N log N), possibly spilled) dominates a single
detector's cost — the per-row window/merge work is only O(N). A refinement removes the sort
entirely in the common case: `_meta.pos` is already ascending within a file, so the scan can
**advertise its order** and the sort is elided.

## Status

**Built:** the shared sorted substrate + its first stateful consumer, the grep -A/-B/-C **context
extractor** — K context detectors sharing a `(keyspace, PARTITION, ORDER)` signature fuse onto ONE
scan + ONE sort feeding a single `engine.OpBroadcastContext`
(`recognizeContextDetector` + `buildContextBroadcast`), with AC-index sparse-match, sort-elision
on `(_meta.path, _meta.pos)`. Part B's temporal-correlation path (ASOF) also lands:
nearest-preceding *and* nearest-following merges with a right-stream residual content filter, plus
a transparent datastore-pipe scan cache (`glue/corpus_cache.go`) sharing both sides of K
correlators' scans (byte-budgeted, spillable).

**Remaining:** multi-column `PARTITION BY` for context detectors (MVP recognizes one column); a
general "source advertises its order" contract beyond the hardcoded `_meta` keys; per-detector hit
stats; MQO across correlators (the stretch sweep-line coordinator — needs the resumable pull-cursor
work, `DESIGN-merging.md §2`); AC-index the shared left scan's per-stream filters + adaptive
scan-cache budget; and the normalized int64 `time:` key (per-source parse-spec layer,
`DESIGN-data.md`) — the load-bearing prerequisite for Part B on real bundles.

## Part A — window / context detectors (grep -A/-B/-C)

Expressible + correct today (verified via `.multi`): a windowed match-flag over the sorted
partition —
```sql
SELECT p, pos, line FROM (
  SELECT _meta.`path` AS p, _meta.pos AS pos, line,
         MAX(CASE WHEN sev = "ERROR" THEN 1 ELSE 0 END)
           OVER (PARTITION BY _meta.`path` ORDER BY _meta.pos
                 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS near
  FROM logs) sub
WHERE sub.near = 1
```
Runs standalone (it has an `OVER`); `gate:` can skip it when the keyspace has no ERROR, but K such
detectors don't share the scan+sort of `logs`.

**The sparse-match decomposition.** grep -C is "emit r iff some match row m is within ±N of r in
the sorted partition," which factors into: (1) **match detection** — rows where `pred` holds;
*stateless, per-row, index-prunable* (what `broadcast` + AC already do best, and matches are rare);
(2) **context expansion** — emit ⋃ of `[m−B, m+A]` intervals around matches. So run cheap sparse
match-detection and expand context only *around* matches — one streaming pass with a bounded ring
buffer of `max(B,A)+1` rows. O(N) time, O(B+A) space, no per-row frame fold, match half
index-pruned. Cost drops from `O(K × scan × sort)` to `O(scan + sort + Σ matches × N)`. This beats
the general window machinery *for this idiom*; the native window functions remain the general
fallback for arbitrary `OVER` shapes.

### Shipped mechanism

- **Engine** (`engine/op_broadcast_context.go`, kind `broadcast-context`): fans ONE pre-sorted,
  partition-grouped child stream to K grep -B/-A extractors — per extractor a look-behind buffer
  (deep-copied, capped at `beforeMatch`) + an `afterMatch` forward counter emits each context row
  once, tagged; an optional partition-key expr resets the window per partition so context never
  crosses files. Interpreter-oriented + copied verbatim into intermed (compiler differential green),
  like `OpBroadcast`.
- **Glue recognition** (`glue/corpus_context.go`): `recognizeContextDetector` paranoidly matches
  the windowed match-flag idiom in the converted plan, extracting the frame→match mapping
  (`beforeMatch`=FOLLOWING count, `afterMatch`=PRECEDING count), the CASE predicate, the (P,O) sort,
  and the keyspace; it descends through passthrough projects + a pure cosmetic outer ORDER BY and
  ⚠ **bails to standalone on any deviation** — an absence `near = 0` polarity, OFFSET/LIMIT,
  multi-column PARTITION, non-MAX. `CorpusCompile` groups recognized detectors by `(keyspace, P, O)`
  and `buildContextBroadcast` emits ONE scan → `order-offset-limit` → `broadcast-context` per group
  (self-rooted exprs via `renameAliasToSelf`), unioned with the fused broadcasts. Result is the
  whole matched/context row (MVP). Differential-tested against each detector's own SQL.
- ⚠ **Recognizers are polarity-sensitive:** the context flag must select "present", not "absent"
  (same fragility hit auto-deriving the `gate:` literal — `DESIGN-prepare.md`).
- **Sort-elision (step 1) for the flagship shape:** a group partitioned by `_meta.`path`` + ordered
  by `_meta.pos` needs NO sort — the file datastore already yields records grouped per file
  (`filepath.Walk` + `sort.Strings`, one file fully before the next) in ascending `_meta.pos`, so
  the raw scan is already in (partition, order) form. Feeds the scan straight to
  `broadcast-context`, O(N) streaming instead of O(N log N) + a spill. ⚠ Guarded narrowly to those
  exact `_meta` keys (`isDotMetaField`); any other (P, O) keeps the explicit sort. ⚠ **Subtlety:**
  `renameAliasToSelf` mutates in place, so the elision test runs on the pristine alias-rooted exprs
  *before* the sort terms are built.
- **AC-index sparse-match:** `BroadcastContextExec` builds an Aho-Corasick index over the
  extractors' necessary literals (reusing `PrefilterLiteral` + `base.AhoCorasick`); one AC pass per
  row wakes only extractors whose literal is present, and a non-woken extractor's (boxed CASE /
  regexp) predicate is NOT evaluated. Every extractor still runs its cheap context bookkeeping (a
  non-match row can be a neighbour's context). The glue lowers each match predicate to its NATIVE
  tree (`contextPredTree`) so a literal is extractable; a non-nativizable pred is always-wake (safe).

## Part B — temporal cross-keyspace correlation (XYZ → ABC)

Expressible in stock SQL++ several ways — the argmax subquery (the ASOF shape) is the canonical one:
```sql
SELECT a.ts, a.msg,
  (SELECT b.msg FROM log2 b
   WHERE b.ts >= a.ts AND b.ts <= a.ts + 5000 AND b.msg LIKE "%ABC%"
   ORDER BY b.ts ASC LIMIT 1) AS following_abc
FROM log1 a WHERE a.msg LIKE "%XYZ%"
```
(Also expressible as a temporal SEMI-join via `EXISTS`, or a plain band `JOIN` — naively O(n×m).
Add `AND b.node = a.node` for same-entity correlation, a partition equi-key.)

**The gap.** The ASOF recognizer (`glue.WireASOFJoin` / `MatchArgmaxAsof`, `DESIGN-merging.md`)
lowers the argmax shape to a streaming K-way merge (O(n+m)) — nearest-preceding *and* -following,
soft/bounded (±Δt), partitioned, cross-node. BUT ⚠ its WHERE recognizer accepts **only** partition
equi-keys and key-band inequalities; **any content predicate on the inner stream bails**
(`MatchArgmaxAsof`'s conjunct loop `default: return nil, false`; `classifyEq` requires both operands
to be field refs, so `b.msg LIKE "%ABC%"` and even `b.act = "ABC"` are rejected). So the very
predicate that makes this "*ABC* soon after XYZ" defeated the merge — (i) fell back to a per-outer-row
correlated subquery (O(n×m)).

**The unlock — a residual content filter on the right stream (DONE).** A content predicate on the
inner stream doesn't break monotonicity; it just filters candidates. `MatchArgmaxAsof` now
recognizes WHERE conjuncts referencing only the right alias (`refsOnlyAlias`) as a residual
(`AsofMatch.RightResidual`), pushed as a filter onto the build scan (`withRightResidual`) so the
merge finds the nearest row that ALSO matches — byte-identical to the correlated baseline
(`TestASOFLoweringRightResidualDifferential`). ⚠ **Latent bug caught here:** nearest-**following**
was recognized but the merge-join op was preceding-only, so following mis-lowered (briefly bailed to
the correlated subquery). Fixed by real following support: `engine/op_merge_join.go` following mode
(`Params[7]` `direction`), a non-consuming forward cursor (first right row with key ≥ left key),
partitioned + unpartitioned (`mergeJoinStepAsofFollowing`); the recognizer accepts the look-AHEAD
soft bound `r.key <= e.key + Δt` (`splitLookahead`). Differential suite
`TestASOFLoweringFollowing{,Residual,Soft,Partitioned}Differential`.

**Then MQO composes** (via the same sorted substrate): per-stream content filters fuse (`a.msg LIKE
XYZ` / `b.msg LIKE ABC` are the stateless fused shape, shared scan + AC index per keyspace); the
sort is shared (all correlators ordering log1/log2 by `time:` share one sorted stream); and
(speculative) the merge could be shared (K correlators over one cursor-advance). Multi-step
sequences (XYZ → ABC → DEF) express as chained ASOF joins — bounded CEP in stock SQL++;
`MATCH_RECOGNIZE` is out of scope (grammar), but 2–3 step chains cover most support needs.

### Corpus scan-sharing (DONE)

Recognition (`glue/corpus_correlate.go`): `analyzeCorrelationDetector` recognizes the shape purely
from the parsed algebra (no plan/convert; independent of ASOF-lowerability) → the
`(left, right, key, direction)` signature; `CorpusCompile` groups them into
`CompiledCorpus.CorrelationGroups`.

Execution (`glue/corpus_cache.go`): rather than rewrite each detector's plan to read a `temp-yield`,
a `corpusScanCache` — a `base.DatastorePipe` installed on the session — intercepts at the
datastore-op boundary and serves each correlation keyspace's FULL scan from a captured, spillable
(memory-bounded) heap: the first full scan of a shared keyspace is captured *while* serving, later
scans replay it. **Transparent** (detector plans unchanged, so it reaches the standalone `s.Run`
detectors too via `s.Pipe`); same materialize-once + replay + spill machinery as
`temp-capture`/`temp-yield`, hooked at the scan-op layer instead of the plan layer (no plan surgery).
Both sides share, keyed by `(QN + pushdown-spec)` (`scanCacheKey`): a FULL scan keys on the QN; a
`project-columns` scan keys on QN + columns, so identically-projecting detectors share the driving
side too. An unrecognized pushdown isn't cached (correctness over sharing). Each capture is
byte-BUDGETED (`CorpusScanCacheBudgetBytes`, default 256 MiB): ⚠ a keyspace larger than the budget
is **ABANDONED mid-capture** (partial heap freed, re-scanned thereafter) rather than mirrored to
disk in full, so a multi-GB keyspace degrades to the no-sharing baseline, labelResults unchanged.

⚠ **Scope caveats:** an UN-lowered correlated subquery evaluates its inner scan via **BOXED cbq**
(not an n1k1 `datastore-scan-records`), which neither the pipe NOR `temp-capture` can intercept — so
the win needs ASOF lowering (sorted-source metadata). Detectors projecting the driving keyspace
DIFFERENTLY don't share its scan. QN match relies on stripping backticks from the algebra
`PathString` to equal the keyspace `QualifiedName`.

**Stretch — sweep-line / shared merge** across correlators with a common `(left, right, key)`: one
watermark-driven cursor advance over the shared sorted streams feeding K predicate-pairs, each
keyspace holding only a bounded `[trailing, leading]` band resident (the streaming interval-join
model). Building blocks exist in fragments (window FRAME edge-cursors + incremental fold, the ASOF
pausing cursors, `OpBroadcastContext`'s before/after buffer, sort-elision); the missing piece is a
K-way watermark coordinator — which, because the merge family is push-based and materializes its
cursors, needs the deferred resumable pull-cursor work (`DESIGN-merging.md §2`). *First step DONE —
spill-backed merge-join build* (`MergeJoinExec`, `MergeJoinBuildSpillBytes`, default 64 MiB): the
build no longer pins the whole right keyspace's row payloads — ASOF/equi read build rows only
at/near the cursor, so once resident payloads cross the budget they spill to a heap and decode on
access, while `keys[]`/`part[]` stay resident as the index (~25× smaller/line). Differential-gated
(`TestMergeJoinBuildSpillMatchesResident`). Bounds the ROW-payload RAM, NOT the O(N) key index.
⚠ **Codegen landmine:** implemented via fully-INFERRED heap closures so this verbatim-copied
(non-lz) op never NAMES `rhmap/store` — the gen-compiler strips that import from `intermed`
(verified `make test-compiler` green).

## Detectability without grammar changes

The hard constraint stands: **no dialect changes**. Three grammar-free levers, usable together:
(1) **recognize the canonical idiom** (like `MatchArgmaxAsof` / `recognizeContextDetector`) — authors
write normal SQL, but recognizers are intricate + polarity-sensitive; (2) **a front-matter
directive** (recipe *metadata*, not grammar — same category as `source:`/`gate:`), e.g.
`context: { match:'sev="ERROR"', before:2, after:2, partition:_meta.path, order:_meta.pos }` or
`correlate: { left, right, left_match, right_match, within, partition }` — trivially fusable +
authored (ideal for AI-generated recipes), but a second representation that can drift from the SQL
(needs a "which wins" rule + a `.multi lint` check); (3) **a blessed template** — one exact SQL
skeleton matched structurally. **Recommendation:** lead with (2) (highest leverage, best for agents),
sharing one lowering target with (1) as sugar for hand-authors. The directive's `match:` predicate
feeds the AC index for free, and `(source/left/right, partition, order)` ARE the fusion signature.

## Honest caveats

- **Signature clustering is unquantified** — the ~K× win assumes many detectors share `(keyspace, P,
  O)`; plausible for context (`_meta.pos`) and time-correlation (`time:`) but needs a real corpus.
- **New stateful engine surface** — the sorted broadcast + ring-buffer extractor + residual-merge are
  genuinely new operators, the biggest build here (prototype one consumer before generalizing).
- **The normalized time key** — cross-log correlation needs many timestamp formats/zones reduced to
  one sortable int64 (the per-source parse-spec layer, `DESIGN-data.md`) — the load-bearing
  prerequisite for Part B on real bundles.
- **When NOT to fuse** — a lone detector, or one with a unique `(P, O)`, gains nothing; fusion is a
  corpus-scale optimization, not a single-query one (`DESIGN-prepare.md §worth-it`).
