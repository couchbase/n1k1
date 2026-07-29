# Design: n1k1 census & doctor — keeping standing questions connected to reality

> **Spec + build plan.** A time-aware key-space **census** over schemaless append-only data, and a
> **doctor** that joins it against a committed `.multi` query pack — so n1k1 can answer the one
> question a corpus of standing questions most needs: *are my questions still connected to reality?*
> Motivated by, and verified against, the `n1k1-for-ai` dogfooding prototype (ISSUE-06/07). Builds on
> [DESIGN-prepare.md](DESIGN-prepare.md) (`.multi`/MQO) and [DESIGN-cep.md](DESIGN-cep.md) (cursors).

## The point of view

Most query tools assume the schema is known **upstream of the query**. That assumption is quietly
expiring. As more data becomes agent exhaust, LLM-emitted records, and tool output, the share with
**no declared schema** goes up, not down — data whose shape is a side effect of fast-moving code
nobody contracted, versioned, or documented. Call it **schema slop**: fields are born, die, fork
spellings (`sessionId` *and* `session_id`, both live), and change type (`usage` an object on most
records and a string on a few) — weekly, with no migration and no deprecation.

n1k1's opening is **not** "SQL over JSONL but faster" — that fight is against DuckDB and at local
scale compute is free. The opening is that **n1k1 is the only tool that holds both a corpus and a
persistent, named, git-committed set of standing questions about it.** A stateless engine has no
notion of "the questions I am standing by," so it structurally cannot tell you those questions have
drifted. Three things n1k1 holds simultaneously make this ours:

1. the shared, fused, index-pruned scan (`.multi`);
2. a persistent, git-committed pack of standing questions — `label`/`gate`/`source`/field
   references/`spec_hash`/`annotations`;
3. cursors — per-source watermarks with history, so it knows what the corpus looked like *last time*.

> `.multi lint` already parses every field path and index literal in the pack. A census knows every
> field path in the corpus and when each was last seen. **Joining those two lists is a small feature
> nobody else is positioned to ship** — and it is the whole game.

**The principle, stated once:** *an empty result is the most dangerous output the system can
produce, and it should be the loudest.* A crash gets fixed in an hour; a quiet zero ships into a
slide deck. Five of the reported issues were the same disease — a zero that reads like an answer
(a UDF returning 0 not erroring; a dropped fixture PASSing; a rejected DAG node counting 0; a
detector aimed at a retired field matching 1 forever). The engine-side vectors are fixed; the
**data-side** vector is the one that can't be fixed by making the engine more correct — the corpus
drifts no matter how good the engine is. Census + doctor are the defense.

## Why the census needs a time axis (not one-shot inference)

Schema inference (`read_json_auto`, `mongodb-schema`, `variety.js`, `_field_caps`, `genson`) answers
*"what shape is this data now."* Every drift finding needs *"when did this field start and stop."*
Sampling makes it strictly worse: a field on 1 record in 184,128 — a detector aimed at a vestigial
field — is invisible to any sampler, and **that near-absence is the signal.** And without a
first_seen, *every trend chart conflates a change in behavior with a change in instrumentation*: a
field that didn't exist for the first 11 days makes a friction chart read "no friction until July"
when the honest reading is "the field didn't exist yet." Clip every series at its field's
`first_seen`, or mislead.

## SHIPPED — `.multi census`

`.multi census <keyspace> [--bind <m>] [--type-field <f>] [--time-field <f>] [--depth 1|2]
[--exclude a,b]` (`glue/census.go`, `cmd/n1k1/multi_census.go`). For every **(record-type,
field-path, value-type)** it emits `docs`, `coverage` (of its type), `first_seen`, `last_seen`, and
`first_id` (the record that first carried the cell), as NDJSON.

Design decisions, verified against the prototype:

- **Every stored column is a mergeable aggregate** — `docs`=SUM, `first_seen`=MIN, `last_seen`=MAX,
  and `first_id` via **argmin-as-MIN** over `"<ts>|<id>"` (ISO-8601 sorts lexically, and MIN merges
  where a real argmin wouldn't). So a census is a **commutative monoid**:
  `census(A) ⊎ census(B) == census(A ∪ B)`. This is the property that makes the *incremental*
  census (next) a plain re-aggregation with no re-read.
- **No ratios, no distinct-counts stored.** `coverage %` doesn't merge (you can't average two
  percentages over different denominators) — keep `docs` and the per-type denominator apart, divide
  at read time. `COUNT(DISTINCT)` doesn't merge either — that's value-level census (a separate node,
  and the one place an HLL-style sketch earns its keep, because sketches *do* merge).
- **A polymorphic field is multiple rows, not a summary.** `value-type` is part of the key, so a
  field that changes shape shows up as two rows with disjoint day windows — never one lossy cell.
- **Per-type grouping is mandatory**, not optional: a global census hid the finding that two
  datasets shared one `*.jsonl` glob (event records with a `timestamp`, settings records without).
- **Bounded depth + escape hatch** for the key-space-explosion hazard (agent transcripts embed
  arbitrary JSON; a naive recursive census over `toolUseResult.**` is unbounded): default depth 2,
  arrays collapsed (never `content[0]`), `--exclude` for subtrees with arbitrary keys.
- **In-flight aggregation** — it emits ~one row per cell, not the prototype's row-per-record
  map/reduce (which materialized 184k array-carrying intermediate rows, ~+22% wall clock). A native
  operator deletes that intermediate, so it's near-free on a scan that's already happening.

## Build plan

**Phase 1 — `.multi census` (SHIPPED).** The operator above.

**Phase 2 — `.multi doctor` — the join (the differentiator).** Cheapest checks first:

| check | needs history? | catches | status |
|---|---|---|---|
| pack references a path **absent from the census** | no | typos, renames, birth-in-error — instant win | **SHIPPED** |
| census paths **no detector references** | no | unexplored surface — a detector-generation queue | **SHIPPED** |
| a referenced path's `last_seen` is stale while the corpus grew | census only | a detector went blind (a retired field) | roadmap |
| a detector's match rate fell off a cliff | cursor history | behavior *or* instrumentation change (see Phase 4) | roadmap |

> **SHIPPED — `.multi doctor --queries <dir> [--bind <m>]`** (`glue/doctor.go`,
> `cmd/n1k1/multi_doctor.go`). The referenced-field set is **planner-sourced** — `EntryReferencedFields`
> parses each entry and walks it with `ExprFieldPath` (the same static-path extractor `conv` uses),
> yielding doc-relative paths rooted at the FROM alias, so `META()`/function args are excluded and
> there's no text-heuristic suffix-match bug (the failure the dogfooding team hit from the wrong side).
> The first two checks are live: `references_absent` (a detector reads a top-level field the corpus
> lacks → **hard-fails**, the birth-in-error catch a yield alarm structurally can't make) and
> `unreferenced` (corpus fields no detector reads). The first check is TOP-LEVEL granularity for
> precision (no false positives against a depth-limited census). *Roadmap:* stale-`last_seen` and
> match-rate checks (need the census's time axis / cursor history); per-record-type scoping;
> `annotations`-based suppression (already the shipped substrate) so a blessed-rare field is quiet.
| **census paths no detector references** | no | unexplored surface — a **detector-generation queue** |

The last row inverts the feature from defensive to offensive: "here are the actively-written fields
nobody is asking about" converts directly into new committed SQL++. *Implementation note:* the
"referenced paths" set is the real work — it must come from the **planner's** field-path extraction
(what `.multi lint` already walks), not a text heuristic, or it will false-positive on aliases and
sub-selects. Suppression **must reuse `annotations`** (already shipped, already outside `spec_hash`):
`annotations: {"expected_rate":"rare"}` blesses a legitimately-rare field so doctor doesn't become
noise everyone mutes — do **not** invent a parallel config surface. And a census-vs-detector delta
needs *explaining*, not just reporting ("gate excluded 8", not "anomaly").

**Phase 3 — Census cursors (incremental, via the monoid).** Snapshot the census per window into the
cursor store; drift is then a `diff` over the *key space* rather than a recomputation. The cursor
machinery already expresses this for rows — `SnapshotFromResults` + `ChangeEvent`
(`insert`/`update`/`delete`) map onto `field_added` / `field_removed` / `type_changed` with no new
concepts, and the monoid guarantees the fold-forward is exact. ⚠ **The two-store atomicity wall:**
the census artifact and the cursor watermark must commit together, or a crash between them
double-counts silently (the prototype hit exactly this). A native census owns both sides and writes
them in one transaction — an argument for the operator that the prototype could not engineer around.

**Phase 4 — Yield-drift alarm (ISSUE-07) — the cheap complement.** Persist the `.multi run`
per-detector funnel (`scanned → woken → matched`) next to the cursor state and alarm when yield moves
in a way the corpus's growth doesn't explain — **matches per N new records** (the cursor already
supplies the denominator). *Which stage* moved is the diagnosis: `woken` flat while `scanned` grew =
the index literal stopped appearing (field retired); `matched` flat while `woken` grew = the gate now
rejects (value/shape changed). This is the **smoke detector** (regressions); the census is the
**diagnosis** (why). It cannot catch a **birth-in-error** — a detector that was *never* right has no
cliff — which is precisely why doctor's static "does this field exist at all?" check (Phase 2) is its
complement, not a nicety. Thresholds declarative and boring: `-- expect-min-rate: 1/10000`,
reviewable in a PR, over any inferred anomaly-detection.

**Phase 5 — Value-level drift.** Opt-in per field (`-- census-values: message.model, type`), capped
distinct with an explicit `too_many_distinct` marker (never silent truncation), emitting
`new_enum_value` — the check that keeps a price/lookup table honest when a de-facto enum
(`message.model`) grows a value the table has never heard of.

## Two concrete engine asks (small, high-leverage)

- **Nativize `TYPE_NAME`.** It is the *only* primitive that boxes the census (the prototype measured
  +3.2s over the corpus); it's a pure scalar on one argument, so it's a natural addition to the native
  byte lane. Type drift is the whole point of a census, so it's worth having native before the
  operator leans on it. (⚠ per the codegen notes — [n1k1-codegen mechanics] — register new native
  functions carefully and run `make test-compiler`; this is a real but bounded task.)
- **A `merge(stateA, stateB)` hook on the JS aggregate API.** Today `foo.agg.js` exposes
  `init`/`update`/`final` — a fold with no way to combine two partial states, so a custom aggregate
  can't participate in the incremental/cursored story at all. An optional `merge` makes **every**
  user-defined aggregate incremental and parallelizable, and it's the natural home for a mergeable
  HLL sketch (the one piece value-level distinct-counting needs). Small API addition, general win.
  (The core census needs no JS — `COUNT`/`MIN`/`MAX`/`SUM` are built-in and already mergeable, which
  keeps the census map tier fused; a JS aggregate in tier 1 would box it.)

## Prior art & the white space (condensed)

Four categories each own one piece, none owns the whole: **schema inference** (Compass schema tab,
`variety.js`, `_field_caps`, `read_json_auto`) has the *shape* but no time axis and samples away the
tail; **data observability** (Monte Carlo, Bigeye, Great Expectations, dbt tests) has the time axis
but watches warehouse `ALTER TABLE`, not the key space *inside* nested documents, and is SaaS+pipeline,
and knows nothing of your query corpus; **schema registries** (Confluent, Avro/Buf, Segment Protocols)
assume you *control the producer* — schema slop is *defined* by the absence of a producer contract;
**log analytics** (Splunk `fieldsummary`, Honeycomb last-seen, Datadog facets) is closest
ergonomically but has no diffable history, no alarm, and no link back to saved searches. The white
space: *incremental, exact, time-aware key-space census over nested append-only files,
cross-referenced against a committed query pack, in a single binary with no service to run* — and
**nobody has the query-pack join, which is the part that turns a report into an alarm.**
