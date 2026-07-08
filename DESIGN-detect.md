# Design: a detector corpus over support bundles — multi-query PREPARE++

Status: proposal (vision / direction — nothing implemented; builds on
[DESIGN-prepare.md](DESIGN-prepare.md))

Support engineers receive **support bundles**: big `*.zip`s a cluster-management tool
gathers on a customer's site — subtrees of log files, JSON, config, stats dumps, mixed
formats. Years of tickets (e.g. `ET-12345`) recur across customers and clusters. The
vision: a **growing, git-maintained repository of SQL++ "detectors"** — filters / scans /
correlations that report *"this bundle shows evidence of ET-12345 and ET-111222"* — and an
engine that applies **thousands** of them to an incoming bundle **without** scanning it
thousands of times.

n1k1 is a good fit because this hits **both** of codegen's payoff regimes at once (see
[DESIGN-prepare.md "Is codegen worth it?"](DESIGN-prepare.md#worth-it)): the corpus is
compiled **once** and EXECUTEd against **every** incoming bundle (PREPARE-once / run-many),
and each bundle is a **large scan** (GBs of logs). The example that looked absurd for the
compiler — `SELECT 1+1 → emit Go` — inverts here: a detector corpus is the compiler's
reason to exist. This doc calls that extension **PREPARE++**.

## Contents

- [The shape of the problem](#shape)
- [Shared scan / multi-query optimization (the core)](#mqo)
- [Temporal correlation: ASOF joins + windows](#temporal)
- [PREPARE++ — compiling the corpus](#prepare-plus-plus)
- [Git-awareness](#git)
- [The evidence / findings output model](#evidence)
- [AI-authored recipes need a test harness first](#ai-recipes)
- [The hard parts (honest)](#hard-parts)
- [Recommendation / phasing](#phasing)
- [Open questions](#open-questions)

## The shape of the problem <a name="shape"></a>

- **Input:** a `*.zip` = a directory tree of heterogeneous files (logs, JSONL, CSV, config,
  stat dumps). n1k1's record providers already read these formats (`DESIGN-data.md`); a
  **zip datastore** presents the archive's tree as keyspaces (`<subdir>/<file>` → keyspace),
  decompressed on the fly like the existing `.gz` path.
- **Corpus:** thousands of **detectors**, each = a SQL++ query + metadata (target ticket,
  target sources, severity) + a golden fixture (below). Maintained in git.
- **Output:** per bundle, a ranked **findings** table — which tickets the bundle shows
  evidence for, with the evidence.
- **Constraint:** running detectors one-at-a-time (or even N-way parallel) each re-scans the
  bundle — wasteful and slow when N is thousands and the bundle is large. The scan, and the
  per-row work, must be **shared**.

## Shared scan / multi-query optimization (the core) <a name="mqo"></a>

Push-based execution is the right substrate. A scan already *pushes* each row (a
`base.Val` = `[]byte`) into a yield function; multi-query = make that yield a **fan-out
(tee)** into K detector pipelines. Native exprs read the shared bytes with **zero boxing**,
so a row is decoded once and every detector evaluates against the same buffer.

- **MVP — broadcast op.** One `broadcast`/`tee` operator: scan once, push each row to K
  detector predicate pipelines, each emitting its matches. This alone beats N separate runs
  (one scan, one decode per row). Easy to build; measure it first.
- **The real win — don't evaluate most detectors on most rows.** Naive fan-out is still
  `K × rows`. With thousands of detectors the bottleneck is per-row predicate work, not I/O.
  Three levers, in increasing effort:
  - **Source routing (cheap, big).** A detector declares its target (`indexer.log`,
    `*.json`); a file only fans out to detectors that target it. Prune before any evaluation.
  - **Corpus CSE.** Detectors share sub-predicates (`level="ERROR"`, `component="indexer"`,
    `line LIKE '%panic%'`). The compiler already stringifies exprs; a **global
    common-subexpression pass over the corpus** computes each shared term once per row, not
    once per detector. (Same expr-identity the [boxed-expr
    stringify](DESIGN-prepare.md#boxed-exprs) relies on.)
  - **Predicate index (the scale trick).** Borrow from pub/sub matching and SIEM rule
    engines: index detectors by their cheapest discriminating literal — an **Aho-Corasick**
    token scan over each log line, or an equality index over structured fields — so a row
    only *wakes* the few detectors whose prefilter hits. Thousands of rules, a handful
    evaluated per row. This is a natural n1k1 operator: a filter-index node feeding a sparse
    fan-out.

The engine already has the pieces the fan-out reuses (batching, spill, native byte eval);
the new surface is the broadcast op and the predicate index, not new datastore logic.

## Temporal correlation: ASOF joins + windows <a name="temporal"></a>

Most real evidence is **correlational and time-based**, not a single-line match:

- "error X in `indexer.log` within 5s of rebalance Y in `query.log`"
- "N connection resets within a 10s window" (burst/rate)
- gap detection, sessionization, "state A never followed by state B"

Two capabilities carry most of this:

- **ASOF join** (join each row to the nearest-preceding row of another stream by timestamp)
  — worth building. Over time-sorted log streams it is a **merge**, which fits push-based
  execution perfectly, and logs are usually near-sorted already (or cheaply sortable /
  spill-sortable — the engine already spills `ORDER BY`).
- **Windowed rate / burst / streak detection** — rides on the existing **window functions**
  (`DESIGN.md`): `count(*) OVER (…10s…)`, streak length, inter-arrival gaps.

ASOF + windows are probably the single most valuable *new engine* capability this vision
needs; everything else leans on what exists.

## PREPARE++ — compiling the corpus <a name="prepare-plus-plus"></a>

PREPARE++ is [PREPARE](DESIGN-prepare.md#the-surface) applied to a **repository**, not a
statement: compile the whole detector corpus into **one fused program** (or a few — see
[granularity](#open-questions)), with the shared-scan fan-out, corpus CSE, and predicate
index baked in. Why codegen genuinely pays here (unlike a one-shot ad-hoc query):

- **PREPARE-once / run-many.** Compile the corpus once; EXECUTE it against every incoming
  bundle forever. The [break-even](DESIGN-prepare.md#worth-it) (`~10M rows / K executions`)
  is met on day one and then amortizes to nothing.
- **Large scans.** GB-scale bundles are exactly the very-large-scan regime where compiled
  operator fusion beats the interpreter outright.
- **Embed-source → portable analyzer.** The [self-contained prepared
  program](DESIGN-prepare.md#embed-source) model ships a **support-bundle analyzer binary**
  with **no `n1k1-query` fork** (parse/plan happened at corpus-build time), runnable inside
  the support pipeline or even on-site. The zip is just a datastore behind the
  [`DatastorePipe`](DESIGN-prepare.md#design-principle) abstraction.

So PREPARE++'s prepared artifact is "the recipe book, compiled" — cached and re-executed,
not rebuilt per bundle.

## Git-awareness <a name="git"></a>

Detectors are versioned artifacts; lean into it.

- **Provenance.** A finding cites the exact rule that fired:
  `ET-12345 detected by recipes/indexer/panic.sql++@<sha>`. Support can trace a verdict to a
  recipe version.
- **Build cache keyed by tree SHA.** The compiled corpus is content-addressed by the recipe
  repo's git tree SHA; only changed detectors recompile. This is the same content-addressed
  build cache already sketched for [embed-source](DESIGN-prepare.md#embed-source), applied to
  the recipe set — and it bounds the "thousands of detectors" compile cost.
- **Reproducibility.** Re-run an old ticket's analysis with the recipe versions current at
  the time.

## The evidence / findings output model <a name="evidence"></a>

A detector yields **evidence**, not a boolean: structured findings — `{ticket, confidence,
source_file, line_range, evidence_rows, summary}`. `UNION ALL` across detectors → one ranked
findings table. This is ordinary SELECT projection over the matched rows; n1k1 already does
it. Confidence/severity ordering and de-duplication (the same ET flagged by several
detectors) are `GROUP BY` / `ORDER BY` on that table.

## AI-authored recipes need a test harness first <a name="ai-recipes"></a>

If agents write thousands of recipes, the **recipe format must be testable**:

- Each recipe = **SQL++ detector + metadata (ticket id, target sources, severity) + a golden
  fixture** (a tiny sample bundle fragment + the expected finding).
- **CI runs the whole corpus** against a labelled bundle library on every change — this is
  what keeps false-positives bounded and lets an agent propose a recipe from a
  freshly-solved ticket with confidence.
- The fixture doubles as documentation and as the corpus's regression suite (mirrors the
  differential-test discipline in `DESIGN-testing.md`).

## The hard parts (honest) <a name="hard-parts"></a>

- **Predicate-index MQO is real work.** The MVP fan-out is small; the token-index + corpus
  CSE engine is the meaty part and where the scale actually comes from.
- **Compiling thousands of detectors into one Go program** stresses compile time + binary
  size. Mitigations: the predicate index (fewer distinct ops), **corpus sharding**, and the
  SHA build cache.
- **Messy unstructured logs** need grok/regex extraction and tokenization. And **regex /
  complex string exprs currently box** (fall back to cbq — see
  [boxed expressions](DESIGN-prepare.md#boxed-exprs)), which **blocks codegen**. So this
  vision directly motivates the [`DESIGN-exprs.md`](DESIGN-exprs.md) native-coverage work,
  especially **string / regex / time** exprs: every one covered natively widens what the
  corpus can compile. This is the tightest coupling between the two efforts.
- **Heterogeneous evidence** (structured JSON vs unstructured text lines) implies two pipeline
  shapes; source routing keeps them apart.
- **Correctness / false-positives** — bounded only by the fixture flywheel, not by the engine.

## Recommendation / phasing <a name="phasing"></a>

1. **Zip datastore + source routing** — scan a bundle's tree as keyspaces; detectors declare
   target sources.
2. **Shared-scan fan-out op (MVP MQO)** — one scan, N predicates, native byte eval; measure
   vs N separate runs.
3. **Predicate index + corpus CSE** — the scale win (Aho-Corasick / equality prefilter +
   shared-subexpression factoring).
4. **Temporal ops** — ASOF join + windowed rate/burst/streak.
5. **PREPARE++ corpus compiler** — fuse the corpus; SHA-keyed build cache; evidence/findings
   output; embed-source analyzer binary.
6. **Recipe format + golden-fixture CI** — the AI-authoring flywheel.

Each phase is independently useful: (1)-(2) already let a human run the recipe book cheaply;
(3)-(4) make it scale and correlate; (5)-(6) make it a maintained product.

## Open questions <a name="open-questions"></a>

- **Corpus granularity.** One giant fused program per bundle, or **sharded** by
  source/subsystem (indexer detectors, query detectors, …) compiled + cached independently?
  Sharding bounds compile time and lets an agent ship one rule without rebuilding the world —
  *leaning sharded*, with the predicate index within each shard.
- **Where the corpus runs.** An [embed-source](DESIGN-prepare.md#embed-source) self-contained
  analyzer binary (portable, runs anywhere, no fork) vs. the in-process CLI reading the zip —
  *leaning embed-source as the target, CLI for the prototype*.
- **Predicate-index structure.** Aho-Corasick over raw log lines, an equality/range index
  over parsed fields, or a hybrid BE-tree — which fits the mix of structured + unstructured
  detectors with least per-row overhead?
- **Detector language surface.** Plain SQL++ only, or a thin recipe wrapper (metadata +
  fixture + one or more SELECTs)? How much can be *inferred* (target sources from the FROM
  clause) vs must be declared?
- **Time model.** How to normalize wildly different log timestamp formats/timezones into one
  sortable key for ASOF joins — a per-source parse spec, or inferred?
- **Native-coverage ordering.** Which string/regex/time exprs to port first
  (`DESIGN-exprs.md`) to unblock the most common detector shapes for full codegen?
