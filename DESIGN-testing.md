# Growing n1k1's SQL++ test battery from the cbq corpus

## Status & remaining TODOs

_Last reviewed: 2026-07-11._

**Done:** The recorded-cbq conformance oracle is live in both interpreter and compiler modes — `TestSuiteCases`/`TestGsiSuiteCases` (multiset `rowsMatch`, `passFloor` regression backstop; defaults floor 1045, gsi floor 833), fed by the constant-expression and data-backed gsi slices imported from the n1k1-query fork; the two-phase compiler-differential sweep (generate into `test/tmp/`, then compile+run) is wired through `make test-compiler`/`test-suite`/`test-suite-all`, `TestNoPanicRegress` guards hand-coded fuzz repros, `-race` is a periodic gate, and the fresh-worktree EE-stub bootstrap recipe below is verified.

**Remaining (headline TODOs):**
- [ ] Import more PURE gsi categories (aggregate/window/etc.).
- [ ] Import PLAN categories, dropping plan/EXPLAIN-shape assertions.
- [ ] Investigate `results-differ` non-passes for real fixes vs. tie-broken-LIMIT noise.
- [ ] Add a guard test enumerating `OptimizableFuncs`, failing on any entry with no compiled-mode case.
- [ ] Land a committed `make bootstrap` / `go.work` to automate fresh-worktree setup.
- [ ] Fix `EXTRACTDDL(...)` panicking rather than erroring cleanly.

## Overview

n1k1 reuses cbq's (`couchbase/query` / n1k1-query fork) parser and planner,
evaluating expressions natively where ported and via cbq otherwise. The fork
carries a large SQL++ test corpus. This doc records how we mine the **pure
SQL++** slice into n1k1's suite harness — in both interpreter and compiler modes
— to confirm n1k1 behaves byte-for-byte like cbq. It also covers the mandatory
two-phase compiler validation and the `-race` concurrency gate.

## Contents

- [The source corpus](#the-source-corpus)
- [Imported: constant-expression cases (no data)](#imported-constant-expression-cases-no-data)
- [Imported: data-backed categories (isolated gsi corpus)](#imported-data-backed-categories-isolated-gsi-corpus)
- [Building the gsi suite in a fresh worktree](#building-the-gsi-suite-in-a-fresh-worktree)
- [Guidance and lessons](#guidance-and-lessons)
- [Verifying the compiler path](#verifying-the-compiler-path)
- [Concurrency testing under `-race`](#concurrency-testing-under--race)
- [Follow-ups and open questions](#follow-ups-and-open-questions)

## The source corpus

`<fork>/test/gsi/test_cases/<category>/` — 68 categories, ~2,352 case
statements. Each dir has `case_*.json` (arrays of `{statements, results}` or
`{statements, error}`) plus usually `insert.json` seeding via
`INSERT INTO <keyspace> (KEY,VALUE) VALUES("<key>", {...})`, tagged with
`test_id` so categories share the
`customer`/`orders`/`product`/`purchase`/`review` keyspaces. Upstream these are
live-cluster integration tests (`GSI_TEST=true`); the JSON is transformable.

### Portability scan (all 68 categories triaged)

| Bucket | Categories | ~Statements | Meaning |
|---|---|---|---|
| **PURE** | ~30 | ~915 | SELECT over data + scalar/aggregate/window exprs. Runs on n1k1 as-is. |
| **PLAN** | ~8 | ~471 | Correct results, but some assert on index usage / EXPLAIN / ADVISE plan shape. Partially portable (run query, drop plan assertions). |
| **INFRA** | ~30 | ~1,400 | Depend on unsupported features — not portable. |

INFRA thing-under-test = mutations (INSERT/UPDATE/DELETE/UPSERT/MERGE),
transactions, sequences (NEXTVAL), xattrs, `system:` keyspaces, CURL(),
JavaScript/UDFs, FTS `SEARCH()`, vector search, TTL, INFER, or UPDATE STATISTICS.

Top PURE categories: number, date, array, string, json, bitwise, typeconv,
comparison, conditional, case, meta, nav, arith functions; plus
where/select/alias/any/from query-shapes. Shared keyspaces (by usage): `orders`
(~19), `customer` (~14), `product` (~12), `purchase` (~9), `review` (2).

## Imported: constant-expression cases (no data)

Easiest, highest-confidence slice: **SELECT with no FROM** — constant
expressions like `SELECT add(1,10)`, `SELECT array_avg([1,2,3])`. Zero dataset,
deterministic, single-row by construction (no result-ordering ambiguity),
exercising the scalar/function evaluator directly. n1k1 delegates un-ported
functions to cbq, so these also guard the fallback path.

`test/suite/import_gsi_cases.py` extracts them into
`test/suite/json/default/cases/case_gsi_<category>.json`, picked up in **both**
modes — `TestSuiteCases` (interpreter) and `TestSuiteWithCompiler` (compiler
codegen).

**Result: 369 cases across 15 categories, all passing in both modes.** Suite
`PASS (results)` 661 → 1030 (`passFloor` bumped). Breakdown: number 86, date 85,
array 59, bitwise 44, string 35, json 29, typeconv 8, conditional 6, case 4,
comparison 4, meta 3, nav 3, arith 1, integers 1, select 1.

### Determinism rules (why the importer excludes things)

- **No wall-clock / random / id functions** (`NOW_*`, `CLOCK_*`, `RANDOM`,
  `UUID`, `NEWID`) — no reproducible expected value.
- **UTC pin** (`test/main_test.go` sets `time.Local = time.UTC`) — epoch-millis
  date functions (`MILLIS_TO_STR`, `MILLIS_TO_LOCAL`, `DATE_*_MILLIS`) format
  through the local zone, so without a fixed zone some cases fail under extreme
  zones. Also fixed **pre-existing latent flakiness** (`case_func_date.json`).
  Verified green across UTC, US, EU, India, Auckland, Kiritimati, GMT+12.
- **Skip DDL-text / UDF categories** (`sanitize_statement_function`,
  `extractddl`, `udf`) — not expression tests.

> **Robustness bug (separate):** `EXTRACTDDL(...)` *panics* the engine rather
> than erroring cleanly. The engine should never panic on an unsupported function.

## Imported: data-backed categories (isolated gsi corpus)

FROM-based categories need datasets. The fork's shared `orders` keyspace would
**collide** with n1k1's existing `orders` (added docs would change default-corpus
results), so fork data lives in a **separate corpus root**, `test/suite/json-gsi/`,
with its own tests.

### Import mechanism

`test/suite/import_gsi_data_cases.py` transforms each `insert.json`
`INSERT INTO <ks> (KEY,VALUE) VALUES("<key>", <obj>)` into file-datastore docs
`json-gsi/default/<ks>/<key>.json` and copies cases to
`json-gsi/default/cases/case_gsi_<cat>.json`. Docs merge into the shared
keyspaces; keys are `test_id`-suffixed to avoid collision, and each case's
`WHERE test_id="..."` scopes it.

Sizing (fork packs ~100 docs per INSERT statement):

- Moderate keyspaces (`customer`/`product`/`orders`) import **fully**.
- **Mega** keyspaces (`purchase`/`review` — 10,000 docs each) are impractical for
  a one-file-per-doc, no-index corpus, so keep a light sample (first doc per
  INSERT).
- Any doc a case references directly (`referenced_keys`) is always imported —
  needed for `USE KEYS "k"`.

**17 categories imported:** ~7,150 docs, ~493 cases.
string/number/array/obj/json/comp/conditional/case/typeconv/select/where/alias/
any/from/order/key/meta functions.

### Harness (DRY refactor)

Interp and compiler suite bodies share root-parameterized helpers —
`runSuiteCases(root, expectedNonPass, groupWhy, passFloor, skipRun)` and
`runSuiteCompiler(root, outFile, funcPrefix, setupCall)` — so
`TestSuiteCases`/`TestGsiSuiteCases` and
`TestSuiteWithCompiler`/`TestGsiSuiteWithCompiler` share one implementation.
`compiledSuiteStore` is root-keyed; `SetupCompiledGsiSuite` runs generated gsi
islands against the gsi store; the gsi generated file uses a `TestGeneratedGsiFS_`
prefix to coexist with the default one in `test/tmp/` (`generated_by_*_test.go`).

### Results

The gsi corpus passes in both modes with no panics (`gsiPassFloor=833`, bumped as
coverage grows; the window and subquery categories have since been imported too).
`USE KEYS` / `USE PRIMARY KEYS` (incl. array / `ARRAY … FOR` / `FIRST … FOR` /
`||` key exprs and `UNNEST`) work.

The remaining non-passes are enumerated in `gsiExpectedNonPass`
(`test/suite_gsi_test.go`) with a per-group rationale in `gsiGroupWhy` — each an
explicit regression-guarded gap, none of which panic. The current groups:

| Group | Reason |
|---|---|
| `mega-order-limit`, `fork-data-missing` | Depend on the full mega `purchase`/`review` datasets or fork-global setup docs our light-sample merged corpus doesn't carry, so `ORDER BY … LIMIT` / `USE KEYS` top-N can't match cbq. |
| `order-agg` | `ORDER BY` an aggregate nested in a larger expr under a `.*`-spread projection (no projected column to bind). |
| `results-differ`, `window-results-differ` | cbq STDDEV/VARIANCE quirks (single-element VAR_SAMP=0; numeric RANGE frame over non-numeric ORDER BY) n1k1 declines to match. |
| `window-nondeterministic`, `nondeterministic` | Frame-position / aggregation-order picks an implementation-defined row within a tied group; matches cbq today but isn't guaranteed, so tracked as non-pass. |
| `prepared` | Mixed-type / parameterized IN-list over a GSI index scan yields a different row set (a GSI index-scan limitation, not a PREPARE/EXECUTE one — those are supported). |

`gsiSkipRun` additionally names cases n1k1 can parse/plan but must not execute
(e.g. `subqexp[1]` would run an O(N²) unguarded correlated scan).

## Building the gsi suite in a fresh worktree

A fresh worktree can't load the module graph: go.mod requires several
**placeholder EE modules** at the `v0.0.0-0001…` non-version, whose go.mod files
exist only in Couchbase's internal repo-sync tree. None are imported in the CE
build, but the graph loader still demands them. Bootstrap = point each at an
empty local stub module via an **uncommitted** `replace`, regenerate gitignored
`intermed/`, and create gitignored `test/tmp/`.

Recipe (verified 2026-07; run from the NEW worktree's repo root):

```sh
# Modules to stub = go.mod's placeholder requires. As of 2026-07, SEVEN:
EE="cbgt query-ee regulator eventing-ee gocbcrypto hebrew n1fty"
#   grep -E '00010101000000-000000000000' go.mod | awk '{print $1}'   # list fresh

# Create empty stub modules somewhere persistent, then replace-point go.mod at them.
STUBS="$PWD/../ee-stubs"           # or $CLAUDE_JOB_DIR/tmp/ee-stubs for an agent
GOVER=$(grep -E '^go [0-9]' go.mod | awk '{print $2}')
for m in $EE; do
  mkdir -p "$STUBS/$m"
  printf 'module github.com/couchbase/%s\n\ngo %s\n' "$m" "$GOVER" > "$STUBS/$m/go.mod"
done
# Uncommitted local-path replaces:
for m in $EE; do echo "replace github.com/couchbase/$m => $STUBS/$m"; done >> go.mod

# Regenerate intermed/, make test/tmp/ (both gitignored), then verify the build.
mkdir -p test/tmp
go build ./cmd/intermed_build/ && ./intermed_build
CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go build -tags n1ql ./glue/... ./test/...
```

Gotchas:

- **Base the worktree on LOCAL master, not origin.** `git worktree add <path> -b
  <branch> master` branches from local `master` (ahead of `origin/master`); a
  tool default of `origin/<default>` silently drops unpushed commits.
- Stub go.mod needs only `module <path>` + a `go <ver>` line — no `.go` files.
- `test/tmp/` is shared + gitignored; concurrent worktrees running the compiler
  generators clobber it. `mkdir -p test/tmp` before regenerating; don't trust it
  across a context switch.
- TODO: a committed `make bootstrap` / `go.work` to automate this.

## Guidance and lessons

- **The suite harness is the right home** for data-driven SQL++ cases: handles
  `{statements, results}`, `{statements, error}`, `errorCode`, `warningCode`,
  `matchStatements`, runs both modes, gates regressions via `expectedNonPass` +
  `passFloor`. Prefer it over bespoke `TestFooBar()` funcs; reserve Go unit tests
  for what the suite can't express — the differential-vs-cbq expression tests in
  `glue/expr_test.go`, or engine-internal harness tests.
- **Keep the suite host-independent.** Pin TZ; exclude wall-clock/random/id;
  prefer `ORDER BY` or single-row cases.
- **Provenance:** `case_gsi_*.json` are regenerable via `import_gsi_cases.py`;
  re-run after a fork bump.

## Verifying the compiler path

The compiler is validated in **two phases, both mandatory**:

1. `go test -run '…WithCompiler' ./test` — runs the generators (`emit.OpToLines`
   over each Op tree), writing Go into gitignored `test/tmp/`. Catches
   *generation-time* faults (a generator that panics).
2. `cd test/tmp && go fmt && go test ./test/tmp` — **compiles and runs** that
   code, whose `TestGeneratedN` funcs execute the compiled query and compare
   results. Only this catches *compiled-code* faults: undefined variables (won't
   build) and wrong per-row output.

The `make test-compiler` / `test-suite` / `test-suite-all` targets chain both in
order. **Phase 1 alone (or a bare `go test ./test/…`) is false confidence** — a
whole class of codegen bugs lives only in phase 2. That is how a broken
`emitCaptured` operand-capture in native `and`/`or` (and older `nullif`) shipped
with phase 1 green.

### Two traps that make green history meaningless for a newly-native op

- **Interpreter-differential ≠ compiled coverage.** The differential tests
  (`glue/expr_test.go`, `nativeEval → engine.MakeExprFunc`) exercise only the
  **interpreter** native path — the `and`/`or` truth-table differential passed
  while the compiled path didn't build.
- **Compiled coverage is incidental, and the fallback masks gaps.** The
  `…WithCompiler` generators only compile+run expressions in *convertible* cases,
  and the cbq `exprTree` fallback has a working compiled path. A predicate using a
  non-native op rides the fallback and looks fine; adding that op to
  `OptimizableFuncs` routes the query onto native codegen **no prior run ever
  generated**. Green `make test-all` history says nothing about an op's native
  compiled path until a convertible case reaches it.

### Mitigation — test each native op's compiled path directly

`TestCasesSimple` (`test/cases.go`) holds hand-built Op trees whose `Params` are
native expr param trees *verbatim*, bypassing the optimizer; they run in BOTH
modes (`TestCasesSimpleWithCompiler`). The `naryProjectCase` helper adds one per
combining op (`and`/`or`/`nullif`/…), making compiled coverage explicit and
optimizer-independent. Next: a guard test enumerating `OptimizableFuncs` that
fails on any entry with no compiled-mode case.

## Concurrency testing under `-race`

Go's race detector earns its keep as a periodic check.

### What it guards

- **Concurrent actors.** `base.Stage` spawns `NumActors` goroutines (UNION ALL
  runs one contributor per child; parallel scans / `GROUP BY` shards on the
  roadmap). UNION ALL sharing one `*GlueContext` across branch actors has bitten
  us three times, each caught by `-race`:
  - `subq-cache-race`: the subquery-compile cache (a map) written concurrently —
    fixed with a mutex.
  - `corrParent`/`withScope`: `EvaluateSubquery` save/set/restores the
    correlated-subquery scope on the context, raced by two branches. Fixed at the
    concurrency boundary — `base.Vars.ChainExtend` clones `Temps[0]` (the context)
    per actor via `base.ChainCloner` (`GlueContext.ChainClone`): each branch gets
    its own scope, sharing caches/error-sink through a `root` pointer. Lesson:
    **per-actor mutable state must live behind `ChainExtend`, not the shared
    context.**
  - cbq's global `AnnotatedValue` pool (`util.LocklessPool`) incremented get/put
    cursors non-atomically; concurrent actors made it a real race. Fixed in the
    fork (`glue/patches/patch-03`, cursors made atomic).
- **Shared, lock-free-by-design state.** The stats counter core
  (`DESIGN-stats.md`) is a flat `[]int64` bumped *without atomics*, justified by
  single-writer-per-slot. `-race` verifies that invariant: two goroutines writing
  the same slot (e.g. same-op parallelism without the per-`(op,actor)` split)
  reports a write/write race instead of a silently wrong count.
- **The borrowed-slice / copy-on-retain contract.** `YieldVals` requires
  consumers to copy inputs they keep (`base/base.go`; the scan reuses one per-row
  buffer, `Stage` deep-copies at actor boundaries). A violation is *silent
  corruption* today; `-race` flags it when one goroutine reads a borrowed slice
  while another overwrites the buffer — hardening retention sites before the mmap
  read path (`DESIGN-data.md`), whose same-bug failure mode is a delayed SIGBUS.

### How to run it

In a bootstrapped worktree (so `./test` builds):

```sh
CGO_ENABLED=1 go test -race -tags n1ql -count=1 ./engine/ ./base/ ./glue/ ./test/
```

- The detector only reports races that *actually occur*, so exercise concurrent
  paths — UNION ALL, subqueries, and (when they land) parallel scans / `GROUP BY`
  shards.
- Needs `cgo` and ~2–10× time/memory, so treat it as a **periodic / pre-merge /
  CI** gate. Both suites benefit; the compiled path's generated goroutines too.
- `-race` catches unsynchronized shared access, **not** use-after-unmap (a SIGBUS
  on a normal run). Complementary: `-race` for shared-slot / borrowed-buffer
  invariants, ordinary runs (and mmap experiments) for lifetime faults.

## Follow-ups and open questions

- More PURE categories (aggregate/window/etc.).
- **PLAN** categories: import query cases, drop plan/EXPLAIN-shape assertions.
- Investigate `results-differ` for real fixes vs. tie-broken-LIMIT noise.
- Guard test enumerating `OptimizableFuncs`, failing on entries with no
  compiled-mode case.
- Committed `make bootstrap` / `go.work` for fresh-worktree setup.
- Fix `EXTRACTDDL(...)` panicking rather than erroring cleanly.
