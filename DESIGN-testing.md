# Growing n1k1's SQL++ test battery from the cbq corpus

## Why

The `couchbase/query` (cbq / n1k1-query) fork carries a large, mature SQL++ test
corpus that n1k1 wasn't tapping. Much of it is infrastructure-bound (cluster
auth, GSI, transactions, FTS), but a substantial slice is **pure SQL++** — the
same language n1k1 runs by reusing cbq's parser + planner and evaluating
expressions (natively where ported, otherwise via the embedded cbq evaluator).
Importing that slice gives us a much broader battery to gain confidence that
n1k1 behaves byte-for-byte like cbq.

This doc records the scan of that corpus, what's been imported, how, and the
roadmap for the rest.

## The source corpus

`<fork>/test/gsi/test_cases/<category>/` — 68 categories, ~2,352 case statements.
Each category dir has `case_*.json` (arrays of `{statements, results}` or
`{statements, error}`) plus usually an `insert.json` that seeds data via
`INSERT INTO <keyspace> (KEY,VALUE) VALUES("<key>", {...})`, tagged with a
`test_id` field so many categories can share the `customer`/`orders`/`product`/
`purchase`/`review` keyspaces. Upstream these run as live-cluster integration
tests (`GSI_TEST=true`); the JSON is static data we can transform.

### Portability scan (all 68 categories triaged)

| Bucket | Categories | ~Statements | Meaning |
|---|---|---|---|
| **PURE** | ~30 | ~915 | Pure SQL++ SELECT over data + scalar/aggregate/window expressions. Runs on n1k1 as-is. |
| **PLAN** | ~8 | ~471 | Correct *results*, but some cases assert on index usage / EXPLAIN / ADVISE plan shape. Partially portable (run the query, drop plan assertions). |
| **INFRA** | ~30 | ~1,400 | Depend on unsupported features — not portable. |

INFRA = the thing-under-test is mutations (INSERT/UPDATE/DELETE/UPSERT/MERGE),
transactions, sequences (NEXTVAL), xattrs, `system:` keyspaces, CURL(),
JavaScript/UDFs, FTS `SEARCH()`, vector search, TTL, INFER, or UPDATE STATISTICS.

Best PURE function categories (by count): number, date, array, string, json,
bitwise, typeconv, comparison, conditional, case, meta, nav, arith functions;
plus where/select/alias/any/from/from-clause query-shape categories.

Shared keyspaces and who seeds them: `orders` (largest, ~19 categories),
`customer` (~14), `product` (~12), `purchase` (~9), `review` (2).

## What's imported so far — constant-expression cases (no data needed)

The easiest, highest-confidence slice: **SELECT queries with no FROM clause** —
constant expressions like `SELECT add(1,10)`, `SELECT bitand(1,3,5,8)`,
`SELECT array_avg([1,2,3])`, `SELECT date_add_str("2006-01-02",1,"year")`. They
need **zero dataset**, are deterministic, and exercise the scalar/function
evaluator directly. n1k1 evaluates natively what it has ported and delegates the
rest to cbq, so these also guard the fallback path.

`test/suite/import_gsi_cases.py` extracts them into
`test/suite/json/default/cases/case_gsi_<category>.json`, where the existing
suite harness picks them up in **both** modes — `TestSuiteCases` (interpreter)
and `TestSuiteWithCompiler` (compiler codegen).

**Result: 369 cases across 15 categories, all passing in both modes.** Suite
`PASS (results)` went 661 → 1030 (`passFloor` bumped to match). Breakdown:
number 86, date 85, array 59, bitwise 44, string 35, json 29, typeconv 8,
conditional 6, case 4, comparison 4, meta 3, nav 3, arith 1, integers 1,
select 1.

### Determinism rules (why the importer excludes things)

- **No wall-clock / random / id functions** (`NOW_*`, `CLOCK_*`, `RANDOM`,
  `UUID`, `NEWID`) — no reproducible expected value.
- **UTC pin** (`test/main_test.go` sets `time.Local = time.UTC`) — epoch-millis
  date functions (`MILLIS_TO_STR`, `MILLIS_TO_LOCAL`, the calendar
  `DATE_*_MILLIS` family) format through the local zone. Without a fixed zone a
  handful of date cases pass in UTC/US/EU but fail under extreme zones
  (Pacific/Auckland +13). This also fixed a **pre-existing latent flakiness** in
  the original corpus (`case_func_date.json`). Verified green across UTC, US,
  EU, India, Auckland, Kiritimati, GMT+12.
- **Skip DDL-text / UDF categories** (`sanitize_statement_function`,
  `extractddl`, `udf`) — not expression tests. (Note: `EXTRACTDDL(...)`
  currently *panics* the engine rather than erroring cleanly — a separate
  robustness bug worth fixing; the engine should never panic on an unsupported
  function.)

## Data-backed categories (DONE — isolated gsi corpus)

The FROM-based function/query categories need their datasets loaded. Because the
fork's shared `orders` keyspace would **collide** with n1k1's existing `orders`
(adding docs would change results of the default corpus's own cases), the fork
data lives in a **separate corpus root**, `test/suite/json-gsi/`, with its own
tests. Implemented:

- **`test/suite/import_gsi_data_cases.py`** transforms each category's
  `insert.json` `INSERT INTO <ks> (KEY,VALUE) VALUES("<key>", <obj>)` into a
  file-datastore doc `json-gsi/default/<ks>/<key>.json` (brace-matched JSON
  payload) and copies its `{statements,results|error}` cases to
  `json-gsi/default/cases/case_gsi_<cat>.json`. Docs from all categories merge
  into the shared keyspaces (`customer`/`orders`/`product`/`purchase`/`review`);
  keys are `test_id`-suffixed so they don't collide and each case's
  `WHERE test_id="..."` scopes it. The fork packs ~100 docs per INSERT statement.
  Moderate keyspaces (`customer`/`product`/`orders`) import **fully** (all VALUES
  tuples). The **mega** keyspaces (`purchase`/`review` — 10,000 docs each) are
  impractical for a one-file-per-doc, no-index corpus (repo bloat + minutes-long
  primary scans), so only a light sample is kept: the first doc of each INSERT
  statement. Either way, a doc whose KEY a case references directly
  (`referenced_keys`) is always imported — needed for `USE KEYS "k"`, which
  fetches an exact doc rather than scanning.
- **17 categories imported:** ~7,150 docs, ~493 cases. string/number/array/obj/
  json/comp/conditional/case/typeconv/select/where/alias/any/from/order/key/meta
  functions.
- **Harness:** the interp and compiler suite bodies were factored into
  root-parameterized helpers — `runSuiteCases(root, expectedNonPass, groupWhy,
  passFloor)` and `runSuiteCompiler(root, outFile, funcPrefix, setupCall)` — so
  `TestSuiteCases`/`TestGsiSuiteCases` and `TestSuiteWithCompiler`/
  `TestGsiSuiteWithCompiler` share one implementation (DRY). `compiledSuiteStore`
  is now root-keyed; `SetupCompiledGsiSuite` runs generated gsi islands against
  the gsi store; the gsi generated file uses a `TestGeneratedGsiFS_` prefix so it
  coexists with the default one in `test/tmp/`.
- **Results:** **465 / 493 pass in interp mode** (`gsiPassFloor=465`), green in
  compiler mode, no panics. `USE KEYS` / `USE PRIMARY KEYS` (incl. array/`ARRAY
  … FOR`/`FIRST … FOR`/`||` key exprs and `UNNEST`) work. Remaining
  `gsiExpectedNonPass` groups: `any-every` and comp `results-differ` (need the
  full mega `purchase`/`review` datasets — the aggregate/`ORDER BY … LIMIT` cases
  can't match cbq on the light sample), `json-funcs` (`JSON_ENCODE`(MISSING)
  semantics), `obj-funcs` (`ORDER BY` on array/object-valued keys). Each is an
  explicit, regression-guarded gap.

**Follow-ups:** more PURE categories (aggregate/window/etc.); **PLAN** categories
(import query cases, drop plan/EXPLAIN-shape assertions since n1k1 does primary
scan + filter); and investigating the `results-differ` bucket for real fixes vs.
tie-broken-LIMIT noise.

### Building the gsi suite in a fresh worktree

A fresh git worktree can't load the module graph out of the box: the repo's
go.mod requires several **placeholder EE modules** (`cbgt`, `query-ee`,
`regulator`, `eventing-ee`, `gocbcrypto`, `hebrew`, `n1fty`, `plasma`) at the
`v0.0.0-0001…` non-version, whose go.mod files exist only in Couchbase's internal
repo-sync tree. None are imported in the CE build (`go mod why`: not needed), but
the graph loader still demands them. To bootstrap a worktree (all local /
uncommitted): add a local-path `replace` for each to an empty stub module, then
regenerate the gitignored `intermed/` (`go build ./cmd/intermed_build/ &&
./intermed_build`) and create the gitignored `test/tmp/` dir. (A committed `make
bootstrap`/`go.work` to automate this is a worthwhile future convenience.)

## Guidance / lessons

- **The suite harness is the right home** for data-driven SQL++ cases: it already
  handles `{statements, results}`, `{statements, error}`, `errorCode`,
  `warningCode`, and `matchStatements`, runs both interpreter and compiler, and
  gates regressions via `expectedNonPass` + `passFloor`. Prefer adding cases here
  over bespoke `TestFooBar()` funcs. Reserve Go unit tests for things the suite
  can't express — e.g. the byte-level differential-vs-cbq expression tests in
  `glue/expr_arith_diff_test.go`, or engine-internal harness tests.
- **Keep the suite host-independent.** Pin TZ; exclude wall-clock/random/id;
  prefer cases with `ORDER BY` or single-row results (no result-ordering
  ambiguity — constant no-FROM cases are single-row by construction).
- **Provenance matters.** `case_gsi_*.json` are regenerable from the fork via
  `import_gsi_cases.py`; re-run it after a fork bump to refresh.
