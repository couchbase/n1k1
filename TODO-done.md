# Done

Gist only -- details live in commit messages, README, and code comments.

## 2026/06 -- build modernization (dusted off after ~5 years)
- Go 1.17 -> 1.25; GOTOOLCHAIN=auto fetches it.
- go mod: dropped local-symlink replaces, pinned ~25 couchbase modules
  to real versions. No more dependency on a local couchbase-server tree.
  - Recipe: GOPRIVATE=github.com/couchbase/* + replace => realversion to
    kill the v0.0.0-00010101 placeholders. cbauth/gomemcached/cbft @ master.
- Core (root pkg, base/, intermed/, cmd/) builds + vets + tests clean.
  - Fixed real go vet issues (op_scan.go Errorf, op_window.go self-assign).
- N1QL engine layer (glue/ + test/) gated behind `//go:build n1ql`, so the
  default build is green. tmp/easy-to-read gated `//go:build ignore`.
- Makefile: default = core flow; `make n1ql` = deferred engine build.
- README: documents the whole recipe + the don't-`go mod tidy` warning.

## 2026/06 -- engine-layer feasibility investigation (no code change)
- Proved cgo is EASY: prebuilt libsigar in Couchbase Server.app + openssl@3
  via brew; wiring CGO_CFLAGS/LDFLAGS clears all sigar/openssl errors.
- Found the real blocker is the goyacc parser-gen gap in query/parser/n1ql
  (generated yyParse/yySymType not shipped by `go get`). See TODO.md.

## 2026/06 -- decouple work started (toward pure-Go CGO_ENABLED=0 binary)
- Stubbed query/system to pure-Go (glue/patches/query-system-stub.go.txt) -- sigar is
  pulled pervasively via query/memory<-query/tenant, so it must be stubbed.
- T1: dropped query/server from glue/exec.go (unused param).
- T2: dropped query/datastore/system from glue/stmt.go (Systemstore=nil).
- Corrected an over-claim: stub+T1+T2 is NOT pure-Go yet. cbft (via execution)
  is cgo (jemalloc) + cbgt->gosigar needs darwin cgo, so CGO_ENABLED=0 REQUIRES
  T3 (drop execution). Also glue/ has 2019->2026 query API drift to fix. See TODO.md.

## 2026/06 -- glue API drift fixed + T3 DONE (pure-Go engine achieved)
- Fixed glue/ 2019->2026 query API drift (plan.Visitor new methods, removed
  types, semantics/planner/Fetch/WriteJSON/attachment signature changes).
- T3: replaced query/execution.Context with glue.GlueContext (embeds
  expression.IndexContext; implements datastore.Context). Dropped execution ->
  the whole cbft/cbgt/indexing/n1fty/query-ee/gocbcrypto/eventing-ee subtree.
- RESULT: `CGO_ENABLED=0 go test -tags n1ql ./glue ./test` is GREEN, and the
  engine cross-compiles to linux/darwin/windows. 3 query patches needed
  (parser-gen, system stub, semchecker enterprise) -- see glue/patches/.

## 2026/06 -- local query fork wired in (reproducible build, staying local)
- Created sibling repo ../n1k1-query as a real git fork: main = pinned
  snapshot; n1k1-pure-go branch = base + the 3 patches as separate commits.
- go.mod: replace query => ../n1k1-query; pruned all sibling-module
  replaces (T3 dropped them) -- only the query replace remains.
- Verified: core default build green, n1ql tests green CGO_ENABLED=0, and a
  missing fork doesn't break the default build (query is n1ql-gated). GitHub
  push of the fork deferred (staying local for now). See [[TODO]].

## 2026/06 -- polish
- Renamed the fork dir query-n1k1-fork -> n1k1-query (shared n1k1- prefix);
  updated the go.mod replace + docs.
- README/Makefile: concise copy-paste build/test instructions; `make n1ql` now
  CGO_ENABLED=0 on ./glue ./test.
- Fixed `go test ./...`: the generated test/tmp compiler test now emits
  `//go:build n1ql` (it imports the gated glue), so the default build is clean.
- Split README.md -> DESIGN.md (internals/performance/idea-backlog moved out;
  README keeps intro, changelog, build/test, feature list).
- Fixed `make benchmark-expr-eq` (needs -tags n1ql + CGO_ENABLED=0; benchmarks
  live in the gated test/).

## 2026/06 -- fork published to GitHub (build no longer needs a local sibling)
- Fork pushed to github.com/couchbase/n1k1-query (main + n1k1-pure-go branches).
- go.mod swapped from `replace query => ../n1k1-query` to a version replace
  `=> github.com/couchbase/n1k1-query <pseudo-version>`. The fork keeps module
  path github.com/couchbase/query, so Go maps it to the query import path; n1k1
  builds with plain `go` (+ GOPRIVATE), verified independent of the local sibling.

## 2026/06 -- cleanup pass (one done, two investigated WON'T-FIX)
- Vestigial files: cpu.pprof, the intermed_build binary, and tmp/ are all
  gitignored (never committed) -- removed the local cruft + dead tmp/ symlinks;
  nothing to commit.
- WON'T-FIX, go.mod indirect block: can't `go mod tidy`. tidy considers
  couchbase's enterprise-tagged imports (query/planner -> query-ee, query/tenant
  -> regulator, ...) pinned at the v0.0.0-00010101 placeholder; giving the fork's
  placeholders real versions just cascades (cbft/n1fty -> cbgt@00010101, ...).
  The community build is fine -- module-graph pruning never compiles those, so
  the entries are harmless graph-mirror noise. Verified empirically, reverted.
- WON'T-FIX / moot, "re-pin to one consistent manifest snapshot": same root
  cause, and moot post-T3 (the drift-prone heavy modules aren't compiled; the
  versions that matter come from the fork's go.mod -- itself one snapshot).

## 2026/06 -- compiler: nested join-family ops (Futamura projection)
- Nested/chained UNNEST and UNNEST-feeding-a-JOIN now compile. They had failed
  with "lzErr redeclared in this block" when the generator fused two join-family
  ops into one Go block (lzErr was just the first of several function-scope vars
  to collide). Fix: wrap OpJoinNestedLoop's runtime body in `if LzScope { ... }`
  -- emitted as `if true { ... }`, a fresh block scope per fused instance, like
  every other nestable op -- with the compile-time setup hoisted above it.
  Dropped the hasNestedJoinFamily() exclusion from the suite compiler generator;
  the differential (compiled output vs interpreter oracle) stays green.

## 2026/06 -- star projection spread, SELECT path.* (conformance 627 -> 631)
- SELECT path.* must SPREAD the fields of an object value into the result row,
  and yield no fields (=> {}) when the value is not an object. n1k1 labeled the
  star term "." (whole-row) and projected the raw value, so a scalar prefix
  (details.format = "DVD") leaked the scalar instead of {}, and multiple stars
  collided ("Convert, v non-nil on '.'").
- Introduced a ".*" spread-merge label: VisitInitialProject emits ".*" for any
  star term (Result().Star()); ConvertVals merges that val's object fields into
  v (non-object => nothing, v forced to {}). Merge composes, so multiple stars
  and star-mixed-with-plain terms combine into one object.
- The ORDER-BY source-scope augmentation now applies only to plain `.["..."]`
  field-path projections (projAllFieldPaths), skipping "." / ".*" / "^" rows.
- Fixed case_select[7] (scalar.* => {}) and promoted [8]/[9]/[12] from
  UNSUPPORTED to PASS. Bare SELECT * (object) is unchanged. The lone remaining
  FAIL is the NOT-FIXABLE array_position(array_agg(...)) ordering case.

## 2026/06 -- ORDER BY a source field after projection (conformance 625 -> 627)
- SELECT dimensions ... ORDER BY dimensions.length: the plan is Order-above-
  InitialProject, and the planner qualifies the order key as source-relative
  ((catalog.dimensions).length). The project op had stripped the row to the
  projected columns, so the source-qualified key resolved to MISSING and didn't
  sort. ORDER BY can also reference projection aliases, so the order op needs
  BOTH scopes. VisitOrder now, when directly above a project, builds an
  augmented project (projected terms + pass-through of the source `.`-path doc
  columns via labelPath), sorts/pages over that union, then strips back to the
  projected columns. Faithful to query's AnnotatedValue (projected value +
  retained original).
- VisitOffset/VisitLimit fold paging into the inner order op through the strip
  project (orderFoldTarget), so a separate plan.Limit/Offset doesn't spawn a
  redundant outer order wrapper (which would double-apply with OFFSET).

## 2026/06 -- META(alias).id document metadata (conformance 623 -> 625)
- META(alias).id yielded {} (MISSING): the `^id` attachment was set on the
  whole row's AnnotatedValue, but META(alias) evaluates its operand first, so
  it needs the metadata on the *keyspace sub-value*, not the outer row. The
  labels already emit each doc as a `.`-path immediately followed by its `^id`
  (see conv.go), so ConvertVals.Convert now attaches `^id` to that preceding
  doc sub-value (wraps it as an AnnotatedValue + SetId). META(u)/META(o) in a
  join resolve independently this way. Fixed the bare-projection + ANY-WHERE
  cases (case_by_id, case_innerjoin).
- Second bug, join-side keys: DatastoreFetch appended the incoming key val to
  `^id` verbatim, but join ON KEYS array keys arrive via ArrayYield, which
  strips JSON string quotes -- so `^id` was an unquoted string read downstream
  as BINARY ("<binary (13 b)>"). Fetch now re-encodes the parsed key with
  strconv.AppendQuote so `^id` is always canonical JSON.

## 2026/06 -- WHERE/ON truth-value semantics (conformance 622 -> 623)
- N1QL truth: a value passes a WHERE/ON condition if value.Truth() is true --
  i.e. unless it is MISSING, NULL, false, 0/NaN, or an empty string/array/object.
  n1k1's filter/join used ValEqualTrue (passes only the literal `true`), so a
  FIRST/ANY comprehension yielding a non-empty STRING (e.g. "Euros Lyn") was
  dropped. Added base.ValTruthy and used it in op_filter.go + op_join_nl.go
  (regenerated intermed). Fixed the FIRST .. FOR .. WHEN case.
- NOT-FIXABLE left behind: array_position(array_agg(array_min(...))) -- the
  result is the position of a value in an ARRAY_AGG, whose element order N1QL
  leaves undefined; n1k1's scan order differs from the corpus (same multiset).

## 2026/06 -- SELECT RAW + array-multiset comparison (conformance 617 -> 622)
- SELECT RAW / ELEMENT: VisitInitialProject labels the projected value "." (the
  whole row) when Projection().Raw(), so the value is not wrapped under an alias.
  Fixed the object_filter cases (the func was already correct). 617 -> 619.
- TestFilestoreCases now compares arrays as multisets too (deepNormalize sorts
  array elements + object keys recursively) -- ARRAY_AGG order is undefined and
  the corpus order reflects upstream scan order. Fixed 3 ARRAY_AGG cases. 619 -> 622.

## 2026/06 -- UNNEST pushed-down filter (conformance 613 -> 617)
- WHERE predicates on an UNNEST alias (e.g. UNNEST x AS child WHERE child.y=...)
  are pushed by the planner INTO the Unnest operator (plan.Unnest.Filter()), not
  emitted as a separate Filter -- so n1k1 dropped them and returned unfiltered
  rows. VisitUnnest now chains a filter op on the unnested output when
  o.Filter() != nil. Fixed the SELECT child / * / contact.* over UNNEST cluster.

## 2026/06 -- COUNT(*) panic + DISTINCT aggregates (conformance 605 -> 613)
- COUNT(*) nil-operand panic: VisitFinalGroup projected agg.Operands()[0], nil
  for COUNT(*), which panicked in the expr evaluator. Now COUNT(*) projects a
  constant so the agg sees a non-MISSING value per row. 605 -> 611.
- DISTINCT aggregates: conv emits count_distinct / array_agg_distinct aggCalc
  names when agg.Distinct(); base.AggCountDistinct / AggArrayAggDistinct share
  aggDistinctUpdate (accumulate unique canonical values). 611 -> 613.
  (A couple ARRAY_AGG cases still differ only in array element ORDER vs
  upstream -- a harness comparison nuance, not a dedup bug.)

## 2026/06 -- no-GROUP-BY aggregation + ARRAY_AGG (conformance 594 -> 605)
- Fixed op_group.go: it gated the whole aggregation on len(groupExprs) > 0, so
  an aggregate with no GROUP BY (empty group keys) produced 0 rows. Now the
  group key is always computed via ValsEncodeCanonical -- empty groupExprs ->
  the canonical "0 vals" key, i.e. one constant group / one output row.
  (Fixes SUM/AVG/MIN/MAX/ARRAY_AGG without GROUP BY.) Regenerated intermed.
- Implemented base.AggArrayAgg (ARRAY_AGG): accumulates the group's non-MISSING
  values; Result is their JSON array. Registered in AggCatalog.
- array_avg/array_count/array_contains(array_agg(...)) etc. now pass.

## 2026/06 -- MISSING-projection omit + harness key-order (conformance 530 -> 594)
- Real fix: glue.ExprTree now yields an empty val (base.ValMissing) when a
  projected expression evaluates to MISSING, so ConvertVals omits the field
  ({} not {"k":null}) -- matching N1QL. (NULL is still kept.) This fixed the
  ARRAY .. FOR comprehensions, array slicing (children[0:2]), and many other
  projections over docs with absent fields. 530 -> 572.
- Harness fix: TestFilestoreCases now canonicalizes object key order on BOTH
  sides (recursive unmarshal->marshal); n1k1 doesn't sort keys, so ~22 cases
  were false-negatives. 572 -> 594. Pass-floor bumped to 594.

## 2026/06 -- fixed keyspace-alias projection (conformance 358 -> 530)
- Root cause of the ANY..SATISFIES failures (and broad field-projection
  breakage): conv.go labeled fetch/join/unnest docs with KeyspaceTerm.As()
  (the EXPLICIT alias, empty for an unaliased `FROM contacts`), but query's
  planner qualifies expressions with the EFFECTIVE alias (`contacts.name`).
  So `contacts.name` couldn't resolve against a "."-labeled doc -> empty rows.
- Fix: use Term().Alias() (effective: explicit, else keyspace name). This
  fixes field projection AND makes SELECT * wrap under the keyspace alias
  ({"contacts": {...}}), matching N1QL.
- Conformance jumped 358 -> 530 / 672; the ANY/EVERY..SATISFIES bucket is
  essentially resolved. Updated the 4 existing SELECT * tests for the (now
  conformant) wrapped output and bumped the pass-floor to 530.

## 2026/06 -- upstream filestore conformance corpus runs against n1k1
- Vendored the couchbase/query "filestore" test corpus (test/filestore/ --
  24 datasets / 510 docs / 46 case files of {statements, results}).
- New TestFilestoreCases harness (test/filestore_test.go) parses+plans+converts+
  executes each statement through n1k1's own ops and compares (multiset
  canonical-JSON) to the expected results, classifying pass / fail / unsupported.
- 358 of ~672 runnable cases PASS (the rest: ANY..SATISFIES result mismatches,
  EXPLAIN / index-union-scan unsupported, a couple UNNEST+GROUP-BY panics).
  A pass-floor assertion guards against regressions; ratchet up as coverage grows.

## 2026/06 -- un-skipped the pre-existing SKIP tests (all pass)
- The 6 SKIP-prefixed tests from the 2021 CB 6.5->7 breakage (UNNEST x3,
  GROUP BY SUM x2 = "TermerPanic"; array-as-FROM + WHERE = "Results3Not1")
  now PASS as-is -- the breakage was resolved by the modernization + T3 decouple
  + glue API-drift fixes. Just stripped the SKIP<reason>_ prefixes; no code
  change needed. No SKIP markers remain in the tree.

## 2026/06 -- SQL++ features: LET / LETTING, WITH (basic), EXPLAIN
- LET / LETTING: glue VisitLet stacks one pass-through "project" per binding
  (so a later binding can reference an earlier one: LET x=1, y=x+1), each
  appending a .["<var>"] column; SELECT * strips the binding names
  (VisitInitialProject + stripBindingNames/OBJECT_REMOVE) so they don't leak.
  LETTING (GROUP BY scope) works over aggregates via the group's ^aggregates
  attachment. Tests: test/cases_test.go.
- WITH (basic, non-recursive): VisitWith visits the child; SELECT * doesn't leak
  the WITH name. WITH-var-as-FROM-datasource is still TODO (see TODO.md
  recursive-CTE roadmap).
- EXPLAIN: glue.Session intercepts the plan.Explain op (under the top Authorize)
  and emits query's plan JSON ({"text","plan"}) -- no conv/exec needed. Matches
  the vendored corpus exactly (the fork's planner reproduces those plans), and
  works even for queries n1k1 can't execute (it only plans). +10 suite cases
  (651 -> 661).

## 2026/06 -- expression subqueries (correlated + uncorrelated)
- GlueContext became an algebra.Context (Datastore / NamedArg / PositionalArg /
  EvaluateSubquery), so query's algebra.Subquery.Evaluate can call back into
  n1k1. EvaluateSubquery plans the sub-SELECT on demand, convs it, runs it on
  n1k1's engine, and returns the rows as an array value. Store.PlanStatementQP
  exposes the whole QueryPlan.
- Correlated: EvaluateSubquery stashes the outer row on GlueContext.corrParent
  (save/restore for nesting); ExprTree wraps each sub-row as a value.ScopeValue
  over corrParent so outer identifiers fall through, and skips the optimized expr
  path (which only knows the sub-op's own labels).
- Works: N IN (SELECT ...), (SELECT ...) in a projection, ARRAY_LENGTH((SELECT
  ...)), WHERE ... IN (correlated/uncorrelated SELECT). Tests: test/cases_test.go.
- Known small gaps -- grep TODO(correlated-subquery) / TODO(subquery-perf) /
  TODO(subquery-plan) in glue/: SELECT * in a correlated subquery can leak outer
  fields; sub-row META/id not preserved through the scope wrap; per-outer-row
  re-execution isn't amortized; sub-SELECTs are re-planned standalone.

## 2026/06 -- cmd/n1k1 CLI + REPL
- Single pure-Go binary running SQL++ over a file datastore. `make cli`
  (install-cli to install). -c / -f / stdin-pipe / REPL (read-until-';',
  multi-line continuation). Dot-commands: .tables/.keyspaces, .schema, .mode,
  .timer, .explain, .maxrows, .maxwidth, .read, .output, .help, .quit.
- Output modes: box (default at a TTY) / jsonlines (default piped) / json / csv
  / markdown / line / list. Renderers live in the reusable, tag-free `cmd`
  package (unit-tested under the default `make test`).
- REPL history + line editing via the pure-Go peterh/liner (~/.n1k1_history);
  tasteful TTY-only colors + emoji status markers (NO_COLOR honored).
- The engine pipeline is glue.Session, extracted from the test harness and
  shared with it (so there's one end-to-end path). See DESIGN-cli.md.

## 2026/06 -- benchmarks (DESIGN-benchmark.md) + test reorg
- test/benchmark/: Phase 1 (intrinsic: garbage avoidance holds -- allocs/op flat
  1K->30M rows; GROUP BY spill point ~4000-5000 distinct keys; graceful
  degradation) + Phase 2 (interpreted vs compiled: fusion cuts allocs ~30-40%,
  wall-time shape-dependent). Phase 3 (vs couchbase/query) BLOCKED in a stock
  env -- see DESIGN-benchmark.md section 10 for the blockers + a future-run recipe.
- Test files reorganized: data-driven test/cases_test.go (local SQL++ cases),
  test/base_test.go, benchmarks consolidated under test/benchmark/, and a shared
  test/emit package for the compiler codegen helpers. `make bench*` documented in
  README.

## 2026/06 -- CTE as a FROM data source (WITH r AS (...) ... FROM r)
- New "expr-scan" glue op (glue/datastore.go, routed via ExecOpEx like datastore
  ops): evaluates a FROM expression at runtime and yields one row per array
  element under the alias label. It handles FROM [array], FROM (subquery) via a
  CTE, and -- reusing GlueContext.EvaluateSubquery -- a subquery-bound CTE that
  runs on the engine + datastore.
- glue VisitWith records each CTE alias -> its binding expression;
  VisitExpressionScan inlines a `FROM cte` (an ExpressionScan over the bare
  identifier) to that binding expression, passed to expr-scan via a vars.Temps
  slot. Replaces the old convert-time temp-yield-var path (which couldn't see a
  CTE binding -- "nil item").
- Fixed an ORDER-BY regression the switch exposed: expr-scan (like scans /
  temp-yield-var) must call yieldErr(nil) at end-of-stream, or a buffering parent
  (ORDER BY drains its heap on yieldErr(nil)) emits nothing.
- Works in the interpreter AND the compiler (expr-scan bakes to a glue.DatastoreOp
  island; the live expression is supplied by SetupCompiledData's re-conv).
  Tests: test/cases.go WithCteFromConstArray / WithCteFromSubquery, run by both
  TestQueryCases and TestQueryCasesWithCompiler. test-all green (suite 661/671).
- Remaining for WITH RECURSIVE: the fixpoint driver. Also: direct
  `FROM (subquery) AS x` (not via WITH) still hits plan.Alias (VisitAlias is NA).

## 2026/06 -- WITH RECURSIVE (the fixpoint)
- New "with-recursive" glue op (glue/recursive.go, via ExecOpEx): runs a
  recursive CTE's fixpoint and yields the accumulated rows under the alias.
  Mirrors query's execution/with.go -- eval the anchor (w.Expression()), then
  repeatedly eval the step (w.RecursiveExpression()) with the CTE alias bound to
  the latest working set, UNION-deduping and accumulating, until the step yields
  nothing.
- The step's `FROM <cte>` is a correlated ExpressionScan over the identifier; the
  fixpoint stashes {alias: workingSet} on GlueContext.corrParent, and expr-scan
  evaluates its FROM identifier against corrParent (so `FROM r` reads the latest
  working set). VisitExpressionScan now allows a correlated *identifier* FROM
  (the CTE case) while still NAing a correlated *subquery* FROM.
- Safety: an implicit depth cap (100) + doc cap (10000) bound the loop even
  without a termination predicate (matches query's implicit caps). Not yet
  honored: the CYCLE clause (w.CycleFields) and explicit OPTIONS (w.Config).
- Works in interpreter AND compiler (with-recursive bakes to a glue.DatastoreOp
  island; the live binding comes from SetupCompiledData's re-conv). Tests:
  test/cases.go RecursiveCount / RecursiveSum, run by both TestQueryCases and
  TestQueryCasesWithCompiler (emitted 20, skipped 0). Verified UNION/UNION ALL,
  downstream aggregation (SUM/COUNT), dedup convergence, and the safety cap
  (a non-terminating UNION ALL stops at depth 100, no hang). test-all green
  (suite 661 results / 671 total, PANIC 0).
