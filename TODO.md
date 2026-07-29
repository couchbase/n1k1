# TODO

Forward-looking only. What's DONE is in TODO-done.md; internals + the
design are in DESIGN.md; build/test commands are in README.md.

Status: a working pure-Go SQL++ engine (CGO_ENABLED=0, cross-compiles;
also GOOS=js/wasm in-browser). Beyond core query it now has temporal ASOF
correlation (streaming merge-join / merge-scan), multi-query corpus fusion
(PREPARE++ CorpusCompile + MULTI_MATCHES), session materialization (TEMP
KEYSPACE, spills to disk), extract recipes, columnar Parquet + Apache Iceberg
queryable keyspaces (projection/predicate/partition pushdown + time-travel),
Parquet VARIANT, vector search (VECTOR_DISTANCE + VECTORIZE_BATCH embeddings),
remote object-store reads (S3/GCS/Azure), goja JS UDFs + native
sparkline()/histogram() aggregates, and a rich CLI (cmd/n1k1). Remaining work:

## Headline priorities

_Last reviewed: 2026-07-23._

- [ ] Fork-free standalone analyzer binary -- the compiled EXECUTE child already codegens to fork-free *.go (base+engine+rt, no cbq); the open item is a `.multi build -o analyzer` embedding a minimal datastore runtime + baked plan.
- [ ] Native-lane ASOF / subquery projection -- kill boxed-value/JSON alloc churn (top perf lever).
- [ ] Columnar step 6: dictionary GROUP BY + more vectorized kernels + optional SIMD leaf (DESIGN-col.md; steps 1-5 done).
- [ ] Raise the SQL++ conformance (TestSuiteCases) pass rate.
- [x] ~~Correlated FROM-clause subqueries / CTE-as-datasource edge cases.~~ DONE. The
      LATERAL / correlated-comma-join form lowers via VisitNLJoin + JoinLateralOp; simple
      correlated derived tables are flattened by the planner (no ExpressionScan); and
      non-flattenable ones (agg/DISTINCT/LIMIT) run within the enclosing correlated
      subquery -- so a correlated subquery ExpressionScan now only arises for LATERAL,
      which is handled (the VisitExpressionScan NA is unreachable for valid queries).
      Also FIXED a CTE-as-datasource bug found here: an EMPTY subquery returned NULL not
      the empty array [] (EvaluateSubquery), so `FROM <empty cte>` yielded a spurious {}
      row (and ARRAY_LENGTH/IN diverged); the ASOF argmax->merge lowering's no-match
      default was aligned to [] to match (optimize_temporal.go). Guards: glue
      TestSubqueryEmptyArray + TestJoinLateral; the ASOF differential suite. Residual: a
      truly bare correlated-subquery FROM with no driving outer is still NA, but that
      shape doesn't arise in valid SQL (a correlation needs an outer).
- [ ] Windows port of the `./glue` + `./cmd/n1k1` test suites (22 failures; found by CI
      2026-07-27). Windows currently gates on bootstrap, vet, the core build, the core
      tests, and that glue/test/cmd COMPILE -- only these two test steps are
      `continue-on-error` there. Four independent causes:
      (a) 13 `TestIceberg*` -- upstream iceberg-go resolves a table location as a URI, so
      a native `C:\...` path gives "IO for file ... not implemented" (`records`' fixture
      writers already `skipIcebergOnWindows`). Fix = hand it a `file://` URI via
      `filepath.ToSlash`, which would also make Iceberg keyspaces usable on Windows.
      (b) `TestGlobKeyspace` -- the TEST interpolates a native `C:\...` path into a SQL++
      backquoted identifier, where `\` is an escape ("invalid escape sequence"). Use
      `filepath.ToSlash` in the query text; globMatch itself now handles either separator.
      (c) `TestExecuteCompiled*` + `TestExtractRecipeNativeDifferential` -- the
      compiled-execution generators (they shell out to the Go toolchain over `test/tmp`).
      (d) 5 `TestStats*` snapshot/race cases -- timing-sensitive on a slower runner.
- [x] ~~Windows `globMatch` bug.~~ DONE. `records/glob.go` split on `filepath.Separator`,
      so on Windows a `/`-separated pattern never split into segments and `filepath.Match`
      let `*` cross `/` (`globMatch("/a/*","/a/b/c")` was true) and `**` missed the
      zero-segment case. Now splits on EITHER separator and matches with `path.Match`
      slash semantics; `GlobBase` got the same treatment (it sliced the original string so
      the caller's separator style -- which `GlobFiles` re-anchors on by `TrimPrefix` -- is
      preserved), verified behaviour-identical against the old implementation. Guarded by
      `filepath.Join`-built and mixed-separator cases in `TestGlobMatchAndFiles`, and the
      Windows CI leg is blocking again.
- [ ] IndexScan2/3 pushdowns: indexProjection / indexOrder / indexGroupAggs.
- [ ] JOIN types: FULL OUTER (cbq-fork grammar does not support FULL).
- [ ] GROUP BY ROLLUP / CUBE / GROUPING SETS (cbq-fork grammar does not support).

## Conformance (SQL++ suite corpus)
- [ ] Raise the TestSuiteCases pass rate.

## NA() operator coverage (glue/conv.go)
Plan operators glue/conv.go still returns NA (unsupported) for, by tractability. (DONE
recently: NEST NL/hash/ON-KEYS, LATERAL, RIGHT OUTER, correlated-FROM subqueries.)
- Plausible, n1k1-only (no cbq changes):
  - VisitIndexNest -- index-driven NEST (NL/hash/ON-KEYS NEST already done).
  - VisitIndexScan2 / VisitIndexScan3 + VisitIndexCountScan / VisitIndexCountProject --
    can DEGRADE to the base datastore-scan-index (projection/order/group-aggs pushdowns
    are a later optimization); VisitPrimaryScan3 already degrades this way.
  - VisitIndexJoin -- index-driven lookup join (ON-KEYS lookup join already done).
- Niche correctness edges (within otherwise-supported ops):
  - VisitProject window guard -- window funcs n1k1 doesn't compute natively.
  - ORDER BY an aggregate over a `.*` projection (order-agg).
- Out of scope / non-goals (need mutation/admin, or served by n1k1's own mechanisms):
  DML (SendInsert/Upsert/Delete/Update/Merge/Clone/Set/Unset -- INSERT-to-file is
  partial); DDL (index/scope/collection/bucket/catalog/sequence/user/group);
  transactions; RBAC (grant/revoke); Prepare/Explain plan-ops (n1k1 has its own);
  Create/Drop/ExecuteFunction (goja UDFs instead); Infer; IndexAdvice/Advise;
  Collect/Receive/Discard.

## Library API surface (for embedders)
n1k1-as-a-library is already broadly exported: glue.Session{Store,Namespace,NamedArgs,
PositionalArgs,PrepareLevel,Pipe,OnRow,MergeStats} + Run/StatementRun/PlanExec/Interrupt;
glue.Result{Rows,Labels,Plan,Stats,Warnings,Count,...}; OpenSession/OpenSessionBound,
Store.PlanStatement[QP], PlanConvert/ExecConv, Conv{Temps,TopPlan,TopOp}; the Register*
suite (ExtensionDir/File/Glob, JS Func/Aggregate/Macro/Module/Stream, JSExtractRecipe);
records.{OpenFile,Source,Record,Recipe,RecipeRegister,ExtractSpec,ReadWholeDecompressed,
OpenReadCloser}; base.{Op,Val,Vals,Vars,Ctx,DatastorePipe,ExprFunc,YieldVals,Labels,
Stats} for custom sources/ops/exprs; plus many Enable*/Disable*/cache/timeout tuning vars.
Candidate ADDITIONS to widen power-user reach (tradeoff: larger, longer-lived contract --
export deliberately, only what has a clear embedder use):
- Promote a few private default consts -> public tunable vars (matching the existing
  Enable*/cache vars): extractHeadSampleBytes + records.describeSampleBytes (extract
  sampling caps), defaultNearDisorderNanos (ASOF reorder window), maxMacroExpansions, and
  the rt spill tunables (rhmap StartSize, maxRecycledBatches, spill byte caps) for memory
  control on constrained/embedded hosts.
- Programmatic keyspace listing + schema inference (today only inside cmd/n1k1).
- records.RegisterExtractor(ext, Extractor) for a Go-native WHOLE-FILE format (the
  framed-format path is already covered by RecipeRegister + a Go Recipe.Extract).
- A documented public helper to register a native-lane (non-goja) expr function into
  engine.ExprCatalog (the map is exported, but the ExprFunc + label-resolution contract
  is low-level today).

## Keeping current with SQL++
n1k1's SQL++ support tracks couchbase/query (parser/algebra/expression/plan/
planner). To move to a newer query, follow "Updating the fork to a newer query"
in glue/patches/README.md.

## More features

- command-line program (cmd/n1k1): v1 DONE (see TODO-done.md). Remaining CLI
  niceties (DESIGN-cli.md §7): tab completion, FROM 'file.csv' table-functions.
  (mid-query cancel: DONE -- Ctrl-C / closed pipe halts a running query.)

- [x] Multiple LOCAL data sources on one command line -> one keyspace per
      source, joinable in a single SQL++ query (DESIGN-data.md §2 Phase 1). E.g.
      `n1k1 drive=~/Drive/** docs=~/Documents/** 'sp=~/SharePoint/**'`. DONE:
      glue.OpenSessionSources/Source (glue/sources.go) turns each source into a
      Binding entry (~ expansion, CWD-anchored abs, bare-dir -> `dir/**`,
      basename/stem name derivation, collision = hard error) over a synthetic
      empty root; CLI arg parser + multi-source `.open` (cmd_sources.go /
      cmd_open.go). One bare path stays the classic single root. No engine change.

- [x] Multi-source `-sources` config file (DESIGN-data.md §2). DONE: a JSON/YAML/
      TOML file (name->path or {path,…}) parsed with n1k1's own decoders
      (records.DecodeConfigFile -> glue.LoadSources/OpenSessionSourcesFile),
      config-dir-relative paths, `-sources <file>` flag + `.open @<file>`. Solves
      space-in-path + many-sources the CLI/`.open` can't. Per-source options
      (formats/namespace/sorted) are parsed but rejected pending the composite.

- [x] Multi-source Phase 2 -- federate heterogeneous KINDS (DESIGN-data.md §2).
      DONE: glue.OpenSessionSources builds a map[name]*flatKeyspace via a per-source
      classifier (sourceFlatKeyspace: {dir,glob} local / {dir,iceberg} local+remote
      table / {parquetURL} remote parquet), federated by wrapFlatKeyspaces over an
      inert base -- KeyspaceRecordsOpen's existing per-kind routing scans them, no new
      scan code. Local dir + local Iceberg join in one query (guard test).

- [x] Multi-source per-source `-formats` (DESIGN-data.md §2). DONE: a config
      source's `formats` restricts that keyspace only -- flatKeyspace carries an
      optional WalkOptions override, applyKeyspaceFormats overlays it (keeping live
      .meta + path prefix) at the single KeyspaceRecordsOpen choke point + in
      keyspaceFiles (so .tables counts match). File/dir/glob only (Iceberg/Parquet
      single-format -> rejected). glue.LoadSources now accepts `formats`.

- [ ] Multi-source remaining niceties (DESIGN-data.md §2): per-source `namespace`
      (needs multi-namespace federation, not the single `default`) + `sorted`
      (sortedness contract) -- both parsed-but-rejected today; a catalog.json
      `"sources"` map (durable twin); a `.source add/list/rm` live-attach
      dot-command; cross-source `_meta` provenance under UNION ALL.

- UI / terminal and/or web-based?

- advanced wizard to show more what-if's?

- conversion of N1QL query-plan into n1k1 query-plan?
  - glue doesn't code-gen to *.go yet.
  - datastore Fetch() API's allocate garbage.
  - gocb multi-get API's allocate garbage.
  - go-couchbase does not pipeline transmit's efficiently,
    ending up with a syscall send()-per-fetch rather than a send()-per-batch.
  - datastore fetch stages should be recycled.
  - what to do with parent value during expression evaluation?
    - solved for the INTERPRETER: correlated subqueries thread the outer row via
      GlueContext.corrParent + a ScopeValue wrap (glue/expr.go, glue/subquery.go).
      Still open for CODEGEN (the emitted Go path).
  - sometimes keyspace terms aren't converted to label names correctly,
    like when there aren't keyspace aliases, which can lead to
    projections to not being able to access expressions
    like (`travel-sample`.`id`) correctly? FIXED already?
  - scan of COVERS: DONE -- covering IndexScan lowers to datastore-scan-index-cover
    (glue/conv.go VisitIndexScan + coverableIndexScan; see DESIGN-indexing.md).
  - scan tracks "setBit()" for intersect scan support? Not needed anymore?
  - scan related bit filters in cbq need revisit?
  - scan expression (ExpressionScan, i.e. FROM (subquery)/FROM cte): a NON-correlated
    subquery/CTE runs via expr-scan; a correlated subquery driven by a nested-loop JOIN
    (LATERAL / correlated comma-join) runs via the glue join-lateral op (VisitNLJoin +
    JoinLateralOp -- DONE). Still NA: a BARE correlated-subquery FROM-expr with no
    driving outer, and correlated CTE-as-datasource (WITH RECURSIVE roadmap below).
    (Expression subqueries -- IN (SELECT), scalar, etc. -- handle correlation in the
    interpreter; see above.)
  - implement parallel operator one day?
    - stage already provides some concurrency between producer & consumer.
  - classic N1QL engine uses recover() -- revisit this?
    - recover() might lead to dangling, unrecoverable resources?

- leveraging multiple cores?
  - scans of different partitions can be on separate cores?
    fetches against different nodes can be on separate cores?
    concurrently, independently building up their own batches?
  - filtering, projection can be multiple core, too?
  - distinct, aggregating, union|intersect|except sorting, can be multiple core, too?
    need a final results merge?
    - perhaps merge-sort, merge-join?

- staging / batchSize might be dynamic / computable?
  - first batch might be "sent early" or ASAP,
    so for example first fetch can be more concurrent?

- aggregate functions, advanced features?
  - IGNORE NULL's? (RESPECT NULLS is default)
  - FROM LAST? (FROM FIRST is default)
  - filter-where clauses?
  - DISTINCT? e.g., COUNT(DISTINCT productId)? -- DONE (base.AggCountDistinct,
    count_distinct; see TODO-done.md).
  - COUNTN versus COUNT? -- DONE (base/agg.go registers countn / countn_distinct
    via AggCountNUpdate; COUNTN counts only NUMBER-typed values).

- ORDER BY ... NULLS FIRST vs NULLS LAST? -- DONE (see TODO-done.md; per-term
  nulls-position in the order-offset-limit op).

- window partitions
  - window frame RANGE only works now for ORDER BY ASC? -- DONE (multi-column
    RANGE, see TODO-done.md).
  - optimizations?
    - inverse optimization on sliding window? -- DONE (invertible O(N) sliding
      COUNT/SUM/AVG + MIN/MAX deque; see TODO-done.md).
    - not materializing partition if possible?
      - for example, when only a count is needed?
  - FILTER (WHERE expr) clause?

- GROUP BY ROLLUP?
- GROUP BY GROUPING SETS?

- correlated subqueries?
  - these should just be yet another expression?
  - analysis of non-correlated vs correlated subqueries should be
    decided at a higher level than at query-plan execution?
  - implementation might store the current lzVal into a vars temp
    slot, which the child or subquery's ExecOp may be able to refer to
    with variables?

- compiled expr support? -- largely DONE: the compiled-lane denylist
  (compiledExprDenylist) is now empty; native comprehensions (ANY/EVERY/FIRST/
  ARRAY/OBJECT/WITHIN), array/object literals, variadic builders, and string
  funcs all codegen in both lanes. See DESIGN-exprs.md.

- expr MISSING or NULL patterns?
  - many expressions check for MISSING or NULL and propagate those,
    so, the first discovery of MISSING or NULL should
    be able to short-circuit and directly break or goto
    some outer handler codepath?

- precompute data based on early constant detection?
  - e.g., ARRAY_POSITION(hobbies, 0) might detect early that args[1]
    is a constant number, rather than rechecking that args[1] is a
    value.NUMBER during every Evaluate()?
    - see the ExprCmp() implementation to see one kind of approach on this.

- JOIN types (analyzed 2026-07) -- three DIFFERENT root causes:
  - RIGHT OUTER: WORKS already. The cbq fork grammar (parser/n1ql/n1ql.y:1547) rewrites
    `A RIGHT OUTER JOIN B` to `B LEFT OUTER JOIN A` at parse time
    (algebra.NewAnsiRightJoin swaps operands + sets outer=true), so n1k1's existing
    left-outer VisitNLJoin handles it -- verified end-to-end.
  - FULL OUTER: cbq FORK PARSER limitation -- no FULL token / no full-join grammar
    production (upstream N1QL lacks it too), so it never reaches a plan; AND n1k1's
    nested-loop op only null-extends the LEFT side, so a new full-outer engine op would
    be needed as well.
  - LATERAL: DONE (glue join-lateral op). The fork parses + plans it -> a correlated
    plan.ExpressionScan {correlated, nested_loop, subqPlan}; VisitNLJoin now detects that
    inner (lateralSubquery) and emits a glue "joinLateral-{inner,leftOuter}" op instead
    of the generic nested-loop. JoinLateralOp drives the left, boxes each outer row into
    the correlated parent (ConvertVals), runs the subquery per row via
    GlueContext.EvaluateSubquery (which owns corrParent + the in-context sub-plan), and
    joins its rows under the alias with the ON clause + left-outer NULL-extension. It's
    interpreter-lane (like all subquery/expr-scan features -- EvaluateSubquery needs cbq,
    so it's not in the fork-free compiled child). A bare correlated-subquery FROM-expr
    with no driving outer is still NA. Guard: glue TestJoinLateral.
  (CROSS / comma-join DONE: a nil ON clause converts to a constant-TRUE nested-loop
  join -- glue/conv.go VisitNLJoin.)

- NEST: DONE for ANSI (`NEST ... ON`) and lookup (`NEST ... ON KEYS`), inner + LEFT
  OUTER -- glue/conv.go VisitNLNest / VisitNest + VisitHashNest, over the engine's
  existing OpJoinNestedLoop isNest path (nestNL / nestKeys). Matches cbq
  execution/nest_nl.go (inner drops a no-match left row; leftOuter keeps it with []).
  Guard: glue TestJoinNest. Remaining: VisitIndexNest (index-driven nest) still NA;
  VisitHashNest currently falls back to the nested-loop nest (correct, not a true
  hash-nest) -- a real hash-nest runtime is the optimization.
- NEST via index scan? (VisitIndexNest -- still NA)

- NEST should spill out to disk when it gets too big?
  - or, perhaps not -- as it ultimately puts array into result,
    which has to fit into memory?

- UNION-ALL data-staging batchSize should be configurable?
- UNION-ALL data-staging batchChSize should be configurable?

- WITH RECURSIVE -- DONE (see TODO-done.md). Built in three steps: (1) subquery
  execution, (2) CTE-as-FROM, (3) the with-recursive fixpoint op
  (glue/recursive.go), honoring UNION / UNION ALL, the CYCLE clause, and OPTIONS
  {levels,documents} (with implicit depth/doc caps 100 / 10000 otherwise). Works
  in interpreter + compiler.

- subquery / CTE known gaps (found while stretch-testing; see test/cases.go):
  - a NON-recursive CTE that references a RECURSIVE CTE's full result (WITH
    RECURSIVE r ..., b AS (SELECT .. FROM r) ... FROM b) isn't supported:
    sub-conversions exclude recursive bindings (so a recursive step's FROM r
    reads corrParent, not the fixpoint), so b's FROM r doesn't route to
    with-recursive. Rare.

- speed mismatch between producers and consumers?
  - e.g., scan racing ahead and filling memory with candidate tuples
    when the fetch / filter is way behind?
  - less of a problem with push-based design?
  - data-staging batch sizes & queue sizes need careful configuration?
  - racing too far ahead is a waste if there's a small OFFSET+LIMIT?
  - racing too far ahead might be ok if there's lots of memory?
    - decision on "too far ahead" might be situational and depend on
      global, process-wide workload?

- how to handle when fields aren't known?
  - such as the immediate output of a scan?
  - use "." as the label and labelPath of ["."]
    to hold the entire document?
  - 'real' fields need a label prefix char, like '.'?
    - example: if labelPath [".", "city"] is projected into label
      `.["city"]`, then it can be referred to efficiently later as
      labelPath [`.["city"]`] from then on directly as a numeric index
      into a Vals slice?

- attachments
  - some encodings of label can mean hidden "attachment"?
    - with the '^' prefix char?
    - example: "^meta", "^smeta", "^id"?
    - these mean these labels are not really in the final output?
    - functions like 'META() AS myMeta' can project the hidden
      "^meta" label to a visible ".myMeta" in final output?
      - Ex: META().id might be implemented by projecting
        the labelPath ["^meta", "id"]?
    - need to check that full-round trip works on attachments?
  - INTERSECT/EXCEPT might incorrectly compare with attachments
    based on exprValsCanonical?
    - need to optionally strip out attachments from exprVarsCanonical?
    - attachments should not be propagated in INTERSECT/EXCEPT?
  - correctly done...?
    - JOIN can ignore attachments based on ON clause expression,
      and correctly propagate attachments.
    - ORDER BY can ignore attachments based on projected exprs,
      and correctly propagate attachments. Based on HeapValsProjected.
    - GROUP BY can ignore attachments based on group by exprs,
      and does not propagate attachments based on aggregate exprs.
    - DISTINCT might correctly ignore attachments,
      depending on how it's called with the group-by expression,
      and does not propagate attachments?

- standalone Op for data-staging / pipeline breaking?

- scan should take an optional params of pushdown field path's
  as optimization?
  - so that scan can return a subset of fields available for fast
    base.Vals access?
  - alternatively, use a project operator right after the scan?

- scans with params or pushdown expressions?
  - RangeScanIndex
  - FlexScanIndex
  - covering / non-covering

- jsonparser doesn't alloc memory, except for ObjectEach()...
  - its `var stackbuf [unescapeStackBufSize]byte` approach
    inadvertently escapes to the heap.
  - need upstream fix / patch?
  - jsonparser might already unescape strings during
    ArrayEach/ObjectEach callbacks, so recursion into
    CompareDeepType() for strings might incorrectly double-unescape?

- early stop handling?
  - when an error or LIMIT is reached?
    - YieldStats() can return a non-nil error, like ErrLimitReached?
    - YieldStats() should be locked for concurrency safety.
  - early stop when processing is canceled?

- prefetching optimizations?
  - this is an issue internal to scan operators?
  - data-staging / pipeline-breaking should be helpful here?
    - but, we don't want to race too far ahead?

- SIMD optimizations possible? see: SIMD-json articles / DESIGN-col.md?
  - UNDERWAY, see DESIGN-col.md: steps 1-5 DONE (fixed-width spike; Parquet
    queryable keyspace; column-projection pushdown reusing plan.Fetch.
    EarlyProjection; @col in-op vectorized aggregation over Parquet AND Iceberg
    columnar sources + a columnar float32 VECTOR_DISTANCE kernel). Step 6
    (dictionary GROUP BY, more kernels, optional SIMD leaf) is the next lever
    -- see headline priorities.

- col versus row optimizations? see: DESIGN-col.md.
  - UNDERWAY (steps 1-4 DONE; see above + DESIGN-col.md).
  - if columns are fixed size or fixed width, then
    a Val in the Vals can be interpreted as having multiple
    values in contiguous sequence.
    - e.g, prices := vals[7]
           numPrices := len(prices) / sizeOfUint64.

- types learned during expression compilation / analysis?
  - example: `sales < 1000`?
    we already have an optimization to evaluate 1000 up-front only
    once, but if we can also tell that `sales` expression
    only produces numbers, or only ever produces missing|null|numbers,
    then we can optimize further?

- compiled accessor(s) to a given JSON-path in a raw []byte value?
  - compiled accessor code versus generic jsonparser.Get() navigation?

- compiled SQL++ might have FastCGI-like child worker processes?

- divide by 0 at compile time should be checked instead of
  panic/recover that can leave unclosed, unreclaimable unresources?

- operator might optionally declare how its output Vals are sorted?
  - PARTIAL: SortedSourceMeta exists and drives the temporal ASOF merge
    (sort-elision + sorted-source gating); a fully general "declared sort
    order" contract across all operators is still open.

- scan should have a lookup table of file suffix handlers?

- advanced scans of indexes?
  - basic + covering secondary-index scans and FTS work now (glue/idx_si.go
    secondaryIndex.Scan, glue/idx_fts.go, glue/conv.go VisitIndexScan). Still
    open: the IndexScan2/IndexScan3 pushdowns below (indexProjection / indexOrder
    / indexGroupAggs), which stay NA (VisitIndexScan2/3 in glue/conv.go).

- PrimaryScan3 Scan3 has advanced pushdowns that we might support...
  - indexProjection, indexOrder, indexGroupAggs?

- integration with scorch TermFieldReaders as a Scan source or operator?
  - merge join by docNum / docId field?
  - in the general case, cannot compile a FTS conjunction/disjunction
    if the children of an FTS conjunction/disjunction
    are not known at compile time, unlike N1QL which has a compile-time
    bounded expr tree...
    - so, it might be more similar to ANY x IN y ... END
      as a hardcoded codepath?
  - some narrow edge cases (like, an explicit end-user term-search)
    have a bounded expression tree, though?
    - this might be ok for keyword type indexed fields?

- merge join - COMPLEX with push-based engine... -- substantially DONE (see
  TODO-done.md). Correlated-argmax / ASOF subqueries lower to a streaming
  merge-join + merge-scan (engine/op_merge_join.go, op_merge_scan.go): a
  memory-bounded two-stream co-advance over near-sorted keyspaces, with a
  K-way pull-coordinator for multi-file merge-scan.
  - merge join needs threading / locking / coroutines
    so that both children can feed the merge-joiner?
  - a variation on the concurrent data-staging that interweaves or
    zippers together batches from children might work?

- merge join needs a skip-ahead ability as an optimization?
  - PARTIAL: the merge co-advance skips/advances past non-matching rows
    (tolerance-bounded), but a general SkipToHints seek pushed down to
    children is still open.
  - idea: can introduce an optional lazy "SkipToHints" object or Vals
    that's available to operator's children?
    - an lzYieldVals callback can optionally provid skip hints via
      something like lzVars.SkipToHints[2] = lzSkipToVal which
      operator #0 and/or operator #1 can check?
    - BUT, this will involve multiple goroutines across a merge join?
      - configuring batchChSize to 0 might help with "interlock"
        so that sibling goroutines don't progress too far ahead?
      - and, SkipToHints might be traded during recycled batch exchange?

- if compilation is concurrent and becomes ready-to-use while
  an inflight query is halfway through, can we switch gears
  to the compiled codepaths?
  - perhaps at the point of yield-stats?

- GROUP-JOIN operator?
  - useful for decorrelating subqueries?

- CONNECT BY?
  - hierarchical queries?
  - [START WITH condition] CONNECT BY [LOOP | NOCYCLE] condition?
  - PRIOR operator / CONNECT_BY_ROOT operator?

- PIVOT aggregate-funcs FOR expression IN expected-values?
  - PIVOT count(*) FOR (time, category, rating) IN ((1, "movie", 5), ...)?

- UNPIVOT?

- SQL 2011 temporal features?
  - transaction time vs effective time?
  - PERIOD OVERLAPS?

- NUMA?
  - pinning threads to specific cores?
  - lock free data structures?
  - per-thread data structures?
  - optimize data structure layout to avoid false sharing
    and accessing non-local memory?

- emit other languages?

- handling of BINARY data type?
  - use a label prefix char?  Perhaps '='?
  - PROBLEM: the operator doesn't know a val is BINARY until runtime,
    so it can't assign a '=' label prefix at query-plan time?
  - the '.' label can still have an UNKNOWN type, though,
    so it might be ok.

- (perhaps this is unneeded?) temporary, but reused (recyclable) raw
  []bytes buf as a per-tuple working area...
  - perhaps the base.Vals could have a hidden labeled "^tmp"?
    - but, unlike other Val's, it would be mutated!
      so, this is not highly favored.
    - and, also need to be careful to carrying the ^tmp
      and propagating it during processing.
  - better: add another struct property to the base.Vars?
    - it's copied as more base.Vars are chained,
      so that you don't need to walk the chain to the root
      every time?
    - any spawned child thread/goroutines can push another Vars
      that shadows the ancestor Var chain to avoid concurrent mutations?

- non-materializing WindowPartition implementation?
  might just borrow the underlying ORDER-OFFSET-LIMIT's backing heap?
  - currently, OpWindowPartition creates a heap-as-chunk-sequence
    that it resets for each partition.
  - ANSWER: borrowing underlying ORDER-OFFSET-LIMIT's backing heap for
    the window partition won't work because the order-by heap is a
    real heap, which is different than the heap-as-chunk-sequence used
    by a window partition.
    - perhaps can have a different sorted data structure based on rhstore?
