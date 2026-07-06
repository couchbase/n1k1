# TODO

Forward-looking only. What's DONE is in TODO-done.md; internals + the
design are in DESIGN.md; build/test commands are in README.md.

Status: modernization + a pure-Go N1QL (SQL++) engine (CGO_ENABLED=0,
cross-compiles) are done. Remaining work:

## Conformance (SQL++ suite corpus)
- [ ] Raise the TestSuiteCases pass rate.

## Keeping current with SQL++
n1k1's SQL++ support tracks couchbase/query (parser/algebra/expression/plan/
planner). To move to a newer query, follow "Updating the fork to a newer query"
in glue/patches/README.md.

## More features

- command-line program (cmd/n1k1): v1 DONE (see TODO-done.md). Remaining CLI
  niceties (DESIGN-cli.md §7): tab completion, FROM 'file.csv' table-functions,
  mid-query cancel.

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
  - scan of COVERS needs support? FIXED already?
  - scan tracks "setBit()" for intersect scan support? Not needed anymore?
  - scan related bit filters in cbq need revisit?
  - scan expression (ExpressionScan, i.e. FROM (subquery)/FROM cte) only handles
    non-correlated right now. (Expression subqueries -- IN (SELECT), scalar,
    etc. -- DO handle correlation now in the interpreter; see above. This item is
    the separate FROM-clause/datasource case, tied to CTE-as-datasource in the
    WITH RECURSIVE roadmap below.)
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
  - count() or COUNT_ALL is different than count(expr) vs count(ALL expr),
    w.r.t. missing/null handling?
  - IGNORE NULL's? (RESPECT NULLS is default)
  - FROM LAST? (FROM FIRST is default)
  - filter-where clauses?
  - DISTINCT? e.g., COUNT(DISTINCT productId)?
  - COUNTN versus COUNT?
    - COUNTN is only used by index scans?

- ORDER BY ... NULLS FIRST vs NULLS LAST?

- window partitions
  - window frame RANGE only works now for ORDER BY ASC?
  - optimizations?
    - inverse optimization on sliding window?
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

- compiled expr support?

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

- hash join in n1k1 fills the probe map with the left-hand-side values,
  but N1QL planner might think the right-hand-side is the probe map?
  - need to double-check this.

- JOIN types: CROSS, FULL, RIGHT OUTER, LATERAL.

- NEST via hash-join?
- NEST via index scan?

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
  - a correlated subquery that CONTAINS an aggregate (e.g. SELECT (SELECT RAW
    COUNT(*) ... WHERE x = o.y)) panics: "*value.ScopeValue is not
    value.AnnotatedValue" -- the correlated ScopeValue wrap collides with the
    aggregate op's AnnotatedValue expectation.
  - UNION ALL of two SELECTs (plan.UnionAll) is still NA.
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

- EXCEPT ALL - tuple should appear MAX(m - n, 0) times in the result,
  given that a tuple appears m times in the left side
  and n times in the right side, where m >= 0 and n >= 0.

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

- col versus row optimizations? see: DESIGN-colmd.
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

- divide by 0 at runtime leads to +Inf, with strconv.AppendFloat()
  converts to "+Inf", which is not JSON -- should be converted into
  JSON null. Also, need to handle -Inf and NaN.

- operator might optionally declare how its output Vals are sorted?

- scan should have a lookup table of file suffix handlers?

- advanced scans of indexes?
  - only basic Index.Scan glue works right now. <== OUTDATED?

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

- merge join - COMPLEX with push-based engine...
  - merge join needs threading / locking / coroutines
    so that both children can feed the merge-joiner?
  - a variation on the concurrent data-staging that interweaves or
    zippers together batches from children might work?

- merge join needs a skip-ahead ability as an optimization?
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
