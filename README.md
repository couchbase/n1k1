n1k1 is a prototype query-plan interpreter and compiler for N1QL.

------------------------------------------
## Performance approaches...

Some design ideas meant to help with n1k1's performance...

- garbage creation avoidance as a major theme.
- avoidance of sync.Pool.
- avoidance of locking and channels as much as possible.
- avoidance of map[string]interface{} and []interface{}.
- avoidance of interface{} and boxed-value allocations.
- []byte and [][]byte instead are used heavily,
  as they are straightfoward to fully recycle and reuse,
  by reslicing to buffer[:0].
- []byte should be faster for GC scanning/marking than interface{}.
- jsonparser is used instead of json.Unmarshal(), to avoid garbage.
  - jsonparser returns []byte values that point into the parsed
    document's []byte's.
- commonly accessed JSON object fields can be promoted to quickly
  accessible, labeled "registers" at early preparation phases...
  - object field access or map lookups (e.g., obj["city"]) can be
    instead replaced by positional slice access (e.g., vals[5])
  - the labeled vals or "registers" are passed amongst operators.
- push-based paradigm for shorter codepaths
  - data transfer between operators (e.g., from scan -> filter ->
    project) is a function call, instead of a send/recv on channels
    between goroutines. These function calls can sometimes be inlined
    by n1k1's code generator, removing function call overhead.
  - the pull-based paradigm, a.k.a. the iterator approach, in
    contrast, involves additional checks for HasNext() across the
    operators in a query-plan.
- data-staging, pipeline breakers (record batching)...
  - batching results between operators may be more friendly to CPU
    instruction & data caches.
  - data-staging also supports optional concurrency -- one or more
    goroutine actors can be producers that feed a channel to a
    consumer goroutine.
  - a data-staging channel send is for a batch of multiple items,
    instead of a channel send for each individual item.
  - max-batch-size and channel buffer size between producer goroutines
    and consumer goroutine are designed to be configurable.
  - batches that are exchanged between producer goroutines and
    consumer goroutine are recycled back to the producer goroutines
    for less garbage creation.
- query compilation to golang...
  - based on Futamura projections / LMS (Rompf, Odersky) inspirations.
  - implements operator fusion, for fewer function calls.
  - lifting vars is implemented to support resource reusability.
- expression optimizations for static parameters.
  - for example, with an expression on `sales < 1000`, the `1000` is
    evaluated early and a single time in preparation phases, instead
    of being re-evaluated for every single tuple that's processed.
  - the static type of the `1000` is also detected in up-front
    preparation phases, which leads to more direct codepaths focused
    on numeric comparison.
- for hashmaps...
  - couchbase/rhmap is a hashmap that supports []byte as a key,
    a'la `map[[]byte][]byte`.
  - couchbase/rhmap is efficient to fully recycle in contrast
    to map[string]interface{}.
  - couchbase/rhmap/store will spill to temporary disk files when
    the hashmap becomes too large, via mmap(), allowing operators
    to process larger datasets that don't fit into memory (hash-joins,
    DISTINCT, GROUP BY, INTERECT, EXCEPT).
  - couchbase/rhmap/store chunk file allows hash-join left-vals to be
    spilled out to temporary disk files when it becomes too large.
- error handling is push-based via an YieldErr callback...
  - the YieldErr callback allows n1k1 to avoid continual, conservative
    error handling checks ("if err != nil { return nil, err }").
- max-heap in ORDER-BY / OFFSET / LIMIT
  - reverse popping of the max-heap produces the final result, which
    avoids a final sort.
  - max-heap that becomes too large will spill to temporary files.
- INTERSECT DISTINCT / ALL and EXCEPT DISTINCT / ALL
  are optimized by reusing hash-join's machinery.
  - hash-join's probe map can optionally track information like...
    - all the left-side values (for hash-join).
    - a count of the left-side values (for INTERSECT ALL, EXCEPT ALL).
    - and/or a probe-count (for multiple use cases).
  - INTERSECT DISTINCT and EXCEPT DISTINCT avoid using an
    additional, chained DISTINCT operator.
- base.ValComparer.CanonicalJSON()
  - provides JSON canonicalization into existing []byte buffers which
    avoids memory allocations.
  - some JSON, such as for objects, need to be canonicalized before
    they can be used as a map[] key (e.g., for GROUP BY, DISTINCT,
    etc).
    - Ex: {a:1,b:2} and {b:2,a:1} are logically the same.
  - numbers also need to be canonicalized.
    - e.g., 0 vs 0.0 vs -0 are logically the same.

------------------------------------------
## Some features...

- types: MISSING, NULL, boolean, number, string, array, object, UNKNOWN (BINARY).
- comparisons follows N1QL type comparison rules.
- glue integration with existing couchbase/query/expression package.
- join nested-loop inner.
- join nested-loop left outer.
- join hash-eq inner
- join hash-eq left outer
- join ON expressions.
- UNNEST inner
- UNNEST left outer.
- WHERE expressions.
- projection expressions.
- ORDER BY multiple expressions & ASC/DESC.
- ORDER-BY / OFFSET / LIMIT.
- DISTINCT.
- GROUP BY on multiple expressions
- aggregate functions: COUNT, SUM.
- HAVING, by reusing the same filter operator as WHERE.
- UNION ALL is concurrent, with the contributing child operators
  having their own goroutines.
- UNION DISTINCT is supported by sequencing UNION ALL with DISTINCT.
- INTERSECT DISTINCT / INTERSECT ALL.
- EXCEPT DISTINCT / EXCEPT ALL.
- data-staging / pipeline-breaker machinery with concurrent child
  pipelines.
- nested object paths (e.g. locations/address/city).
- scans of simple files (CSV's and newline delimited JSON).
- automatic spilling from memory to temporary files...
  - hashmaps (for joins, distinct, group-by, etc).
  - max-heap's (for sorting).
- runtime variables / context passed down through ExecOp().

-------------------------------------------------------
## DEV SHORTCUTS...

    go test . && go build ./cmd/intermed_build/ && ./intermed_build && go test ./... && go fmt ./... && go test -v ./...

    go build ./cmd/expr_build/ && ./expr_build && go fmt ./...

-------------------------------------------------------
## The way the n1k1 compiler works...

Or, how intermed_build generates a N1QL compiler...

- 1: First, take a look at the n1k1/*.go files. You'll see a simple,
interpreter for a "N1QL" query-plan. In ExecOp(), it recursively walks
through a query-plan tree, and processes the query-plan by pushing (or
yield()'ing) data records from child nodes (e.g., a scan) up to parent
nodes (e.g., filters) from the query-plan tree.

- 1.1: As part of that, you'll also see some variables and functions
that follow a naming convention with "lz" (e.g., "lazy") in their
names. The "lz" naming convention is a marker that tells us whether
some variables are lazy or late-bound (they need actual data records),
versus other variables that are early-bound (they use information
that's already available at query-plan compilation time).

- 1.2: Of note, the n1k1/*.go files are written in a careful subset of
golang. It's all legal golang code, but it follows additional rules
and conventions (like the "lz" conventions and directives in code
comments) to make parsing by n1k1's intermed_build tool easy.

- 2: The intermed_build tool parses the "lz" conventions and other
markers (e.g., code comment directives) from the n1k1/*.go source
files to translate that interpreter code into a intermediary, helper
package, called n1k1/intermed.

- 2.1: The n1k1/intermed package will be used later by the final n1k1
query compiler.

- 2.2: The way the intermed_build tool works is that it processes the
n1k1/*.go source files line-by-line, and translates any "lz" lines
into printf's. Non-lazy expressions are turned into printf'ed
placeholder vars. Non-lazy lines are emitted entirely as-is, as they
are early-bound.

- 3: Finally, the n1k1 compiler, which imports and uses the generated
n1k1/intermed package, will take the user's input of a N1QL query-plan
and will emit *.go code (or possibly other languages) that can
efficiently execute that query-plan.

------------------------------------------
## TODO...

- speed mismatch between producers and consumers?
  - e.g., scan racing ahead and filling memory with candidate tuples
    when the fetch / filter is way behind?
  - less of a problem with push-based design?
  - data-staging batch sizes & queue sizes need careful configuration?
  - racing too far ahead is a waste if there's a small OFFSET+LIMIT?
  - racing too far ahead might be ok if there's lots of memory?
    - decision on "too far ahead" might be situational and depend on
      global, process-wide workload?

- conversion of N1QL query-plan into n1k1 query-plan?

- NEST - a kind of join

- numbers
  - need to treat float's different than int's?

- subqueries & correlated subqueries?
  - these should just be yet another expression
  - analysis of non-correlated vs correlated subqueries should be
    decided at a higher level than at query-plan execution
  - what about subqueries that return huge results?
    - the arrays might get huge?
    - perhaps an attachment or label can be for a named cursor, such
      as "&cursor-2341", that's registered into the Ctx?
      - the cursor might to a pipeline-breaking batch provider?

- compiled expr support?

- expr MISSING or NULL patterns?
  - many expressions check for MISSING or NULL and propagate those,
    so, the first discovery of MISSING or NULL should
    be able to short-circuit and directly break or goto
    some outer handler codepath?

- temporary, but reused (recyclable) raw []bytes buf
  as a per-tuple working area might be associated with...
  - perhaps the base.Vals could have a hidden labeled "^tmp"?
    - but, unlike other Val's, it would be mutated!
      so, this is not highly favored.
    - and, also need to be careful to carrying the ^tmp
      and propagating it during processing.
  - better: add another struct property to the base.Vars?
    - it's copied as more base.Vars are chained,
      so that you don't need to talk the chain to the root
      every time?
    - any spawned child thread/goroutines can push another Vars
      that shadows the ancestor Var chain to avoid concurrent mutations?

- precompute data based on early constant detection?
  - e.g., ARRAY_POSITION(hobbies, 0) might detect early that args[1]
    is a constant number, rather than rechecking that args[1] is a
    value.NUMBER during every Evaluate()?
    - see the ExprCmp() implementation to see how this works.

- UNION-ALL data-staging batchSize should be configurable?
- UNION-ALL data-staging batchChSize should be configurable?

- more GROUP BY aggregates: min, max, average?

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
      and correctly propagate attachments.  Based on HeapValsProjected.
    - GROUP BY can ignore attachments based on group by exprs,
      and does not propagate attachments based on aggregate exprs.
    - DISTINCT might correctly ignore attachments,
      depending on how it's called with the group-by expression,
      and does not propagate attachments?

- handling of BINARY data type?
  - use a label prefix char?  Perhaps '='?
  - PROBLEM: the operator doesn't know a val is BINARY until runtime,
    so it can't assign a '=' label prefix at query-plan time?
  - the '.' label can still have an UNKNOWN type, though,
    so it might be ok.

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

- LET / LETTING are parser-time expression expansions (like macros?)
  so are not part of query-plan execution?
  - needs more research.

- compiled accessor(s) to a given JSON-path in a raw []byte value?
  - compiled accessor code versus generic jsonparser.Get() navigation?

- prefetching optimizations?
  - this is an issue internal to scan operators?
  - data-staging / pipeline-breaking should be helpful here?
    - but, we don't want to race too far ahead?

- SIMD optimizations possible?  see: SIMD-json articles?

- col versus row optimizations?
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

- divide by zero should be checked instead of panic/recover
  that can leave unclosed, unreclaimable unresources.

- operator might optionally declare how its output Vals are sorted?

- scan should have a lookup table of file suffix handlers?

- scans of indexes?

- integration with scorch TermFieldReaders as a Scan source or operator?
  - merge join by docNum / docId field?
  - in the general case, cannot compile a FTS conjunction/disjunction
    if the children of an FTS conjunction/disjunction
    are not known at compile time, unlike N1QL which has a compile-time
    bounded expr tree...
    - so, it might be more similar to ANY x IN y ... END -- hardcoded codepath.
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

- NUMA?
  - pinning threads to specific cores?
  - lock free data structures?
  - per-thread data structures?
  - optimize data structure layout to avoid false sharing
    and accessing non-local memory?

- emit other languages?
