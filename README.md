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
  as they are easy to fully recycle and reuse.
- []byte is faster for GC scanning/marking than interface{}.
- jsonparser is used instead of json.Unmarshal(), to avoid garbage.
  - jsonparser returns []byte values that point into the parsed
    document's []byte's.
- commonly accessed JSON object fields can be promoted
  to quickly accessible, labeled "registers" at compile time...
  - object field access or map lookups (e.g., obj["city"]) can be
    instead replaced by positional slice access (e.g., vals[5])
  - the labeled vals or "registers" are passed amongst operators.
- push-based paradigm for shorter codepaths
  - data transfer between operators (e.g., from scan -> filter ->
    project) is a function call (which can sometimes be removed by
    compilation), instead of a send/recv on channels between
    goroutines.
  - the pull-based paradigm, or the iterator approach, in contrast,
    involves additional checks for HasNext() across the operators
    in a query-plan.
- data-staging, pipeline breakers (batching)...
  - batching results between operator may be more friendly to CPU
    instruction & data caches.
  - data-staging also supports optional concurrency -- one or more
    goroutine actors can be producers that feed a channel to a consumer.
  - a channel send is for a batch of items, instead of a separate send
    for each each individual item.
  - max-batch-size and channel buffer size between producer goroutines
    and consumer goroutine are designed to be configurable.
  - recycled batch exchange between producer goroutines and consumer
    goroutine for less garbage creation.
- query compilation to golang...
  - based on Futamura projections / LMS (Rompf, Odersky) inspirations.
  - supports operator fusion, for fewer function calls.
  - lifting vars for resource reusability.
- expression optimizations for static parameters.
  - for example, with a `sales < 1000` expression, the `1000` can be
    evaluated once up-front, instead of being re-evaluated for every
    single tuple that's processed.
  - and, the `1000` can lead to a more direct numeric comparison.
- for hashmaps...
  - couchbase/rhmap is a hashmap that supports []byte as a key,
    a'la `map[[]byte][]byte`.
  - couchbase/rhmap is efficiently, fully recyclable in contrast
    to map[string]interface{}.
  - couchbase/rhmap is also intended to easily spill out to disk
    via mmap(), allowing hash-joins and DISTINCT processing
    on larger datasets.
- error handling is push-based via a YieldErr callback...
  - the YieldErr callback allows n1k1 to avoid continual, conservative
    error handling instructions ("if err != nil { return nil, err }").
- max-heap in ORDER-BY / OFFSET / LIMIT reuses memory from "too large" tuples.
- INTERSECT DISTINCT / ALL and EXCEPT DISTINCT / ALL
  are optimized by reusing hash-join machinery.

------------------------------------------
## Some features...

- types: MISSING, NULL, boolean, number, string, array, object, UNKNOWN (BINARY).
- collation follows N1QL type comparison rules.
- glue integration with existing couchbase/query/expression's.
- join nested-loop inner.
- join nested-loop outer-left.
- join hash-eq inner
- join hash-eq outer-left
- join ON expressions.
- filtering (WHERE expressions).
- projection expressions.
- ORDER BY multiple expressions & ASC/DESC.
- ORDER-BY / OFFSET / LIMIT via max-heap.
- DISTINCT.
- GROUP BY on multiple expressions
- COUNT().
- SUM().
- HAVING.
- UNION ALL is concurrent (one goroutine per contributor).
- UNION DISTINCT is supported by sequencing UNION ALL with DISTINCT.
- INTERSECT DISTINCT / INTERSECT ALL.
- EXCEPT DISTINCT / EXCEPT ALL.
- data-staging / pipeline-breaker facilities along with concurrency.
- nested object paths (e.g. locations/address/city).
- scans of simple files (CSV's and newline delimited JSON).
- runtime variables / context passed down through ExecOp().

-------------------------------------------------------
## DEV SHORTCUTS...

    go test . && go build ./cmd/intermed_build/ && ./intermed_build && go test ./... && go fmt ./... && go test -v ./...

    go build ./cmd/expr_build/ && ./expr_build && go fmt ./...

-------------------------------------------------------
## The way the n1k1 compiler works...

Or, how intermed_build generates a N1QL compiler...

- 1: First, take a look at the n1k1/*.go files.  You'll see a simple,
interpreter for a "N1QL" query-plan.  In ExecOp(), it recursively
walks through a query-plan tree, and processes the query-plan by
pushing (or yield()'ing) data records from child nodes (e.g., a scan)
up to parent nodes (e.g., filters) from the query-plan tree.

- 1.1: As part of that, you'll also see some variables and functions
that follow a naming convention with "lz" (e.g., "lazy") in their
names.  The "lz" naming convention is a marker that tells us whether
some variables are lazy or late-bound (they need actual data records),
versus other variables that are early-bound (they use information
that's already available at query-plan compilation time).

- 1.2: Of note, the n1k1/*.go files are written in a careful subset of
golang.  It's all legal golang code, but it follows additional rules
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
into printf's.  Non-lazy expressions are turned into printf'ed
placeholder vars.  Non-lazy lines are emitted entirely as-is, as they
are early-bound.

- 3: Finally, the n1k1 compiler, which imports and uses the generated
n1k1/intermed package, will take the user's input of a N1QL query-plan
and will emit *.go code (or possibly other languages) that can
efficiently execute that query-plan.

------------------------------------------
## TODO...

- precompute data based on early constant detection?
  - e.g., ARRAY_POSITION(hobbies, 0) might detect early that args[1]
    is a constant number, rather than rechecking that args[1] is a
    value.NUMBER during every Evaluate()?

- compiled expr support?

- expr MISSING or NULL patterns?
  - many expressions check for MISSING or NULL and propagate those,
    so, the first discovery of MISSING or NULL should
    be able to short-circuit and directly break or goto
    some outer handler codepath?

- UNION-ALL data-staging batchSize should be configurable?
- UNION-ALL data-staging batchChSize should be configurable?

- more GROUP BY aggregates: min, max, average?

- HAVING (should be able to reuse existing filter operator).

- UNNEST - a kind of self-join

- NEST - a kind of join

- how to handle when fields aren't known?
  - such as immediate output of a scan?
  - use "." as the label and labelPath of ["."]
    to hold the entire document?
  - 'real' fields need a label prefix char, like '.'?
    - example: if labelPath [".", "city"] is projected into label
      `.["city"]`, then it can be referred to efficiently later as
      labelPath [`.["city"]`] from then on directly as a numeric index
      intoa Vals slice?

- attachments
  - some encodings of label can mean hidden "attachment"?
    - with the '^' prefix char?
    - example: "^meta", "^smeta"?
    - these mean these labels are not really in the final output?
    - functions like 'META() AS myMeta' can project the hidden
      "^meta" label to a visible ".myMeta" in final output?
      - Ex: META().id might be implemented by projecting
        the labelPath ["^meta", "id"]?

- handling of BINARY data type?
  - use a label prefix char?  Perhaps '='?

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

- subqueries & correlated subqueries?
  - these should just be yet another expression
  - analysis of non-correlated vs correlated subqueries should be
    decided at a higher level than at query-plan execution

- base.ValComparer.CanonicalJSON()
  - need the JSON for objects to be canonicalized before they can be
    used as a map[] key, as {a:1,b:2} and {b:2,a:1} are
    logically the same?
  - numbers might also need to be canonicalized?
    - e.g., 0 vs 0.0 vs -0 are logically the same?

- jsonparser doesn't alloc memory, except for ObjectEach()...
  - its `var stackbuf [unescapeStackBufSize]byte` approach
    inadvertently escapes to the heap.
  - need upstream fix / patch?
  - jsonparser might already unescape strings
    during ArrayEach/ObjectEach callbacks, so recursion into
    CompareDeepType() for strings might incorrectly double-unescape?

- early stop when an error or LIMIT is reached?
  - YieldStats() can return a non-nil error, like ErrLimitReached?
  - YieldStats() should be locked for concurrency safety.

- early stop when processing is canceled?

- conversion of real N1QL query-plan into n1k1 query-plan

- LET / LETTING are parser-time expression expansions (like macros) so
  are not part of query-plan execution?
  - needs more research.

- SIMD optimizations possible?  see: SIMD-json articles?

- prefetching optimizations?
  - this is an issue internal to scan operators?
  - data-staging / pipeline-breaking should be helpful here?

- compiled accessor(s) to a given JSON-path in a raw []byte value?

- col versus row optimizations?
  - if columns are fixed size or fixed width, then
    a Val in the Vals can be interpreted as having multiple
    values in contiguous sequence.
    - e.g, prices := vals[7]
           numPrices := len(prices) / sizeOfUint64.

- types learned during expression compilation / analysis?

- operator can optionally declare how the Vals are sorted?

- scan should have a lookup table of file suffix handlers?

- couchbase/rhmap should be able to spill out to disk via mmap()?

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

- emit other languages?
