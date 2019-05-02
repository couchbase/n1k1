n1k1 is an experimental query compiler and execution engine for N1QL.

-------------------------------------------------------
DEV SHORTCUTS...

    go test . && go build ./cmd/intermed_build/ && ./intermed_build && go test ./... && go fmt ./... && go test -v ./...

    go build ./cmd/expr_build/ && ./expr_build && go fmt ./...

-------------------------------------------------------
The way n1k1 works...

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
Performance approaches...

What are some design approach that help with n1k1's performance...

- garbage creation avoidance as a major theme.
- avoidance of sync.Pool.
- avoidance of locking and channels as much as possible.
- avoidance of map[string]interface{} and []interface{}.
- []byte and [][]byte instead are used heavily,
  as they are easy to completely recycle and reuse.
- jsonparser is used instead of json.Unmarshal(), to avoid garbage.
  - jsonparser returns []byte values that point into the parsed
    document's []byte's.
- commonly accessed JSON object fields can be promoted
  to quickly accessible "registers" at compile time...
  - object field access or map lookups (e.g., obj["city"]) can be
    instead replaced by positional slice access (e.g., vals[5])
  - this is similar to an analogy of RAM access versus faster CPU
    register access.
  - the vals "register" is passed from operator to operator.
- push-based paradigm for shorter codepaths (in contrast to
  pull-based, iterator-style paradigm).
- data-staging, pipeline breakers (batching)...
  - batching results between operators may be more friendly to the CPU
    data instruction cache.
  - batch sizes and queue sizes between producers and cosunmer are
    designed to be configurable.
  - data-staging also supports optional concurrency -- one or more
    goroutine actors can be producers in a pipeline.
  - recycled batch exchange between producer goroutine and consumer
    goroutine for less garbage creation.
- query compilation to golang...
  - supports operator fusion, for fewer function calls.
- for hashmaps...
  - couchbase/rhmap supports a fully recyclable hashmap that supports
    []byte as a key, like "map[[]byte][]byte".

------------------------------------------
Some features...

- join nested-loop inner
- join nested-loop outer-left
- filtering (WHERE, HAVING)
- projections
- scans of simple files (CSV's)
- ORDER BY of multiple expressions & ASC/DESC
- ORDER-BY / OFFSET / LIMIT via max-heap
- OFFSET / LIMIT
- max-heap reuses memory slices from "too large" tuples
- identifier paths (e.g. locations/address/city)
- lifting vars to avoid local closures
- capturing emitted code to avoid local closures
- data-staging / pipeline-breaker facilities along with concurrency
- UNION ALL is concurrent (one goroutine per contributor).
- avoid json.Unmarshal & map[string]interface{} allocations
- runtime variables / context passed down through ExecOp()

------------------------------------------
TODO...

- expr support
  - easy: convert Val to query/value.Value and run the existing
    query/expression.
  - hard: compiled expr's

- how to handle when fields aren't known?
  - such as immediate output of a scan?
  - use "." for field name and fieldPath of ["."]
    to hold the entire document?
  - 'real' fields need a field name prefix char, like '.'?
    - example: if fieldPath [".", "city"] is projected
      into field ".city", then it can be referred to
      efficiently later as fieldPath [".city"] from then on
      directly from the Vals slice?

- attachments
  - some encodings of field name can mean hidden "attachment"?
    - with the '^' prefix char?
    - example: "^meta", "^smeta"?
    - these mean these fields are not really in the final output?
    - functions like 'META() AS myMeta' can project the hidden
      "^meta" field to a visible ".myMeta" in final output?
      - META().id might be implemented by projecting
        the fieldPath ["^meta", "id"]?

- handling of BINARY data type?
  - use a field name prefix char?  Perhaps '='?

- temporary, but reused (recyclable) raw []bytes buf
  as a per-tuple working area might be associated with...
  - the base.Vals as a hidden field "^tmp"?
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

- scan should take an optional params of pushdown fieldPath's
  as optimization?
  - so that scan can return a subset of fields available for fast
    base.Vals access?
  - alternatively, use a project operator right after the scan?

- scans with params or pushdown expressions?
  - RangeScanIndex
  - FlexScanIndex
  - covering / non-covering

- UNNEST - a kind of self-join

- NEST - a kind of join

- UNION-ALL data-staging batchSize should be configurable
- UNION-ALL data-staging batchChSize should be configurable

- standalone Op for data-staging / pipeline breaking

- GROUP BY / aggregates
  - SELECT country, SUM(population) FROM ... GROUP BY country

- subqueries & correlated subqueries?
  - these should just be yet another expression
  - choice between non-correlated vs correlated subqueries should be
    decided at a higher level than at query-plan execution

- need the JSON for objects to be canonicalized before they can be
  used as a DISTINCT map[] key, as {a:1,b:2} and {b:2,a:1} are
  logically the same?
  - numbers might also need to be canonicalized (0 vs 0.0 vs -0)?

- DISTINCT

- UNION
- INTERSECT / INTERSECT ALL
- EXCEPT / EXCEPT ALL

- jsonparser doesn't alloc memory, except for ObjectEach() on it's
  `var stackbuf [unescapeStackBufSize]byte`, which inadvertently
  escapes to the heap.
  - need upstream fix / patch?

- early stop when an error or LIMIT is reached?
  - YieldStats() can return an non-nil error, like ErrLimitReached

- early stop when processing is canceled?

- hash join?

- conversion of real N1QL query-plan into n1k1 query-plan

- LET / LETTING are parser-time expression expansions (like macros) so
  are not part of query-plan execution?

- SIMD optimizations possible?  see: SIMD-json articles?

- prefetching optimizations?

- compiled accessor(s) to a given JSON-path in a raw []byte value?

- col versus row optimizations?
  - need base.Vals that allows for optional col based representation?
    - a single col is easy -- same as Vals?
    - need a merge-join & skip-ahead optimization?
  - YieldVals() might take []Vals instead of Vals?
    - that would allow an []Records interpretation?
    - or, an []Columns interpretation, using same signature?

- multi-threading / multi-core optimizations?

- types learned during expression processing?

- operator can optionally declare how the Vals are sorted?

- scan should have a lookup table of file suffixes and handlers?

- positional fields versus access to the full record?
- perhaps the 0'th field might represent the full record?

- hashmap should be able to spill out to disk via mmap()?

- integration with scorch TermFieldReaders as a Scan source or operator?
  - merge join by docNum / docId field?
  - UNFORNUTATELY, probably cannot compile a FTS conjunction/disjunction
    as the children of an FTS conjunction/disjunction
    are not known at compile time, unlike N1QL which has a compile-time
    bounded expr tree
    - so, it might be more similar to ANY x IN y ... END -- hardcoded codepath.

- merge join - COMPLEX with push-based engine...
  - merge join needs threading / locking / coroutines
    so that both children can feed the merge-joiner?

- merge join needs a skip-ahead ability?
  - idea: can introduce an optional lazy "SkipToHints" object or Vals
    that's passed down to operator's children?
    - an lzYieldVals callback can optionally add hints via
      something like lzSkipToHints[2] = lzSkipToVal which operator #2 can check?

- emit other languages?

