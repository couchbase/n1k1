# n1k1 — design & internals

How n1k1 works internally, its performance approaches, and the running
TODO / ideas list. For the feature list and build/test instructions, see
README.md.

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
- push-based paradigm for shorter codepaths...
  - data transfer between operators (e.g., from scan -> filter ->
    project) is a function call, instead of a send/recv on channels
    between goroutines. These function calls can sometimes be inlined
    by n1k1's code generator, removing function call overhead.
  - the pull-based paradigm, a.k.a. the iterator approach, in
    contrast, involves additional checks for HasNext() and for errors
    across the operators in a query-plan.
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
  - based on Futamura projections / LMS (Rompf, Odersky) approaches.
  - implements operator fusion, for fewer function calls.
  - lifting vars is implemented to support resource reusability.
- expression optimizations for static parameters.
  - for example, with an expression on `sales < 1000`, the `1000` is
    evaluated early and a single time in a preparation phase, instead
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
    DISTINCT, GROUP BY, INTERECT, EXCEPT, etc).
  - couchbase/rhmap/store chunk files allow hash-join left-vals to be
    spilled out to temporary disk files when it becomes too large.
- error handling is push-based via an YieldErr callback...
  - the YieldErr callback allows n1k1 to avoid continual, conservative
    error handling checks ("if err != nil { return nil, err }").
- max-heap in ORDER-BY / OFFSET / LIMIT
  - reverse popping of the max-heap produces the final, ordered
    result, which avoids a final sort.
  - max-heap that becomes too large will spill to temporary files.
- INTERSECT DISTINCT / ALL and EXCEPT DISTINCT / ALL
  are optimized by reusing hash-join's machinery.
  - hash-join's probe map can optionally track information like...
    - all the left-side values (for hash-join).
    - a count of the left-side values (for INTERSECT ALL, EXCEPT ALL).
    - and/or a probe-count (for multiple use cases).
  - INTERSECT DISTINCT and EXCEPT DISTINCT can avoid using an
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
  - reuses a json.Encoder instance to avoid allocations.

