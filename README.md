n1k1 is an experimental query compiler and execution engine for N1QL.

-------------------------------------------------------
DEV SHORTCUT...

    go test . && go build ./cmd/intermed_build/ && ./intermed_build && go test ./... && go fmt ./... && go test -v ./...

-------------------------------------------------------
The way n1k1 works...

Or, how intermed_build generates a N1QL compiler...

- 1: First, take a look at the n1k1/*.go files.  You'll see a simple,
bare-bones interpreter for a "N1QL" query-plan.  In ExecOperator(), it
basically recursively walks down through a query-plan tree, and
processes it by pushing (or yield()'ing) data records from leaf nodes
(e.g., a scan) up to higher nodes in the query-plan tree.

- 1.1: As part of that, you'll also see some variables and functions
that follow a naming convention with "lz" (e.g., "lazy") in their
names.  The "lz" naming convention is a marker that tells us whether
some variables are lazy or late-bound (they need actual data records),
versus other variables that are early-bound (they use information
that's already available at query-plan compilation time).

- 1.2: Of note, the n1k1/*.go files are written in a careful subset of
golang.  It's all legal golang code, but it follows more rules and
conventions (like the "lz" conventions) to make parsing by n1k1's
intermed_build tool easy.

- 2: The intermed_build tool parses the "lz" conventions and other
markers (e.g., special code comments) from the n1k1/*.go source files
in order to translate that interpreter code into a intermediary,
helper package, called n1k1/intermed.

- 2.1: The n1k1/intermed package will be used later by the final n1k1
compiler.

- 2.2: The way the intermed_build tool works is that it processes the
n1k1/*.go source files line-by-line, and translates any "lz" lines
into printf's.  Non-lazy expressions are turned into printf'ed
placeholder vars.  Non-lazy lines are emitted entirely as-is, as they
are early-bound.

- 3: Finally, the n1k1 compiler, which uses that generated
n1k1/intermed package, will take the user's input of a N1QL query-plan
and will emit *.go code (or possibly other languages) that can
efficiently execute that query-plan.

------------------------------------------
Some features...

- lifting vars to avoid local closures
- capturing emitted code to avoid local closures

------------------------------------------
TODO...

- pipeline breakers / data staging nodes
- batching (or staging) optimizations?

- GROUP BY / aggregates
  - SELECT country, SUM(population) FROM ... GROUP BY country

- HAVING

- ORDER BY
  - operator can optionally declare which fields are sorted asc/desc?

- OFFSET / LIMIT

- integration with scorch TermFieldReaders as a Scan source or operator?
  - merge join by docNum / docId field?
  - need a skip-ahead ability?

- early stop when an error or LIMIT is reached?
  - YieldStats() can return an non-nil error, like ErrLimitReached

- early stop when processing is canceled?

- hash join?

- conversion of real N1QL query-plan into n1k1 query-plan

- SIMD optimizations possible?  see: SIMD-json articles?

- prefetching optimizations?

- compiled accessor(s) to a given JSON-path in a raw []byte value?

- the yield callback might return slice that the next yield
  can place data into, to avoid append-copying items?

- col versus row optimizations?
  - need base.Vals that allows for optional col based representation?
    - a single col is easy -- same as Vals?
    - need a merge-join & skip-ahead optimization?
  - YieldVals() might take []Vals instead of Vals?
    - that would allow an []Records interpretation?
    - or, an []Columns interpretation, using same signature?

- multi-threading / multi-core optimizations?

- types learned during expression processing?

- positional fields versus access to the full record?
- perhaps the 0'th field might represent the full record?

- emit other languages?
