n1k1 is a prototype query-plan interpreter and compiler for N1QL.

------------------------------------------
Latest...

2026/06 - Modernized, and made the N1QL engine pure-Go. Upgraded to
Go 1.25 + go modules with pinned versions (the old setup symlinked into
a couchbase-server checkout). The self-contained core builds/tests
cleanly by default. The N1QL-engine layer (glue/ + test/), gated behind
the "n1ql" build tag, now builds with CGO_ENABLED=0 and cross-compiles,
by decoupling from query/execution and building against a small patched
fork of couchbase/query. See "Building & testing" below.

2021/12 - While upgrading from CB 6.5 query to CB 7 query, UNNEST stopped
working and array-as-FROM source stopped working, leaving SKIP-prefixed
unit tests. (Resolved by the 2026/06 modernization -- all un-skipped and
passing; see above.)

------------------------------------------
## Building & testing

### TL;DR -- copy/paste

    # Core (self-contained, no external setup):
    make                       # regenerate intermed/ + run core tests
    go test ./...              # just the core tests

    # N1QL engine layer (needs GOPRIVATE for the couchbase/query fork):
    export GOPRIVATE='github.com/couchbase/*'
    make test-all              # build + test glue/ + test/ (pure-Go, CGO off)
    make test-glue             # just the glue/ unit tests
    make test-suite            # just the 600+ SQL++ conformance-suite cases, verbose
    make test-compiler         # generate Go from the compiler + run the generated tests

    # Same, spelled out without make:
    CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./glue ./test
    CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql -v -run TestSuiteCases ./test

`make test-suite` prints a summary plus a grouped table of the expected
non-pass cases (and flags any unexpected regression); see the conformance note
below. Details for each layer follow.

### Core (default -- no external setup)

    go build ./...
    go vet ./...
    go test ./...

`make` runs the same core flow (and regenerates intermed/ via the
intermed_build codegen tool).

### N1QL engine layer (glue/ + test/, gated behind the "n1ql" tag)

This layer reuses couchbase/query for SQL++ parse+plan, then executes with
n1k1's own operators. It builds pure-Go (CGO_ENABLED=0) against a small
patched fork of couchbase/query -- github.com/couchbase/n1k1-query -- pinned
via a go.mod `replace` (no local checkout needed; see patches/README.md for
how the fork is maintained):

    export GOPRIVATE='github.com/couchbase/*'    # couchbase modules are fetched over git

    CGO_ENABLED=0 go build -tags n1ql ./glue/... ./test/...
    CGO_ENABLED=0 go test  -tags n1ql ./glue ./test

Cross-compile the (cgo-free) engine to any target:

    CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 go build -tags n1ql ./glue/...
    CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 go build -tags n1ql ./glue/...
    CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -tags n1ql ./glue/...

`make test-all` runs the engine build + tests; `make test-glue` runs just the
glue/ unit tests.

The n1ql layer includes TestSuiteCases, which runs the SQL++ conformance suite:
the upstream couchbase/query corpus (from their test/filestore tests), vendored
under test/suite/ -- {query, expected-results} cases over a small JSON dataset --
against n1k1. ("suite" because it's a data-driven set of cases stored as files,
run over glue.FileStore; it isn't itself a test of file-store features.) n1k1
implements a subset of N1QL, so it's a pass-rate guard, not 100%: ~632 of ~680
runnable cases currently pass, and the test fails if that count regresses (ratchet
it up as coverage grows). Three case shapes are run: {statements, results}
(compare rows), {statements, matchStatements} (two statements must yield equal
results), and {statements, error} as negative tests (n1k1 reuses query's parser/
planner, so it should reject an invalid query with the same error -- a PASS is
when its error text matches the corpus's). Use `make test-suite` (or add `-v -run
TestSuiteCases` yourself) to see: a per-case table of the non-pass cases (status,
category, loc, full SQL -- EXPLAIN clipped); a prominent PANICS section (the
engine should never panic -- these are bugs, surfaced separately from the benign
UNSUPPORTED count); the full statement of each "exotic" case skipped because it
isn't one of the three runnable shapes (resultset/prepared/pre-post/etc.); a
summary (PASS rows / PASS error-rejected / UNSUPPORTED / PANIC / FAIL / exotic);
a grouped table of the expected non-pass cases (with a short why for each group);
and any unexpected regressions. The accepted non-pass cases are enumerated in the
`expectedNonPass` table in test/suite_test.go -- shrink it as coverage grows.

`make test-compiler` exercises the n1k1 *compiler* (not just the interpreter).
Two generators emit Go source into test/tmp/ (gitignored), then that package is
compiled and run -- its TestGeneratedN / TestGeneratedFS_N funcs execute the
*compiled* query and compare results:

- TestCasesSimpleWithCompiler -- the hand-built TestCasesSimple Op trees.
- TestSuiteWithCompiler -- a *differential* test: Op trees the glue layer derives
  from real SQL++ conformance-suite queries are compiled and checked against the
  same expected results the interpreter is checked against (~600 cases, covering
  WHERE / ORDER BY / GROUP BY / HAVING / DISTINCT / UNNEST / aggregates over real
  datastore scans). Two things make a conv-derived plan expressible as generated
  source:
  - Expressions: the conv emits live query/expression objects (exprTree); these
    are serialized back to N1QL text (exprStr) and re-parsed at runtime.
  - Datastore scans/fetches: the op is baked to a Go literal and run as a
    glue.DatastoreOp "island"; its int params are vars.Temps indices, and the
    live query-plan objects are supplied at the generated program's runtime by
    test.SetupCompiledSuite (which rebuilds vars.Temps). So the compiled code
    carries the SQL++ shape and the datastore is passed in as runtime data.
  Excluded: non-deterministic cases (NOW_*/CLOCK_*/UUID/random), and plans using
  the temp-*/sequence ops (subquery/LET machinery) -- not yet bridged.

The two steps are ordered so the generated files are never stale.

NOTE: do not run `go mod tidy` -- query is reached only via the n1ql-gated
glue/, so tidy would prune it (tidy doesn't enable custom build tags).

------------------------------------------
## Some features...

- types: MISSING, NULL, boolean, number, string,
         array, object, UNKNOWN (BINARY).
- comparisons follows N1QL type comparison rules.
- glue integration with existing couchbase/query/expression package.
- join nested-loop inner.
- join nested-loop left outer.
- join hash-eq inner.
- join hash-eq left outer.
- join ON expressions.
- join ON KEYS.
- UNNEST inner.
- UNNEST left outer.
- NEST nested-loop inner.
- NEST nested-loop left outer.
- NEST ON KEYS.
- WHERE expressions.
- projection expressions.
- ORDER BY multiple expressions & ASC/DESC.
- ORDER-BY / OFFSET / LIMIT.
- DISTINCT.
- GROUP BY on multiple expressions.
- aggregate functions: COUNT, SUM, MIN, MAX, AVG.
- HAVING, by reusing the same filter operator as WHERE.
- WINDOW functions.
  - aggregate functions: COUNT().
  - numbering functions: ROW_NUMBER, RANK, DENSE_RANK
  - navigation functions:
    - FIRST_VALUE, LAST_VALUE, NTH_VALUE, LEAD, LAG.
  - window frame OVER types: ROWS, GROUPS, RANGE (ASC).
  - window frame clause...
    - preceding: UNBOUNDED, CURRENT ROW, numeric offset.
    - following: UNBOUNDED, CURRENT ROW, numeric offset.
    - exclude: NO OTHERS, CURRENT ROW, GROUP, TIES.
  - window partitions spill to the disk if too large.
- UNION ALL is concurrent, with the contributing child operators
  having their own goroutines.
- UNION DISTINCT is supported by sequencing DISTINCT after UNION ALL.
- INTERSECT DISTINCT / INTERSECT ALL.
- EXCEPT DISTINCT / EXCEPT ALL.
- temp table operators.
- sequence operator.
- subqueries (uncorrelated) by capturing the subquery
  into a temp table, which a later sequence'd operator can retrieve.
- data-staging / pipeline-breakers with concurrent children.
- nested object paths (e.g. locations/address/city).
- scans of simple files (CSV's and newline delimited JSON).
- automatic spilling from memory to temporary files...
  - hashmaps (for joins, distinct, group-by, etc).
  - max-heap's (for sorting).
  - position addressable sequence of appended []byte entries.
- runtime variables / context passed down through ExecOp().

-------------------------------------------------------
## DEV SHORTCUTS...

See Makefile

-------------------------------------------------------
## Design & internals...

See DESIGN.md for how the n1k1 compiler works, the performance
approaches, etc.
