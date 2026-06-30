# TODO

Forward-looking only. What's DONE is in TODO-done.md; how the engine is built &
patched is in patches/README.md; internals + the design-era idea backlog are in
DESIGN.md; build/test commands are in README.md.

Status: modernization + a pure-Go N1QL engine (CGO_ENABLED=0, cross-compiles)
are done. Remaining work:

## Shipping
- [x] Pushed the query fork to github.com/couchbase/n1k1-query; go.mod now pins
      `replace github.com/couchbase/query => github.com/couchbase/n1k1-query
      <pseudo-version>`, so n1k1 builds with plain `go` (+ GOPRIVATE) -- no local
      checkout needed.
- [ ] Add a cmd/ main(): take a SQL++ string + a file datastore and print
      results -- the actual downloadable binary. glue/ is library-only today.

## Cleanups
- [ ] go.mod indirect block is stale (still lists the dropped cbft/cbgt/
      indexing/n1fty/query-ee/... subtree). Can't `go mod tidy` -- it would
      prune the n1ql-gated query. Cosmetic; fix if/when query becomes an
      untagged dep.
- [ ] Vestigial files: tmp/ symlinks, committed cpu.pprof, intermed_build binary.
- [ ] tmp/easy-to-read is regenerable via `make easy-to-read` -- consider deleting.

## Tests / dependencies
- [ ] Revisit the pre-existing SKIP tests: UNNEST + array-as-FROM (broke in the
      2021 CB 6.5 -> 7 upgrade). `git grep SKIP`.
- [ ] Re-pin the couchbase modules to ONE consistent manifest snapshot instead
      of per-module @master (current pins are ~contemporaneous, not exact).

## Keeping current with SQL++ (recurring upkeep)
n1k1 tracks query's parser/algebra/expression/plan/planner -- that IS the SQL++
feature set. Each query bump costs: (a) re-run goyacc, since query doesn't ship
the generated parser; (b) maybe touch up glue's GlueContext / conv.go visitor
when expression.Context / plan.Visitor / datastore signatures drift. Worth
automating (a) into the fork's update flow.

## Bigger / someday
- [ ] The design-era feature & optimization backlog lives in DESIGN.md "## TODO"
      (CONNECT BY, PIVOT, merge join, SIMD, NUMA, correlated subqueries, ...).
