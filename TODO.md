# TODO

Gist only -- see README "Building the N1QL engine layer" for specifics.

## Next: revive the N1QL engine layer (glue/ + test/, behind `-tags n1ql`)

Investigated 2026/06. Distinct blockers:

- cgo (KEEP-execution path): a prebuilt libsigar.dylib ships in
  "Couchbase Server.app" + openssl@3 via brew; wiring CGO_CFLAGS/LDFLAGS (below)
  clears the cgo errors IF you build with CGO enabled and keep query/execution.
  NOTE: for a CGO_ENABLED=0 / cross-compile binary this is NOT the path -- see
  the "Decouple" section; cgo must instead be removed (stub query/system + T3).
- **goyacc parser-gen gap (always required)**: query/parser/n1ql ships the
  grammar (n1ql.y) but NOT the generated parser (yyParse/yySymType). Query's
  build.sh runs goyacc at build time and gitignores the output, so `go get`
  never produces it, and you can't generate into the read-only module cache.
- minor: cbft<->cbgt version drift (bump cbgt@master too) -- the usual
  "tags lag HEAD" chase.

CGO recipe that works on this machine:
  CGO_CFLAGS="-I<cb-src>/sigar/include -I$(brew --prefix openssl@3)/include"
  CGO_LDFLAGS="-L<Couchbase Server.app>/Contents/Resources/couchbase-core/lib \
               -lsigar -L$(brew --prefix openssl@3)/lib"

How to source a buildable query (solves the parser-gen gap) -- pick one:
- [ ] **Fast proof**: replace query => local cb checkout, run its build.sh
      once to gen the parser, build with CGO flags. Re-couples locally and
      drops the pinned-versions property, but proves n1k1 vs modern query now.
- [ ] **Reproducible** (keeps pinned versions): fork query (+cbft) at the
      pinned SHA, commit the generated parser, replace => fork. More setup;
      stays pure `go get`. (Also the place to stub cgo if ever wanted -- but
      cgo is easy, so don't.)

## Decouple toward a self-contained, pure-Go, CGO_ENABLED=0 binary

DONE so far (2026/06):
- [x] query/system stubbed to pure-Go (patches/query-system-stub.go.txt). Needed
      regardless: sigar is pulled pervasively via query/memory <- query/tenant by
      the WHOLE query stack (even the parser), so you can't decouple your way out.
- [x] T1: dropped query/server from glue/exec.go (was an unused param).
- [x] T2: dropped query/datastore/system from glue/stmt.go (Systemstore=nil).

- [x] **Fixed glue/ API drift**: conv.go plan.Visitor (new methods, removed
      types), semantics.GetSemChecker, planner.NewPrepareContext/Build,
      Fetch/WriteJSON/attachment/Descending signatures.
- [x] **T3: dropped query/execution**. glue.GlueContext (glue/context.go) embeds
      expression.IndexContext for expression.Context and implements
      datastore.Context; scan accessors replaced with constants; Fetch uses
      datastore.NULL_QUERY_CONTEXT. Dropped cbft/cbgt/indexing/n1fty/query-ee/
      gocbcrypto/eventing-ee. RESULT: CGO_ENABLED=0 build + tests GREEN,
      cross-compiles to linux/darwin/windows.

REMAINING for a truly shippable binary:
- [ ] **Reproducible query sourcing** (packaging): the build still uses a local
      patched copy of query (tmp/query-local, via go.mod replace) -- not
      reproducible for others. Fork couchbase/query at the pinned SHA, commit the
      3 patches (gen parser, system stub, semchecker_ce) from patches/, and
      replace => fork@sha. Then prune the now-unneeded replaces from go.mod
      (cbft/cbgt/indexing/n1fty/query-ee/etc. -- T3 dropped them).
- [ ] **A main()**: glue is a library; add a cmd/ that takes a SQL++ string +
      file datastore and prints results, to actually produce the downloadable binary.

SQL++ CURRENCY (key requirement) -- tension to respect:
  n1k1 stays current by tracking query's parser/algebra/expression/plan/planner
  (we keep those; that IS the SQL++ feature set). BUT staying current has costs:
  (a) every query bump => re-run goyacc (parser not shipped) -- automate it;
  (b) every query bump can change expression.Context / plan.Visitor / datastore
      signatures => the T3 hand-rolled context + conv.go visitor need touch-ups.
  So T3 trades "heavy deps, query maintains the context for us" for "lean deps,
  we maintain a context that drifts with query." Worth it for the binary, but
  budget recurring upkeep when chasing latest SQL++.

Note: parser-gen gap is build-time only -- it does NOT affect the shipped
binary's self-containedness (generated parser is baked in at compile time).

## After the engine compiles again
- [ ] Un-gate test/ and see what passes vs the new query version.
- [ ] Revisit the pre-existing SKIP tests: UNNEST + array-as-FROM
      (broke during the CB 6.5 -> 7 upgrade in 2021). `git grep SKIP`.
- [ ] Re-pin couchbase modules to ONE consistent manifest snapshot rather
      than per-module @master (current pins are ~contemporaneous, not exact).

## Housekeeping (low priority)
- [ ] Drop vestigial tmp/ symlinks + committed cpu.pprof / intermed_build.
- [ ] Consider deleting tmp/easy-to-read (regenerable via `make easy-to-read`).
