# Done

Gist only -- details live in commit messages, README, and code comments.

## 2026/06 -- build modernization (dusted off after ~5 years)
- Go 1.17 -> 1.25; GOTOOLCHAIN=auto fetches it.
- go mod: dropped local-symlink replaces, pinned ~25 couchbase modules
  to real versions. No more dependency on a local couchbase-server tree.
  - Recipe: GOPRIVATE=github.com/couchbase/* + replace => realversion to
    kill the v0.0.0-00010101 placeholders. cbauth/gomemcached/cbft @ master.
- Core (root pkg, base/, intermed/, cmd/) builds + vets + tests clean.
  - Fixed real go vet issues (op_scan.go Errorf, op_window.go self-assign).
- N1QL engine layer (glue/ + test/) gated behind `//go:build n1ql`, so the
  default build is green. tmp/easy-to-read gated `//go:build ignore`.
- Makefile: default = core flow; `make n1ql` = deferred engine build.
- README: documents the whole recipe + the don't-`go mod tidy` warning.

## 2026/06 -- engine-layer feasibility investigation (no code change)
- Proved cgo is EASY: prebuilt libsigar in Couchbase Server.app + openssl@3
  via brew; wiring CGO_CFLAGS/LDFLAGS clears all sigar/openssl errors.
- Found the real blocker is the goyacc parser-gen gap in query/parser/n1ql
  (generated yyParse/yySymType not shipped by `go get`). See TODO.md.

## 2026/06 -- decouple work started (toward pure-Go CGO_ENABLED=0 binary)
- Stubbed query/system to pure-Go (patches/query-system-stub.go.txt) -- sigar is
  pulled pervasively via query/memory<-query/tenant, so it must be stubbed.
- T1: dropped query/server from glue/exec.go (unused param).
- T2: dropped query/datastore/system from glue/stmt.go (Systemstore=nil).
- Corrected an over-claim: stub+T1+T2 is NOT pure-Go yet. cbft (via execution)
  is cgo (jemalloc) + cbgt->gosigar needs darwin cgo, so CGO_ENABLED=0 REQUIRES
  T3 (drop execution). Also glue/ has 2019->2026 query API drift to fix. See TODO.md.

## 2026/06 -- glue API drift fixed + T3 DONE (pure-Go engine achieved)
- Fixed glue/ 2019->2026 query API drift (plan.Visitor new methods, removed
  types, semantics/planner/Fetch/WriteJSON/attachment signature changes).
- T3: replaced query/execution.Context with glue.GlueContext (embeds
  expression.IndexContext; implements datastore.Context). Dropped execution ->
  the whole cbft/cbgt/indexing/n1fty/query-ee/gocbcrypto/eventing-ee subtree.
- RESULT: `CGO_ENABLED=0 go test -tags n1ql ./glue ./test` is GREEN, and the
  engine cross-compiles to linux/darwin/windows. 3 query patches needed
  (parser-gen, system stub, semchecker enterprise) -- see patches/.

## 2026/06 -- local query fork wired in (reproducible build, staying local)
- Created sibling repo ../n1k1-query as a real git fork: main = pinned
  snapshot; n1k1-pure-go branch = base + the 3 patches as separate commits.
- go.mod: replace query => ../n1k1-query; pruned all sibling-module
  replaces (T3 dropped them) -- only the query replace remains.
- Verified: core default build green, n1ql tests green CGO_ENABLED=0, and a
  missing fork doesn't break the default build (query is n1ql-gated). GitHub
  push of the fork deferred (staying local for now). See [[TODO]].

## 2026/06 -- polish
- Renamed the fork dir query-n1k1-fork -> n1k1-query (shared n1k1- prefix);
  updated the go.mod replace + docs.
- README/Makefile: concise copy-paste build/test instructions; `make n1ql` now
  CGO_ENABLED=0 on ./glue ./test.
- Fixed `go test ./...`: the generated test/tmp compiler test now emits
  `//go:build n1ql` (it imports the gated glue), so the default build is clean.
- Split README.md -> DESIGN.md (internals/performance/idea-backlog moved out;
  README keeps intro, changelog, build/test, feature list).
- Fixed `make benchmark-expr-eq` (needs -tags n1ql + CGO_ENABLED=0; benchmarks
  live in the gated test/).

## 2026/06 -- fork published to GitHub (build no longer needs a local sibling)
- Fork pushed to github.com/couchbase/n1k1-query (main + n1k1-pure-go branches).
- go.mod swapped from `replace query => ../n1k1-query` to a version replace
  `=> github.com/couchbase/n1k1-query <pseudo-version>`. The fork keeps module
  path github.com/couchbase/query, so Go maps it to the query import path; n1k1
  builds with plain `go` (+ GOPRIVATE), verified independent of the local sibling.

## 2026/06 -- cleanup pass (one done, two investigated WON'T-FIX)
- Vestigial files: cpu.pprof, the intermed_build binary, and tmp/ are all
  gitignored (never committed) -- removed the local cruft + dead tmp/ symlinks;
  nothing to commit.
- WON'T-FIX, go.mod indirect block: can't `go mod tidy`. tidy considers
  couchbase's enterprise-tagged imports (query/planner -> query-ee, query/tenant
  -> regulator, ...) pinned at the v0.0.0-00010101 placeholder; giving the fork's
  placeholders real versions just cascades (cbft/n1fty -> cbgt@00010101, ...).
  The community build is fine -- module-graph pruning never compiles those, so
  the entries are harmless graph-mirror noise. Verified empirically, reverted.
- WON'T-FIX / moot, "re-pin to one consistent manifest snapshot": same root
  cause, and moot post-T3 (the drift-prone heavy modules aren't compiled; the
  versions that matter come from the fork's go.mod -- itself one snapshot).

## 2026/06 -- MISSING-projection omit + harness key-order (conformance 530 -> 594)
- Real fix: glue.ExprTree now yields an empty val (base.ValMissing) when a
  projected expression evaluates to MISSING, so ConvertVals omits the field
  ({} not {"k":null}) -- matching N1QL. (NULL is still kept.) This fixed the
  ARRAY .. FOR comprehensions, array slicing (children[0:2]), and many other
  projections over docs with absent fields. 530 -> 572.
- Harness fix: TestFilestoreCases now canonicalizes object key order on BOTH
  sides (recursive unmarshal->marshal); n1k1 doesn't sort keys, so ~22 cases
  were false-negatives. 572 -> 594. Pass-floor bumped to 594.

## 2026/06 -- fixed keyspace-alias projection (conformance 358 -> 530)
- Root cause of the ANY..SATISFIES failures (and broad field-projection
  breakage): conv.go labeled fetch/join/unnest docs with KeyspaceTerm.As()
  (the EXPLICIT alias, empty for an unaliased `FROM contacts`), but query's
  planner qualifies expressions with the EFFECTIVE alias (`contacts.name`).
  So `contacts.name` couldn't resolve against a "."-labeled doc -> empty rows.
- Fix: use Term().Alias() (effective: explicit, else keyspace name). This
  fixes field projection AND makes SELECT * wrap under the keyspace alias
  ({"contacts": {...}}), matching N1QL.
- Conformance jumped 358 -> 530 / 672; the ANY/EVERY..SATISFIES bucket is
  essentially resolved. Updated the 4 existing SELECT * tests for the (now
  conformant) wrapped output and bumped the pass-floor to 530.

## 2026/06 -- upstream filestore conformance corpus runs against n1k1
- Vendored the couchbase/query "filestore" test corpus (test/filestore/ --
  24 datasets / 510 docs / 46 case files of {statements, results}).
- New TestFilestoreCases harness (test/filestore_test.go) parses+plans+converts+
  executes each statement through n1k1's own ops and compares (multiset
  canonical-JSON) to the expected results, classifying pass / fail / unsupported.
- 358 of ~672 runnable cases PASS (the rest: ANY..SATISFIES result mismatches,
  EXPLAIN / index-union-scan unsupported, a couple UNNEST+GROUP-BY panics).
  A pass-floor assertion guards against regressions; ratchet up as coverage grows.

## 2026/06 -- un-skipped the pre-existing SKIP tests (all pass)
- The 6 SKIP-prefixed tests from the 2021 CB 6.5->7 breakage (UNNEST x3,
  GROUP BY SUM x2 = "TermerPanic"; array-as-FROM + WHERE = "Results3Not1")
  now PASS as-is -- the breakage was resolved by the modernization + T3 decouple
  + glue API-drift fixes. Just stripped the SKIP<reason>_ prefixes; no code
  change needed. No SKIP markers remain in the tree.
