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
