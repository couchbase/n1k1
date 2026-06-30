# patches/ — building the gated N1QL engine layer (`-tags n1ql`)

n1k1's `glue/` reuses Couchbase `query` for SQL++ parse+plan, then executes with
n1k1's own operators. As of the 2026 decouple work, the engine builds **pure-Go,
`CGO_ENABLED=0`, and cross-compiles** (linux/darwin/windows). Getting there
needs three small local fixups to the `query` module, since it isn't designed to
be consumed as an external module:

1. **Generated parser** — `query/parser/n1ql` ships the grammar (`n1ql.y`) but
   not the goyacc output (`y.go`, defining `yyParse`/`yySymType`). Generated at
   build time upstream and gitignored.
2. **Pure-Go `query/system`** (`query-system-stub.go.txt`) — the real one is cgo
   (sigar), pulled pervasively via `query/memory` ← `query/tenant` by nearly the
   whole query stack (even the parser). The stub returns benign memory stats.
3. **Enterprise semantics in community build** (`query-semantics-semchecker_ce.go.txt`)
   — a 1-line change so enterprise-level SQL++ (e.g. window functions) parses
   without the `enterprise` build tag (which would pull cgo deps like
   eventing-ee/V8). n1k1 implements these features itself.

## Recipe (iteration scaffold)

```bash
export GOFLAGS=-mod=mod GOPRIVATE='github.com/couchbase/*'

# 1. copy the pinned query module to a writable, gitignored local dir
QDIR=$(go list -m -f '{{.Dir}}' github.com/couchbase/query)
rm -rf tmp/query-local && cp -R "$QDIR" tmp/query-local && chmod -R u+w tmp/query-local

# 2. generate the parser
go install golang.org/x/tools/cmd/goyacc@latest
(cd tmp/query-local/parser/n1ql && "$(go env GOPATH)/bin/goyacc" n1ql.y && rm -f y.output)

# 3. drop in the two source patches
cp patches/query-system-stub.go.txt          tmp/query-local/system/systemStats.go
cp patches/query-semantics-semchecker_ce.go.txt tmp/query-local/semantics/semchecker_ce.go

# 4. point go.mod at the local copy
go mod edit -replace github.com/couchbase/query=./tmp/query-local
```

After T3 (dropping `query/execution`), glue's dependency graph no longer pulls
cbft/cbgt/indexing/n1fty/query-ee/gocbcrypto/eventing-ee, so those replaces are
no longer needed. The replace block in go.mod can be pruned to just `query`
(plus whatever the parse+plan slice still requires; `go build` will tell you).

## Build & test

```bash
CGO_ENABLED=0 go build -tags n1ql ./glue/... ./test/...   # builds, no cgo
CGO_ENABLED=0 go test  -tags n1ql ./glue ./test           # all green
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags n1ql ./glue/...  # cross-compiles
```

## The fork (github.com/couchbase/n1k1-query)

These patches live as real git commits in a published fork of couchbase/query:

    github.com/couchbase/n1k1-query
      main          - verbatim pinned snapshot (query @ v0.0.0-20260627002010)
      n1k1-pure-go  - main + 3 commits: gen parser, system stub, semchecker

The fork keeps its go.mod module path as github.com/couchbase/query (so its
internal imports and n1k1's `query/...` imports are unchanged); only the repo
URL differs. n1k1's go.mod pins it via a version replace:

    replace github.com/couchbase/query => github.com/couchbase/n1k1-query <pseudo-version>

So n1k1 builds with plain `go` (GOPRIVATE for git fetch) -- no local checkout
needed. Because glue/ is gated behind `-tags n1ql`, the default (core) build
never resolves query at all. After T3 the sibling-module replaces (cbft/cbgt/
indexing/n1fty/query-ee/...) were pruned -- only the query replace remains.

### Updating the fork to a newer query (recurring)

1. On the fork's `main`: replace contents with the new pinned query snapshot,
   commit. (Or add couchbase/query as a remote and merge.)
2. Re-create the `n1k1-pure-go` branch = main + the 3 patches (run steps 1-3 of
   the recipe above: goyacc the parser, apply patches/*.txt), commit, push.
3. In n1k1: `go get github.com/couchbase/n1k1-query@n1k1-pure-go` to resolve the
   new pseudo-version, then `go mod edit -replace github.com/couchbase/query=\
   github.com/couchbase/n1k1-query@<that-version>`.
4. Rebuild + test n1k1 (`make n1ql`). Tracking query's parser/algebra/
   expression/plan/planner IS how n1k1 stays current with SQL++, so a newer
   query may shift APIs n1k1 relies on -- expect occasional touch-ups in glue/
   (GlueContext, conv.go's plan.Visitor methods, or Fetch / expression.Context /
   value signatures). The goyacc step in (2) is the only part worth automating.

Note: `go mod tidy` does NOT work for n1k1 -- query's enterprise modules pin each
other at the v0.0.0-00010101 placeholder, which tidy can't resolve (the build
itself is fine: module-graph pruning never compiles those packages).
