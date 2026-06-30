# patches/ — building the gated N1QL engine layer (`-tags n1ql`)

n1k1's `glue/` reuses Couchbase `query` for SQL++ parse+plan. Consuming `query`
as an external module needs two local fixups it doesn't ship:

1. **Generated parser** — `query/parser/n1ql` ships the grammar (`n1ql.y`) but
   not the goyacc output (`y.go`, defining `yyParse`/`yySymType`). It's
   generated at build time and gitignored upstream.
2. **Pure-Go `query/system`** — the real one is cgo (sigar) and is pulled
   pervasively via `query/memory` ← `query/tenant` by the *entire* query stack
   (even the parser). `query-system-stub.go.txt` here is a pure-Go drop-in
   replacement so n1k1 can target `CGO_ENABLED=0`.

## Recipe (iteration scaffold)

```bash
# 1. copy the pinned query module to a writable, gitignored local dir
QDIR=$(GOPRIVATE='github.com/couchbase/*' go list -m -f '{{.Dir}}' github.com/couchbase/query)
rm -rf tmp/query-local && cp -R "$QDIR" tmp/query-local && chmod -R u+w tmp/query-local

# 2. generate the parser
go install golang.org/x/tools/cmd/goyacc@latest
(cd tmp/query-local/parser/n1ql && "$(go env GOPATH)/bin/goyacc" n1ql.y && rm -f y.output)

# 3. drop in the pure-Go system stub
cp patches/query-system-stub.go.txt tmp/query-local/system/systemStats.go

# 4. point go.mod at the local copy + align co-developed module versions
export GOFLAGS=-mod=mod GOPRIVATE='github.com/couchbase/*'
go mod edit -replace github.com/couchbase/query=./tmp/query-local
go get github.com/couchbase/cbft@master github.com/couchbase/cbgt@master \
       github.com/couchbase/cbauth@master github.com/couchbase/gomemcached@master \
       github.com/cloudfoundry/gosigar@latest
```

## KNOWN REMAINING BLOCKERS (as of 2026/06 — not yet resolved)

The above gets the deps to RESOLVE, but the gated build does NOT yet succeed:

- **cbft is cgo** (`c_malloc.go` / jemalloc `cHeapAlloc`), pulled via
  `query/execution`. So `CGO_ENABLED=0` is impossible while `glue/` imports
  `execution`. Reaching a pure-Go binary REQUIRES T3 (drop `query/execution`).
- **glue/ has 2019→2026 API drift** vs current query: `plan.Visitor` gained
  methods (`VisitAlterBucket`, …), `plan.ParentScan`/`FinalProject` removed,
  `keyspace.Fetch` / `value.WriteJSON` / `Descending` signatures changed.
  `glue/conv.go` + `glue/datastore_fetch.go` must be updated to compile.

See ../TODO.md for the plan.
