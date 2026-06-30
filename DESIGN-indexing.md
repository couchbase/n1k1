# Design: Integrating Indexes into n1k1

Status: proposal / for review

This document describes how to add index support — a GSI-like **secondary
index** first, then a **full-text index** via embedded bleve — to n1k1's
standalone SQL++ CLI (`cmd/n1k1`), **without** depending on cbft, cbgt, n1fty,
or cbauth and their distributed-systems machinery.

## Motivation

Today every n1k1 query over the file datastore is a full keyspace (primary) scan
plus a residual filter. We want the planner to use an index when one applies, so
that selective queries don't read the whole keyspace. The goal is to do this
in-process — no FTS cluster, no GSI service — keeping n1k1 self-contained.

## Background: how index selection actually works in n1k1

The central fact that shapes this whole design:

> **n1k1 has no planner of its own.** `glue/stmt.go:PlanStatement` calls
> cbq-query's real `planner.Build()`. Index selection — "index vs no index,
> which index" — is decided entirely by cbq-query's planner, driven by **what the
> datastore advertises** through the `Keyspace → Indexer → Index` / `FTSIndex`
> interface tree (`datastore/index.go`).

The query pipeline (`glue/session.go:Run`):

```
SQL++ → ParseStatement (n1ql parser) → algebra.Statement
      → PlanStatement (cbq planner.Build) → plan.Operator tree
      → conv.go (plan.Visitor) → base.Op tree
      → engine.ExecOp → glue datastore-scan/fetch ops → rows
```

Because the planner is cbq-query's, **giving it an index is a matter of having
the datastore advertise one.** Specifically:

- `planner/build_scan_secondary.go:sargableIndexes()` reads each
  `index.RangeKey()` (line 564) and calls `SargableFor(pred, …)` (line 599). It
  DNF-normalizes the `WHERE` clause, matches it against the index's key
  expressions (honoring the partial-index `Condition()`), builds `datastore.Span`s,
  and emits an `IndexScan` + `Fetch` + residual `Filter`.
- **GSI sargability is built into the core planner.** No external helper is
  needed for a secondary index — only the index's `RangeKey()` expressions.
- **FTS sargability is externalized** into the `datastore.FTSIndex` interface
  (`Sargable` / `SargableFlex` / `Pageable`), so a full-text index must provide
  it — but a small in-process shim suffices.

### Why n1fty is not required

n1fty bundles two responsibilities:

1. **Planner-facing sargability/metadata** — it implements `datastore.FTSIndex`
   so the planner can decide to use an FTS index.
2. **Runtime executor** — its `Search()` ships the request over GRPC/REST to a
   remote **cbft** cluster and streams back document keys/scores.

Only the *shape* of (1) is needed. (2) is replaced by an in-process
`bleve.Index.Search()`. So cbft, cbgt, n1fty, and cbauth all drop out. (n1fty
isn't even downloaded in this module today — it's a placeholder
`v0.0.0-00010101…` version — yet n1k1 builds; `blevesearch/bleve/v2` *is*
resolvable.)

### What already exists (so a secondary index is mostly a datastore problem)

- **Execution glue.** `glue/datastore.go` routes `"datastore-scan-index"` →
  `glue/datastore_scan.go:DatastoreScanIndex`, which evaluates `plan.Span`s
  (`EvalSpan`), calls `scan.Index().Scan(reqId, span, distinct, limit, cons,
  vector, conn)`, drains `conn.Sender().GetEntry()`, and yields `entry.PrimaryKey`
  to `datastore-fetch`. **No n1k1-side read-path changes are needed** for
  secondary scans.
- **Plan-op selection is by interface assertion.** `planner/spans_term.go:CreateScan`
  emits `plan.IndexScan3` only if the index is a `datastore.Index3` (line 49),
  `IndexScan2` only if `Index2` (line 106), else the base `plan.IndexScan`
  (line 133). Therefore **an index that implements only the base
  `datastore.Index` interface forces `plan.IndexScan`** — which
  `conv.go:VisitIndexScan` already converts. (`VisitIndexScan2/3` and
  `VisitIndexFtsSearch` currently return `NA()`.) `IndexApiVersion` stays at
  `INDEX_API_MAX`; it doesn't matter, because the interface gates the choice.
- **The file datastore lives in an editable fork.** `github.com/couchbase/query`
  is `replace`d with `github.com/couchbase/n1k1-query`, checked out at
  `../n1k1-query`. The file datastore is `datastore/file/file.go`; its
  `fileIndexer` already owns `indexes map[string]datastore.Index` and a working
  `primaryIndex` to model from. `fileIndexer.Indexes()` (line 816) currently
  returns only `fi.primary` — this must change to expose secondaries.
- **CREATE INDEX DDL is not wired** into n1k1's executor
  (`conv.go:VisitCreateIndex` returns `NA()`; n1k1 runs its own `base.Op` tree,
  not cbq's `execution` package). v1 defines indexes via a sidecar file.

## Phase 1 — GSI-like secondary index

All Phase 1 code is in the fork (`../n1k1-query/datastore/file/file.go`), except
the `go.mod` pin bump and a direct `bbolt` import in n1k1.

### Storage backing: `go.etcd.io/bbolt`

- **rhmap — rejected.** `rhmap/store.RHStore.Visit` iterates in hash-bucket
  order, not key order → no range scans. (n1k1 uses rhmap only for
  join/group/window spill.)
- **bbolt — recommended.** B+tree, persistent, `Cursor.Seek`/`Next` give exactly
  the ordered range iteration `Scan()` needs; one file per index; already in the
  module graph (`go.etcd.io/bbolt v1.4.0`, currently indirect — promote to a
  direct require). moss is a viable but heavier (LSM/compaction/goroutines)
  alternative we don't need for a read-mostly index.

**Key encoding.** bbolt orders by raw bytes (memcmp), which is *not* N1QL
collation order. v1: store each bbolt key as `encode(secondaryKey) + 0x00 +
docID` (the docID suffix disambiguates duplicate secondary values; value is
empty). On `Scan`, decode keys back to `value.Value` and do boundary/inclusion
checks via `value.Value.Collate` (the N1QL comparator). A fully order-preserving
byte encoder is a v2 optimization. **This collation correctness is the highest
risk — the `Collate` boundary check must be right.**

### Index definition & build (sidecar, since DDL isn't wired)

- **Definition:** a per-keyspace JSON sidecar
  `<datastoreDir>/<namespace>/<keyspace>/.indexes.json`:
  `[{ "name": "...", "keys": ["expr", ...], "where": "expr"? }]`.
- **Load:** in `newKeyspace` (file.go ~line 734), after the primary index is
  created, read `.indexes.json`; parse each key/where string with the n1ql
  expression parser; call a new `fileIndexer.createSecondaryIndex(name,
  rangeKeys, where)` that constructs the index, opens/creates its bbolt file
  (e.g. `.idx_<name>.bolt`), and registers it in `fi.indexes`.
- **Build (v1 = rebuild-on-open):** full keyspace scan (reuse the
  `primaryIndex.ScanEntries` directory-read pattern) → fetch each doc → evaluate
  the parsed key expressions (and `where`) against the doc `value.Value` → insert
  `(encodedKey → docID)` into bbolt. Gate behind sidecar presence so unindexed
  keyspaces are unaffected.

### The `secondaryIndex` type (base `datastore.Index` only)

Model on `primaryIndex`; hold a `*bbolt.DB` + bucket name + the parsed
`rangeKeys`/`where`.

| Method | Implementation |
|---|---|
| `KeyspaceId` / `Id` / `Name` / `Indexer` | trivial accessors |
| `Type()` | `datastore.GSI` |
| `IsPrimary()` | `false` |
| `SeekKey()` | `nil` |
| `RangeKey()` | the parsed key expressions — **drives sargability** |
| `Condition()` | the partial-index `where` expr, or `nil` |
| `State()` | `(datastore.ONLINE, "", nil)` |
| `Statistics()` | `(nil, nil)` — safe while `useCBO=false` |
| `Drop()` | remove from `fi.indexes` + delete bbolt file |
| `Scan(reqId, span, distinct, limit, cons, vector, conn)` | core — see below |

**`Scan()` contract** (per `datastore/index.go:712` and the drain loop in
`datastore_scan.go:175-184`): `defer conn.Sender().Close()`; open a read cursor;
`Seek` to `span.Range.Low` (or first if unbounded); iterate ascending; decode
key and stop at `span.Range.High`, honoring
`span.Range.Inclusion & datastore.LOW/HIGH` via `Collate`; for each match
`conn.Sender().SendEntry(&datastore.IndexEntry{PrimaryKey: docID})`; respect
`limit`. `EntryKey`/`MetaData` may be left empty — n1k1's drain reads only
`PrimaryKey` (covering-index `EntryKey` is a commented-out TODO in
`datastore_scan.go`).

### Step sequence (Phase 1)

1. (fork) Add the `secondaryIndex` type implementing base `datastore.Index`.
2. (fork) Add `fileIndexer.createSecondaryIndex(name, rangeKeys, where)`.
3. (fork) Implement the build routine (full-scan → eval keys → bbolt insert).
4. (fork) Implement `secondaryIndex.Scan()` (cursor seek/iterate/`Collate`).
5. (fork) Wire sidecar `.indexes.json` loading in `newKeyspace`.
6. (fork) **Fix `fileIndexer.Indexes()`** (file.go:816) to return primary **plus**
   all secondaries; verify `IndexNames`/`IndexIds`/`IndexByName` cover the map.
7. (fork) Commit; bump the `replace … => …/n1k1-query <newver>` pin in n1k1's
   `go.mod`; promote `go.etcd.io/bbolt` to a direct require.
8. (n1k1) Verify end-to-end. No n1k1 read-path code changes expected.

## Phase 2 — FTS via embedded bleve

The planner hook already exists (`planner/build_scan_search.go` + the
`SargableFlex` path); set `useFts=true` in `glue/stmt.go:PlanStatement`. Because
FTS sargability is externalized into `datastore.FTSIndex`, we provide it — with a
small in-process shim, not n1fty.

- Implement an `Indexer` + `FTSIndex` (in the fork's file datastore alongside the
  secondary index, or in a small new datastore package) backed by an embedded
  `bleve.Index`:
  - `Sargable(field, query, options, mappings)` / `SargableFlex(req)` /
    `Pageable(...)` — answer from the bleve index mapping. **Salvage** n1fty's
    predicate→bleve-query mapping logic (the fiddly part) rather than depending on
    the package.
  - `Search(reqId, searchInfo, cons, vector, conn)` — run `bleveIndex.Search()`
    locally and push `datastore.IndexEntry{PrimaryKey: docID, MetaData: score}`
    into `conn.Sender()` — the same drain pattern Phase 1 uses.
- **conv.go gap:** `VisitIndexFtsSearch` currently returns `NA()`. Implement it
  (plus a `datastore-scan-fts` execution op mirroring `DatastoreScanIndex`) so
  the `plan.IndexFtsSearch` the planner emits for `SEARCH()` is converted.
- Definition/build: extend the `.indexes.json` sidecar with FTS specs (or add a
  separate `.fts.json`); build the bleve index on open from a full scan.

## Verification

- **Phase 1:** create a keyspace with a `.indexes.json` over a field, run a query
  whose `WHERE` matches the index's leading key, and confirm via the CLI's plan
  output (`Result.Plan`) that it is an **`IndexScan`, not `PrimaryScan`**, and
  that results match the same query without the index. Run
  `go test -tags n1ql ./glue/...` and the conformance harness in `test/`.
- **Phase 2:** a `SELECT … WHERE SEARCH(ks, "…")` query returns the expected docs
  with scores, with no cbft/network calls (purely local bleve).

## Risks & open questions

- **Collation correctness (highest).** bbolt byte-order vs N1QL `Collate`.
  Mitigate with decode-and-`Collate` boundary checks in v1; order-preserving
  encoder is v2.
- **Plan-op assertion assumption.** Relies on `spans_term.go:CreateScan` choosing
  base `IndexScan` for a base-only index (verified in the current fork). A future
  cbq rebase that changes this would require implementing
  `conv.go:VisitIndexScan2/3`.
- **Index freshness.** File datastore mutations (`performOp`, `Delete`) have no
  index-maintenance hooks. v1 = rebuild-on-open (document the index as static);
  incremental maintenance on insert/update/delete is v2.
- **Composite indexes.** v1 targets single-leading-key indexes; confirm
  `EvalSpan` / `DatastoreScanIndex` handle multi-column `Range.Low/High` arrays
  before doing composite keys.
- **CBO.** `Statistics()` returning nil is safe while `useCBO=false`; revisit if
  CBO is ever enabled.

## Affected files

- `../n1k1-query/datastore/file/file.go` — `secondaryIndex`,
  `createSecondaryIndex`, build, sidecar load, fix `Indexes()`; later the bleve
  `FTSIndex`.
- `glue/datastore_scan.go` — verify-only for Phase 1; add `datastore-scan-fts` in
  Phase 2.
- `glue/conv.go` — verify-only for Phase 1 (`VisitIndexScan` works); implement
  `VisitIndexFtsSearch` in Phase 2; `VisitCreateIndex` is a future DDL hook.
- `glue/stmt.go` — leave `IndexApiVersion` as-is for Phase 1; set `useFts=true`
  in Phase 2.
- `go.mod` — bump the `n1k1-query` pin; add direct `go.etcd.io/bbolt` (Phase 1)
  and `blevesearch/bleve/v2` (Phase 2).
