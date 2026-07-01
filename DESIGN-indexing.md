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

### Why not Parquet/Iceberg/Delta as the index store?

Since those columnar formats offer ordering and stats, it's tempting to build the
secondary index *on* them. Short answer: use them for **coarse pruning**, not as
the fine-grained ordered index. Two different things get called "index":

- **Clustered / data-skipping** (coarse, format-native): sort/cluster the data by
  the key on write, then skip blocks that can't match. Columnar formats are
  excellent here — Parquet/ORC footer **min/max stats**, the **page/column index**
  (per-page min/max → binary-search-like skipping on a *sorted* column), and
  **bloom filters** (equality); Iceberg/Delta add a cross-file **manifest** layer
  (per-file min/max, sort orders / Z-order, and Iceberg **Puffin** sidecars for
  sketches, bloom filters, and V3 deletion vectors). In Go, `arrow-go` reads/writes
  bloom filters + exposes stats/page-index (v18.3.0+); `parquet-go/parquet-go` too.
  **This is exactly the "index-everything-lite" tier** below and the manifest
  zone-maps in `DESIGN-data.md §5` — and it needs no cbq planner changes (pruning
  is a scan-layer concern).
- **Secondary, non-clustered, ordered index** (fine-grained GSI/b-tree): a compact
  `key → docID` map with O(log n) seek to an *arbitrary* key, point-precise and
  cheaply mutable. Columnar formats are **not** a substitute: pruning granularity
  is row-group/page/file (not row), files are immutable (per-row upsert/delete is
  merge-on-read, not index maintenance), and neither Iceberg nor Delta has an
  ordered-secondary-index spec (Puffin carries sketches/bitmaps/deletion-vectors,
  not a b-tree). Writing `(key, docID)` as a sorted Parquet file and binary-
  searching row groups just reimplements a b-tree, badly, on immutable storage.
  **bbolt is the right tool** for this. (Avro is row-oriented — no columnar
  pruning at all; it's for append logs/manifests, not indexes.)

**Use the columnar libraries to *help build* the bbolt index, not to *be* it:**
(1) **build accelerator** — read only the key column + a row locator via Arrow
projection pushdown, far cheaper than reading whole rows; (2) **metadata layer** —
Iceberg manifests/Puffin can serve as the change-detection + zone-map + per-file
bloom layer (potentially replacing the bespoke `.n1k1` manifest); (3) **doc-ID** —
for columnar sources the natural key is `file#row_position` (what Iceberg
position-deletes use), matching `DESIGN-data.md §6`.

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

## "Index everything": dynamic / wildcard / automatic secondary indexes

bleve's **dynamic mapping** = "index every field, choosing the structure by
type" (for bleve, text → inverted index). The natural question: what's the
**B-tree / GSI equivalent** — "index every scalar path with an ordered/range
structure so any `WHERE`/`ORDER BY` on any field is fast"? There's strong prior
art, in three families.

### Prior art

**Eager "index everything up front":**
- **Azure Cosmos DB — the closest analog, and it's the default.** Cosmos DB
  **automatically indexes every property of every item**, with no schema or
  secondary-index setup; the default policy enforces a **range index** (an ordered
  tree-like structure) on every string/number path — i.e. exactly the B-tree
  equivalent of bleve dynamic mapping, *on by default*. Under the hood it's an
  **inverted index mapping each JSON path → the items that contain that value**,
  with three index kinds: **Range** (scalar range/equality/`ORDER BY`),
  **Composite** (multi-property `ORDER BY` / multi-filter), and **Spatial** (geo).
  The model is **opt-out**: `excludedPaths` / `includedPaths` with a `/*` wildcard,
  most-precise path wins. This is the design to emulate for "everything indexed by
  default."
- **MongoDB wildcard index `{"$**": 1}`** — eager index of every field path
  (traverses embedded docs/arrays); supports include/exclude subtrees
  (`wildcardProjection`). Caveats that matter: the planner can use it for **only
  one predicate field per query**, it can't do equality on whole objects/arrays,
  array handling is subtle, and it's slower than a targeted index. Opt-in (you
  create it), unlike Cosmos's default.
- **Elasticsearch / Lucene dynamic mapping** — closest to bleve's own model:
  auto-detect the field type and pick the structure per type — numeric/date/geo →
  **BKD tree (points)**, the range workhorse; keyword → doc-values; text →
  analyzed inverted. Lesson: "index everything" should **route by inferred type**,
  not force one structure onto all fields.
- **PostgreSQL GIN on `jsonb`** — a single inverted index over *all* key/value
  pairs of a document (containment `@>`, existence `?`, equality). "Index
  everything" for JSON, but inverted — good for equality/containment, weak for
  ranges (`jsonb_path_ops` is the compact variant).

**Cheap always-on *approximate* "index-everything" (prune, don't seek):**
- **BRIN / min-max / zone maps** (Postgres BRIN, Oracle zone maps, Netezza,
  Infobright data packs, ORC/Parquet stats, MonetDB) — summarize each block by
  min/max; tiny enough to keep on *every* column; prune blocks/files that can't
  match a predicate.
- **Parquet column bloom filters** (Split Block Bloom Filter) — per-column-chunk
  bloom for equality/point lookups on high-cardinality columns (IDs/UUIDs);
  30–50× point-lookup speedups reported. "Index every column for equality,"
  cheaply.

**Adaptive / workload-driven ("index what's queried," not everything):**
- **Oracle Automatic Indexing (19c)** — background task (~every 15 min) creates
  candidate indexes *invisible*, verifies they actually improve queries, then
  makes them visible; drops unhelpful ones.
- **Azure SQL automatic tuning** — auto create/drop indexes from workload; drops
  unused/duplicate indexes over time.
- **RavenDB auto-indexes** — the doc-DB take: when a query has no matching index,
  it auto-creates one, then merges/garbage-collects unused ones.
- **SQLite automatic (transient) indexes** — builds a throwaway B-tree for the
  duration of a single query when it beats repeated scanning.
- **Database cracking / adaptive indexing** (Idreos, MonetDB) — the index
  self-organizes as a *side effect* of query processing; each range query "cracks"
  the column into progressively-sorted pieces.

### Recommendation for n1k1 — three tiers, mapped to our machinery

1. **Default "index-everything-lite": always-on zone maps + optional per-file
   bloom filters** at the scan/datastore layer (the *approximate* family). This
   fits n1k1 today: cheap, always-on, and — crucially — **needs no cbq-query
   planner changes**, because pruning is a datastore/scan concern (skip a file
   whose min/max range or bloom filter rules out the predicate), *not* planner
   index-selection. It's already half-designed: it's exactly the manifest zone
   maps in `DESIGN-data.md` §5. Recommended as the pragmatic "index everything"
   default.
2. **Adaptive auto-index (RavenDB/Oracle-style)** as the self-managing GSI: log
   the predicates / residual filters the planner produces, and
   auto-`createSecondaryIndex` an ordinary ordered index for the hot field(s), GC
   unused ones. Big advantage: the created index is a **normal `RangeKey` index
   the cbq planner already understands** (Phase 1 machinery) — so this needs **no
   wildcard-planner work**. Recommended as the realistic medium-term path.
3. **Eager wildcard GSI (Cosmos/Mongo-style)** — a bbolt store keyed
   `encode(path) + encode(value) + docID` so any single-path equality/range is
   contiguous. Feasible to *build*, but the hard part is **planner integration**:
   cbq-query's `sargableIndexes` matches predicates against a *fixed*
   `index.RangeKey()` and has no concept of a wildcard index covering arbitrary
   paths (Cosmos/Mongo have bespoke wildcard planner support; cbq-query doesn't).
   A true wildcard GSI would need fork-side planner work — recognize a wildcard
   index and synthesize a `RangeKey`/span from whatever path the predicate names —
   and inherits Mongo's caveats (one field per query, no whole-object equality,
   array subtleties). Flag as a research / hard item, not Phase 1.

**Symmetry with FTS:** bleve dynamic mapping already gives "index all text" for
free. So n1k1's full "index everything" posture = **bleve dynamic (text)** +
**zone-maps/bloom (cheap scalar pruning)** + **adaptive auto-index (hot scalar
fields)** — without forcing a giant always-on wildcard structure. If we ever do
build the eager wildcard GSI, follow the Cosmos/ES lesson and **route by inferred
type** (ordered bbolt store for scalars, bleve for text/geo), rather than one
structure for everything.

### Prior-art links
- Cosmos DB indexing overview / policies:
  https://learn.microsoft.com/en-us/azure/cosmos-db/index-overview ,
  https://learn.microsoft.com/en-us/azure/cosmos-db/index-policy
- MongoDB wildcard indexes:
  https://www.mongodb.com/docs/manual/core/indexes/index-types/index-wildcard/
- Elasticsearch dynamic field mapping:
  https://www.elastic.co/docs/manage-data/data-store/mapping/dynamic-field-mapping
- Postgres GIN (jsonb): https://www.postgresql.org/docs/current/gin.html ;
  BRIN: https://www.postgresql.org/docs/current/brin.html
- Parquet bloom filters: https://parquet.apache.org/docs/file-format/bloomfilter/
- Oracle Automatic Indexing: https://oracle-base.com/articles/19c/automatic-indexing-19c
- Azure SQL automatic tuning:
  https://learn.microsoft.com/en-us/azure/azure-sql/database/automatic-tuning-overview
- RavenDB auto-indexes: https://ravendb.net/features/indexes/intelligent-auto-indexes
- SQLite automatic indexes: https://sqlite.org/optoverview.html
- Database cracking (Idreos/MonetDB): https://www.vldb.org/pvldb/vol4/p586-idreos.pdf

## COUNT(*) / count-scan pushdown

`SELECT COUNT(*)` should never enumerate or fetch documents when the count can be
pushed down to the datastore or an index. cbq-query's planner already knows how to
do this; n1k1 just doesn't convert the resulting operators yet.

### How the planner expresses it
- **`plan.CountScan`** — whole-keyspace count (no sargable predicate). Holds the
  `datastore.Keyspace` and calls `keyspace.Count(context)`.
- **`plan.IndexCountScan` / `IndexCountScan2`** — count with a sargable predicate,
  pushed to an index that implements `datastore.CountIndex` (`Count(span, …)
  int64`) / `CountIndex2`. Holds `Index()` + `Spans()`.
- **`plan.IndexCountDistinctScan2`** — `COUNT(DISTINCT …)` pushdown
  (`CountIndex2.CountDistinct`).
- **`plan.IndexCountProject`** — the projection wrapper that turns the pushed-down
  scalar count into the result column.

### Current state (the gap)
- `glue/conv.go` returns `NA()` for **every** count operator:
  `VisitCountScan`, `VisitIndexCountScan`, `VisitIndexCountScan2`,
  `VisitIndexCountDistinctScan2` (lines 179–184) and `VisitIndexCountProject`
  (line 635). So any statement the planner turns into a count-scan currently
  fails to convert.
- **Datastore side is partly done already:** the file datastore's
  `keyspace.Count()` (returns `len(ReadDir)`) and `Size()` already exist
  (`n1k1-query/datastore/file/file.go:467`). So whole-keyspace `COUNT(*)` is
  almost entirely a conv + execution wiring job.

### Implementation (lowest-friction first)
1. **Whole-keyspace `COUNT(*)` (primary).** Implement `conv.go:VisitCountScan` →
   emit a new `base.Op` (e.g. `"datastore-count"`) carrying the keyspace; add a
   glue execution op that calls `keyspace.Count(context)` and yields a **single
   row with one int64**. Implement `VisitIndexCountProject` to shape the projected
   column. No datastore changes needed (`Count()` exists). Do this first.
2. **Predicated `COUNT(*)` via secondary index.** Make the Phase-1
   `secondaryIndex` also implement `datastore.CountIndex.Count(span, …)` — with
   bbolt, count entries within the span range (cursor walk, or a maintained
   counter). Implement `conv.go:VisitIndexCountScan` → evaluate spans (reuse
   `EvalSpan`) + call `index.Count(span)` → yield one row.
   - **Same interface-assertion lever as Phase 1:** the planner emits
     `IndexCountScan` only when the index is a `datastore.CountIndex`, and
     `IndexCountScan2` only when it's a `CountIndex2` (+ API version). Implement
     **only the base `CountIndex`** to keep the planner on the simpler
     `plan.IndexCountScan`.
3. **`COUNT(DISTINCT …)`.** Needs `CountIndex2.CountDistinct` over the index;
   defer (harder) — without it the planner falls back to the normal
   distinct+aggregate path, which still works, just slower.

### Manifest synergy (ties to `DESIGN-data.md` §5)
Once the change-detection manifest tracks per-file / per-partition **`doc_count`**,
`COUNT(*)` over a whole keyspace or partition can be answered **O(1) from
metadata** — no `ReadDir`, no scan — exactly how Parquet/Iceberg answer count from
row-group / manifest row counts. Concretely: back `keyspace.Count()` with the
manifest count when present, and for predicated counts, **sum precomputed counts
for fully-covered partitions** (via partition zone maps) and only actually scan the
boundary partitions. This makes `COUNT(*)` nearly free on large, mostly-static
datasets.

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

## Dependency licensing

Policy: permissive licenses only — **no GPL / AGPL**. The new dependencies this
design introduces are all compliant: `go.etcd.io/bbolt` (MIT),
`blevesearch/bleve/v2` (Apache-2.0), and the alternatives considered
`couchbase/moss` / `couchbase/rhmap` (Apache-2.0). See the full dependency
license table in `DESIGN-data.md`.
