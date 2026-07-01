# Design: Integrating Indexes into n1k1

Status: proposal / for review (revised: index code lives in **n1k1**, registered
into **thin hook seams** in the fork — the `engine.ExecOpEx` IoC pattern — rather
than living inside the fork's `datastore/file`; see "Where this code lives")

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
  returns only `fi.primary` — the one irreducible fork edit is to make it (and
  `IndexByName`/`IndexById`/`IndexNames`) also return whatever the **`SecondaryIndexes`
  hook** contributes (see "Where this code lives"). The advertised index objects
  are built in n1k1, not here.
- **CREATE INDEX DDL is not wired** into n1k1's executor
  (`conv.go:VisitCreateIndex` returns `NA()`; n1k1 runs its own `base.Op` tree,
  not cbq's `execution` package). v1 defines indexes via a sidecar file.

## Where this code lives (thin hook seams, not fork code)

The Background above is the whole leverage: index selection is driven by **what
the datastore advertises**. So n1k1 does not need to *host* index code in the
fork — it only needs the fork to **advertise n1k1-built index objects**. This
mirrors `DESIGN-data.md`'s decision and the IoC pattern n1k1 already uses
(`engine.ExecOpEx = glue.DatastoreOp`): the fork gets **thin hook seams**; all
real logic lives in n1k1.

Why it works: `datastore.Index`, `datastore.Indexer`, and `datastore.FTSIndex`
are **interfaces** in `query/datastore/index.go`, and `glue` already imports
`query/datastore` (see `glue/datastore_scan.go`). Go interfaces are structural,
so an **n1k1 package can fully implement `datastore.Index`/`Indexer`/`FTSIndex`**
— the planner calls those methods polymorphically and neither knows nor cares
where the concrete type is defined. Execution is *already* n1k1-side:
`glue/datastore_scan.go:DatastoreScanIndex` calls `scan.Index().Scan(...)`, so
once the advertised index object is an n1k1 type, its `Scan()` runs in n1k1.

The seams (verified in `datastore/file/file.go`):
- **GSI/secondary:** `fileIndexer.Indexes()` (file.go:817, today returns only
  `fi.primary`), plus `IndexNames`/`IndexById`/`IndexByName`/`IndexIds` — add a
  hook `var SecondaryIndexes func(ks datastore.Keyspace) []datastore.Index` that
  each merges into its result (defaulting to today's primary-only when unset).
- **FTS:** `keyspace.Indexers()` (file.go:491) — add a hook
  `var ExtraIndexers func(ks datastore.Keyspace) []datastore.Indexer` so n1k1 can
  append a whole FTS `Indexer` (a *different* `IndexType`, so it's a clean append,
  not a merge into the GSI indexer).

Everything else — the `secondaryIndex`/`ftsIndex` types, their bbolt/bleve
backing, the build routine, `.n1k1/catalog.json` loading, and doc-ID handling —
is an **ordinary n1k1 package** (e.g. `n1k1/index` or in `glue`), registered into
those hooks at startup and torn down with the same save/restore discipline
`Session.Run` already uses for `ExecOpEx`. `bbolt`/`bleve` become **n1k1** direct
deps, not fork deps.

What must still touch the fork (irreducible): the seam declarations + the
`fileIndexer.Indexes()`/`IndexByName` fix to consult the hook (a few lines each,
easy to carry across a cbq rebase). Same global-vs-per-store caveat as the data
doc — prefer hanging the hook off the store/namespace instance over a package
global where the seam can reach it.

## Phase 1 — GSI-like secondary index

Phase 1 code lives in **n1k1** (the `secondaryIndex` type, its build, sidecar
loading), registered into the fork's thin `SecondaryIndexes` seam at startup. The
only fork edits are that seam + the `fileIndexer.Indexes()`/`IndexByName` fix;
`bbolt` becomes a direct n1k1 require.

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

- **Definition:** index definitions live in the sidecar catalog
  `.n1k1/catalog.json` (canonical) as
  `[{ "name": "...", "keys": ["expr", ...], "where": "expr"? }]` — see the
  **Sidecar layout** section for the full `.n1k1/` naming scheme. (A per-keyspace
  `indexes.json` remains an option for portability.)
- **Load (in n1k1, via the `SecondaryIndexes` hook):** the hook n1k1 registers is
  called with the keyspace when the planner asks for its indexes; n1k1 reads the
  catalog, parses each key/where string with the n1ql expression parser, and
  returns `secondaryIndex` objects (opening/creating each bbolt file at
  `.n1k1/<ns>/<ks>/idx/<name>__gsi__<defhash>/data.bolt`). No `newKeyspace` fork
  edit — the fork just calls the hook from `fileIndexer.Indexes()`. Cache per
  keyspace so repeated planning doesn't reopen bbolt.
- **Build (v1 = rebuild-on-open), also n1k1-side:** full keyspace scan (drive it
  through the same primary `ScanEntries`/`Fetch` the datastore already exposes) →
  evaluate the parsed key expressions (and `where`) against each doc → insert
  `(encodedKey → docID)` into bbolt. Gate behind sidecar presence so unindexed
  keyspaces are unaffected. All of this is ordinary n1k1 code — it uses the
  fork's datastore only through its public `datastore.Keyspace` methods.

### The `secondaryIndex` type (n1k1-side, base `datastore.Index` only)

An **n1k1 package** type implementing `datastore.Index`; hold a `*bbolt.DB` +
bucket name + the parsed `rangeKeys`/`where`. (It's an n1k1 type advertised via
the `SecondaryIndexes` hook — the planner uses it polymorphically; see "Where
this code lives".)

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
| `Drop()` | drop from n1k1's index registry + trash the bbolt file (catalog edit) |
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

Fork edits are just the seam (steps 1–2); everything else is n1k1.

1. **(fork, once)** Add `var SecondaryIndexes func(datastore.Keyspace)
   []datastore.Index` and call it from `fileIndexer.Indexes()` (file.go:816),
   merging its result with `fi.primary`; do the same in
   `IndexByName`/`IndexById`/`IndexNames`/`IndexIds`. Default (unset hook) =
   today's primary-only behavior. Commit; bump the `replace … => …/n1k1-query
   <newver>` pin.
2. **(fork, once)** Prefer wiring the hook to the store/namespace instance if
   reachable from `fileIndexer`; else a package global with `Session.Run`
   save/restore (matches `ExecOpEx`).
3. **(n1k1)** Add the `secondaryIndex` type implementing base `datastore.Index`
   (incl. `Scan()`: cursor seek/iterate/`Collate`; see contract above).
4. **(n1k1)** Add the catalog reader + build routine (parse key/where exprs;
   full-scan → eval → bbolt insert); cache per keyspace.
5. **(n1k1)** Register the `SecondaryIndexes` hook at startup (alongside
   `engine.ExecOpEx = glue.DatastoreOp`); add `go.etcd.io/bbolt` as a **direct
   n1k1** require.
6. **(n1k1)** Verify end-to-end. `conv.go:VisitIndexScan` + `datastore_scan.go`
   already handle the read path — no changes there.

## Phase 2 — FTS via embedded bleve

The planner hook already exists (`planner/build_scan_search.go` + the
`SargableFlex` path); set `useFts=true` in `glue/stmt.go:PlanStatement`. Because
FTS sargability is externalized into `datastore.FTSIndex`, we provide it — with a
small in-process shim, not n1fty.

- Implement an `Indexer` + `FTSIndex` **as an n1k1 package** (bleve becomes a
  direct n1k1 require), advertised through the fork's **`ExtraIndexers` seam**
  (`keyspace.Indexers()` appends it — a distinct `IndexType`, so it's a clean
  append, not a merge into the GSI indexer). Backed by an embedded `bleve.Index`:
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
- Definition/build: add FTS index specs to `.n1k1/catalog.json` (`kind: fts`);
  build the bleve index into `.n1k1/<ns>/<ks>/idx/<name>__fts__<defhash>/bleve/`
  on open from a full scan.

## Sidecar layout (`.n1k1/`): naming for many index schemes

A dataset accumulates *many* independent derived artifacts — several secondary
(GSI) indexes, FTS/bleve indexes, always-on zone-maps/bloom, count caches, and
change-detection manifests — across multiple keyspaces, each with its own
definition, format version, and rebuild lifecycle. The `.n1k1/` layout must let
these coexist, be built / dropped / GC'd independently, be swapped atomically, and
be matched back to the exact definition **and** source state they were built from.

### Directory tree
```
<dataRoot>/.n1k1/
  LAYOUT                       # one line: sidecar layout format version
  catalog.json                # source of truth: all index definitions + config fingerprint (DESIGN-data §2.3/§5)
  <namespace>/
    <keyspace>/
      manifest.json           # source fingerprints + zone-maps for change detection (DESIGN-data §5)
      idx/
        <name>__<kind>__<defhash>/    # one directory per built index instance
          meta.json                   # def, kind, key exprs, format_version, built_from, build state, stats
          data.bolt                   # kind=gsi   : bbolt B+tree
          bleve/                      # kind=fts   : bleve index directory
          zonemap.cbor | bloom.bin    # kind=zonemap | bloom : lightweight artifacts
          count.json                  # kind=count : cached COUNT(*) / per-partition counts
      tmp/
        <name>__<kind>__<defhash>.<gen>/   # in-progress build; atomically renamed into idx/
      trash/                               # dropped/orphaned instances awaiting lazy delete
```

### The instance name: `<name>__<kind>__<defhash>`
- **`name`** — the user-facing index name, filesystem-sanitized (slugify/percent-
  escape unsafe chars; the true name lives in `meta.json`).
- **`kind`** — the scheme: `gsi` | `fts` | `zonemap` | `bloom` | `wildcard` |
  `count`. Lets many schemes coexist on the same keyspace, even on the same key.
- **`defhash`** — short hex hash of the *normalized* definition (key expressions +
  `WHERE` + options + collation/format version). This is the workhorse:
  - **Redefinition safety:** reusing a name with a changed definition yields a new
    `defhash` ⇒ a new directory; the old one is orphaned and GC'd — no in-place
    corruption, no stale-definition reads.
  - **Planner matching:** "is there a built index for *this* definition?" becomes a
    directory-existence check; the datastore advertises only instances whose
    `built_from` matches the current source manifest (otherwise it's stale).
  - **Self-describing:** `catalog.json` can be reconstructed by scanning `idx/`.

### Atomic build, versioning, lifecycle
- Build into `tmp/…​.<gen>/`, then **atomic rename** into `idx/…/` (POSIX dir
  rename on the same filesystem) so readers never see a half-built index. For
  concurrent readers during rebuild, use a `<gen>` suffix + a `CURRENT` pointer per
  instance (LevelDB/RocksDB-style); simplest v1 = single instance + rename swap.
- Rebuild is triggered by any of: `meta.json.format_version` bump, changed
  `defhash`, or `built_from` ≠ current source manifest / `catalog.config_fingerprint`
  (DESIGN-data §5).
- **GC:** on open, reconcile `idx/` against `catalog.json` + the source manifest;
  orphans and stale instances move to `trash/` and are deleted lazily. Dropping an
  index removes its catalog entry and trashes its instance dir.

### Encryption & definition home
- If encryption-at-rest is on (DESIGN-data §6), artifact payloads (`data.bolt`,
  `bleve/`, `zonemap`, manifests) are encrypted with the dataset DEK; `meta.json`
  records the wrapping key id. The whole `.n1k1/` tree is a derived-data leak
  surface, so it is in-scope for the encryption guarantee.
- Index **definitions** are the source of truth in `.n1k1/catalog.json` (dataset-
  wide), superseding Phase 1's per-keyspace `.indexes.json` sketch; a per-keyspace
  `indexes.json` remains an option for portability, but built artifacts are always
  keyed by `defhash`, so a definition edited anywhere triggers a clean rebuild.
- **Declared vs machine-managed (single-writer rule — see `DESIGN-data.md` §5
  "Comingling in `catalog.json`").** `catalog.json` comingles source mappings +
  index defs safely *only because it stays single-writer*: it holds **declared
  intent** (human/generator-authored, slow-changing). Everything machine-managed
  and dynamic — build-state, stats, and **adaptive/auto-created** indexes (the
  tier-2 auto-index) — lives in **self-describing per-instance dirs**
  (`idx/<name>__<kind>__<defhash>/meta.json`), never written back into
  `catalog.json`. So >0 indexers (each a `kind`) can build/rebuild/drop
  concurrently by touching **different dirs** with no shared-file contention, and
  the set of built indexes is discoverable by scanning `idx/`. Adaptive indexes
  that rewrote the human `catalog.json` would break the single-writer property —
  don't.

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

- **Phase 1:** define an index in `.n1k1/catalog.json` over a field, run a query
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
- **Interface-drift now lands in n1k1 (a feature, not a cost).** Because the
  `datastore.Index`/`Indexer`/`FTSIndex` implementations live in n1k1, a cbq
  rebase that changes those signatures breaks the n1k1 build — but n1k1 already
  tracks these interfaces (`conv.go`, `datastore_scan.go`), so it's the natural
  owner, and the break is a compile error in n1k1 rather than a silent drift
  inside the fork. The fork carries only the tiny seam declarations.

## Affected files

**Fork (thin seams only — the whole point):**
- `../n1k1-query/datastore/file/file.go` — add `var SecondaryIndexes
  func(datastore.Keyspace) []datastore.Index` and consult it in
  `fileIndexer.Indexes()`/`IndexByName`/`IndexById`/`IndexNames`; add `var
  ExtraIndexers func(datastore.Keyspace) []datastore.Indexer` and append it in
  `keyspace.Indexers()` (Phase 2). Both default to today's behavior when unset.
  **No index types, build, or sidecar code here.**

**n1k1 (all the real logic — a new `n1k1/index` pkg, or in `glue`):**
- The `secondaryIndex` type (`datastore.Index`, incl. `Scan()`/`CountIndex`), the
  bleve-backed FTS `Indexer` + `FTSIndex` (Phase 2), the catalog reader, the build
  routine, and hook registration at startup.
- `glue/datastore_scan.go` — verify-only for Phase 1 (already calls
  `scan.Index().Scan`); add `datastore-scan-fts` in Phase 2.
- `glue/conv.go` — verify-only for Phase 1 (`VisitIndexScan` works); implement
  `VisitIndexFtsSearch` in Phase 2; `VisitCreateIndex` is a future DDL hook.
- `glue/stmt.go` — leave `IndexApiVersion` as-is for Phase 1; set `useFts=true`
  in Phase 2. Register `SecondaryIndexes`/`ExtraIndexers` near
  `engine.ExecOpEx = glue.DatastoreOp`.
- `go.mod` — bump the `n1k1-query` pin (for the seams); add direct
  `go.etcd.io/bbolt` (Phase 1) and `blevesearch/bleve/v2` (Phase 2) as **n1k1**
  deps.

## Dependency licensing

Policy: permissive licenses only — **no GPL / AGPL**. The new dependencies this
design introduces are all compliant: `go.etcd.io/bbolt` (MIT),
`blevesearch/bleve/v2` (Apache-2.0), and the alternatives considered
`couchbase/moss` / `couchbase/rhmap` (Apache-2.0). See the full dependency
license table in `DESIGN-data.md`.
