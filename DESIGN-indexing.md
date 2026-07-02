# Design: Integrating Indexes into n1k1

Status: **Phase 1 (secondary index) shipped** — see "Implementation status" below;
rest is proposal / for review. Companion: `DESIGN-data.md`. Revision changelog is
in git history.

---

## Implementation status (what has actually landed)

**Phase 1 — the GSI-like secondary index — is implemented and passing**, with the
same headline learning as `DESIGN-data.md`: **it needed ZERO changes to the
`n1k1-query` fork.** The design below proposed a `SecondaryIndexes` fork seam;
that turned out to be unnecessary. The cbq planner collects candidate indexes by
iterating every indexer from `keyspace.Indexers()`
(`planner/build_scan.go:allIndexes`), so n1k1 advertises a secondary index purely
by **wrapping** the file datastore's namespaces/keyspaces to append an extra
indexer — exactly the "fake it by wrapping `datastore/virtual` building blocks"
move that gave data-sources zero fork edits. **The `SecondaryIndexes` /
`ExtraIndexers` fork seams described later in this doc are therefore superseded
for Phase 1** (kept below as the original proposal / an alternative).

Naming: this is a **local secondary index** ("si"), not Couchbase Server's GSI
service. Code uses the `si` prefix (`glue/si.go`, `si_encode.go`, `si_catalog.go`,
type `secondaryIndex`, sidecar `<name>__si__<defhash>`); it still advertises
`Type() == datastore.GSI`, because that is cbq's enum for an ordered range
secondary index (what drives sargability) — distinct from the GSI *service*.

Landed n1k1-side (all `//go:build n1ql`):
- **`glue/si_encode.go`** — order-preserving, self-delimiting key encoding of
  `value.Value` scalars (type-tag + payload) so bbolt's byte order == N1QL
  collation order and a real `Cursor.Seek` prunes range scans. Numbers use the
  IEEE-754 order-preserving transform; strings/containers use `0x00`-escaped
  self-delimiting bytes so a composite key can always recover its docID suffix.
- **`glue/si_catalog.go`** — reads `.n1k1/catalog.json`
  `{ "indexes": [ { name, namespace?, keyspace, keys[], where? } ] }`, parsing
  key/where strings via `n1ql.ParseExpression`. Missing sidecar ⇒ no indexes
  (behave exactly as before). `defHash` = short hash of the normalized def.
- **`glue/si.go`** — the `secondaryIndex` (`datastore.Index`) + a read-only
  `siIndexer` (`datastore.GSI`), advertised by wrapping the datastore
  (`maybeSecondaryIndexes`, wired in `FileStore`). bbolt-backed; **rebuild-on-open
  validated by a source signature** (file count + newest mtime — the "assume
  static data, validate by timestamp" model the reviewer asked for; no fingerprint
  manifest yet). A process-global cache keyed by bbolt path opens/builds each index
  once (bbolt takes an exclusive file lock, so re-opening per Store would
  deadlock). Build scans the keyspace n1k1-native via `records.Walk`, evals the
  key/where exprs per doc, inserts `encode(keyValues)+docID`.
- **Read path reused as-is.** `conv.go:VisitIndexScan` → `datastore-scan-index` →
  `DatastoreScanIndex` → `secondaryIndex.Scan` yields docIDs; the following Fetch
  reads docs via the (embedded, real) keyspace's `Fetch`. Verified end-to-end:
  the planner emits an `IndexScan` (not `PrimaryScan`) and results match the
  no-index path (`test/secondary_index_test.go`, and a CLI diff battery).

Learnings that changed the plan (all forced by getting real queries to pass):
- **Covering scans (biggest surprise).** cbq turns a query whose projected/filtered
  fields are all index keys + `META().id` into a **covering `IndexScan` with no
  Fetch**, rewriting field refs into `expression.Cover` nodes that read a per-value
  cover slot n1k1 never fills → every field came back MISSING. Covering is on by
  default in the planner and can't be disabled from n1k1's side without a fork
  edit. Fix, entirely n1k1-side: `glue/expr.go:stripCovers` peels every
  `expression.Cover` back to its underlying expression before eval, so the field
  refs resolve against a materialized `.` document. **True covering execution is now
  shipped** — instead of always fetching, when the index is *coverable* (every key
  is a plain field ref and there are no filter-covers: `indexDef.coverable`),
  `VisitIndexScan` emits a **`datastore-scan-index-cover`** op that reconstructs the
  projected doc straight from the decoded index-key values (`si.go` sets
  `IndexEntry.EntryKey`; `datastore_scan.go:reconstructCoverDoc` rebuilds
  `{field: value}`, including nested paths, in the exact row shape a fetch would) —
  **no fetch at all**. The peeled covers and `META().id` resolve against the
  reconstructed doc identically to the fetched one. A **non**-coverable covering
  scan (an expression key like `LOWER(name)`, a partial index, or a non-n1k1 index)
  falls back to the correct scan+fetch: `VisitIndexScan` synthesizes a
  `datastore-fetch` when `len(Covers())>0`. n1k1 has no cover slots on its
  `[]byte`-valued rows, so doc-reconstruction — not cbq's `SetCover` mechanism — is
  how covering is realized (`test/secondary_index_test.go:TestSecondaryIndexCovering`).
- **Multi-span sender close.** `DatastoreScanIndex` ran a goroutine per span, each
  `Close`-ing the shared sender — so an IN-list / same-field-OR / `DistinctScan`
  (several spans) had the first span truncate the drain and drop the rest. Now all
  spans for an n1k1 secondary index run in one goroutine sharing the sender, closed
  once, deduping docIDs across spans (`secondaryIndex.scanSpan`).
- **Intersect/Union/Distinct scans.** A predicate over *two* indexed fields makes
  the planner emit an `IntersectScan` (AND), `UnionScan` (OR), or `DistinctScan`
  (same-field OR / IN) that n1k1 didn't convert (→ "Unsupported"). Handled n1k1-side
  in `conv.go`: `IntersectScan`/`OrderedIntersectScan` → convert the **first** child
  scan and let the residual Filter enforce the rest (a superset the Filter narrows —
  correct); `UnionScan` → fall back to a **full records scan** + Filter (can't drop
  an OR branch); `DistinctScan` → convert the inner scan (its spans are disjoint).
- **Build/scan number-encoding must agree.** A JSON number reaches the build path
  and the predicate-bound path as *different* Go types under `value.Value.Actual()`
  (`float64` vs `int64`); `toFloat64` must handle both or bounds encode as 0 and
  every numeric scan returns nothing.

**Composite (multi-key) indexes work** (`keys: ["region","product"]`): the build
encodes every key component and the self-delimiting encoding makes prefix matching
correct, so leading-key-only, full-key, leading+range, and IN predicates all use
the index and match the no-index result (`test/secondary_index_test.go`
`TestSecondaryIndexComposite`). Per-component boundary *exactness* on a composite
prefix is approximate, but the always-present residual `Filter` enforces the exact
predicate, so results are correct (just occasionally a slightly wider index walk).

Phase 2 (FTS via embedded bleve) is also shipped: `SELECT … WHERE SEARCH(ks,
"query")` runs locally against a `kind: fts` bleve index (see "Phase 2" below).
**True covering execution is shipped too** — a covering scan over an index whose
keys are all plain field refs is answered straight from the index (no fetch), by
reconstructing the projected doc from the decoded key values (see the covering
learning below). The FTS follow-ups
`SEARCH_SCORE()`/`SEARCH_META()` surfacing and declared field mappings are shipped
too (see "Phase 2"). Not yet built (still proposal below): incremental index
maintenance, a fingerprint/zone-map manifest, predicated `CountIndex` pushdown
(blocked on exact-spans), and the FTS `SargableFlex`/implicit-predicate flex path.
Known v1 limitations: freshness is a coarse (count, newest-mtime) signature, so a
change that keeps both identical (rare) won't trigger a rebuild — run `.index
rebuild` to force one; and array/object index *values* sort by byte order, not
collation (fine — predicates range over scalars).

---

This document describes how to add index support — a GSI-like **secondary
index** first, then a **full-text index** via embedded bleve — to n1k1's
standalone SQL++ CLI (`cmd/n1k1`), **without** depending on cbft, cbgt, n1fty,
or cbauth and their distributed-systems machinery.

## CLI control (build timing & introspection)

Index lifecycle is otherwise implicit (build-on-first-use), so the CLI exposes two
controls. Both live in n1k1 (`cmd/n1k1`, `glue/si.go`); the fork is untouched.

**`-index=eager|lazy|off`** (flag) — *when* catalog indexes build, via the
process-global `glue.SecondaryIndexMode` (set before `OpenSession`, re-read on
every `maybeSecondaryIndexes`, so a mid-session `.open` re-applies it):
- **`lazy`** (default) — advertise indexes; each builds on first use (the first
  query over its keyspace, or `.indexes`). First such query pays the build cost.
- **`eager`** — after opening the datastore, `EagerBuildSecondaryIndexes` opens/
  builds *every* catalog index up front, so no query pays the build cost. Builds run
  **concurrently** (one worker per CPU, capped at the index count): each index is an
  independent bbolt file, so they don't contend — the only shared state is the
  read-only source dir and a briefly-locked slot map. The open/build cache is a
  per-path `indexSlot` (a `once` opens the OS-lock-contended bbolt file; a per-slot
  mutex serializes that one index's rebuilds), so different indexes build in
  parallel while the same index never double-opens. Progress is streamed as
  serialized `IndexBuildEvent`s (start/progress/done/error) to an optional reporter;
  the CLI renders a live multi-line bar per index on a TTY (`cmd/n1k1/indexprogress.go`),
  or one plain line per finished index when piped. The per-index bar's denominator
  is the keyspace's source **file count** (exact for one-doc-per-file; a lower bound
  otherwise, so the bar may saturate before "done").
- **`off`** — `maybeSecondaryIndexes` returns the datastore unwrapped, so no
  secondary index is advertised and the planner always primary/records-scans.
  The A/B-timing switch (and an escape hatch if an index ever misbehaves).

**`.index` command family** (dot-commands) — all indexing dot-commands live under
`.index <subcommand>` so the family is discoverable:
- **`.index list`** (or bare `.index`; `.indexes` is an alias) — one line per
  declared index: `ns:keyspace.name (keys…) [WHERE …]  [N entries, SIZE]`,
  opening/building any not-yet-built index to report live bbolt stats
  (`glue.SecondaryIndexInfos` → entry count via `Bucket.Stats().KeyN`, file size via
  `os.Stat`). Under `-index=off` it prints "disabled". Because listing can trigger a
  build, it doubles as an explicit "build now".
- **`.index show <name>`** — the full detail of one index (keyspace, keys, WHERE,
  entries, size, on-disk path).
- **`.index rebuild [<name>]`** — force-rebuild all catalog indexes (or the one
  named) regardless of the freshness signature (`glue.RebuildSecondaryIndexes` →
  `buildIndexesConcurrent(force=true)`, same concurrent build + progress as eager).
  The escape hatch for the coarse (file count, newest-mtime) freshness check — e.g.
  an edit within the same mtime tick — without deleting the `.n1k1` artifact.
- **`.index help`** (alias `example`) — prints the subcommand syntax plus a
  copy-pasteable `catalog.json` example (the definition format isn't otherwise
  discoverable from the CLI). Shipped.
- **`.index suggest [<keyspace>]`** — the advisor (see "adaptive auto-index"):
  samples docs, scores selective scalar/nested-no-array fields, and prints an
  editable `catalog.json` fragment (each def carries a `why` the loader ignores).
  Shipped (`glue.SuggestIndexes` in `si_suggest.go`).
- **`.index create ...`** — add index def(s) to `catalog.json` and build them, in
  two input forms: a DSL `.index create <name> on <keyspace> (<expr>[, <expr>])
  [where <expr>]`, or a JSON fragment `.index create {"indexes":[…]}` (pastes back
  `.index suggest` output — the `why` field is accepted and dropped on write). It
  validates the def(s), merges into `catalog.json` (dup names rejected; won't
  clobber non-index sections), re-opens the session, and builds. Shipped
  (`glue.CatalogAddIndexes` in `si_catalog.go`). Writing the human catalog is fine
  here — it's explicit user intent (single-writer rule bars only *background*
  rewriting).

**Design stance on scope.** Per-index knobs (collation, the "index-everything"
value-size cap + truncation marker, `defer`, CBO stats) belong in `catalog.json`
as per-index fields, *not* as global flags — they're properties of a definition,
and `catalog.json` is the single-writer source of truth (see "Sidecar layout").
Reserve flags/dot-commands for process-wide *timing/introspection/lifecycle*
(`-index`, and the `.index list|show|rebuild` family; a future `.index drop
<name>`). DDL (`CREATE/DROP INDEX`) stays unwired in v1 (`conv.go: VisitCreateIndex`
is `NA()`); the catalog is the definition surface.

## Motivation

Today every n1k1 query over the file datastore is a full keyspace (primary) scan
plus a residual filter. We want the planner to use an index when one applies, so
that selective queries don't read the whole keyspace. The goal is to do this
in-process — no FTS cluster, no GSI service — keeping n1k1 self-contained.

## Background: how index selection actually works in n1k1

The central fact that shapes this whole design:

> **n1k1 takes cbq's *plan*, not its runtime.** `glue/stmt.go:PlanStatement` calls
> cbq-query's real `planner.Build()`, so index selection — "index vs no index,
> which index" — is decided entirely by cbq's planner, driven by **what the
> datastore advertises** through the `Keyspace → Indexer → Index` / `FTSIndex`
> interface tree (`datastore/index.go`). But n1k1 **replaces cbq's execution
> runtime** (its tuple-by-tuple iteration over boxed `value.AnnotatedValue`s) with
> its own `base.Op` engine over `base.Val = []byte` — buffer reuse, not per-tuple
> boxing. So the fork is a source of *plans* (and index metadata), and n1k1 owns
> execution. This is why index code belongs in n1k1 behind thin seams (below).

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

Since the planner is driven by **what the datastore advertises** (Background),
n1k1 need not *host* index code in the fork — only get the fork to **advertise
n1k1-built index objects**. This is the same thin-seam / IoC decision as
`DESIGN-data.md` ("Where this code lives"): `datastore.Index`/`Indexer`/`FTSIndex`
are interfaces `glue` already imports, and Go interfaces are structural, so **n1k1
implements them** and the planner uses them polymorphically. Execution is already
n1k1-side — `glue/datastore_scan.go:DatastoreScanIndex` calls
`scan.Index().Scan(...)`, so once the advertised index is an n1k1 type its
`Scan()` runs in n1k1 (over `[]byte`, not cbq's boxed values).

The two seams (verified in `datastore/file/file.go`):
- **GSI/secondary:** `var SecondaryIndexes func(datastore.Keyspace)
  []datastore.Index`, merged into `fileIndexer.Indexes()` (file.go:817, today
  primary-only) + `IndexNames`/`IndexById`/`IndexByName`/`IndexIds`.
- **FTS:** `var ExtraIndexers func(datastore.Keyspace) []datastore.Indexer`,
  appended in `keyspace.Indexers()` (file.go:491) — a distinct `IndexType`, so a
  clean append, not a merge.

Everything else — the `secondaryIndex`/`ftsIndex` types, bbolt/bleve backing, the
build routine, `.n1k1/catalog.json` loading, doc-IDs — is an ordinary n1k1 package
(`n1k1/index` or `glue`), registered at startup with the `ExecOpEx` save/restore
discipline; `bbolt`/`bleve` become **n1k1** direct deps. The only irreducible fork
edits are the seam declarations + the `Indexes()`/`IndexByName` fix. Same
global-vs-per-store caveat as the data doc.

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
`limit`. `MetaData` may be left empty. `EntryKey` is filled only for a covering
scan (`scanSpan`'s `projectKeys`): it carries the decoded key values so the
covering drain can reconstruct the projected doc without a fetch — the default
(non-covering) drain reads only `PrimaryKey`.

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

## Phase 2 — FTS via embedded bleve ✅ SHIPPED

`SELECT … WHERE SEARCH(ks, "query")` now runs locally against an embedded
`bleve.Index` — no cbft cluster, no n1fty, zero fork edits. The planner hook
already existed (`planner/build_scan_search.go` + the `SargableFlex` path); we
set `useFts=true` in `glue/stmt.go` and provide the `datastore.FTSIndex`
in-process (a small shim, not n1fty). Landed in `glue/fts.go`:

- **`ftsIndexer` + `ftsIndex`** — an `Indexer` (`Name()==datastore.FTS`) and an
  `FTSIndex`, advertised by appending to `keyspace.Indexers()` (a distinct
  `IndexType`, so a clean append alongside the GSI indexer — no merge). Backed by
  an embedded `bleve.Index`:
  - `Sargable(field, query, options, mappings)` returns `exact=true` so the
    planner drops the residual predicate; `SargableFlex` is stubbed (`nil,nil`)
    and `Pageable` is `false` in v1.
  - `Search(...)` runs `bleveIndex.Search()` locally (`req.Size=DocCount`) and
    pushes `datastore.IndexEntry{PrimaryKey: hit.ID, MetaData: hit.Score}` into
    `conn.Sender()` — the same drain pattern Phase 1 uses.
- **conv.go / DatastoreScanFTS:** `VisitIndexFtsSearch` emits a single
  `datastore-scan-fts` op (labels `.alias` + `^id` + `^smeta`). `DatastoreScanFTS`
  runs the bleve search, **fetches the matching docs itself**, and emits each as
  the doc + id + search-meta. It fetches in the op (not via a following
  `plan.Fetch`) because the hit score is only available at the scan and would be
  lost across a separate fetch — so `VisitFetch` passes through after an FTS scan.
  The residual `SEARCH()` the planner leaves in the `Filter` — which n1k1 can't
  re-evaluate (it would return false and drop every row) — is rewritten to `TRUE`
  by `stripSearch` (`glue/expr.go`), gated on a `sawFTS` flag so a genuine
  co-predicate like `… AND d.id = "d1"` is preserved. (Analogous to `stripCovers`.)
- **Score/meta surfacing (follow-up, shipped):** `SEARCH_SCORE(alias)` and
  `SEARCH_META(alias)` now return the bleve relevance score / meta. The score rides
  the `^smeta` label as `{outname: {score, id}}`, which `ConvertVals` binds under
  `value.ATT_SMETA` on the alias value — exactly where the `search` package's
  `SearchMeta`/`SearchScore` read it.
- **Catalog/build:** FTS index specs use `kind: fts` in `.n1k1/catalog.json`. The
  bleve index is built into `.n1k1/<ns>/<ks>/idx/<name>__fts__<defhash>/bleve/`
  from a full scan on open, with the Phase 1 source-signature freshness check.
  `.index list` shows a `fts` row; `.index rebuild` / `.index build` work.
- **Declared mappings (follow-up, shipped):** a def's `keys` now scope the bleve
  mapping (`ftsMapping`): empty `keys` = dynamic (index every field, the default);
  listed field keys build a **non-dynamic** mapping indexing exactly those fields
  (nested dotted paths → sub-document mappings), so a `SEARCH()` on any other field
  matches nothing.

**Remaining follow-ups:** the implicit-predicate → FTS "flex" path (`SargableFlex`)
is still stubbed, so only explicit `SEARCH()` is handled; and declared fields are
all mapped as text (per-field analyzers/types not yet configurable).

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

   **Seeding candidates by sampling doc cardinality (reviewer idea — with one
   inversion).** Workload logging is *reactive*: an index only appears after
   queries reveal a hot field. A cheap *proactive* cold-start is to sample N docs
   (first/random, O(sample)) and, per top-level scalar path, estimate
   **cardinality**, **presence** (fraction of docs that have it), **type
   stability** (mostly-scalar, indexable), and **value size** (huge values →
   skip / truncate, per the value-size cap above). But the naive reading —
   "*low*-cardinality fields → index them" — is **backwards for a b-tree / GSI
   secondary index.** A range/equality index earns its keep on **high
   selectivity**: a *high*-cardinality field (values are rare) means an
   equality/range matches few docs, so the index prunes hard; a *low*-cardinality
   field (e.g. `status` with 3 values, `country` with ~200) matches a large
   fraction, so the scan yields most docIDs and then fetches most docs — barely
   better than a primary scan, and a cost-based planner would reject it. So the
   sampling signal for a **b-tree auto-index is HIGH cardinality (÷ doc count) +
   queried**, not low. Low-cardinality fields are still valuable — but for the
   *other* machinery: **zone-maps / partition pruning** (tier 1), **bitmap**-style
   structures, or as a **composite leading key** (low-card leading + high-card
   trailing). Best policy = combine both signals: sampling proposes candidates
   (structure + selectivity), the **workload confirms** which are actually queried
   — auto-create where `queried ∧ selective`. Caveats: sample cardinality
   extrapolates imperfectly (a field rare in the sample may be common overall, and
   vice-versa), so treat it as a prior, not truth; and these estimates are exactly
   what a future `Index.Statistics()` should return to feed cbq's CBO
   (`useCBO=true`) instead of the current `nil`.

   **Which fields are eligible (path walk during sampling).** Walk each sampled
   doc's paths (`buger/jsonparser` `ObjectEach`, recursively) and classify each
   leaf:
   - **Scalar leaf, no array crossed** — the b-tree candidates. A top-level scalar
     (`country`) or a pure-object nested path (`personal_details.state`) resolves to
     exactly one scalar per doc; the key expression is just the dotted path (cbq
     handles it, the build evals it — already true for our nested-key tests).
   - **Any array segment on the path** (or the value itself is an array) — **skip
     for the scalar tier.** A field under an array is multi-valued per doc, which a
     plain scalar index can't represent; that needs cbq's **array index**
     (`(DISTINCT) ARRAY … FOR … END`, sargable only via `ANY`/`UNNEST`) — a separate,
     harder candidate class (flag it, don't auto-create in v1).
   - **Type-unstable** (scalar in some docs, object/array in others — schema drift)
     — skip or mark low-confidence; **synthetic `_meta`** and **oversized values**
     (value-size cap) — exclude.

   **CLI ladder: advise → human edits → create/build** (naming resolved; respects
   the single-writer-catalog rule). The elegant bit is **output format == input
   format**: the advisor emits exactly a `catalog.json` fragment.
   - **`.index suggest`** *(advisor, read-only; aka `auto-plan`)* — **SHIPPED.**
     Samples docs, scores selective scalar / nested-no-array fields, and prints a
     `{"indexes":[…]}` fragment with a per-suggestion `why` field (the catalog loader
     ignores it, so the fragment pastes back verbatim). No side effects; the natural
     companion to cbq's `ADVISE`. You paste it into an editor, prune / rename / add
     `where`, then drop it into `.n1k1/catalog.json` or feed it to:
   - **`.index create ...`** *(explicit apply)* — **SHIPPED.** A DSL form
     (`<name> on <keyspace> (<expr>…) [where <expr>]`) or the JSON fragment; append
     the def(s) to `.n1k1/catalog.json` and build. Writing the human catalog is fine
     here because it's **explicit user intent**; the single-writer rule only forbids
     *background* machinery from rewriting `catalog.json`.
   - **`.index auto`** *(fully autonomous, later)* — sampling + workload
     confirmation + GC, writing a **separate machine-managed auto-catalog**, never
     the human `catalog.json`.
   Both the **advisor (`.index suggest`) and explicit apply (`.index create`) are
   shipped** — the advise→edit→apply loop is complete. Still ahead: the
   fully-autonomous `.index auto` (workload confirmation + GC into a machine-managed
   auto-catalog).
3. **Eager wildcard GSI (Cosmos/Mongo-style)** — a bbolt store keyed
   `encode(path) + encode(value) + docID` so any single-path equality/range is
   contiguous.

   **Key layout / physical-storage constraints (reviewer note, worth building
   around).** A range-scannable KV library like bbolt (or moss) does *not* want a
   separate bucket/collection per field-path — there can be thousands of paths, and
   many KV stores cap or slow down badly with lots of containers. So encode the
   **field-path into the key** rather than using a bucket-per-path:
   `<fieldPathShortPrefix> : <encodedValue> : <docID>`. To keep keys compact and
   the prefix fixed-width (so a path's entries stay contiguous and seekable),
   don't put the raw dotted path in every key — maintain a small **dictionary
   index mapping field-path → short byte prefix** (assign a monotonic id the first
   time a path is seen; store the id↔path map in its own bucket). A wildcard scan
   for `WHERE a.b.c = v` becomes: look up the prefix for `a.b.c`, then
   `Seek(prefix + encode(v))`. This reuses Phase 1's order-preserving
   `encodeValue` for the `<encodedValue>` segment unchanged.
   - **Value size limits — yes, expect them.** bbolt keys are bounded (max ~32 KB,
     and large keys wreck the B+tree's fan-out/page efficiency long before that),
     and huge indexed values bloat the index for little selectivity gain. So the
     encoder must **cap the encoded value**: truncate long strings/blobs to a
     prefix (e.g. first N bytes) and set a "truncated" marker bit in the key so the
     scan knows the residual predicate must be re-checked against the fetched doc
     (which already happens — index results always feed a residual Filter). Values
     over the cap become a *prefix probe*, not an exact index entry. Document the
     cap and the truncation-marker convention next to `encodeValue`.

   Feasible to *build*, but the hard part is **planner integration**:
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

### Current state
- **Whole-keyspace `COUNT(*)` is done** (item 1 below): `conv.go:VisitCountScan`
  de-optimizes to a records scan + `count(*)` group-aggregate (correct for every
  format; a true O(1) count is the manifest item below).
- The predicated/index count operators (`VisitIndexCountScan`, `IndexCountScan2`,
  `IndexCountDistinctScan2`, `VisitIndexCountProject`) still return `NA()` — but see
  item 2: the base-index versions can't be reached anyway (the planner won't emit
  them without exact spans / `Index2`), so `NA()` is currently unreachable, not a gap.
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
2. **Predicated `COUNT(*)` via secondary index — BLOCKED on `Index2` (verified).**
   The mechanical parts are easy and were prototyped: `secondaryIndex.Count(span)`
   (a bbolt cursor tally over the span, sharing the `Scan` walk), plus
   `conv.go:VisitIndexCountScan` (a `datastore-index-count` op that sums
   `index.Count` over the spans and yields one row holding the int64 under a
   `^count` label) and `VisitIndexCountProject` (projects that scalar into the
   result column, reading the `^count` label for the aggregate term). **All
   correct — but the cbq planner never emits `plan.IndexCountScan` for a base
   `datastore.Index`,** so the wiring is dead. Root cause (traced through the
   planner): count pushdown lives in the *covering* path
   (`build_scan_covering.go:buildCoveringPushdDownIndexScan2` →
   `build_scan_pushdowns.go:indexCoveringPushDownProperty`) and is gated on
   **`_PUSHDOWN_EXACTSPANS`**. A base (API1) index's spans are **never** marked
   exact — which is *also* why every base-`IndexScan` n1k1 produces carries a
   residual `Filter` (the planner assumes the index over-returns and re-checks).
   No exact spans ⇒ no `_PUSHDOWN_GROUPAGGS` ⇒ no `IndexCountScan`; the planner
   instead does primary-scan + filter + aggregate. Confirmed empirically: `COUNT(*)`,
   `COUNT(1)`, `COUNT(custId)` with a sargable `WHERE` all plan `datastore-scan-records`,
   never a count scan.
   - **`Index2` is necessary but NOT sufficient (verified — deeper than expected).**
     A second prototype implemented `datastore.Index2` (`RangeKey2` + `Scan2` over
     `Spans2`) + `conv.go:VisitIndexScan2` + a `datastore-scan-index2` op. This DID
     make the planner emit `plan.IndexScan2` (confirmed: the op became
     `datastore-scan-index2`) — but **the residual `Filter` still was not dropped**,
     so spans still weren't treated as exact and count pushdown still didn't fire.
     Neither advertising `IK_MISSING` on the leading key nor enabling the
     `N1QL_INDEX_MISSING` feature control changed that (and `IK_MISSING` without
     actually indexing MISSING values would be a *correctness* bug for `IS MISSING`
     queries). `sarg_eq` builds the equality span as exact (`NewSpan2(…, true)`), so
     exactness is being cleared/ignored somewhere further along (the sarge AND-wrap,
     the covering-filter/`filterCovers` logic, or a CBO-off path) that this pass did
     not pin down. Both prototypes were reverted; **filter-elimination and count
     pushdown remain open, pending a deeper trace of why cbq keeps the post-scan
     `Filter` for an `IndexScan2` even on an exact single-key equality.** Likely
     next probes: `useCBO=true`; the `filterCovers`/`coveringScan` filter-retention
     path; and whether `Index3`/`IndexScan3` (group-agg pushdown) behaves differently.
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
- **Covering (done):** project only index-key fields (e.g. `SELECT s.region,
  s.product FROM s WHERE s.region="US"` over `keys:["region","product"]`) and
  confirm the plan is a **`datastore-scan-index-cover`** with **no
  `datastore-fetch`** and no full records scan, yet results match the index-off
  run (verified for scalar, numeric, and nested-path keys —
  `TestSecondaryIndexCovering`).
- **Phase 2 (done):** a `SELECT … WHERE SEARCH(ks, "…")` query returns the
  expected docs, with no cbft/network calls (purely local bleve). Confirm the plan
  uses a `datastore-scan-fts` op (`TestFTSSearch` asserts this) and results match
  the whole-doc and field forms (`SEARCH(d,"quick")` → `d1,d2`; `SEARCH(d.title,
  "world")` → `d2`). `SEARCH_SCORE(d)`/`SEARCH_META(d)` return the bleve score/meta
  (`TestFTSScoreMeta`), and a `kind:fts` def with `keys` scopes matches to those
  fields (`TestFTSDeclaredMapping`).

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
- **Composite indexes — DONE.** Multi-key indexes work (`EvalSpan` /
  `DatastoreScanIndex` handle the multi-column `Range.Low/High` arrays, and the
  self-delimiting composite key encoding makes prefix matching correct). Verified
  by `TestSecondaryIndexComposite`; the residual `Filter` covers any per-component
  boundary imprecision, so results stay exact.
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
