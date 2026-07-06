# Design: Integrating Indexes into n1k1

Status: **Phase 1 (secondary index) and Phase 2 (FTS) both shipped.** The
"index everything", COUNT pushdown, and incremental-maintenance material is
proposal / for review. Companion: `DESIGN-data.md`.

## Overview

Adds index support to n1k1's standalone SQL++ CLI (`cmd/n1k1`): a GSI-like
**secondary index** (bbolt-backed) and a **full-text index** via embedded
**bleve** — with no dependency on cbft, cbgt, n1fty, or cbauth.

Core mechanism (state ONCE): n1k1 takes cbq's *plan* but owns *execution*.
cbq's planner selects indexes purely from **what the datastore advertises**, so
n1k1 advertises n1k1-built index objects by **wrapping** the file datastore and
runs their scans over its own `[]byte` engine — needing **zero fork edits**.

## Contents

1. Implementation status
2. Motivation
3. Background: how index selection works
4. Where the code lives (thin hook seams)
5. CLI control (build timing & introspection)
6. Phase 1 — GSI-like secondary index
7. Phase 2 — FTS via embedded bleve
8. Sidecar layout (`.n1k1/`)
9. "Index everything": dynamic / wildcard / automatic indexes
10. COUNT(*) / count-scan pushdown
11. Verification
12. Risks & open questions
13. Affected files
14. Dependency licensing

---

## 1. Implementation status

### Phase 1 — GSI-like secondary index (shipped)

**Zero changes to the `n1k1-query` fork.** The cbq planner collects candidate
indexes by iterating every indexer from `keyspace.Indexers()`
(`planner/build_scan.go:allIndexes`), so n1k1 advertises a secondary index by
**wrapping** the file datastore's keyspaces to append an extra indexer. The
fork-seam design in §4 is superseded for Phase 1 — kept only as an alternative.

Naming: a **local secondary index** ("si"), not Couchbase Server's GSI service.
Code uses the `si` prefix (`glue/idx_si.go`, `idx_si_encode.go`,
`idx_si_catalog.go`, type `secondaryIndex`, sidecar `<name>__si__<defhash>`); it
advertises `Type() == datastore.GSI` (cbq's enum for an ordered range secondary
index, which drives sargability) — distinct from the GSI *service*.

Landed n1k1-side (all `//go:build n1ql`):

- **`glue/idx_si_encode.go`** — order-preserving, self-delimiting key encoding of
  `value.Value` scalars (type-tag + payload). Numbers use the IEEE-754
  order-preserving transform; strings/containers use `0x00`-escaped bytes. bbolt
  byte order == N1QL collation order, so a real `Cursor.Seek` prunes range scans.
- **`glue/idx_si_catalog.go`** — reads `.n1k1/catalog.json`
  `{ "indexes": [ { name, namespace?, keyspace, keys[], where? } ] }`, parsing
  key/where strings via `n1ql.ParseExpression`. Missing sidecar ⇒ no indexes.
  `defHash` = short hash of the normalized def.
- **`glue/idx_si.go`** — the `secondaryIndex` (`datastore.Index`) + read-only
  `siIndexer` (`datastore.GSI`), advertised by wrapping the datastore
  (`maybeSecondaryIndexes`, wired in `FileStore`). bbolt-backed; rebuild-on-open
  validated by a **source signature** (file count + newest mtime). A
  process-global cache keyed by bbolt path opens/builds each index once (bbolt
  takes an exclusive file lock, so re-opening per Store would deadlock). Build
  scans the keyspace via `records.Walk`, evals key/where exprs per doc, inserts
  `encode(keyValues)+docID`.
- **Read path reused as-is.** `conv.go:VisitIndexScan` → `datastore-scan-index`
  → `DatastoreScanIndex` → `secondaryIndex.Scan` yields docIDs; the following
  Fetch reads docs via the keyspace's `Fetch` (`test/secondary_index_test.go`).

**Composite (multi-key) indexes work** (`keys: ["region","product"]`): the
self-delimiting encoding makes prefix matching correct, so leading-key-only,
full-key, leading+range, and IN predicates all use the index
(`TestSecondaryIndexComposite`). Per-component boundary exactness is
approximate, but the always-present residual `Filter` enforces the exact
predicate — correct, occasionally a slightly wider walk.

### Phase 2 — FTS via embedded bleve (shipped)

`SELECT … WHERE SEARCH(ks, "query")` runs locally against a `kind: fts` bleve
index (§7). Also shipped: `SEARCH_SCORE()`/`SEARCH_META()` surfacing, declared
field mappings, and the `SargableFlex` implicit-predicate flex path.

### Learnings that changed the plan

- **Covering scans (biggest surprise).** cbq turns a query whose
  projected/filtered fields are all index keys + `META().id` into a covering
  `IndexScan` with no Fetch, rewriting field refs into `expression.Cover` nodes
  that read a per-value cover slot n1k1 never fills → every field came back
  MISSING. Covering is on by default and can't be disabled without a fork edit.
  Fixes, entirely n1k1-side: `glue/expr.go:stripCovers` peels every
  `expression.Cover` to its underlying expression before eval. **True covering
  execution is shipped**: when the index is *coverable* (`indexDef.coverable` —
  every key a plain field ref, no filter-covers), `VisitIndexScan` emits a
  **`datastore-scan-index-cover`** op that reconstructs the projected doc from
  the decoded index-key values (`si.go` sets `IndexEntry.EntryKey`;
  `datastore_scan.go:reconstructCoverDoc` rebuilds `{field: value}`, nested paths
  included) — **no fetch**. A non-coverable covering scan (expression key like
  `LOWER(name)`, partial index, or non-n1k1 index) falls back to scan+fetch:
  `VisitIndexScan` synthesizes a `datastore-fetch` when `len(Covers())>0`. n1k1
  has no cover slots on its `[]byte` rows, so doc-reconstruction (not cbq's
  `SetCover`) realizes covering (`TestSecondaryIndexCovering`).
- **Multi-span sender close.** `DatastoreScanIndex` ran a goroutine per span,
  each `Close`-ing the shared sender — so an IN-list / same-field-OR /
  `DistinctScan` had the first span truncate the drain. Now all spans run in one
  goroutine sharing the sender, closed once, deduping docIDs
  (`secondaryIndex.scanSpan`).
- **Intersect/Union/Distinct scans.** A predicate over two indexed fields makes
  the planner emit `IntersectScan` (AND), `UnionScan` (OR), or `DistinctScan`
  (same-field OR / IN). Handled in `conv.go`: `IntersectScan`/
  `OrderedIntersectScan` → convert the **first** child, residual Filter enforces
  the rest (a superset the Filter narrows); `UnionScan` → fall back to a full
  records scan + Filter (can't drop an OR branch); `DistinctScan` → convert the
  inner scan (spans disjoint).
- **Build/scan number-encoding must agree.** A JSON number reaches build vs
  predicate-bound paths as different Go types (`float64` vs `int64`); `toFloat64`
  must handle both, or numeric scans return nothing.

### Not yet built (proposal below)

- Incremental index maintenance (insert/update/delete).
- A fingerprint / zone-map manifest.
- Predicated `CountIndex` pushdown (blocked on exact-spans — §10).

### Known v1 limitations

- Freshness is a coarse (file count, newest-mtime) signature; a change keeping
  both identical won't trigger a rebuild — run `.index rebuild`.
- Array/object index *values* sort by byte order, not collation (fine —
  predicates range over scalars).

---

## 2. Motivation

Today every n1k1 query over the file datastore is a full primary scan + residual
filter. We want the planner to use an index when one applies — in-process, no FTS
cluster or GSI service — so selective queries don't read the whole keyspace.

---

## 3. Background: how index selection works

> **n1k1 takes cbq's *plan*, not its runtime.** `glue/stmt.go:PlanStatement`
> calls cbq's real `planner.Build()`, so index selection is decided entirely by
> cbq's planner, driven by what the datastore advertises through the
> `Keyspace → Indexer → Index` / `FTSIndex` interface tree
> (`datastore/index.go`). n1k1 **replaces cbq's execution runtime** with its own
> `base.Op` engine over `base.Val = []byte`. The fork is a source of *plans* +
> index metadata; n1k1 owns execution.

Pipeline (`glue/session.go:Run`):

```
SQL++ → ParseStatement → algebra.Statement
      → PlanStatement (cbq planner.Build) → plan.Operator tree
      → conv.go (plan.Visitor) → base.Op tree
      → engine.ExecOp → glue datastore-scan/fetch ops → rows
```

Giving the planner an index is a matter of the datastore advertising one:

- `planner/build_scan_secondary.go:sargableIndexes()` reads each
  `index.RangeKey()` and calls `SargableFor(pred, …)`: DNF-normalizes `WHERE`,
  matches it against the index's key expressions (honoring the partial-index
  `Condition()`), builds `datastore.Span`s, emits `IndexScan` + `Fetch` +
  residual `Filter`.
- **GSI sargability is built into the core planner** — only the index's
  `RangeKey()` expressions are needed.
- **FTS sargability is externalized** into `datastore.FTSIndex` (`Sargable` /
  `SargableFlex` / `Pageable`); a small in-process shim suffices.

### Why n1fty is not required

n1fty bundles (1) planner-facing sargability/metadata (`datastore.FTSIndex`) and
(2) a runtime executor shipping requests to a remote **cbft** cluster. Only the
*shape* of (1) is needed; (2) is replaced by in-process `bleve.Index.Search()`.
So cbft, cbgt, n1fty, and cbauth drop out.

### What already exists

- **Execution glue.** `glue/datastore.go` routes `"datastore-scan-index"` →
  `glue/datastore_scan.go:DatastoreScanIndex`, which evaluates `plan.Span`s,
  calls `scan.Index().Scan(...)`, drains `conn.Sender().GetEntry()`, and yields
  `entry.PrimaryKey` to `datastore-fetch`. No read-path changes needed.
- **Plan-op selection is by interface assertion.** `planner/spans_term.go:
  CreateScan` emits `plan.IndexScan3` only for a `datastore.Index3`, `IndexScan2`
  only for `Index2`, else base `plan.IndexScan`. So an index implementing only
  base `datastore.Index` forces `plan.IndexScan` — which `conv.go:VisitIndexScan`
  already converts. `IndexApiVersion` stays `INDEX_API_MAX` (irrelevant; the
  interface gates the choice).
- **The file datastore lives in an editable fork.** `github.com/couchbase/query`
  is `replace`d with `github.com/couchbase/n1k1-query` at `../n1k1-query`. The
  file datastore is `datastore/file/file.go`; its `fileIndexer` owns
  `indexes map[string]datastore.Index` and a `primaryIndex` to model from.
  `fileIndexer.Indexes()` originally returned only `fi.primary`.
- **CREATE INDEX DDL is not wired** (`conv.go:VisitCreateIndex` returns `NA()`);
  v1 defines indexes via the sidecar catalog.

---

## 4. Where the code lives (thin hook seams)

> **Superseded for Phase 1** by datastore wrapping (§1) — kept as the original
> proposal and as a fallback if wrapping proves insufficient.

`datastore.Index`/`Indexer`/`FTSIndex` are interfaces `glue` imports; Go
interfaces are structural, so n1k1 implements them and the planner uses them
polymorphically. The two proposed fork seams (in `datastore/file/file.go`):

- **GSI/secondary:** `var SecondaryIndexes func(datastore.Keyspace)
  []datastore.Index`, merged into `fileIndexer.Indexes()` +
  `IndexNames`/`IndexById`/`IndexByName`/`IndexIds`.
- **FTS:** `var ExtraIndexers func(datastore.Keyspace) []datastore.Indexer`,
  appended in `keyspace.Indexers()` (a distinct `IndexType` — clean append).

Everything else (index types, bbolt/bleve backing, build, catalog loading) is an
ordinary n1k1 package registered at startup; `bbolt`/`bleve` become n1k1 direct
deps. The wrapping approach eliminated even these seam edits.

---

## 5. CLI control (build timing & introspection)

Index lifecycle is build-on-first-use, so the CLI exposes controls. All live in
n1k1 (`cmd/n1k1`, `glue/idx_si.go`); the fork is untouched.

### `-index=eager|lazy|off` — *when* catalog indexes build

Via process-global `glue.SecondaryIndexMode` (re-read on every
`maybeSecondaryIndexes`). All three give identical *results* (builds cached +
freshness-validated); they trade off build *timing*:

- **`lazy`** (default) — advertise indexes; each builds on first use. Normal use.
- **`eager`** — `EagerBuildSecondaryIndexes` builds *every* catalog index up
  front (clean `-timer` benchmarks; surfaces build errors early). Builds run
  **concurrently** (one worker per CPU, capped at index count) — each index is an
  independent bbolt file. The open/build cache is a per-path `indexSlot` (a `once`
  opens the OS-lock-contended bbolt file; a per-slot mutex serializes that index's
  rebuilds), so different indexes build in parallel and the same index never
  double-opens. Progress streams as `IndexBuildEvent`s to an optional reporter;
  the CLI renders a live per-index bar on a TTY (`cmd/n1k1/indexprogress.go`), or
  one line per finished index when piped. Bar denominator is the source **file
  count** (exact for one-doc-per-file; a lower bound otherwise).
- **`off`** — `maybeSecondaryIndexes` returns the datastore unwrapped; no index
  advertised, planner always primary/records-scans. The A/B baseline + escape
  hatch.

### `.index` command family

- **`.index list`** (bare `.index`; `.indexes` alias) — one line per index:
  `ns:keyspace.name (keys…) [WHERE …] [N entries, SIZE]`, opening/building any
  not-yet-built index for live bbolt stats (`Bucket.Stats().KeyN`, `os.Stat`).
  "disabled" under `-index=off`. Doubles as "build now".
- **`.index show <name>`** — full detail of one index (keyspace, keys, WHERE,
  entries, size, on-disk path).
- **`.index rebuild [<name>]`** — force-rebuild regardless of freshness signature
  (`glue.RebuildSecondaryIndexes` → `buildIndexesConcurrent(force=true)`). Escape
  hatch for the coarse freshness check without deleting the `.n1k1` artifact.
- **`.index help`** (alias `example`) — subcommand syntax + a copy-pasteable
  `catalog.json` example.
- **`.index suggest [<keyspace>]`** — the advisor (§9): samples docs, prints an
  editable `catalog.json` fragment (each def carries a `why` the loader ignores).
  Advises **both types by query shape**: a `gsi` index for selective scalar
  (`nested-no-array`) fields, a `kind:fts` index for text fields (multi-word/long
  strings, poor b-tree keys). A field fitting both yields both suggestions, each
  tagged. With ≥2 text fields, also suggests a whole-keyspace dynamic FTS. Dedup
  is per-kind (`glue.SuggestIndexes`, `idx_si_suggest.go`).
- **`.index create ...`** — add def(s) to `catalog.json` and build. Two forms: a
  DSL `.index create <name> on <keyspace> (<expr>[, <expr>]) [where <expr>]`, or
  a JSON fragment `.index create {"indexes":[…]}` (pastes back `.index suggest`
  output; `why` accepted and dropped on write). Validates, merges (dup names
  rejected; won't clobber other sections), re-opens, builds
  (`glue.CatalogAddIndexes`). Explicit user intent, so writing the catalog is
  fine (single-writer rule bars only *background* rewriting).

### Design stance on scope

Per-index knobs (collation, value-size cap + truncation marker, `defer`, CBO
stats) belong in `catalog.json` — properties of a definition, the single-writer
source of truth (§8). Flags/dot-commands are reserved for process-wide
timing/introspection/lifecycle. DDL (`CREATE/DROP INDEX`) stays unwired in v1.

---

## 6. Phase 1 — GSI-like secondary index

Phase 1 code lives in **n1k1** (`secondaryIndex` type, build, sidecar loading),
wired by wrapping the datastore (§1). `bbolt` becomes a direct n1k1 require.

### Storage backing: `go.etcd.io/bbolt`

- **rhmap — rejected.** `RHStore.Visit` iterates in hash-bucket order, not key
  order → no range scans.
- **bbolt — chosen.** B+tree, persistent; `Cursor.Seek`/`Next` give the ordered
  range iteration `Scan()` needs; one file per index; already in the module graph
  (`v1.4.0`, promoted to direct). moss is a viable but heavier (LSM/compaction)
  alternative not needed for a read-mostly index.

**Key encoding** (self-delimiting, stated once): each bbolt key is
`encode(keyValue) + 0x00 + docID` — the `0x00`-delimited docID suffix
disambiguates duplicate secondary values and is always recoverable; value is
empty. The shipped `idx_si_encode.go` is order-preserving, so bbolt byte order
matches collation order and `Cursor.Seek` prunes directly. **Collation
correctness is the highest-risk area.**

### Why not Parquet/Iceberg/Delta as the index store?

Use them for **coarse pruning**, not the fine-grained ordered index:

- **Clustered / data-skipping** (coarse, format-native): Parquet/ORC footer
  min/max, page/column index, bloom filters; Iceberg/Delta manifests + Puffin.
  This is the "index-everything-lite" tier (§9) / the manifest zone-maps in
  `DESIGN-data.md §5` — no cbq planner changes (pruning is a scan-layer concern).
- **Secondary, non-clustered, ordered index** (GSI/b-tree): a compact
  `key → docID` map with O(log n) seek to an arbitrary key. Columnar formats are
  **not** a substitute — pruning granularity is row-group/page/file, files are
  immutable, no columnar spec has an ordered-secondary-index. **bbolt is the right
  tool.** (Avro is row-oriented — for logs/manifests, not indexes.)

Use columnar libraries to *help build* bbolt: (1) Arrow projection pushdown reads
only the key column + row locator; (2) Iceberg manifests/Puffin as change-detection
+ zone-map layer; (3) for columnar sources the docID is `file#row_position`.

### Index definition & build (sidecar)

- **Definition:** `.n1k1/catalog.json` (canonical),
  `[{ "name", "keys": ["expr", …], "where"? }]` — §8 for the full `.n1k1/` scheme.
- **Load:** parse each key/where string with the n1ql parser, return
  `secondaryIndex` objects, opening/creating each bbolt file at
  `.n1k1/<ns>/<ks>/idx/<name>__si__<defhash>/data.bolt`. Cached per keyspace.
- **Build (v1 = rebuild-on-open):** full keyspace scan (`records.Walk`) → eval key
  exprs + `where` per doc → insert `(encodedKey → docID)`. Gated behind sidecar
  presence.

### The `secondaryIndex` type (base `datastore.Index` only)

Holds a `*bbolt.DB` + bucket name + parsed `rangeKeys`/`where`. Used
polymorphically.

| Method | Implementation |
|---|---|
| `KeyspaceId`/`Id`/`Name`/`Indexer` | trivial accessors |
| `Type()` | `datastore.GSI` |
| `IsPrimary()` | `false` |
| `SeekKey()` | `nil` |
| `RangeKey()` | parsed key expressions — **drives sargability** |
| `Condition()` | partial-index `where` expr, or `nil` |
| `State()` | `(datastore.ONLINE, "", nil)` |
| `Statistics()` | `(nil, nil)` — safe while `useCBO=false` |
| `Drop()` | drop from registry + trash the bbolt file |
| `Scan(reqId, span, distinct, limit, cons, vector, conn)` | core — below |

**`Scan()` contract:** `defer conn.Sender().Close()`; `Seek` to `span.Range.Low`
(or first if unbounded); iterate ascending; decode key, stop at
`span.Range.High`, honor `span.Range.Inclusion & datastore.LOW/HIGH` via
`Collate`; for each match `SendEntry(&datastore.IndexEntry{PrimaryKey: docID})`;
respect `limit`. `EntryKey` is filled only for a covering scan (carries decoded
key values for `reconstructCoverDoc`); the default drain reads only `PrimaryKey`.

---

## 7. Phase 2 — FTS via embedded bleve ✅ SHIPPED

`SELECT … WHERE SEARCH(ks, "query")` runs locally against an embedded
`bleve.Index` — no cbft, no n1fty, zero fork edits. The planner hook existed
(`planner/build_scan_search.go` + `SargableFlex`); we set `useFts=true` in
`glue/stmt.go` and provide the `datastore.FTSIndex` in-process (a small shim).
Landed in `glue/idx_fts.go`:

- **`ftsIndexer` + `ftsIndex`** — an `Indexer` (`Name()==datastore.FTS`) and an
  `FTSIndex`, advertised by appending to `keyspace.Indexers()` (distinct
  `IndexType` — clean append). Backed by an embedded `bleve.Index`:
  - `Sargable(field, query, options, mappings)` returns `exact=true` so the
    planner drops the residual predicate; `SargableFlex` stubbed and `Pageable`
    `false` in v1 (flex later shipped).
  - `Search(...)` runs `bleveIndex.Search()` (`req.Size=DocCount`) and pushes
    `datastore.IndexEntry{PrimaryKey: hit.ID, MetaData: hit.Score}` into
    `conn.Sender()`.
- **conv.go / DatastoreScanFTS:** `VisitIndexFtsSearch` emits one
  `datastore-scan-fts` op. `DatastoreScanFTS` runs the bleve search and
  **fetches the matching docs itself** (not via a following `plan.Fetch`, because
  the hit score is only available at the scan and would be lost across a separate
  fetch — so `VisitFetch` passes through after an FTS scan). The residual
  `SEARCH()` the planner leaves in the `Filter` (which n1k1 can't re-evaluate) is
  rewritten to `TRUE` by `stripSearch` (`glue/expr.go`), gated on a `sawFTS` flag
  so a genuine co-predicate (`… AND d.id = "d1"`) is preserved.
- **Score/meta surfacing (shipped):** `SEARCH_SCORE(alias)` / `SEARCH_META(alias)`
  return the bleve score/meta. The score rides the `^smeta` label as
  `{outname: {score, id}}`, which `ConvertVals` binds under `value.ATT_SMETA` —
  where the `search` package's `SearchMeta`/`SearchScore` read it.
- **Catalog/build:** `kind: fts` in `catalog.json`; bleve index built into
  `.n1k1/<ns>/<ks>/idx/<name>__fts__<defhash>/bleve/` from a full scan on open,
  with the Phase 1 source-signature freshness check.
- **Declared mappings (shipped):** empty `keys` = dynamic (index every field,
  default); listed field keys build a non-dynamic mapping (nested dotted paths →
  sub-document mappings), so `SEARCH()` on other fields matches nothing.
- **Flex / implicit predicates (shipped):** `SargableFlex` lets a plain `WHERE`
  predicate (no explicit `SEARCH()`) be served by bleve. It translates the
  sargable part (`Eq`/`LT`/`LE`, `AND`/`OR`; `>`/`>=` → swapped `LT`/`LE`) over
  indexed fields into a bleve query DSL; the planner wraps it as synthetic
  `SEARCH(ks, <query>)` → `plan.IndexFtsSearch`. **Correctness is independent of
  translation precision:** we never set `FTS_FLEXINDEX_EXACT`, so the planner
  keeps the original predicate in the residual `Filter`, which n1k1 re-evaluates —
  the bleve query need only be a superset. An `AND` may drop an untranslatable
  conjunct (still a superset); an `OR` bails if any disjunct is untranslatable; a
  wholly untranslatable predicate falls back to a records scan.

**Remaining follow-up:** declared fields are all mapped as text (per-field
analyzers/types not yet configurable).

---

## 8. Sidecar layout (`.n1k1/`)

A dataset accumulates many independent derived artifacts (several GSI indexes,
FTS/bleve indexes, zone-maps/bloom, count caches, change manifests) across
keyspaces, each with its own definition, format version, and rebuild lifecycle.
`.n1k1/` must let these coexist, build/drop/GC independently, swap atomically, and
match back to the exact definition + source state.

### Directory tree
```
<dataRoot>/.n1k1/
  LAYOUT                       # sidecar layout format version
  catalog.json                # source of truth: all defs + config fingerprint
  <namespace>/<keyspace>/
      manifest.json           # source fingerprints + zone-maps (DESIGN-data §5)
      idx/
        <name>__<kind>__<defhash>/    # one dir per built index instance
          meta.json                   # def, kind, key exprs, format_version, built_from, state, stats
          data.bolt                   # kind=gsi   : bbolt B+tree
          bleve/                      # kind=fts   : bleve index directory
          zonemap.cbor | bloom.bin    # kind=zonemap | bloom
          count.json                  # kind=count : cached COUNT(*)
      tmp/<name>__<kind>__<defhash>.<gen>/   # in-progress build; atomically renamed
      trash/                                 # dropped/orphaned, awaiting lazy delete
```

### Instance name: `<name>__<kind>__<defhash>`
- **`name`** — user-facing name, filesystem-sanitized (true name in `meta.json`).
- **`kind`** — `gsi` | `fts` | `zonemap` | `bloom` | `wildcard` | `count`. Lets
  schemes coexist on the same keyspace, even the same key.
- **`defhash`** — short hex hash of the normalized definition (key exprs + `WHERE`
  + options + collation/format version). The workhorse:
  - **Redefinition safety:** a changed def yields a new `defhash` ⇒ new directory;
    the old is orphaned and GC'd — no in-place corruption or stale reads.
  - **Planner matching:** "is there a built index for this def?" = a
    directory-existence check; only instances whose `built_from` matches the
    current source manifest are advertised.
  - **Self-describing:** `catalog.json` reconstructable by scanning `idx/`.

### Atomic build, versioning, lifecycle
- Build into `tmp/….<gen>/`, then **atomic rename** into `idx/…/` (POSIX dir
  rename, same filesystem) so readers never see a half-built index. Concurrent
  readers during rebuild: `<gen>` suffix + `CURRENT` pointer; simplest v1 = single
  instance + rename swap.
- Rebuild triggered by: `format_version` bump, changed `defhash`, or `built_from`
  ≠ current source manifest / `catalog.config_fingerprint`.
- **GC:** on open, reconcile `idx/` against `catalog.json` + source manifest;
  orphans/stale instances move to `trash/`, deleted lazily. Drop removes the
  catalog entry and trashes the instance dir.

### Encryption & definition home
- With encryption-at-rest (DESIGN-data §6), artifact payloads (`data.bolt`,
  `bleve/`, `zonemap`, manifests) are encrypted with the dataset DEK; `meta.json`
  records the wrapping key id. The whole `.n1k1/` tree is in scope.
- **Single-writer rule.** `catalog.json` comingles source mappings + index defs
  safely *only because it stays single-writer*: it holds **declared intent**
  (human/generator-authored, slow-changing). Everything machine-managed and
  dynamic — build-state, stats, and adaptive/auto-created indexes — lives in
  self-describing per-instance dirs (`idx/<…>/meta.json`), never written back into
  `catalog.json`. So indexers of any `kind` build/rebuild/drop concurrently in
  **different dirs** with no shared-file contention. Adaptive indexes that rewrote
  `catalog.json` would break the single-writer property — don't.

---

## 9. "Index everything": dynamic / wildcard / automatic indexes

bleve's **dynamic mapping** = "index every field, structure by type." The
question: what's the **B-tree / GSI equivalent** — "index every scalar path with
an ordered/range structure"? Strong prior art in three families.

### Prior art

**Eager "index everything up front":**
- **Azure Cosmos DB** — the closest analog, and the default. Automatically
  indexes every property, no schema; the default policy enforces a **range index**
  on every string/number path. An inverted index (JSON path → items), three kinds:
  Range, Composite, Spatial. Opt-out via `excludedPaths`/`includedPaths` + `/*`.
  The design to emulate for "everything indexed by default."
- **MongoDB wildcard index `{"$**": 1}`** — eager index of every field path;
  include/exclude via `wildcardProjection`. Caveats: planner uses it for only one
  predicate field per query, no equality on whole objects/arrays, subtle arrays,
  slower than a targeted index. Opt-in.
- **Elasticsearch / Lucene dynamic mapping** — auto-detect type, pick structure
  per type: numeric/date/geo → BKD tree (points), keyword → doc-values, text →
  inverted. Lesson: "index everything" should **route by inferred type**.
- **PostgreSQL GIN on `jsonb`** — one inverted index over all key/value pairs
  (containment `@>`, existence `?`, equality); weak for ranges.

**Cheap always-on approximate (prune, don't seek):**
- **BRIN / min-max / zone maps** (Postgres BRIN, Oracle zone maps, ORC/Parquet
  stats, MonetDB) — summarize each block by min/max; tiny; prune blocks/files.
- **Parquet column bloom filters** — per-column-chunk bloom for equality on
  high-cardinality columns (IDs/UUIDs). "Index every column for equality," cheaply.

**Adaptive / workload-driven ("index what's queried"):**
- **Oracle Automatic Indexing (19c)** — background task creates candidates
  invisible, verifies, then makes visible; drops unhelpful ones.
- **Azure SQL automatic tuning** — auto create/drop from workload.
- **RavenDB auto-indexes** — auto-creates on an unmatched query, merges/GCs.
- **SQLite transient indexes** — throwaway B-tree for one query.
- **Database cracking** (Idreos, MonetDB) — the index self-organizes as a side
  effect of query processing.

### Recommendation — three tiers

**Tier 1 — "index-everything-lite": always-on zone maps + optional per-file
bloom** at the scan/datastore layer. Cheap, always-on, and **needs no cbq planner
changes** (pruning is a datastore/scan concern, not index-selection). Already the
manifest zone maps in `DESIGN-data.md §5`. The pragmatic default.

**Tier 2 — Adaptive auto-index (RavenDB/Oracle-style)** as the self-managing GSI:
log the predicates the planner produces, auto-`createSecondaryIndex` an ordered
index for hot field(s), GC unused. The created index is a **normal `RangeKey`
index the cbq planner already understands** (Phase 1 machinery) — no
wildcard-planner work. The realistic medium-term path.

*Seeding candidates by sampling (proactive cold-start).* Sample N docs, per
top-level scalar path estimate cardinality, presence, type stability, value size.
The **b-tree auto-index signal is HIGH cardinality (÷ doc count) + queried**, not
low: a high-cardinality field prunes hard; a low-cardinality field (`status`,
`country`) matches a large fraction and barely beats a primary scan. Low-card
fields are still valuable — for zone-maps/partition pruning (tier 1), bitmaps, or
a composite leading key. Best policy: sampling proposes candidates, the
**workload confirms** which are queried — auto-create where `queried ∧ selective`.
These estimates are what a future `Index.Statistics()` should return to feed cbq's
CBO instead of `nil`.

*Which fields are eligible (path walk during sampling):*
- **Scalar leaf, no array crossed** — the b-tree candidates. A top-level scalar
  or pure-object nested path (`personal_details.state`) resolves to one scalar per
  doc; the key expression is the dotted path.
- **Any array segment** (or the value is an array) — **skip for the scalar tier.**
  Multi-valued per doc needs cbq's **array index** (`(DISTINCT) ARRAY … FOR … END`,
  sargable via `ANY`/`UNNEST`) — a separate, harder class (flag, don't
  auto-create in v1).
- **Type-unstable / synthetic `_meta` / oversized values** — skip or mark
  low-confidence.

*CLI ladder: advise → human edits → create/build* (output format == input format;
the advisor emits a `catalog.json` fragment):
- **`.index suggest`** *(advisor, read-only)* — **SHIPPED.** Samples docs, scores
  selective scalar / nested-no-array fields, prints a `{"indexes":[…]}` fragment
  with a per-suggestion `why` (loader ignores it, so it pastes back verbatim).
- **`.index create ...`** *(explicit apply)* — **SHIPPED.** DSL or JSON fragment;
  append def(s) to `catalog.json` and build. Explicit user intent, so writing is
  fine.
- **`.index auto`** *(autonomous, later)* — sampling + workload confirmation + GC,
  writing a **separate machine-managed auto-catalog**, never the human catalog.

**Tier 3 — Eager wildcard GSI (Cosmos/Mongo-style)** — a bbolt store keyed
`encode(path) + encode(value) + docID` so any single-path equality/range is
contiguous.

*Key layout constraints.* Don't use a bucket/collection per field-path (thousands
of paths; KV stores cap/slow with many containers). Encode the field-path into the
key: `<fieldPathShortPrefix> : <encodedValue> : <docID>`. Keep the prefix
fixed-width via a **dictionary mapping field-path → short byte prefix** (monotonic
id per path, stored in its own bucket). `WHERE a.b.c = v` → look up prefix, then
`Seek(prefix + encode(v))`. Reuses Phase 1's order-preserving `encodeValue` for
`<encodedValue>`.
- **Value size limits.** bbolt keys are bounded (~32 KB, and large keys wreck
  B+tree fan-out well before that). The encoder must **cap the encoded value**:
  truncate long strings/blobs to a prefix and set a "truncated" marker bit so the
  scan knows the residual predicate must be re-checked (it always is). Over-cap
  values become a *prefix probe*.

Feasible to build; the hard part is **planner integration** — cbq's
`sargableIndexes` matches predicates against a *fixed* `index.RangeKey()`, with no
concept of a wildcard index over arbitrary paths. A true wildcard GSI needs
fork-side planner work and inherits Mongo's caveats. Research item, not Phase 1.

**Symmetry with FTS:** bleve dynamic mapping gives "index all text" for free. So
n1k1's full "index everything" posture = **bleve dynamic (text)** +
**zone-maps/bloom (cheap scalar pruning)** + **adaptive auto-index (hot scalar
fields)** — no giant always-on wildcard structure. If we ever build the eager
wildcard GSI, **route by inferred type** (bbolt for scalars, bleve for text/geo).

### Prior-art links
- Cosmos DB: https://learn.microsoft.com/en-us/azure/cosmos-db/index-overview ,
  /index-policy
- MongoDB wildcard: https://www.mongodb.com/docs/manual/core/indexes/index-types/index-wildcard/
- Elasticsearch dynamic mapping: https://www.elastic.co/docs/manage-data/data-store/mapping/dynamic-field-mapping
- Postgres GIN: https://www.postgresql.org/docs/current/gin.html ; BRIN:
  https://www.postgresql.org/docs/current/brin.html
- Parquet bloom filters: https://parquet.apache.org/docs/file-format/bloomfilter/
- Oracle Automatic Indexing: https://oracle-base.com/articles/19c/automatic-indexing-19c
- Azure SQL automatic tuning: https://learn.microsoft.com/en-us/azure/azure-sql/database/automatic-tuning-overview
- RavenDB auto-indexes: https://ravendb.net/features/indexes/intelligent-auto-indexes
- SQLite automatic indexes: https://sqlite.org/optoverview.html
- Database cracking: https://www.vldb.org/pvldb/vol4/p586-idreos.pdf

---

## 10. COUNT(*) / count-scan pushdown

`SELECT COUNT(*)` should never enumerate/fetch docs when the count can be pushed
to the datastore or an index. cbq's planner already knows how; n1k1 just doesn't
convert the resulting operators yet.

### How the planner expresses it
- **`plan.CountScan`** — whole-keyspace count; calls `keyspace.Count(context)`.
- **`plan.IndexCountScan` / `IndexCountScan2`** — count with a sargable predicate,
  pushed to a `datastore.CountIndex` (`Count(span, …) int64`) / `CountIndex2`.
- **`plan.IndexCountDistinctScan2`** — `COUNT(DISTINCT …)` (`CountDistinct`).
- **`plan.IndexCountProject`** — projects the pushed-down scalar into the column.

### Current state
- **Whole-keyspace `COUNT(*)` done** (item 1): `conv.go:VisitCountScan`
  de-optimizes to a records scan + `count(*)` group-aggregate (correct for every
  format; O(1) count is the manifest item below).
- The predicated/index count visitors return `NA()`, but that `NA()` is currently
  **unreachable** — the planner won't emit them without exact spans / `Index2`
  (item 2), so it's not a gap.
- **Datastore side partly done:** `keyspace.Count()` (returns `len(ReadDir)`) and
  `Size()` exist (`file.go:467`). Whole-keyspace `COUNT(*)` is mostly a conv +
  execution wiring job.

### Implementation (lowest-friction first)
1. **Whole-keyspace `COUNT(*)`.** `conv.go:VisitCountScan` → emit a
   `"datastore-count"` op carrying the keyspace; a glue op calls
   `keyspace.Count(context)` and yields a single row / one int64. Implement
   `VisitIndexCountProject` to shape the column. No datastore changes. Do first.
2. **Predicated `COUNT(*)` via secondary index — BLOCKED on `Index2` (verified).**
   The mechanical parts were prototyped: `secondaryIndex.Count(span)` (bbolt
   cursor tally over the span), `conv.go:VisitIndexCountScan` (a
   `datastore-index-count` op summing `index.Count`, yielding one int64 under a
   `^count` label), `VisitIndexCountProject`. **All correct — but the cbq planner
   never emits `plan.IndexCountScan` for a base `datastore.Index`.** Root cause:
   count pushdown lives in the *covering* path
   (`build_scan_covering.go:buildCoveringPushdDownIndexScan2` →
   `build_scan_pushdowns.go:indexCoveringPushDownProperty`), gated on
   **`_PUSHDOWN_EXACTSPANS`**. A base (API1) index's spans are never marked exact
   — which is also why every base-`IndexScan` carries a residual `Filter`. No
   exact spans ⇒ no `_PUSHDOWN_GROUPAGGS` ⇒ no `IndexCountScan`; the planner does
   primary-scan + filter + aggregate instead. Confirmed empirically.
   - **`Index2` is necessary but NOT sufficient (verified).** A second prototype
     implemented `datastore.Index2` (`RangeKey2` + `Scan2` over `Spans2`) +
     `conv.go:VisitIndexScan2` + a `datastore-scan-index2` op. This DID make the
     planner emit `plan.IndexScan2` — but **the residual `Filter` still was not
     dropped**, so spans still weren't treated as exact and count pushdown still
     didn't fire. Neither `IK_MISSING` on the leading key nor the
     `N1QL_INDEX_MISSING` feature control changed that (and `IK_MISSING` without
     actually indexing MISSING values would be a correctness bug for `IS MISSING`).
     `sarg_eq` builds the equality span exact (`NewSpan2(…, true)`), so exactness
     is cleared/ignored somewhere further along that this pass didn't pin down.
     Both prototypes reverted; **filter-elimination and count pushdown remain
     open**, pending a deeper trace. Likely next probes: `useCBO=true`; the
     `filterCovers`/`coveringScan` filter-retention path; whether
     `Index3`/`IndexScan3` (group-agg pushdown) differs.
3. **`COUNT(DISTINCT …)`.** Needs `CountIndex2.CountDistinct`; defer — the planner
   otherwise falls back to distinct+aggregate (works, slower).

### Manifest synergy (`DESIGN-data.md §5`)
Once the manifest tracks per-file/per-partition **`doc_count`**, `COUNT(*)` over a
whole keyspace/partition is answered **O(1) from metadata** — no `ReadDir`, no
scan. For predicated counts, sum precomputed counts for fully-covered partitions
and only scan boundary partitions.

---

## 11. Verification

- **Phase 1:** define an index over a field, run a query whose `WHERE` matches the
  leading key, confirm via `Result.Plan` it is an **`IndexScan`, not
  `PrimaryScan`**, and results match the no-index run. `go test -tags n1ql
  ./glue/...` + the `test/` conformance harness.
- **Covering (done):** project only index-key fields (`SELECT s.region, s.product
  FROM s WHERE s.region="US"` over `keys:["region","product"]`); confirm the plan
  is **`datastore-scan-index-cover`** with **no `datastore-fetch`** and no records
  scan, yet results match (scalar, numeric, nested-path keys —
  `TestSecondaryIndexCovering`).
- **Phase 2 (done):** `SELECT … WHERE SEARCH(ks, "…")` returns expected docs, no
  cbft/network. Plan uses `datastore-scan-fts` (`TestFTSSearch`); results match
  whole-doc and field forms (`SEARCH(d,"quick")` → `d1,d2`; `SEARCH(d.title,
  "world")` → `d2`). `SEARCH_SCORE`/`SEARCH_META` return score/meta
  (`TestFTSScoreMeta`); `kind:fts` `keys` scope matches (`TestFTSDeclaredMapping`);
  a plain `WHERE` is served via the flex path, falling back to records scan for
  untranslatable predicates (`TestFTSFlexIndex`).

---

## 12. Risks & open questions

- **Collation correctness (highest).** bbolt byte-order vs N1QL `Collate`. The v1
  mitigation was decode-and-`Collate` boundary checks; the shipped
  order-preserving encoder makes bbolt byte order match collation directly.
- **Plan-op assertion assumption.** Relies on `spans_term.go:CreateScan` choosing
  base `IndexScan` for a base-only index (verified). A cbq rebase changing this
  would require implementing `conv.go:VisitIndexScan2/3`.
- **Index freshness.** File datastore mutations have no index-maintenance hooks.
  v1 = rebuild-on-open; incremental maintenance is v2.
- **Composite indexes — DONE.** Multi-key indexes work (`EvalSpan` /
  `DatastoreScanIndex` handle multi-column `Range.Low/High`; self-delimiting
  composite key encoding makes prefix matching correct;
  `TestSecondaryIndexComposite`); residual `Filter` covers boundary imprecision.
- **CBO.** `Statistics()` returning nil is safe while `useCBO=false`.
- **Interface-drift now lands in n1k1 (a feature).** The `datastore.Index`/
  `Indexer`/`FTSIndex` implementations live in n1k1, so a cbq rebase changing
  those signatures is a compile error in n1k1 (its natural owner — `conv.go`,
  `datastore_scan.go` already track them) rather than silent fork drift. The fork
  carries only the tiny seam declarations.

---

## 13. Affected files

**Fork (thin seams only; superseded by wrapping for Phase 1):**
- `../n1k1-query/datastore/file/file.go` — the seam alternative adds
  `var SecondaryIndexes func(...) []datastore.Index` (consulted in
  `fileIndexer.Indexes()`/`IndexByName`/`IndexById`/`IndexNames`) and
  `var ExtraIndexers func(...) []datastore.Indexer` (appended in
  `keyspace.Indexers()`, Phase 2). Both default to today's behavior. **No index
  types, build, or sidecar code here.**

**n1k1 (all real logic — `glue`):**
- `glue/idx_si.go`, `idx_si_encode.go`, `idx_si_catalog.go`, `idx_si_suggest.go`,
  `idx_fts.go` — `secondaryIndex` (`datastore.Index` incl. `Scan()`/`CountIndex`),
  bleve-backed FTS `Indexer` + `FTSIndex`, catalog reader, build routine,
  hook/wrapping registration.
- `glue/datastore_scan.go` — `DatastoreScanIndex`, `reconstructCoverDoc`
  (covering), `DatastoreScanFTS`.
- `glue/conv.go` — `VisitIndexScan` (+ `datastore-scan-index-cover`),
  Intersect/Union/Distinct handling, `VisitIndexFtsSearch`; `VisitCreateIndex` a
  future DDL hook.
- `glue/expr.go` — `stripCovers`, `stripSearch`.
- `glue/stmt.go` — `IndexApiVersion` (Phase 1); `useFts=true` (Phase 2); register
  wrapping/hooks near `engine.ExecOpEx = glue.DatastoreOp`.
- `cmd/n1k1` — the `-index` flag and `.index` dot-command family
  (`indexprogress.go`).
- `go.mod` — direct `go.etcd.io/bbolt` (Phase 1) and `blevesearch/bleve/v2`
  (Phase 2).

---

## 14. Dependency licensing

Policy: permissive only — **no GPL / AGPL**. New deps are compliant:
`go.etcd.io/bbolt` (**MIT**), `blevesearch/bleve/v2` (**Apache-2.0**); alternatives
considered `couchbase/moss` / `couchbase/rhmap` (Apache-2.0). Full table in
`DESIGN-data.md`.
