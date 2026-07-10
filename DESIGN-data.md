# Design: Data Sources for n1k1

Status: **partially implemented** (MVP + several post-MVP items shipped — see
"Implementation status"); the rest is proposal. Companion: `DESIGN-indexing.md`
(read together — see "Relationship"). Changelog lives in git history.

## Overview

This document designs the source data n1k1 ingests — file formats (JSONL,
multi-doc JSON, CSV/TSV, YAML, Parquet/ORC/Avro, extracted office/PDF), directory
layouts, compression/containers (`.gz`/`.zst`/`.zip`), how a `FROM` term resolves
to a set of files, synthetic `META().id`s / doc-IDs, `_meta` metadata injection,
and the `.n1k1/` sidecar (`catalog.json`, `manifest.json`) that keeps derived
artifacts (indexes, caches, zone maps) in sync with changing sources. It takes
inspiration from DuckDB, Spark, AWS Athena/Glue, ClickHouse, and Iceberg.

A reworked **§4 extract provider** and a new **[sorted-source contract](#sorted-sources)**
generalize how n1k1 ingests any tree of **heterogeneous, messy, domain-specific
files**: a two-phase `describe`/`extract` model with **pluggable extract recipes**
(matched by extension/regexp, loaded from a git repo) that turn irregular inputs into
typed rows plus the **timestamp-normalization + sortedness metadata** a **K-way
near-sorted merge join** (and ASOF temporal correlation) needs. See
`DESIGN-extensions.md` for the extract-function surface.

**n1k1 core stays domain-agnostic.** It knows nothing about any particular file
family; *all* format/domain knowledge lives in the pluggable recipes. One motivating
example — the one that drove `DESIGN-prepare.md`'s **PREPARE++** detector corpus — is
Couchbase `cbcollect_info` support bundles (a tree of a dozen log formats, JSON dumps,
and blobs), and it recurs as a concrete worked example ([P](#extract-bundle)). But the
*same* mechanism serves any such tree: SEC/financial filings, tax records,
astronomical survey catalogs, IoT/sensor streams, genomics runs, web/access logs. The
support bundle is an *example*, not a built-in.

The load-bearing decision: **new datastore behavior lives entirely n1k1-side in
thin glue seams over `[]byte`, not in the forked `couchbase/query` (cbq)
runtime** — and everything shipped so far needed **zero fork changes**. The two
organizing axes: *layered concerns* (decoder / layout / compression / derived
artifacts, each independently pluggable) and *allocation discipline*
(`base.Val = []byte`, buffer-reuse, no per-value boxing).

## Contents

1. [Implementation status](#implementation-status)
2. [Relationship to `DESIGN-indexing.md`](#relationship-to-design-indexingmd)
3. [Motivation, scope & starting point](#motivation-scope--starting-point)
4. [Architecture: where the code lives, compiler safety, MVP line](#architecture-where-the-code-lives-compiler-safety-mvp-line)
5. [Design principle: separate the concerns into layers](#design-principle-separate-the-concerns-into-layers)
6. [§1 File formats & decoders](#1-file-formats--decoders)
7. [§1 Allocation model & the read/fetch path](#1-allocation-model--the-readfetch-path)
8. [§2 Directory layouts & FROM-term resolution](#2-directory-layouts--from-term-resolution)
9. [§2 Query-defined virtual datasources (VIEWs & generated catalogs)](#2-query-defined-virtual-datasources-views--generated-catalogs)
10. [§3 Compression & containers](#3-compression--containers)
11. [§4 The `extract` provider — unstructured & semi-structured sources](#4-the-extract-provider--unstructured--semi-structured-sources)
12. [Sorted & near-sorted sources: the merge-join contract](#sorted-sources)
13. [Worked examples: sample trees and their FROM clauses](#worked-examples-sample-trees-and-their-from-clauses)
13. [§5 Indexes & derived artifacts: storage + change detection](#5-indexes--derived-artifacts-storage--change-detection)
14. [§6 Primary keys / document IDs (`META().id`) & `_meta`](#6-primary-keys--document-ids-metaid--_meta)
15. [Dependency licensing (permissive only)](#dependency-licensing-permissive-only)
16. [Testing strategy](#testing-strategy)
17. [Phasing](#phasing)
18. [Open questions](#open-questions)
19. [Sources](#sources)

---

## Implementation status

**The data-source work needed ZERO changes to the `n1k1-query` fork (cbq).** The
design predicted this ("fake it" for plan-time metadata; execution in n1k1 glue
ops) and it held. The fork's only n1k1-specific commits are build-plumbing, none
touching datasources: `semantics/semchecker_ce.go` (EE SQL++ semantics in the
community build), `system/systemStats.go` (pure-Go sigar-cgo stub), and a
committed goyacc-generated `parser/n1ql/y.go`. No `datastore/file`,
`datastore/virtual`, `algebra/`, or planner edits; no `DiscoverKeyspaces` seam.
Plan-time flat-root discovery was done n1k1-side by *wrapping* the fork's
datastore with `datastore/virtual` building blocks (`glue/flatroot.go`).

Landed n1k1-side (all `records/` + `glue/`, `//go:build n1ql`):

- **Flat-root keyspaces** (ex. B) — `glue/flat.go` wraps the datastore to
  advertise a synthetic `default:<basename>` keyspace with a primary index; the
  records-scan reads the root via `RecordsDir()`.
- **Single file as a keyspace** (B2) — the CLI arg may be a lone record file
  (`events.jsonl`, `orders.jsonl.gz`); `maybeFlatFile` fakes a `default:<stem>`
  keyspace whose `RecordsFile()` points the scan at the one file.
  (`test/flatfile_test.go`).
- **Grab-bag directory** (B3) — a dir with loose data files *and* unrelated
  subdirs exposes one keyspace per top-level *structured* file, by stem
  (`maybeFlat`), merging any real `default` namespace. (`glue/flat_test.go`).
- **Multi-file keyspace = union of files, recursing** (C, E) — the `records`
  package walks the dir and unions all decodable files.
- **Decoders:** JSONL/ndjson, multi-doc JSON (array + `.jsons`), CSV/TSV (J), and
  **YAML** (`.yaml`/`.yml`: `---`-separated stream or top-level sequence). CSV
  decodes each row into **one JSON object keyed by the header** (light
  int/float/bool inference), riding the **opaque-document path** — no typed-label
  work needed (§2 "Integration gap").
- **Office/PDF text extraction** (L) — `records/extract.go`, pure-Go
  (`.pdf`/`.docx`/`.xlsx` → one `{filename, kind, text}` record each), also
  opaque-document. Tika/extractous+OCR backend is future.
- **Transparent gzip** (`.gz`, H). `.zst` is *recognized* by the walker but
  decode is a stub (`records: .zst not yet supported`).
- **`-formats` lockdown flag** (`records.ParseModes`) — allow-list of
  formats/`recurse`/`gzip`.
- **`-meta` flag + `_meta` sub-object** (`records/meta.go`) — `on|off|auto`,
  injecting `path`/`name`/`ext`/`size`/`mtime`/`pos`. See §6.
- **`COUNT(*)` pushdown** — `glue` `VisitCountScan`.
- **Native byte-path fetch + per-request caches** (§1) — `DatastoreFetch` reads
  docs directly to `base.Val`, plus doc / scan-key-listing caches.
- **Fetch-by-key into `.jsonl` and `---` YAML containers** via byte offsets baked
  into the doc-ID (§1, §6).
- **Compiler-differential + decoder-golden tests** — flat-root diff, decoder
  interp-vs-compiler proof, a 443-interp/439-compiler data-backed GSI suite.

Still proposal / not built: catalog/sidecar (`.n1k1/catalog.json`), manifests &
zone maps (§5), Parquet/ORC/Avro, `.zip` containers, zstd decode, composite
offset doc-IDs & seekable-fetch for compressed containers, query-defined VIEWs
(the `UNION ALL` prerequisite has since landed — remaining gap is predicate pushdown
through the view), object-store backend, encryption-at-rest, inline table functions
(needs a grammar fork). **Also proposal (this rework):** the two-phase
`describe`/`extract` provider with pluggable JS extractors + ext/regexp matching
(§4); the sorted-source contract (normalized time key, `disorder_bound`, time zone
maps) and the K-way near-sorted **merge join** that consumes it
([sorted sources](#sorted-sources)) — the PREPARE++ enabler. The shipped extract
provider (ex. L) is the built-in baseline the rework generalizes.

## Relationship to `DESIGN-indexing.md`

These two docs are one design split in two and must stay coherent. Ownership:

- **This doc (data):** source formats/layouts, how a `FROM` term resolves to
  files, compression/containers, document extraction, synthetic `META().id`s, and
  the **change-detection manifest** (fingerprints + zone-map *data*).
- **`DESIGN-indexing.md` (indexing):** how the cbq planner comes to *use* an index
  (GSI via `RangeKey` sargability, FTS via `datastore.FTSIndex`), COUNT(*)
  pushdown, "index-everything" tiers, and the **canonical `.n1k1/` sidecar
  layout**.

Where they touch:

1. **Fork = plan-time metadata only; execution in n1k1 — fork untouched.** Both
   keep the fork thin via the `engine.ExecOpEx` IoC pattern. Here a keyspace's
   existence is faked by **wrapping** the fork's datastore with `datastore/virtual`
   building blocks (`glue/flatroot.go`), not the once-anticipated
   `DiscoverKeyspaces` seam. All execution runs in n1k1 glue ops over `[]byte`.
2. **The `.n1k1/` sidecar is shared.** The indexing doc owns the *canonical* tree;
   this doc owns `catalog.json`'s source/layout half and `manifest.json` (§5).
   `catalog.json` holds **both** source mappings **and** *declared* index defs —
   safe only because it stays **single-writer**; adaptive index state lives in
   per-instance dirs (§5 "Comingling").
3. **Zone maps are the load-bearing shared artifact.** The indexing doc's tier-1
   "index-everything-lite" *consumes* the zone maps this doc's manifest *produces*.
   Pruning is only "no-planner-change" once the **predicate reaches the scan** (§5
   caveat).
4. **Doc-IDs match:** columnar `file#row_position` = this doc's
   `<relpath>#<offset|line>` (§6).
5. **COUNT(*) synergy:** the indexing doc answers `COUNT(*)` from per-file
   `doc_count`.

## Motivation, scope & starting point

Today n1k1 reads one shape: a directory of single-document `*.json` files as
`<datastoreDir>/<namespace>/<keyspace>/<key>.json`. To be a useful local SQL++ CLI
("DuckDB for SQL++/JSON"), it should ingest the formats people actually have —
CSV/TSV, JSON Lines, multi-record JSON, logs, compressed archives, PDFs/office
docs — across their real directory layouts.

> **Note:** "Starting point" below is the *pre-MVP* landscape; the `records`
> package now supersedes the naive path-B decoders. Read it as historical
> motivation, not current state.

There were **two separate, only-partially-connected** data paths:

**A. The SQL++ / file-datastore path (what FROM uses):** layout
`<dir>/<namespace>/<keyspace>/<key>.json`, one JSON object per file, base name =
key. `FROM default:orders` → dir `<dir>/default/orders/` (case-insensitive).
**No** multi-file-per-keyspace, compression, or non-JSON formats.

**B. The engine's direct-file scan path (NOT reachable from FROM):**
`engine/op_scan.go` scan kinds `"filePath"`/`"csvData"`/`"jsonsData"`; `ScanFile()`
routes by extension. CSV reader is **naive** (splits on raw commas, no
quoting/type inference). A low-level primitive, not wired to keyspaces/FROM.

**Existing infrastructure to reuse:**
- A **working Iceberg + Arrow reader** in the fork
  (`primitives/external/iceberg_reader.go`, iterates Arrow RecordBatches over an
  Iceberg table scan) — **implemented but not wired in**; a natural target for
  Parquet/Iceberg/CSV and §5 change-tracking.
- A schema-inferencer hook (`GetDefaultInferencer`), but it's an EE feature the
  pure-Go build drops.
- **Deps already in `go.mod`:** `apache/iceberg-go`, `apache/arrow-go/v18`,
  `substrait-io/substrait`, `scritchley/orc`, `hamba/avro`, `klauspost/compress`,
  `blevesearch/bleve/v2`, `go.etcd.io/bbolt`; `buger/jsonparser` (direct, for JSON
  decode).

## Architecture: where the code lives, compiler safety, MVP line

### Where this code lives (the load-bearing decision)

n1k1 reuses cbq for its parser + **planner output, the `plan.Operator` tree**
(index selection, spans, join order, pushdowns). It does **not** use cbq's
**execution runtime** (tuple-by-tuple iteration over boxed
`value.AnnotatedValue`s) — that boxes a value per tuple/field, the opposite of
n1k1's `base.Val = []byte` buffer-reuse engine. `glue.Conv` lowers the plan tree
into n1k1 `base.Op`s over `[]byte`. So the fork is a source of *plans*, not a
runtime — which is why new datastore behavior belongs in **thin seams**. (cbq's
expression evaluation is reused as a fallback for complex expressions.)

`FROM default:orders` is resolved by cbq's planner, which asks the
`datastore.Datastore` interface for keyspace metadata. **Two needs hide behind
"the datastore" — only one touches the fork:**

- **(A1) Plan-time keyspace metadata — n1k1 fakes it, NO fork change.** The planner
  must believe the keyspace exists with a primary index. n1k1 **wraps the datastore**
  to advertise a *synthetic* namespace + keyspace (planner-facing metadata, no
  physical dir), reusing the fork's importable `datastore/virtual`
  (`virtual.NewVirtualKeyspace` + `NewVirtualIndex(isPrimary)`) so it emits a
  `PrimaryScan`. Implemented for flat roots in `glue/flatroot.go`; the synthetic
  keyspace's `RecordsDir()` points records-scan at the root. Extends to
  catalog-defined names later.
- **(A2) Execution-time scan/fetch — already n1k1's, no fork seam.** `conv` lowers
  `PrimaryScan`/`Fetch` to n1k1 `datastore-scan`/`datastore-fetch` `base.Op`s run by
  `glue.DatastoreOp`. Those ops **read the directory and decode records directly**
  (glue's `Store` knows the root; the keyspace gives ns+name ⇒ dir path), yielding
  `base.Val = []byte` and bypassing cbq's boxing. So the earlier
  `OpenRecordStream`/`FetchRecord`/`DiscoverKeyspaces` fork seams are **not needed**.
  All decoder/layout/doc-ID/compression code is ordinary n1k1 (the `records`
  package), registered via IoC (`engine.ExecOpEx = glue.DatastoreOp`).

**Rejected alternatives:**
- **(B) Wire path-B `op_scan.go` ops to `FROM`** — rejected: FROM resolution is in
  the planner, upstream of `conv`; the engine ops never see a keyspace.
- **(C) A new n1k1-side `datastore.Datastore`** — deferred: cleanest isolation but
  re-implements the whole datastore/index interface. Fallback of last resort.

Get right: **(1)** the synthetic keyspace is *minimal* — primary index only,
`Count()` may be lazy/0 (safe while `useCBO=false`). **(2)** it traffics in
`datastore.Keyspace`/`errors` (glue already imports them). **(3)** prefer hanging
the hook off the store/namespace instance; `ExecOpEx` is a process global (fine
for a one-process CLI).

### Compiler compatibility (don't break the Futamura path)

n1k1 is an interpreter **and** a compiler; a design that only works in the
interpreter would silently break compiled `FROM <file>` queries. **If FROM-file
scans keep flowing through the existing `datastore-scan`/`datastore-fetch` op
path, they compile for free** — that path is already compiler-safe (ops carry only
int `Temps`-indices as Params; the live datastore arrives at runtime via
`SetupCompiled*` re-planning). Consequences:

- **Do NOT introduce new engine scan *kinds* for new formats.** A `parquetData`
  op would fork the interpreter/compiler paths again. Decode the format **inside
  the existing glue ops** so the op *kind* is unchanged and the differential keeps
  passing.
- **Anything that can't be a Go literal arrives via `Temps`.** A live
  `RecordSource`/decoder isn't bakeable; supply it at runtime like the store.
  Format/layout *choices* (static strings/ints) go in Params, live handles in
  `Temps`.
- **Test hook:** the queryCases differential harness
  (`test/query_compiler_test.go`) is where a `FROM read_csv`-style case belongs.

### MVP line (what actually moves the needle next)

> **MVP = relax the file datastore so a keyspace directory is the union of *all*
> its files (recursing subdirs), add two decoders — JSONL and multi-doc JSON —
> plus transparent `.gz`. Prove one multi-file-keyspace case in the compiler
> differential.**

Phasing steps 1, 2a, and the gzip half of 3. Delivers ~80% of the "DuckDB for
SQL++/JSON" value, additive to the fork, compiler-transparent.

> **✅ SHIPPED — and then some.** The MVP landed, plus CSV/TSV (J), office/PDF
> extraction (L), flat-root keyspaces (B), and `COUNT(*)` pushdown — because CSV
> and office both fit the *opaque-document path* (emit a JSON object per row/doc)
> rather than needing typed-label work. The differential has flat-root + decoder
> cases. **Still behind the line:** Parquet, catalog/sidecar, manifests/zone-maps,
> `.zip`, zstd decode, encryption.

## Design principle: separate the concerns into layers

The lesson from DuckDB: **decouple four things** that n1k1 currently fuses into "a
keyspace is a directory of json files":

1. **Record format / decoder** — bytes → rows/JSON values.
2. **Layout / discovery** — a FROM term → the *set of files* to read, optionally
   deriving path-based columns (partitions).
3. **Compression / container** — transparently un-gzip/un-zst, or enumerate a
   `.zip`, beneath the decoder.
4. **Derived artifacts** — indexes/caches/extracted text + change-detection
   metadata.

Each layer is independently pluggable. The rest of this doc designs each.

## §1 File formats & decoders

### What DuckDB provides (reference)
- `read_csv` / `read_csv_auto`: a **sniffer** auto-detects delimiter, quoting,
  header, types (and gzip). CSV and TSV share the reader with a different
  delimiter.
- `read_json` / `read_ndjson`: JSONL *and* JSON arrays;
  `format='auto'|'newline_delimited'|'array'`.
- `read_parquet`, plus ORC/Avro/Arrow via extensions.
- **Replacement scans:** `FROM 'data/foo.csv'` works directly, reader by extension.

### Recommendation for n1k1
- Define a small `RecordSource` interface shaped **for buffer reuse**: prefer
  `ReadInto(rec *Record) error` (or `ForEach`) where `rec` holds **`[][]byte`
  field slices borrowed from a reused read buffer**, valid only until the next
  call — *not* a `Read() (value.Value, error)` that allocates per row.
- Priority order: **JSONL** and **multi-doc JSON** → **CSV/TSV** (one shared
  reader, delimiter param, DuckDB-style sniffer) → **Parquet** (via
  `apache/arrow-go`, reusing `iceberg_reader.go` as the columnar backbone) →
  ORC/Avro later (deps present).
- **Type handling:** JSON is naturally typed/loose. For CSV/TSV, sniff types but
  always allow "everything is a string" fallback; expose per-column overrides
  (DuckDB's `columns=`/`types=`).
- **Format selection:** by file extension, overridable by an explicit FROM-term
  option (§2); content sniffing only as a tiebreaker.

### Why Parquet sits below the line
n1k1's engine is row-at-a-time (`base.Vals` per row), built around
garbage-avoidance. Feeding Arrow *columnar* RecordBatches in means transposing to
rows and allocating per value — throwing away Parquet's columnar/pushdown
advantage. So Parquet's win requires a vectorized/column-batch op path **the
engine doesn't have today**. Treat Parquet as a *correctness* feature first
(query it at all), defer the *performance* win until/unless the engine grows
column-batch ops. (It's also the one format that genuinely forces real labels
rather than the opaque-document path — §2.)

## §1 Allocation model & the read/fetch path

n1k1 avoids garbage: **`base.Val` is `[]byte`**, and the engine parses values
*allocation-free* via `buger/jsonparser` (`base/arith.go`, `base/canonical.go` —
returns `[]byte` sub-slices, never boxing). A decoder that allocates an
`interface{}`/`string`/`map` **per value or tuple** blows that up over a large
file. Allocation behavior is a **selection criterion on par with correctness**.

### The rule
- Prefer readers that **read into a reused buffer** (`ReadInto`) or **return
  sub-slices borrowing a reused read buffer** (valid until the next `Read`, à la
  `csv.Reader.ReuseRecord` / jsonparser), over any `Read()` that allocates per
  row. State the **borrow/lifetime contract** explicitly — "copy to persist" —
  since retaining a borrowed slice past the next read corrupts data. (What
  `base.Val`'s "usually immutable" already assumes.)
- **Today already gets this right.** `ScanReaderAsCsv` reuses its row slice
  (`lzValsScan[:0]`) and yields fields as sub-slices of the scanner buffer
  (zero-copy). The replacement CSV reader must keep this; only the comma-splitting
  *correctness* is naive.
- **JSONL / JSON → `buger/jsonparser`** (direct dep): hands back `[]byte`
  sub-slices, no map materialization. On the FROM path raw record bytes go to the
  fork's lazy `value.NewValue(bytes)` (parses on demand), so JSONL stays
  near-zero-alloc end to end.
- **CSV/TSV — the real tradeoff.** Go's `encoding/csv` is correct but `Read()`
  returns freshly-allocated `[]string`; even `ReuseRecord=true` reuses only the
  slice header — field strings still allocate, and `string` is the wrong target
  for a `[]byte` engine. Best: (1) a `[]byte`-oriented CSV reader yielding
  sub-slices into a reused buffer (or fix the hand-rolled scanner's
  quoting/escaping while keeping its borrow model); else (2) `encoding/csv` +
  `ReuseRecord` as a correctness-first fallback.
- **Arrow / Parquet:** values live in pooled contiguous buffers, and
  `array.String/Binary.Value(i)` returns a **borrowed** sub-slice — use those,
  `Release()` each batch, reuse the allocator. Crossing into n1k1's row world
  still costs the transpose/copy.
- **Make it measurable.** Treat **allocations/op** (`go test -benchmem`, the
  `benchmark/` harness) as an acceptance metric per decoder — "allocs per row"
  near-constant regardless of file size.

### Measured (2026-07): the *fetch* path is where this broke
A 3-way nested-loop self-join (`SELECT COUNT(*) FROM orders o1, orders o2, orders
o3`, 262,144 rows) allocated **~931 MB** yet only ~3 MB live at exit — pure GC
churn. Via `go tool pprof -alloc_space`:

- **~71% (662 MB) is `glue.DatastoreFetch` → the fork's file `(*keyspace).Fetch`**,
  which materialized `value.AnnotatedValue`s and re-parsed with
  **`encoding/json.Unmarshal`, not jsonparser** — the eager boxing this section
  warns against.
- **~120 MB** copying file bytes onto the heap (`os.ReadFile`); **~135 MB**
  `readdir`/`lstat`/`keyPath` *locating* each doc (no key→path index). All
  amplified **O(|L|×|R|)** by the join re-fetching docs.

Leverage order: **(1)** don't re-read the same doc 262K times (hash join or a
decoded-doc cache); **(2)** route fetch through the **native byte path**
(`base.Val`+jsonparser); **(3)** a key→path index; only **(4)** the read-copy.

### Implemented (2026-07): native byte-path fetch + per-request caches
All landed **inside n1k1** (no fork change):

- **Native byte-path fetch (`glue.DatastoreFetch`).** For the directory-backed file
  keyspace, reads each `<dir>/<key>.json` into a **reused growable buffer** via
  `io.ReaderAt.ReadAt(buf, 0)` and yields raw JSON as `base.Val` — no
  `AnnotatedValue` boxing, **no standard-JSON parsing** (even `^id` via jsonparser).
  Replicates cbq's key→path exactly (`.json`-only, path-traversal guard,
  missing-file-⇒-skip). Measured: **~2.0 GB → ~917 MB, fetch subtree ~1468 → ~377
  MB, GCs 420 → 200.** `N1K1_FETCH_CBQ=1` forces the old path. **Fallbacks** (still
  cbq `Fetch`): a `.json` key with a `SubPaths` projection pushed down, or a
  synthetic keyspace whose records aren't byte-seekable. Dispatch by key form: a
  container id `<relpath>#<line>@<offset>` seeks into the multi-doc file; a plain
  key reads `<dir>/<key>.json`.
- **Per-request doc cache.** The residual ~377 MB was per-key file-open churn from
  the join re-opening the same files. `fetchCache` memoizes doc bytes per request,
  two-level (dir → key → owned immutable copy); after the first pass every fetch is
  a map hit. Bounded by `DatastoreFetchCacheMaxBytes` (64 MiB), lives one request.
  Measured: **fetch subtree 377 → 78 MB, total ~917 → ~541 MB.**
  `N1K1_FETCH_NOCACHE=1` disables.
- **Per-request scan key-listing cache.** The dominant residual ~470 MB was the
  *scan* re-reading the directory (`readdir` per invocation, O(|L|²)). `scanKeyCache`
  memoizes the full doc-key listing per request; `DatastoreScanIndex` serves an
  unbounded full scan over `#primary` **natively** (list+cache once, yield directly,
  bypassing cbq's `readdir` and `IndexConnection`). Replicates cbq faithfully
  (name-sorted, honors `LIMIT`); **ranged/seeked spans and n1k1 secondary indexes
  keep the cbq path**. End to end: **~2.0 GB → ~152 MB (~92%), GCs 420 → 31.**
  `N1K1_SCAN_NOCACHE=1` disables.
- **Fetch-by-key into an (uncompressed) `.jsonl` container.** The record's **byte
  offset is baked into its id** at scan time: the JSONL reader emits `META().id` =
  `<relpath>#<line>@<offset>` for a seekable file (offsets tracked alloc-free).
  `DatastoreFetch` parses `@<offset>`, `os.Seek`s, reads one line — O(1) per key.
  Makes `USE KEYS`, `ON KEYS` joins, and non-covering index fetches work against
  `.jsonl` keyspaces. Paired fix: a full `#primary` `IndexScan` over a
  flat/container keyspace used to **hang** (cbq's `IndexConnection` can't scan such
  a virtual primary index); `DatastoreScanIndex` now yields ids from the records
  source. A *covering* primary scan (`SELECT meta().id`) routes to a records-scan.
- **Fetch-by-key into a multi-document (`---`) YAML stream.** Same scheme: `records`
  finds each doc's `---` marker (`yamlDocOffsets`), bakes `<relpath>#<i>@<offset>`
  into the id; fetch seeks and decodes one doc (`records.DecodeYAMLDoc`).
- **Still future: compressed / non-seekable containers.** A `.gz` offset is into the
  *decompressed* stream, so a `.gz` record omits `@<offset>` and isn't key-fetchable
  yet. No-clean-byte-span records also carry no offset: CSV rows, JSON-array
  elements, top-level YAML sequence elements.

### mmap vs read-into — choose per file shape
`blevesearch/mmap-go` is an (indirect) dep; mmap-ing a file as `[]byte` is
zero-copy (jsonparser subslices into the mapping). But it only removes the ~120 MB
read-copy (not boxing/parse/locate) and has sharp edges:

- **Lifetime / SIGBUS — bounded by an existing contract.** A retained mmap
  sub-slice dangles into unmapped memory (segfault). But n1k1's `YieldVals` contract
  *already* says consumers must copy inputs they keep, and it's load-bearing today
  (`Stage` deep-copies at actor boundaries). So a mapping need only outlive the
  **scan of its own file**; unmap at end-of-scan is safe *if the contract is
  honored*. A violation is a silent data-corruption bug today; under mmap it becomes
  a delayed SIGBUS — same bug class, changed failure mode. Worth a hardening pass,
  not a redesign.
- **Bad for large/compressed/container/tiny files.** `*.jsonl.gz`/`.zst` hand you
  *compressed* bytes (stream-decompress anyway); PDF/XLSX are *extracted*, not
  sub-sliced; one-doc-per-file + 4 KB page granularity make mapping a 200-byte doc
  cost more than reading it. mmap's payoff is coupled to a **packed/segment layout**.
- **The portable alternative: read-into a reused buffer.** `io.ReaderAt.ReadAt(p,
  off)` *is* the `ReadInto(prealloc, pos, numBytes)` API (`*os.File` implements it).
  Paired with a pooled buffer, reads are amortized zero-alloc **without** mmap's
  lifetime hazard, work for large files (read only the needed range), skip page
  waste. **Rule of thumb: mmap for a packed segment of many docs; `ReadAt` + reused
  buffer for everything else.**

### Push down what the query needs
The cheapest read is the one you skip. Thread referenced fields (and predicates)
down to the scan/fetch op:
- **`_meta`-only queries need no file read at all.** `_meta` (§6) comes from the
  directory entry / `stat`; `SELECT _meta.size … WHERE _meta.size > …` or a bare
  `COUNT(*)` answers from `readdir`.
- **Partial decode.** jsonparser's `EachKey`/`Get(path…)` pulls only requested paths
  — 2 of 50 fields parses ~2 fields. Needs the referenced-path set threaded from
  `conv`.
- **Range pushdown.** With `ReadAt`, a reader fetches only the byte ranges it needs
  (a Parquet column chunk, a manifest-known offset — §5).

Net: the win is less *how* we get bytes than **not materializing, not re-reading, and
not reading at all what the query doesn't touch.**

## §2 Directory layouts & FROM-term resolution

The crux: flat vs two-level vs deep, auto-detect vs explicit.

### What the ecosystem does
- **DuckDB forces no layout.** Point a glob/list at files (`'dir/**/*.csv'`, brace,
  or a list); `union_by_name=true` merges differing schemas by column name;
  `filename=true` adds a source column.
- **Hive partitioning:** `.../year=2026/month=01/file.parquet` — auto-detect
  `key=value` segments as virtual columns, enabling **partition pruning**
  (`WHERE year=2026` skips files before reading). Spark/Hive/Trino do the same.
- **Bare date-partition dirs** (`.../20260101/*.log.gz`, *no* `key=value`) are the
  "almost invisible container" case. The ecosystem answer is **AWS Athena
  partition projection**: declare a template (a `date` column with `range` +
  `format`), and the engine **computes** candidate paths from the predicate,
  avoiding listing.
- **Log tools** (lnav, Loki/Vector) auto-detect formats or attach labels rather
  than relying on directory structure.

### Recommendation: convention by default, explicit when needed
Three resolution modes, increasing power:

**Mode 1 — Convention (zero-config), backward-compatible.** Keep today's
`<dir>/<namespace>/<keyspace>/...`, relaxed so a keyspace directory may contain
*any* supported format and **many records across many files** (directory =
keyspace = union of its files). Recurse into subdirs by default.
- **Don't force two levels.** Allow a flat root: if `<dir>` directly holds data
  files, the directory name is the keyspace (auto-detect). (**Shipped** — ex. B.)
- **Go flatter: the CLI arg may be a *single file*** (**shipped**).
  `n1k1 -c "SELECT * FROM events" events.jsonl` — a one-file keyspace named after
  the base name with record/compression extensions stripped
  (`orders.jsonl.gz`→`orders`). DuckDB's `FROM 'foo.jsonl'` analogue. See B2.

**Mode 2 — Explicit table functions / globs in FROM** (DuckDB-style) — **blocked on
a grammar fork; not near-term.** Aspiration: `FROM read_csv('sales/*.csv',
header=true) AS t`. The fork's parser rejects it (`FROM read_csv('foo.csv')`:
*"Invalid function read_csv"*; bare `FROM 'foo.csv'`: *"must have a name or alias"*).
No table-valued-function machinery in `algebra/`, so mode 2 needs patching the goyacc
grammar + a `FromTerm`/algebra node + planner support — the merge-hostile fork change
we avoid. **Verdict: defer; make the catalog (mode 3) the power path.** The cheapest
inline-glob surface would be a `read_csv(...)`-shaped keyspace-name *convention*, not
a grammar extension.

**Mode 2b — Backtick-quoted glob as a keyspace name (✅ SHIPPED — the fork-free inline
glob).** DuckDB's `FROM 'data/**/*.json'` translates to n1k1, no grammar/parser change,
by **always backtick-quoting** the glob: `` FROM `./data/**/*.json` ``. Backticks make
it a single quoted *identifier* (a keyspace name), and — per `DESIGN-extensions.md`'s
namespacing note — stop the parser splitting on `:`/`.`, so `.`/`/`/`*` don't disturb
the namespace/scope grammar; the parser just hands n1k1 the literal string. n1k1 then
**recognizes a glob-shaped keyspace name and expands it in the datastore wrapper** — a
new `maybeGlob` sibling of `maybeFlat`/`maybeFlatFile` (`glue/flat.go`) backing a
`virtual.NewVirtualKeyspace` whose records-scan unions the matches (reusing
`records.Walk`: `**` = the recursive walk, `*.json` = the format filter). No fork
change; still a `PrimaryScan` → `datastore-scan` op, so it **compiles** like any FROM.
Decisions: (a) a name is a glob **only if it contains glob metacharacters**
(`*`/`?`/`[`/`**`) — a plain `` `orders` `` stays an ordinary keyspace; (b) the base
directory follows a **prefix convention** (no ugly `$ROOT` sigil), mirroring shell
intuition *and* n1k1's existing "bare name = under the datastore root" rule:
- `` `./data/**/*.json` `` or `../…` → **CWD-relative** (explicit, DuckDB-parity);
- `` `/var/log/**/*.json` `` → **absolute**;
- `` `foo/bar/**/*.json` `` (bare, no leading `./`·`../`·`/`) → **datastore-root-
  relative**, consistent with how bare keyspace names already resolve under the root
  (falling back to CWD when no data-root is set).

(c) `-formats` lockdown still governs what the matched files may decode; absolute/`../`
globs can read outside the root, which for a local CLI is the user's own files (note
it, but not blocked). `**` needs a doublestar matcher (Go's `filepath.Glob` lacks it)
or just root+recurse+pattern-filter over `records.Walk`. This settles the open
question below in favor of the convention over a grammar fork.

**Mode 3 — Catalog / sidecar mapping** (`.n1k1/catalog.json`) — **the realistic
power path.** A per-root config maps a keyspace name to a root glob, format,
partition columns (hive or **projected** date templates à la Athena), and
compression. Handles the invisible-date-container case: declare `ecommerce` →
`ecommerce/{date:YYYYMMDD}/*.log.gz` with `date` a projected partition column, so
`WHERE date >= ...` prunes by *computing* directory names.

**Auto-detect vs override:** convention for common cases, catalog (mode 3) for
control, mode 2 deferred. Hive `key=value` auto-detects within any mode; bare date
partitions require a projection template (mode 3) because they're ambiguous.

### Lockdown flag (`-formats`)
A user whose tree has subdirs/formats they *don't* want scanned can restrict n1k1
to an explicit set, e.g. `-formats=json,jsonl` (no `recurse` ⇒ don't descend; no
`gzip` ⇒ ignore `.gz`). Empty/`all` ⇒ everything flexible. `records.ParseModes`
builds the discovery/decoder filter. The REPL's `.formats` command shows/sets it
and **persists** it into `<sidecar>/catalog.json` (`"formats"` field). Precedence:
explicit `-formats` flag, else persisted catalog value, else flexible default.
(Named `-formats`, not `-mode`, vs the `-mode` output flag.)

### Integration gap: schemaless docs vs positional labels
n1k1's engine identifies fields by **positional `base.Labels`**, not by name; an
op tree is built against a known label vector. A multi-file keyspace whose files
have *different* shapes (the `union_by_name` case) has no single fixed vector, and
JSON docs are schemaless anyway. Two stances:

- **Opaque-document scan (recommended default, matches today).** Yield each record
  as a single self-value (projections pull fields by name at expr-eval time), so
  the scan needs only a trivial label vector and heterogeneous shapes "just work."
  Why the MVP (JSONL/multi-doc JSON) is easy.
- **Typed/columnar labels (CSV/Parquet).** Formats with a real header/schema have
  a stable column set; there the inferred schema becomes the label vector, and
  `union_by_name` means computing the union column set up front (a listing pass) or
  falling back to opaque per-row objects. Partition virtual-columns append to that
  vector.
  - **Reframed by what shipped.** CSV/TSV landed **without** typed-label-vector
    work — the decoder converts each row into **one JSON object keyed by the
    header** (light inference), so CSV rides the same **opaque-document path** and
    `union_by_name` becomes trivial. Office extraction did the same
    (`{filename,kind,text}` per doc). So "labels are the real cost boundary" was
    too pessimistic for row-shaped formats. Typed-label reconciliation is forced
    only by **columnar Parquet** (where you want columns *without* a JSON object
    per row) and by hive/projected partition virtual-columns.

## §2 Query-defined virtual datasources (VIEWs & generated catalogs)

**The idea (S3 scenario):** a bucket's ingest layout/schema *morphed over time* —
early flat `{ts, user}` JSON, later renamed/nested fields, then Parquet under
`year=/month=`. You want it all to look like **one coherent keyspace** — a
**VIEW** — so `FROM events` just works and the mess is hidden. A natural extension
of the catalog (mode 3), in two capabilities:

- **(a) VIEW = a catalog entry whose definition is a SQL++ query.** `FROM events`
  expands to a stored `SELECT` that unions and normalizes the heterogeneous
  physical sub-sources (rename, nest/unnest, cast, add-missing-as-NULL, tag with an
  era column).
- **(b) Generated catalog = a query that *produces* the catalog.** A bootstrap
  query over a listing/metadata source (S3 inventory, Glue table, manifest) *emits*
  the sub-sources + partition columns — the crawler pattern.

### Why this fits: a view is an implicit WITH binding
The expansion machinery **already exists** — the WITH/CTE stack in glue. `Conv`
threads `withBindings`; `FROM <cte>` expands via
`VisitExpressionScan`/`VisitAlias`; CTE-ref-CTE is threaded; `WITH RECURSIVE` runs
a fixpoint. **A catalog VIEW is an implicit, always-available WITH binding**: seed
`Conv.withBindings` from the catalog before planning, so `FROM events` is planned
exactly as `WITH events AS ( <stored SELECT> ) SELECT … FROM events`.
Consequences: **pure glue-layer** (no fork change for expansion); **views over
views** compose via CTE-ref threading; **recursive views** ride `WITH RECURSIVE`;
**compiler-safe** (expansion before `conv`).

### UNION ALL — landed (was the one blocker)
The normalizing view is a union of per-era projections:
```sql
  SELECT ts,               user_id,        action, "era1" AS _era FROM events_era1
  UNION ALL
  SELECT event_time AS ts, uid AS user_id, act AS action, "era2" FROM events_era2
  UNION ALL
  SELECT meta.ts,          meta.user AS user_id, kind AS action, "era3" FROM events_era3
```
**This once-blocking case now works: `VisitUnionAll` has landed** (`glue/conv.go`,
kind `union-all`) — each child is a self-contained SELECT sub-plan converted as its
own branch, run concurrently by `OpUnionAll` with each branch's vals **remapped to a
by-name union of labels** (so differently-shaped era projections reconcile). The
recursive-CTE work had already built the union *execution* substrate (data-staging
batches + `trackSet` dedup in `glue/recursive.go`); this wired the top-level
`plan.UnionAll` → `base.Op` conversion on top. `INTERSECT`/`EXCEPT` (`[ALL|DISTINCT]`)
landed too (`setOp` → hash set-op). What *remains* for the morphing view is not the
union itself but **predicate pushdown through the view** (below, and the open
question) — so a `WHERE`/partition predicate prunes whole eras rather than the view
reading all history. (Views that *don't* union — a single reshaping `SELECT` — work as
soon as catalog-view expansion lands.)

### S3 / object store: orthogonal, deps already here
The VIEW idea is independent of *where* bytes live. That's a separable backend
concern, and **dep-ready:** `go.mod` carries (indirect)
`aws-sdk-go-v2/service/s3` + `feature/s3/manager`, `aws-sdk-go-v2/service/glue`,
and `gocloud.dev` (its `blob` abstracts S3/GCS/Azure). An object-store
`RecordSource` backend slots under the same decoder/layout layers (catalog
`root`/`layout` glob points at `s3://…`); the **Glue** client hints at capability
(b) — read an existing Glue Data Catalog rather than crawling raw S3.

### Virtual vs materialized (ties to §5)
- **Virtual view** — re-expanded and re-scanned every query. Simple, always fresh,
  but pays the full union/normalize/scan cost each time; depends on **predicate
  pushdown through the view** to be fast.
- **Materialized view** — run once, cache the flattened rows as a derived artifact
  in `.n1k1/`, rebuilt via the §5 change-detection manifest when any sub-source
  changes. The answer for expensive normalization over huge, mostly-static trees.

### `INSERT INTO` — user-driven materialization (landed)
The **explicit, user-driven** counterpart to the automatic materialized view: run a
query *now* and write its rows to a keyspace file for later slicing & dicing. Drove
by the PREPARE++ / `RULE_MATCHES()` flow (`DESIGN-prepare.md`) — materialize a
scan/rule-match result once, then run many cheap analytic queries over it.

```sql
INSERT INTO `analysis/errors-20260609.jsonl` (KEY UUID(), VALUE self)
  SELECT l.sev, l.code FROM logs l WHERE l.sev = "ERROR";
-- later, over the new `analysis` keyspace (the directory):
SELECT sev, COUNT(1) FROM analysis GROUP BY sev;
```

- **Where it lives** — `glue/insert.go`, intercepted at the statement level in
  `Session.Run` (like PREPARE/EXECUTE), *before* the cbq planner. This sidesteps
  cbq's `plan.SendInsert`, which requires the target keyspace to already exist —
  whereas the default (`"new"`) mode writes a **brand-new** file. Zero fork changes.
- **Keyspace layout (ties to §2 resolution)** — the file datastore makes a
  *directory* under the namespace a keyspace (its files unioned); a loose file is
  not. So `` INSERT INTO `analysis/x.jsonl` `` writes `<root>/<ns>/analysis/x.jsonl`
  and the queryable keyspace is `analysis`. Dated files accumulate into one keyspace.
- **cbq INSERT-SELECT semantics** — the `VALUE` expression is evaluated against each
  SELECT **output** row (the projection), not the `FROM` alias. `VALUE self` writes
  the whole projected row; `VALUE {"k": projectedField}` constructs. A `VALUE` that
  references a `FROM` alias resolves to MISSING — faithful to cbq, not a bug.
- **Streaming + stage breaker** — rows are **never materialized in memory**. The
  source query (producer) evaluates each row's `VALUE` and hands the doc to a
  dedicated **writer goroutine** over a small bounded channel (`insertWriterQueue`),
  so JSON encoding + file I/O overlap with query compute instead of blocking the
  producer on each flush syscall. Error state is split across the two goroutines
  (producer owns eval errors, writer owns write errors; combined only after join) so
  no field is touched concurrently — verified under `-race`. The retained doc is a
  **copy** of the reused `OnRow` buffer (the async path outlives the callback).
- **`RETURNING` (landed)** — a `RETURNING` projection makes the statement return a
  row per inserted doc *instead of* the mutation summary, streamed through the
  caller's `OnRow` as each doc is written (so the returned rows honor the same stage
  breaker) or handed back in `Result.Rows`. Since INSERT runs outside the planner
  there is no projection operator, so `insertReturner` evaluates the
  `*algebra.Projection` directly against the inserted doc, mirroring cbq's formalized
  shape: a bare `RETURNING code` formalizes to `(alias.code)`, so exprs are evaluated
  against a one-key wrapper `{alias: doc}`; `RETURNING *` is a star+self term → the
  whole doc; `RETURNING RAW <expr>` yields the bare value, not an object. A RETURNING
  eval failure aborts the whole insert (the temp file is discarded, nothing lands).
  *Limitation:* `META().id` in RETURNING is not meaningful (ids are positional, not
  content keys), and the doc carries no annotated metadata.
- **Scope** — `KEY` is accepted but record ids stay positional (flat-keyspace rule);
  every write goes via a `.tmp` sibling renamed into place, so a mid-stream failure
  never leaves a partial keyspace file. Still unsupported: the faithful cbq
  `SendInsert` path (a later phase).

#### Write mode via the `OPTIONS` clause (landed)
The standard SQL++ `OPTIONS` clause chooses brand-new vs append vs overwrite —
**no grammar or fork changes**. It's a first-class part of the insert spec
(`INSERT INTO ks (KEY …, VALUE …, OPTIONS <objExpr>) …`), parses in our
`INSERT … SELECT` form, and reaches `InsertRun` via `ins.Options()`; `insertWriteMode`
constant-folds it (`Evaluate` against a NULL row — the same clause cbq uses for
`{"expiration": …}` on Server) and reads a `"mode"` key.
- **`mode` enum** — one knob: `{"mode": "new"}` (**default** = brand-new-only, errors
  if the file exists), `"append"` (create-or-append), `"overwrite"` (atomic replace).
  `"replace"` is a synonym for overwrite; an absent/NULL mode is `"new"`; anything else
  errors. (Chosen over boolean flags — `{"append":true}`/`{"overwrite":true}` — which
  can contradict.) The mutation summary echoes the mode: `{"inserted":N,"keyspace":…,"mode":…}`.
- **Atomicity per mode.**
  - `new` / `overwrite` are **atomic** — write the temp, then rename (overwrite's rename
    replaces the existing file). No partial keyspace file on failure.
  - `append` uses **copy-then-rename** (`jsonlWriter.seed`): the temp is pre-filled with
    the existing file's bytes, new rows are appended, then rename — crash-safe at an
    O(existing) copy (chosen over a bare `O_APPEND`, which would leave a partly-appended
    file on failure). The seeder forces a trailing newline (`lastByteWriter`) so the
    first appended row can't run onto a seeded file that lacked one.
  - Appending to a JSONL keyspace file is otherwise clean: existing lines don't move, so
    **positional record-ids stay stable** and the directory-union read path is unaffected
    — new rows just get appended offsets.
- **Not `UPSERT INTO`.** `algebra.Upsert` exists, but its semantic is insert-or-replace
  *by key*; our file keys are positional, so overloading UPSERT for "overwrite ok" is a
  stretch. Reserve it for a genuine by-key upsert later.

### The hard part: predicate pushdown through a view (open question)
A `WHERE ts >= '2023-01-01'` on `events` must reach the *sub-source* scans —
ideally pruning whole eras/partitions — or the view reads all history every time.
After expansion the planner sees a union of subqueries; whether it pushes the outer
predicate into each branch depends on cbq's rewrite rules + the §5
predicate-to-scan work. A naive virtual view is correct but can be catastrophically
slow on morphing histories; materialization or pushdown makes it practical.

### Prior art
- **DuckDB `CREATE VIEW` + macros** — query-defined logical tables/functions.
- **Trino / Athena VIEWs + the Glue crawler** — the crawler *generates* the catalog
  (b); views normalize on top.
- **Iceberg schema evolution** — column-ID mapping makes renamed/added/dropped
  columns coherent across snapshots *without* a union view. A query-defined view is
  the poor-man's version when the data was never a managed table.
- **dbt models** — query-defined, optionally materialized, dependency-tracked.

### Recommendation & sequencing
Model views as **catalog entries with a `query` field**, expanded as implicit WITH
bindings before planning. Sequencing: (1) single-source reshaping views land with
catalog-view expansion; (2) union/normalize views are unblocked now that
`VisitUnionAll` has landed — the remaining gap is predicate pushdown into the branches;
(3) object-store backend + generated/Glue catalogs are a separable track;
(4) materialization + pushdown are the perf follow-ups (§5). See example O.

**Rejected: the fork's `datastore/virtual` package for views.** It's a
*metadata-only, planner-facing shim*, not a datasource: `virtualKeyspace` refuses
`Fetch`/mutations (`Count()`→0), `VirtualIndex.Scan` yields nothing. It exists so
the planner can hold a keyspace/index *object* without backing data, and (its one
active use) to run `SargableFor` against a throwaway `partitionVirtualIndex` for
**partition elimination** (§2 F). Macro-expansion is better for views: the view
name is rewritten away before planning, so no keyspace object is needed.

## §3 Compression & containers

### What DuckDB does
gzip (`.gz`) and zstd (`.zst`) decompressed **transparently** by extension, no temp
files. `.zip` archives are **not** transparently read (a container of many entries,
not a single stream).

### Recommendation for n1k1
- Treat **single-stream compression** (`.gz`, `.zst`, optionally `.bz2`/`.xz`) as a
  transparent decode layer *under* the decoder, keyed off the inner extension
  (`foo.jsonl.gz` → gzip → JSONL). Use `compress/gzip` and `klauspost/compress`
  (`zstd`, a dep).
- Treat **`.zip`** as a *container/layout* concern, not a codec: enumerate entries
  (`archive/zip`) and feed each through format detection, as if it were a directory.
  Also covers office formats (§4: docx/xlsx/pptx are zip files).
- Caveat: gzip/zstd streams aren't seekable, so columnar formats (Parquet) lose
  random-access/pushdown when gzipped — fine for row formats.

## §4 The `extract` provider — unstructured & semi-structured sources

Crack open files that aren't clean rows — office/PDF documents **and** the messy
semi-structured bulk of the real world: log files, command-output dumps, config
concatenations, opaque binary blobs, domain-specific record dumps. Turn each into
queryable rows **plus** the file-level **metadata** that makes the rest of the engine
work (pruning, `_meta`, doc-IDs, and — the load-bearing new consumer — the
[sorted-source merge](#sorted-sources)).

**Domain knowledge lives in recipes, never in n1k1 core.** The engine provides the
generic *seam* (match a file → describe it → extract rows); a user brings a git repo
of recipes carrying the knowledge of *their* files. A support engineer's recipes
understand `cbcollect_info` logs; a fintech's understand SEC EDGAR filings; an
astronomer's understand FITS headers or survey catalogs. The concrete worked example
throughout this doc ([P](#extract-bundle)) is a `cbcollect_info` bundle because it
drove the design (`DESIGN-prepare.md` PREPARE++), but read it as *one instance* of the
generic mechanism — nothing about bundles is baked in.

> **Status: proposal (rework).** The *shipped* extract provider (`records/extract.go`,
> ex. L) does one narrow thing — one file → one `{filename, kind, text, …}` record,
> keyed by extension, pure-Go. Everything below **re-examines and reworks** that seam
> to serve the bundle use case: two-phase (describe/extract), pluggable (JS
> extractors from a git repo), matched by extension **or** regexp, streaming, and
> metadata-rich. The shipped office/PDF/media extractors become the *built-in
> baseline* under the new model.

### Two things an extractor produces — and why they split
A real extractor produces **two** outputs on two very different cadences:

1. **`describe(file) → metadata`** — a *cheap, once-per-file* pass that may only
   **sample** (head/tail/a few KB), returning what the planner and manifest need
   *before* a full scan: the format, how records are framed, the **timestamp / sort
   key contract**, sortedness + zone maps on that key, provenance, record count. This
   is the new load-bearing output — it feeds §5 (manifest/zone maps), §6 (doc-IDs),
   source-routing (`DESIGN-prepare.md` MQO), and the [merge join](#sorted-sources).
   Memoized in `.n1k1/` keyed by the file fingerprint (§5), so it runs once per bundle.

2. **`extract(file, meta) → records`** — the *streaming, per-record* pass at scan
   time: frame the file into records and emit typed rows (a log line's `ts`/`level`/
   `node`/`msg`; a doc's page text). Crucially it is **handed the earlier `describe`
   result** (`meta`) rather than re-deriving it — no re-sniffing the format, no
   re-scanning to find the timestamp column or record boundaries. Streams with bounded
   memory (reusing the push-based source op — `DESIGN-extensions.md` streaming
   sources), so a 400 MB log never materializes.

**`describe` feeds `extract`, and the sidecar makes it once-ever.** The `describe`
result is memoized in `.n1k1/` keyed by the file fingerprint (§5), so the *expensive,
format-specific* work happens **once per file across all queries** — the first scan (or
an explicit pre-scan pass) runs `describe`; every later `extract` (this query and every
future one) reads the cached `ExtractSpec` + measured metadata (zone maps, sync points,
`disorder_bound`) straight from the sidecar and just executes it. On a re-scan of an
unchanged file, `describe` doesn't even re-run. So the split isn't only "cheap vs hot"
— it's "compute the description once, reuse it forever." A changed file (fingerprint
mismatch) re-describes only that file.

Splitting them is the whole point. **Description is where the hard, format- and
use-case-specific knowledge lives** — what a `cbcollect` banner means, which regex
pulls the timestamp out of *this* log, how near-sorted it is — and it is cheap and
runs once. **Extraction is hot** (GB of lines) and must stay fast. So description is
the pluggable seam, and extraction, wherever possible, runs **natively from a
declarative spec** the describe pass returns — keeping per-row work off the JS/boxed
lane.

### Declarative spec (fast) vs imperative extract (flexible)
Most log formats are *regular*: a line regex, a timestamp regex, a multiline
continuation rule. So the preferred contract is **`describe` returns a declarative
`ExtractSpec`, and n1k1 applies it natively** (byte-oriented, per-record, zero JS on
the hot path). Only formats too irregular for a spec — crack a binary blob, stateful
multiline assembly, a format that genuinely needs code — fall back to an **imperative
`extract(file, meta, emit)`** (handed this file's cached describe result as `meta`)
that runs per-record in the chosen runtime (JS today; a Go
builtin or Wasm later) through the streaming source op — flexible, but paying the
boundary cost. The spec covers the bundle's logs; the imperative escape hatch covers
`event_log`-style blobs and `users.dets`.

`ExtractSpec` (the declarative core — see `DESIGN-extensions.md` for the JS surface
that *produces* it):

- **`framing`** — how bytes split into records: `line` (one per line); `multiline`
  (a lead line plus continuation lines matching a `continuation` regex — the
  ns_server/diag Erlang term dumps span lines); `json` (JSONL — `master_events.log`);
  `section` (one record per `====`-banner block — `couchbase.log` is **302**
  concatenated command outputs); or `whole` (one record — the office/PDF baseline).
- **`fields`** — how to lift typed columns out of each framed record: named regex
  captures or a grok-style pattern (`ts`, `level`, `node`, `module`, `msg`). Native,
  reusing the byte-regex work (`DESIGN-exprs.md`) so it stays on the fast lane.
- **`time`** — the sort-key contract: which field is the timestamp, its `layout`
  (`RFC3339` / `epoch_s` / `epoch_ms` / a strftime), default timezone → normalized to
  one sortable **int64 epoch-nanos** key. The single field the merge join requires.
- **`order`** — `sorted: strict|near|none`, and for `near` a **`disorder_bound`**
  (see [sorted sources](#sorted-sources)).
- **`provenance`** — constants lifted from the file once (the banner `command`; the
  `node` id parsed from log content, e.g. `ns_1@MXCPD1001814944.eci.geci`) that ride
  every record's `_meta`.

### Matching a file to an extractor — extension **and** regexp
The shipped registry is keyed by extension alone (`.pdf`, `.log`). The bundle breaks
that: nearly everything is `.log`, yet `ns_server.info.log`, `diag.log`,
`memcached.log`, and `cbcollect_info.log` are four *different* formats, and
`master_events.log` is JSONL. So matching gains a **regexp over the bundle-relative
path**, plus a **priority** to resolve overlap (a specific `ns_server\..*\.log` beats
a generic `\.log$`). An extractor declares `{exts, names (regexps), priority}`; the
highest-priority match wins. **This is the same matcher `DESIGN-prepare.md`'s
source-routing uses** to decide which detectors fan out to which file — one mechanism,
two consumers.

### This *is* `DESIGN-prepare.md`'s late binding — from the data side
`DESIGN-prepare.md`'s "Late binding" section frames the same machinery from the corpus
side: a prepared detector corpus `FROM`s **logical** keyspaces (`indexer_log`,
`orders`), and each bundle resolves logical → physical at EXECUTE time — because the
next bundle's files are named/laid out differently (`indexer.log.3`,
`2024Q4_results.parquet`). The two halves line up exactly:

- **The binding resolver = this matcher.** Its robustness ladder — *explicit* glob →
  *convention* (regex tolerant of version suffixes / layout drift) → *content/schema
  sniffing* — is precisely an extractor's `{names (regexps), exts}` plus what `describe`
  learns by sampling. Resolving `indexer_log → glob("**/indexer*.log")` is a
  higher-priority `match` entry.
- **The adapter = the `ExtractSpec` `describe` returns.** `DESIGN-prepare.md`'s "thin
  adapter (a SQL++ view / small **extract spec**) normalizing raw records into the
  logical keyspace's canonical schema" *is* `describe`'s `fields`/`time`/`provenance`
  spec — including "the log time model … one sortable key for the merge-based ASOF is
  one adapter concern," which is exactly the `time` → int64 epoch-nanos normalization
  ([sorted sources](#sorted-sources), `DESIGN-merging.md`).
- **Data, not code → no recompile.** Both the binding manifest and the memoized
  `describe` result are *data* the datastore opens/reads at run time, so rebinding a
  compiled corpus to a new bundle needs no recompilation — the property that makes
  PREPARE-once / rebind-per-bundle pay off.

So the recipe repo versions, per logical keyspace, three coupled things: the
**detectors** (`DESIGN-prepare.md`), the **adapter/extract recipe** (this §4 +
`DESIGN-extensions.md`), and the per-bundle **binding manifest**.

### Built-in extractors stay; the seam becomes open
The pure-Go office/PDF/media extractors (`records/extract.go`, shipped — ex. L)
remain the **built-in baseline**, re-expressed as extractors that return an
`ExtractSpec{framing: whole}`. What's new is that the registry is **open**: an
extractor can also come from a **git-cloned repo of JS** — the "recipe repo" sibling
of the detector corpus (`DESIGN-prepare.md`, `DESIGN-extensions.md`) — matched by
ext/regexp, contributing `describe`/`extract`. n1k1 ships built-ins for the common
cbcollect formats; users `git pull` more. Because describe output is content-addressed
into the sidecar (§5), a new extractor version invalidates only the files it matches.

### Libraries (built-in document extractors; permissive only)
Document extraction is where the licensing landmines are — MIT/Apache-2.0/BSD only.
- **Breadth/OCR:** Apache **Tika** (Apache-2.0, 60+ formats; Java sidecar) or
  **`extractous`** (core Apache-2.0; Rust wrapping Tika + Tesseract OCR; Go
  bindings) — at the cost of a cgo/native dependency.
- **Pure-Go, narrower:** `xuri/excelize` (XLSX; BSD-3), `ledongthuc/pdf` (BSD-3)
  and/or `pdfcpu` (Apache-2.0) for PDF text.
- **Avoid (viral):** `go-fitz`/MuPDF (AGPLv3); UniDoc/unipdf (AGPL/commercial);
  `sajari/docconv` (shells out to GPL `wv`/`poppler-utils`/`unrtf`/`antiword`).
- **Recommendation:** a pluggable backend — a pure-Go default (excelize +
  ledongthuc/pdf or pdfcpu) and an optional Tika/extractous build-tag backend for
  breadth + OCR. Office docs being zip-based dovetails with §3.

## Sorted & near-sorted sources: the merge-join contract <a name="sorted-sources"></a>

This is the payoff of the describe metadata, and the enabler for temporal correlation
over any time-ordered records — log lines, trades, sensor readings, telescope
observations, transactions. (`DESIGN-prepare.md`'s support engineers phrase it as ASOF:
*"what was the rebalance state when this error fired?"*; a fintech user asks the same
of a quote stream vs a trade stream.) Such records are **sorted or near-sorted by
time**; a K-way **merge** across many files is O(N log K) and streams — vastly cheaper
than sorting the whole corpus, and than the O(n²) naive correlated subquery. But a merge is correct only if the
extract layer hands it a trustworthy **sort key + sortedness contract**. That contract
is `ExtractSpec`'s `time` + `order`, materialized into the manifest (§5).

### The normalized sort key
`describe`'s `time` spec normalizes each source's wildly different timestamps —
`2026-05-17T15:36:11.198+02:00` (ns_server, ms), `2026-05-20T08:50:17.593648+02:00`
(memcached, µs), `1779150134.812159` (master_events, epoch float), `[2026-…]`
(cbcollect) — into **one comparable int64 epoch-nanos** key, timezone-normalized (the
bundle spans `+02:00`). Only then are streams from different files and nodes directly
comparable. Without this the merge cannot order across sources at all; it is the
single most important extract output.

### Sortedness, classified
- **`strict`** — every record's key ≥ its predecessor. Merge is a plain K-way
  min-heap.
- **`near`** — mostly sorted, **bounded** disorder. Real logs are near-sorted: threads
  flush buffers slightly out of order, µs ties reorder, an occasional late line. A
  merge still yields globally-sorted output *if the disorder is bounded* — buffer a
  small reordering window and gate emission on a **watermark**.
- **`none`** — unsorted; must be spill-sorted before merging (falls back to the
  existing ORDER BY / `base/heap.go` machinery).

### The `disorder_bound` (the load-bearing number)
For `near`, describe states *how* out-of-order the file can be, as either:
- **`{window: Δt}`** — a record's key is never more than Δt behind an already-seen
  key (bounded lateness — the Flink/Dataflow watermark model). Natural for time.
- **`{span: N}`** — a record is never more than N positions from its sorted place.

Where the bound comes from: **declared** by the format author (they know the logger's
buffering), or **measured** by describe from its sample (count/size the inversions in
a head/tail window, take a conservative max). Either way it is a *claim*, and a wrong
claim silently corrupts a merge — so the merge operator must **validate** it (below).

### The merge operator (a separate task — designed-for here)
A K-way merge source op, one cursor per file/stream:
- **Disjoint ranges → concatenate, no heap.** If the zone maps show
  `max_key(fᵢ) ≤ min_key(fᵢ₊₁)` (daily logs rarely overlap), stream files in order —
  the cheapest case, common for dated partitions (ex. E/G).
- **Strict → min-heap merge.** Pop the smallest key, advance that cursor. O(N log K).
- **Near → watermarked buffer.** Hold a record until the **watermark** —
  `min over live cursors(frontier_key) − max disorder_bound` — passes its key,
  guaranteeing no earlier record can still arrive; then emit in order. Buffer size is
  bounded by `disorder_bound × arrival rate`, so memory stays bounded (spill if not).
- **Validate the claim.** If a record arrives with a key *older than the current
  watermark* (the `disorder_bound` was too small), the merge must NOT silently emit
  out of order: it **widens the buffer and warns**, or errors, per a strictness knob;
  a source whose bound can't be trusted falls back to a full spill-sort. Honesty here
  is non-negotiable — a wrong bound is a *correctness* bug, not a perf one.

### ASOF / temporal join rides the merge
`DESIGN-prepare.md`'s ASOF ("join each error to the nearest-preceding rebalance
state") is the merge with a join twist: advance both key-ordered streams together,
keeping the latest left-of-key row from the other stream. The stock-SQL++ correlated
"argmax" subquery the planner recognizes then runs as this O(n) merge instead of
O(n²). Windowed rate/burst/streak detectors ride the same ordered stream. So the
throughline is: **extract's `time`+`order` metadata → merge op → ASOF/window
temporal detectors** — from raw bundle bytes to the correlations engineers write.

### What the manifest stores (feeds §5)
Per sorted source, alongside the change-detection fields: `sort_key` (the normalized
field + how to derive it), `sortedness`, `disorder_bound`, `min_key`/`max_key` (the
**time zone map** — powers both merge concatenation *and* `WHERE ts BETWEEN`
pruning), `record_count`, and periodic **key→offset sync points** (every N records)
that double as the §6 seekable doc-ID index and let a merge cursor *seek* to a start
time rather than scan from the top. See §5 "Manifest contents."

## Worked examples: sample trees and their FROM clauses

CLI: `n1k1 [-c "<stmt>"] [-ns <namespace>] <dataRoot>` (default `-ns default`).
`FROM default:orders` reads `<dataRoot>/default/orders/`. Status legend (tied to
Phasing): ✅ shipped · 🟡 not built, decoder/convention (Parquet, `.zip`, zstd) ·
🟣 not built, needs `.n1k1/catalog.json` · 🔴 deferred, needs grammar fork.

*(Representative subset. Merged/dropped as re-illustrations: **D** (mixed formats)
folded into C/J — same opaque-doc union point; **I** (`.zip`) folded into §3 + the
doc-ID note; **M** (sidecar tree) folded into §5; **N** (inline table functions)
folded into §2 mode 2.)*

**A. Today's convention — one JSON document per file ✅**
```
shop/default/orders/  order-001.json  order-002.json
```
`FROM default:orders WHERE total > 100` → reads `shop/default/orders/*.json`;
`META().id` = filename stem (`order-002`).

**B. Flat root — a bare directory of files = one keyspace ✅**
```
sales/  2026-01.json  2026-02.json  2026-03.json
```
`FROM sales` on `sales sales` → no ns/keyspace subdirs, so auto-detect treats the
whole dir as one flat keyspace. **RESOLVED:** `glue/flatroot.go` names it after the
root basename under a synthetic `default` namespace (`default:sales`).

**B2. Single file as a keyspace — no directory ✅**
```
events.jsonl
```
`FROM events` on `events.jsonl` → CLI arg is a **regular file**; a one-file keyspace
named after the base name with record/compression extensions stripped
(`orders.jsonl.gz`→`orders`). `FileStore` `os.Stat`s the arg; if a record file, the
fork's datastore is built against the file's **parent dir**, wrapped by
`maybeFlatFile` (synthetic `default:<stem>`). The synthetic keyspace's
`RecordsFile()` routes `DatastoreScanRecords` to `records.File`. Compiler-transparent
(still `PrimaryScan`). `-formats` lockdown applies. (`test/flatfile_test.go`.)
`META().id` = `events.jsonl#57@4210` for JSONL, or the stem for single-doc `.json`;
the `@<offset>` is present only for a seekable (uncompressed) file and enables
key-based fetch (`USE KEYS`/join/non-covering scan).

**B3. Grab-bag directory — loose files + unrelated subdirs ✅**
```
~/Desktop/  people-100.csv  Sales.csv  2025-W2.pdf  notes/  projects/  ...
```
`` FROM `people-100` `` on `~/Desktop`. Previously reported "no keyspaces" (the file
datastore read every subdir as a namespace, suppressing flat-root). **Resolved:**
`glue/flat.go` `maybeFlat` exposes **one keyspace per top-level structured file, by
stem** (`people-100.csv` → `` `people-100` ``) — "directory = database, file =
table." Differs from B (which would recurse into unrelated subdirs). Limits:
structured files only (JSON-family + CSV/TSV; PDF/DOCX/XLSX *not* auto-exposed —
query via B2); additive/non-hiding (merges a real `default` namespace); first-seen
wins on a stem collision. (`glue/flat_test.go`.)

**C. Multi-file keyspace, many records per file ✅**
```
logs/default/events/  2026-01-01.jsonl  2026-01-02.jsonl  2026-01-03.jsonl
```
`FROM default:events` → keyspace = **union of every record across all `.jsonl`
files**; `META().id` = `events/2026-01-02.jsonl#57@4210` (dir-relative path + line +
offset). The core MVP relaxation, opaque-document path. *(Ex. D — CSV+JSONL+JSON
mixed in one keyspace — works the same way: CSV also decodes to a JSON object per
row, so heterogeneous shapes coexist with no label reconciliation. Only caveat: CSV
`qty` is int-inferred, JSON `qty` keeps its JSON type.)*

**E. Deep / recursive tree as an unkeyed union ✅**
```
metrics/default/cpu/hostA/2026/01/data-0001.jsonl  hostB/2026/01/data-0003.jsonl
```
`FROM default:cpu` → recurse all subdirs, union every `.jsonl`. The
`hostA`/`2026`/`01` segments are **invisible** (not columns) — expose via Hive
naming (F) or a catalog projection (G).

**F. Hive partitioning — `key=value` dirs become virtual columns 🟡**
```
events/default/clicks/year=2026/month=01/part-0.parquet  year=2025/month=12/part-2.parquet
```
`FROM default:clicks WHERE year = 2026` → `year`/`month` auto-detected as virtual
columns; the predicate **prunes the 2025 file before opening it**. Depends on
partition/zone-map pruning at the scan layer (§5 caveat: the predicate must reach
the scan; the fork's `partitionVirtualIndex` + `SargableFor` provides the
sargability test).

**G. Bare date-partition dirs + compression — catalog projection 🟣**
```
ecommerce/20260101/access-0.log.gz  20260102/access-0.log.gz  .n1k1/catalog.json
```
No `key=value`, so declare them:
```json
{ "keyspaces": { "access": {
  "root": "ecommerce",
  "layout": "ecommerce/{date:YYYYMMDD}/*.log.gz",
  "format": "jsonl", "compression": "gzip",
  "partitions": [ { "name": "date", "type": "date", "projection": "YYYYMMDD" } ]
} } }
```
`FROM access WHERE date >= '2026-01-02'` → the engine **computes** candidate
directory names from the predicate (Athena-style projection) instead of listing.
The marquee case for why the catalog (mode 3) exists.

**H. Transparent compression, single file ✅ gzip / 🟡 zstd**
```
archive/default/orders/  2025.jsonl.gz  2026.jsonl.zst
```
`FROM default:orders` → decompressed by *inner* extension. **gzip shipped**
(`compress/gzip`); zstd is a stub (`openDecompressed` returns "not yet supported" —
fast-follow is wiring `klauspost/compress`). *(Ex. I — `.zip` container 🟡 — treated
as a directory of entries, §3: enumerate, decode each by inner extension, union;
`META().id` = `reports/2026-q1.zip!feb.csv#L12`.)*

**J. CSV/TSV with header + sniffer ✅**
```
finance/default/txns/  2026.csv    # header: id,amount,currency,ts
```
`FROM default:txns WHERE currency = 'USD'` → **shipped** (`records/records.go`
`csvSource`): the header names columns, each row becomes **one JSON object** keyed by
header (light int/float/bool inference); TSV is the same reader, tab delimiter. Built
on Go's `encoding/csv` (quoting/escaping/embedded-newlines correct). **Allocation
caveat:** this cut allocates field strings per row; the zero-copy `[]byte`-borrow
reader is a later optimization. Emitting JSON ⇒ opaque-document path (§2 reframe).

**K. Parquet 🟡 (correctness-first)**
```
warehouse/default/sales/  part-0.parquet  part-1.parquet
```
`FROM default:sales` → read via `arrow-go` / `iceberg_reader.go`. Correctness-first:
the columnar→row transpose (§1) means no vectorized speedup until column-batch ops.
Footer min/max later feed §5 zone-map pruning.

**L. Unstructured docs & media → `extract`-provider rows ✅ (pure-Go text + media
metadata; OCR later)**
```
kb/default/docs/  handbook.pdf  q1-report.docx  budget.xlsx  deck.pptx  notes.txt  memo.rtf
          media/  logo.png  clip.mp4
```
`FROM default:docs WHERE text LIKE '%vacation%'` → **shipped**
(`records/extract.go`, pure-Go). Each file yields **one** `{filename, kind, text, …}`
record via a per-extension `Extractor` registry producing `ExtractedDoc{Kind, Text,
Meta}` (the seam the bleve FTS indexer consumes — `DESIGN-indexing.md` Phase 2).
- **Text:** `.pdf` (content-stream show-text + zlib inflate), `.docx`/`.pptx`/`.xlsx`
  (`archive/zip`+`encoding/xml` OOXML), `.txt`/`.log`/`.md`, `.rtf` (de-controlled).
- **Media metadata** (no `text`): `.png`/`.jpg` → `width`/`height`; `.mp4`/`.mov` →
  `duration_secs`/`width`/`height`/`created` (minimal ISO-BMFF box reader).

**Deliberately narrow:** no OCR, no image/speech text, one record per file (not one
per spreadsheet row) — those want the optional Tika/extractous+Tesseract backend
(§4). `-formats` groups: `doc`, `text`, `image`, `video`, or `extract`.

**O. Query-defined VIEW over a morphed-over-time source 🟣 (UNION ALL landed; needs
view expansion + pushdown)**
```
s3-events/  events_era1/2019/*.json  events_era2/2021/*.jsonl.gz
            events_era3/year=2023/*.parquet  .n1k1/catalog.json
```
`.n1k1/catalog.json` defines each era as a keyspace **plus a view** reconciling them:
```json
{ "views": { "events": { "query":
  "SELECT ts, user_id, action, 'era1' AS _era FROM events_era1
   UNION ALL SELECT event_time AS ts, uid AS user_id, act AS action, 'era2' FROM events_era2
   UNION ALL SELECT meta.ts, meta.user AS user_id, kind AS action, 'era3' FROM events_era3" } } }
```
`FROM events WHERE ts >= '2023-01-01' GROUP BY _era` → `events` expands as an
implicit WITH binding (CTE machinery); the eras present as one keyspace. The `UNION
ALL` itself now converts (`VisitUnionAll`); what remains is **catalog-view expansion**
(seed the binding from `.n1k1/catalog.json`) and **predicate pushdown** so the `WHERE`
prunes whole era sub-scans instead of the view reading all history (the open question).
A single-source reshaping view (no UNION) needs only the expansion half.

**P. Support bundle (`cbcollect_info`) — heterogeneous logs via describe/extract +
merge 🟡 (the driving `DESIGN-prepare.md` case)**
```
support-bundle-ex01/
  ns_server.info.log  diag.log  memcached.log  cbcollect_info.log  # 4 log formats, all *.log
  master_events.log                                                # JSONL, epoch-float ts
  couchbase.log                                                    # 302 ====-banner sections
  rebalance_report_*.json  goxdcr_*.json                           # JSON dumps
  event_log  users.dets  stats_snapshot/                          # opaque blobs
```
An extractor recipe repo, matched by **regexp** (not just extension, since all are
`.log`), describes each format via `describe(file) → ExtractSpec`:
- `ns_server\..*\.log`, `diag.log` → `{framing: multiline, continuation: /^\s|^\[/,
  fields: {ts, level, node, module, msg}, time: {field: ts, layout: RFC3339,
  tz: "+02:00"}, order: {near, {window: "2s"}}, provenance: {command, node}}`
- `master_events.log` → `{framing: json, time: {field: ts, layout: epoch_s}}`
- `couchbase.log` → `{framing: section}` (each `====` block a record tagged with the
  shell command that produced it — provenance for free)
- `event_log`, `users.dets` → imperative `extract(file, meta, emit)` (crack the blob), or
  skipped via `-formats`.

Then a detector reads clean, time-ordered rows across nodes:
```sql
-- errors across all node logs, globally time-ordered via a K-way near-sorted merge
SELECT l._meta.node, l.ts, l.msg
FROM ns_logs l                    -- keyspace = union of ns_server.*.log, MERGED by ts
WHERE l.level = 'error'
  AND l.ts BETWEEN '2026-05-17T00:00' AND '2026-05-18T00:00'
```
`WHERE ts BETWEEN` prunes files by the time zone map; the union scans as a
**watermarked near-sorted merge** (globally time-ordered, bounded memory); `level`
and `node` came from the declarative `fields`/`provenance` — no per-row JS. This is
**one** detector; PREPARE++ runs thousands over the same single merged scan (MQO).

### What the examples reveal
1. Flat-root naming (B) resolved to *basename* and shipped.
2. The cheap cases (A/B/C/D/E/H-gzip/J/L) all stay on the opaque-document path — why
   they shipped fast — and it stretched further than expected: CSV (J) and office (L)
   fit it by emitting a JSON object per row/doc.
3. Typed-label reconciliation is forced only by **columnar Parquet (K)** and
   **partition virtual-columns (F/G)** — not CSV.
4. Partition pruning (F/G) is the first feature needing the predicate pushed to the
   scan layer (links to the indexing doc's zone-map tier).
5. The VIEW case (O) reuses WITH/CTE for free; with `VisitUnionAll` now landed, the
   remaining gate is catalog-view expansion + predicate-pushdown — the highest-leverage
   feature for "morphed-over-time."
6. The support-bundle case (P) is a *different* kind of hard: not schema morphing but
   **format heterogeneity + irregular framing + per-source timestamp normalization**.
   It's why extract splits into cheap-`describe` (pluggable, format-specific) and
   fast-`extract` (native from a spec), and why the sortedness/time metadata is
   first-class — it's the input to the merge join and to PREPARE++ source-routing.

## §5 Indexes & derived artifacts: storage + change detection

Two questions: **where** do derived artifacts live, and **how** do we know source
data changed?

> **Scope note — everything from here down (and §6) is post-MVP / aspirational.**
> §6 (doc-ID synthesis beyond the stem) is needed only once multi-record files land,
> and even then the minimal `<relpath>#<line>` with rescan-based `Fetch` suffices;
> the seekable/byte-offset machinery is a later optimization. **Encryption-at-rest**
> is a much-later enterprise feature, written down at design fidelity but not near-
> term. Don't read §5/§6 length as near-term effort.

### Where: a sidecar directory, content-addressed
- A **single sidecar root per dataset** (`<dir>/.n1k1/`, hidden, co-located, easy to
  `.gitignore`/delete) holding: the catalog (mode 3), index files (bbolt for GSI,
  bleve dirs for FTS — indexing doc), extracted caches, and a **manifest**
  describing the source state each artifact was built from. Location configurable.
- **Canonical layout lives in `DESIGN-indexing.md` ("Sidecar layout")** (`LAYOUT`,
  `catalog.json`, `<ns>/<ks>/manifest.json`,
  `<ns>/<ks>/idx/<name>__<kind>__<defhash>/…`, `tmp/`, `trash/`). This doc owns two
  things inside it: **`catalog.json`'s source/layout half** (keyspace →
  glob/format/partition/compression) and the **`manifest.json`** contents.
- **Manifest placement:** per-keyspace (`<ns>/<ks>/manifest.json`), aligning with the
  indexing doc — read "per-root" below as **per-keyspace-root**. Dataset-global bits
  (`manifest_schema_version`, `config_fingerprint`, top `root_merkle_hash`) live in
  `catalog.json`/`LAYOUT`.

### Comingling in `catalog.json`: separate by writer & lifecycle
`catalog.json` carries *both* source/layout mappings and index definitions. Safe
because of a **one-way data-flow** and a **single-writer input file**:
```
source/layout config → source manifest → index defs → index instances (build-state)
  (human/gen INPUT)      (derived)          (intent)     (per-indexer OUTPUT)
```
- **Declared input — safe to comingle.** Source mappings **and *declared* index
  intents** are human/generator-authored, slow-changing, single-writer. Clean
  subkeys (`sources`, `indexes`, `views`) in one file are fine. (Split into separate
  files only if you want them versioned separately.)
- **Machine-managed output — must NOT live in `catalog.json`.** Everything fast-
  changing, per-indexer, dynamically rebuilt/removed belongs in **self-describing
  per-instance dirs** (`idx/<name>__<kind>__<defhash>/meta.json`) + per-keyspace
  `manifest.json`. This answers every worry: many indexers each own their own dir (no
  shared-file contention); a new indexer type = a new **`kind`**
  (`gsi`/`fts`/`zonemap`/`bloom`/`count`) dropping into its own dirs, never editing
  `catalog.json`; removing an index trashes one dir (blast radius = one dir);
  discovery is by scanning `idx/` (the filesystem is the source of truth for what's
  built).
- **The rule this forces:** `catalog.json`'s `indexes` is **declared intent only**.
  Adaptive/auto-created indexes live **purely as instance dirs** and must **not**
  rewrite `catalog.json` — else the single-writer property (and comingling safety) is
  lost. Declared-vs-adaptive maps to file-vs-dir.

### When: the trigger and concurrency model (the hard part)
- **Trigger.** Default **lazy check-on-query**: `stat` the tree (Merkle-pruned) and
  rebuild only stale artifacts before scanning. Optional **TTL** to skip the `stat`
  storm on hot queries, and `--no-revalidate` for known-static trees (ties to
  `sealed?`). A background `fsnotify` watcher is a *later* nicety — a CLI process is
  short-lived and shouldn't depend on a daemon.
- **Files changing mid-scan.** Snapshot the manifest fingerprints at query start; if
  a file's `(size,mtime)` changed since when we open it, error clearly or re-read —
  don't silently mix old and new. The file datastore offers no MVCC.
- **Concurrency on `.n1k1/`.** Need a **lockfile / atomic rename-into-place** for
  manifest writes; readers must tolerate a concurrently-updating sidecar (or fall
  back to reading source directly). bbolt already gives single-writer file locking.

### How: a manifest with per-file fingerprints, Merkle-rolled
- **Per-file fingerprint:** `(relative_path, size, mtime, content_hash?)`.
  `(size, mtime)` alone is the cheap **Spark/Hive/Delta-class** check (fast; can miss
  same-size same-mtime edits) — good default. Add an optional **content hash**
  (xxhash/blake3) for correctness-critical use, computed only when `(size, mtime)`
  says a file might have changed. (DuckDB by contrast mostly *re-reads* per query with
  no persistent manifest — this mtime-cache framing is the Spark/lakehouse lineage.)
- **Merkle rollup:** hash each directory node from its children (git's tree-object
  model). One root-hash compare answers "did anything change?"; on mismatch, descend
  only into changed subtrees. Cheap re-validation over huge, mostly-static trees.
- **Append-only optimization (the log case):** for files that only grow, store
  `(known_offset, hash_up_to_offset)`. If the prefix hash matches and size grew,
  index only the **tail** beyond `known_offset` and advance it — never re-read old
  data. New dated dirs appear as new manifest entries, so only new partitions get
  indexed. (Assumes `known_offset` sits on a *record boundary*; re-scan from the prior
  boundary if a partial trailing record was appended.)

### Manifest contents — what to track (per file, partition, root)
The richer the manifest, the more we can *skip* — change detection, partition
**pruning**, **cardinality**, **incremental** index builds. Three levels:

**Per source file:** `relpath`; identity `size`/`mtime` (opt. `inode`/`dev`/`ctime`);
`content_hash` + `prefix_hash`/`known_offset` (append-only tail); `format`/
`compression`/`encryption`/`codec_seekable?` (drives the §6 doc-ID scheme);
`doc_count` (cardinality & `LIMIT`); **zone map** `min_id`/`max_id` **and** min/max
(+ null-count, distinct-estimate) for indexed columns — prune the file without reading
it (Parquet/Iceberg do this); `schema_fingerprint` (drives `union_by_name`);
`partition_values` (from the path); per-index `built_through_offset`/`built?`;
`status`+`error` (failed files recorded, not dropped); `last_scanned_at`.
**Extract/sorted-source fields (§4):** the memoized `ExtractSpec` (or its hash) —
`format`/`framing`/`fields`/`provenance`; and the **sorted-source contract** —
`sort_key`, `sortedness` (`strict`/`near`/`none`), `disorder_bound`, the **time zone
map** `min_key`/`max_key` (int64 epoch-nanos), and periodic **key→offset sync points**
(double as the §6 seekable doc-ID index and let a merge cursor seek to a start time).
These are what the [K-way merge / ASOF](#sorted-sources) reads; computed by `describe`,
so they cost one sampling pass per file, not a full scan.

**Per partition / subdirectory (Merkle rollup):** `merkle_hash` (subtree-skip);
aggregates `doc_count`/`byte_count`/`file_count` + rolled `min_id`/`max_id` + column
min/max (partition pruning); `partition_key`/`value`; `sealed?` — immutability hint: a
past date partition trusted from cache, skipped even for the `(size,mtime)` check (big
win for huge historical trees); `last_visited_at`.

**Per manifest (root):** `manifest_schema_version`+`producer_version` (bump ⇒
rebuild); `root_merkle_hash`; `config_fingerprint` — hash of catalog + index/extraction
defs, so if the *definitions* change, derived data invalidates even when source bytes
didn't; `encryption` info if the sidecar is itself encrypted (§6); global aggregates +
`last_full_scan_at`.

Rule of thumb: **stat-level fields** (size/mtime/hashes/offsets/merkle) serve *change
detection*; **stats fields** (zone maps, counts, schema) serve *pruning + planning*;
**build-state** serves *incremental indexing*. Start minimal (identity + hash + merkle
+ offset), add zone maps/counts when the planner can exploit them.

> **Caveat — the stats fields need a consumer, and it isn't free (reconciling with
> `DESIGN-indexing.md`).** That doc's tier-1 pitches always-on zone maps as needing no
> planner change (file-skipping is a *scan-layer* concern). True — but a prerequisite
> it glosses: **the predicate has to reach the scan.** Today a primary scan doesn't get
> the `WHERE`; the planner emits a residual `Filter` op *above* the scan, so the
> datastore never sees what to prune by. Zone-map pruning needs either (a) filter
> **pushdown into the primary scan** (a conv + fork datastore-interface change —
> modest, recommended) or (b) a datastore-side predicate hook. Cardinality/distinct
> estimates only pay off with **CBO** (off today). **Sequencing:** the first manifest
> carries only change-detection + build-state fields; add zone maps with the
> predicate-pushdown work (when F/G pruning lights up); defer cardinality until CBO.
> The single most important point for keeping the two docs coherent. **Head start:**
> the fork already runs `SargableFor` against a throwaway `partitionVirtualIndex` for
> partition elimination — the sargability test exists; what's missing is delivering
> that verdict to the scan layer.

### Libraries
- **Don't hand-roll a table format.** `apache/iceberg-go` (a dep; read + increasingly
  write, V3 spec, manifests with per-file stats + snapshots + time-travel) is the
  mature expression of exactly this manifest/snapshot idea. Leaning on it (or DuckDB's
  **DuckLake**, which keeps metadata in a SQL database) gives partition pruning +
  change tracking for free, at the cost of its on-disk conventions.
- **Hashing:** `cespare/xxhash` (a dep) or blake3 for fingerprints; `restic/chunker`
  (FastCDC) if we ever want sub-file dedup. For directory Merkle state there's no
  famous Go drop-in — git's model (`go-git`) and Iceberg manifests are the closest.
- **Recommendation:** start with a **thin custom manifest** in `.n1k1/` (per-file
  `size+mtime+xxhash`, Merkle-rolled, append-only offsets) — small, testable, exact
  fit. Keep **Iceberg-go** as the upgrade path for a real interoperable table format.

## §6 Primary keys / document IDs (`META().id`) & `_meta`

Once past one-doc-per-file, "what is a record's key?" stops being obvious and couples
to fetch, indexing, compression, encryption. (§5 scope note: for the MVP the doc-ID is
still the filename stem or `<relpath>#L<lineno>`; the machinery below is post-MVP.)

### Implemented: file metadata via a `_meta` doc field (not `META()`)
The fork's `META()` exposes only a **fixed bitmask** (id/cas/keyspace/type/flags/
expiration/xattrs), so per-file metadata can't ride `META()` without a fork change.
Instead the records layer injects a reserved **`_meta`** sub-object — `` `path` ``
(dir-relative), `name`, `ext`, `size`, `mtime`, `pos` (0-based ordinal within a
container file; absent for one-doc-per-file) — controlled by CLI `-meta`:
`off`/`on`/`auto` (default). Under `auto` each *provider* decides: office/PDF include
it, structured JSON/CSV don't (keeps the exact-match conformance suite unchanged —
it never sees `_meta`). `META().id` stays the stable key (stem / `relpath#i`), since
`USE KEYS`/`JOIN ON KEYS` depend on its format. (`path` is a SQL++ reserved word →
query as `` _meta.`path` ``.)

### Why it matters
`USE KEYS`, `JOIN … ON KEYS`, and the fetch-after-scan path all need a stable
per-record key; an index `Scan()` emits a `PrimaryKey` string that `Fetch` resolves.
One-doc-per-file makes the filename stem the key; multi-record formats have no natural
key, so we synthesize.

### Requirements for a synthesized ID
**Deterministic** (same input ⇒ same ID); **unique within a keyspace** (may span many
files ⇒ composite with source file identity); **self-describing/addressable** (ideally
enough for O(1) `Fetch`); **stable under the expected mutation pattern** (append-only
vs editable).

### Strategies (configurable per source)
1. **Filename stem** (today) — one-doc-per-file. Human-meaningful, stable.
2. **User-designated natural key** — the catalog names a key column/expression (a real
   PK). Best when the data has a true key; stable across re-ingest.
3. **Ordinal / line number** — zero-padded for lexicographic order. Simple, stable for
   append-only; `Fetch` needs a rescan unless paired with a sync index.
4. **Byte offset** in the **logical (decompressed/decrypted) stream** — O(1) `Fetch`
   given a seekable substrate. Preferred for large files.
5. **Content hash** — stable across reorder/move, dedup-friendly; not addressable.

**Recommended default** for multi-record sources: a composite
`<source-relpath>#<logical-offset>` — globally unique; `Fetch` parses it, opens the
file (through decrypt→decompress), seeks, decodes one record. Fall back to
`#L<lineno>` when not seekable; offer the natural-key option for keyed data. (Shipped
form: `<relpath>#<line>@<offset>` for seekable containers — §1.)

### Tweak: compressed containers
Plain gzip/zstd streams aren't randomly seekable, so a byte offset alone can't give
O(1) fetch. Two fixes, both reusing §5 checkpoints:
- **Seekable formats for data we own:** BGZF or seekable-zstd
  (`SaveTheRbtz/zstd-seekable-format-go` exposes `ReadAt`/`Seek` by *decompressed*
  offset). The doc-ID stores the logical offset; the seek table maps it to the block.
- **Opaque/plain-gzip inputs:** keep ordinal/line IDs + periodic **sync points**
  (offset every N records) in the manifest, bounding `Fetch` re-scan to one span (à la
  `zindex`/`gztool`).
- **`.zip`:** the ID includes the entry name (`<zip-path>!<entry>#<offset>`); the
  central directory gives per-entry offsets, the stream caveats apply within an entry.

### Tweak: encrypted containers (encryption-at-rest)
Design as another transparent layer: raw → **decrypt** → decompress → decode.
- **Random access needs segmented/chunked encryption, not whole-file AEAD:** Google
  **Tink** `streamingaead` (AES-GCM-HKDF, ~1 MB segments) or **age**'s STREAM (its
  `DecryptReaderAt` implements `io.ReaderAt`). Both give plaintext-offset random
  access. So **seekable-compression and seekable-encryption share one mechanism:** the
  doc-ID's logical offset is mapped through the format's segment/block table.
- **Key management:** envelope encryption — a DEK wrapped by a KEK from a
  KMS/keyring/passphrase. Use **`gocloud.dev/secrets`** (a dep), or age
  recipients/passphrase for the local case.
- **Critical coupling — derived artifacts leak plaintext.** Indexes, extracted text,
  and the manifest are built from *decrypted* content; storing them in the clear
  defeats encryption-at-rest. The `.n1k1` sidecar must itself be encrypted at rest
  (same DEK/KEK) or kept only in memory. A hard requirement.

### Stability coupling with §5
Positional IDs (offset/line) are durable only if content *above* them is immutable —
exactly the append-only log case §5 optimizes, where per-file offset checkpoints double
as change-detection state **and** the `Fetch` seek index. For mutable files, prefer a
natural key (2) or content-hash (5), and document that positional IDs may shift on
edit.

## Dependency licensing (permissive only)

Policy: **MIT / Apache-2.0 / BSD** only — no GPL/AGPL/copyleft/viral.

| Library | Role | License |
|---|---|---|
| `go.etcd.io/bbolt` | GSI ordered store | MIT |
| `blevesearch/bleve/v2` | FTS index | Apache-2.0 |
| `couchbase/rhmap`, `couchbase/moss` | spill / alt store | Apache-2.0 |
| `apache/iceberg-go` | table format / manifests | Apache-2.0 |
| `apache/arrow-go/v18` | columnar / Parquet / CSV | Apache-2.0 |
| `substrait-io/substrait` | plan IR | Apache-2.0 |
| `scritchley/orc` | ORC reader | MIT |
| `hamba/avro` | Avro | MIT |
| `klauspost/compress` | gzip/zstd | BSD-3-Clause |
| `SaveTheRbtz/zstd-seekable-format-go` | seekable zstd | MIT |
| `buger/jsonparser` | JSON decode | MIT |
| Go stdlib (`encoding/csv`, `compress/gzip`, `archive/zip`) | formats | BSD-3 (Go) |
| `cespare/xxhash`, `lukechampine/blake3` | fingerprints | MIT |
| `restic/chunker` | FastCDC dedup | BSD-2-Clause |
| `google/tink-go` | streaming AEAD (encryption) | Apache-2.0 |
| `FiloSottile/age` | file encryption (STREAM) | BSD-3-Clause |
| `gocloud.dev/secrets` | KMS envelope keys | Apache-2.0 |
| Apache **Tika**, **extractous**, **Tesseract** | doc extraction / OCR | Apache-2.0 |
| `xuri/excelize` | XLSX | BSD-3-Clause |
| `ledongthuc/pdf` | PDF text | BSD-3-Clause |
| `pdfcpu/pdfcpu` | PDF text/tooling | Apache-2.0 |
| `go-git/go-git` | Merkle/tree reference | Apache-2.0 |

**Excluded (viral / non-permissive) — do NOT use:** `go-fitz`/MuPDF (AGPLv3);
UniDoc/unipdf (AGPL/commercial); `sajari/docconv` (shells out to GPL `wv`/
`poppler-utils`/`unrtf`/`antiword`). DuckDB (MIT) is design inspiration, not a dep.

## Testing strategy

- **Interpreter/compiler differential.** Every new format/layout needs a case in the
  queryCases harness (`test/cases.go` + `test/query_compiler_test.go`) so the compiled
  path is proven to match the interpreted path. **Done:** flat-root + the decoders each
  have interp-vs-compiler cases, plus a data-backed GSI suite (443 interp / 439
  compiler). Parquet will want one.
- **Golden fixtures for decoders.** Small input fixtures with an expected row set
  (`records/records_test.go`), table-driven. The CSV reader on `encoding/csv` handles
  quoting/escaping/embedded-newlines via stdlib.
- **Conformance suite.** The existing JSON one-doc-per-file corpus validates the
  convention path unchanged; new formats need their own fixtures.
- **Differential vs DuckDB (optional).** For CSV type-inference and JSON
  array-vs-ndjson edge cases, comparing to DuckDB on the same file is a cheap, strong
  oracle — a small opt-in target, not a dependency.
- **Change-detection tests.** Manifest logic (mtime skip, merkle subtree skip,
  append-only tail, concurrent-writer race) is pure logic over a temp dir — unit-test
  directly; the part most likely to be subtly wrong.
- **Allocation benchmarks (a gate).** Benchmark each decoder with `go test -benchmem`
  in `benchmark/` and assert **allocs/op stays ~flat as row count grows** — a rising
  curve means a per-value allocation leaked in.

## Phasing

All new logic lands in **n1k1** — scan/fetch/decode in the glue
`datastore-scan`/`datastore-fetch` ops (compile for free), via
`engine.ExecOpEx = glue.DatastoreOp`. **The fork needed no changes** — plan-time
discovery was done by wrapping the fork's datastore with `datastore/virtual`
(`glue/flatroot.go`), so `DiscoverKeyspaces` was never built.

1. ✅ Relax the file datastore: directory = keyspace = union of *all* supported files;
   recurse; keep `<ns>/<keyspace>` + flat-root auto-detect. Opaque-document path.
2. Decoders: ✅ **JSONL + multi-doc JSON**, ✅ **CSV/TSV** (emit-JSON-per-row, stayed
   opaque — no typed-label story needed), ✅ **YAML**; ⬜ *then* Parquet (arrow-go;
   correctness-only until column-batch ops — the one decoder needing real labels).
3. ✅ **gzip**; ⬜ zstd decode (walker recognizes `.zst`, decode is a stub); ⬜ `.zip`
   as a container.

   **← MVP LINE (crossed).** Steps 1, 2a, gzip were the planned win and shipped, *plus*
   CSV/TSV (2b), YAML, flat-root, office extraction (step 8), `COUNT(*)` pushdown.
   Everything below waits behind demonstrated demand.

4. ⬜ Explicit `read_*('glob', opts)` table functions in FROM — **blocked on a grammar
   fork; deferred for step 5.**
5. ⬜ Catalog/sidecar (`.n1k1/catalog.json`) with hive + projected-date partitions.
6. ◐ Synthetic document IDs: **partially done** — `_meta.pos` in-file ordinal shipped,
   plus `<relpath>#<line>@<offset>` for seekable JSONL/YAML; the general composite +
   natural-key option remain.
7. ⬜ Index/cache sidecar + manifest with Merkle + append-only offsets, where the offset
   checkpoints double as the `Fetch` seek index (joins `DESIGN-indexing.md`).
8. ✅ (basic) Office/unstructured extraction — **pure-Go default shipped**
   (`records/extract.go`, one record/file); ⬜ Tika/extractous+OCR backend and
   per-spreadsheet-row extraction remain; FTS wiring joins `DESIGN-indexing.md`.
9. ⬜ Encryption-at-rest: transparent decrypt layer (Tink/age segmented), envelope keys
   via `gocloud.dev/secrets`, and **encrypted sidecar artifacts**.

**Extract rework + sorted-source track (the PREPARE++ enabler — §4, [sorted
sources](#sorted-sources); JS surface in `DESIGN-extensions.md`):**

E1. ✅ **Two-phase extract seam** — `describe(file) → ExtractSpec` + native `extract`
    from spec; extractor registry matches on ext **and** name-regexp with priority
    (`records.ExtractMatch`/`RecipeRegister`/`RecipeFor`), whole-file office/PDF are
    `{whole}` specs. LANDED (`records/recipe.go`, `records/spec.go`, `records/extract.go`).
E2. ✅ **Native declarative execution** — `framing` (line/multiline/json/section/whole),
    `fields` (byte-regex), and `time` (normalized to int64 epoch-nanos) applied
    per-record on the fast lane, no per-row JS (`records/recipe.go` `SpecApply`).
    `section` frames cbcollect ====-banner command dumps into one `{title,text}` record
    per section (see `frameSection`); example recipe
    `extensions/extract_recipes/couchbase_log.extract.js`. LANDED.
E3. ✅ **Pluggable JS extractors** — `*.extract.js` recipes loaded from a `-ext` recipe
    dir; module-scope `match={exts,names,priority}` + `describe(file)→ExtractSpec` keeps
    JS off the hot path (`glue/ext_extract_jsvm.go`, `DESIGN-extensions.md`); the describe
    result is memoized into the `.n1k1/` sidecar, content-addressed by a recipe
    Fingerprint (`DescribeMemo`; JS recipes fingerprint on source hash). LANDED. (Only
    auto-cloning the recipe repo from git remains a convenience wrapper.)
E4. ✅ **Sorted-source manifest fields** — `SortedSourceMeta`
    (`sort_key`/`sortedness`/`disorder_bound`/min-max time zone map/sync points/record
    count) is produced by the recipe sample in `describe` (`records/spec.go`,
    `records/recipe.go`). LANDED.
E5. ✅ **K-way merge source op** — `OpMergeScan` with all three regimes (disjoint→concat,
    strict→heap, near→watermarked buffer); feeds ASOF + windowed temporal detectors
    (`engine/op_merge_scan.go`, wired via `glue/optimize_temporal.go`). LANDED.

Separable tracks:
- **Query-defined VIEWs:** (i) single-source reshaping views land with catalog-view
  expansion (WITH/CTE machinery); (ii) union/normalize views are now unblocked on the
  union itself — **`VisitUnionAll` has landed** in `glue/conv.go` (`union-all`, by-name
  label union) — so what remains for them is catalog-view expansion + branch pushdown;
  (iii) materialized views + predicate pushdown are perf follow-ups (§5).
- **Object-store backend** (S3/GCS/Azure via `gocloud.dev/blob` or `aws-sdk`, both
  indirect deps) — lets any catalog `root`/glob point at `s3://…`. Reading an existing
  **Glue Data Catalog** (`aws-sdk-go-v2/service/glue`, present) is the "generated
  catalog" variant.

## Open questions

- **`RecordSource` signature & CSV reader choice (allocation). (Partly settled.)**
  Shipped decoders use `Next(rec *Record) (bool, error)`; CSV is on `encoding/csv`
  (correctness-first), which allocates field strings per row. **Open:** replace with a
  `[]byte`-oriented zero-copy reader and add the §1 allocation benchmark gate.
- **SQL++ surface for table functions / globs. (RESOLVED.)** The parser rejects both
  `FROM read_csv('foo.csv')` and bare `FROM 'foo.csv'`; no table-valued-function
  machinery in `algebra/`. Mode 2 (`read_csv(...)`) needs a goyacc grammar + algebra +
  planner fork — **not paid.** Instead: **inline globs land as the backtick-quoted
  keyspace-name convention (Mode 2b above) — no fork** (`` FROM `./data/**/*.json` ``,
  CWD-relative, expanded by a `maybeGlob` datastore wrapper); the catalog (mode 3)
  remains the power path for named/partitioned sources.
- **Fork divergence budget. (RESOLVED — zero.)** Everything shipped — flat-root,
  multi-file union, JSONL/JSON/CSV/YAML/office decoders, gzip, `COUNT(*)`, `_meta`,
  native byte-path fetch — landed **without a single datasource change to the fork**
  (only build-plumbing commits). Plan-time discovery was done by *wrapping* the
  datastore with `datastore/virtual`. Remaining: whether a future **catalog**
  (non-directory keyspace names) can still be faked by the same trick — expectation:
  **yes, still no fork change**; home C (full n1k1-side datastore) stays the fallback.
- **Columnar-source performance.** Add column-batch ops so Parquet/Arrow is a real
  perf win, or accept the transpose-to-rows cost and treat columnar as
  correctness-only? (§1.)
- **Partition columns vs document shape.** How do hive/projected partition values
  (virtual columns) coexist with SQL++'s schemaless document model?
- **Bespoke manifest vs Iceberg-go.** Adopt Iceberg's proven metadata, or keep a
  minimal custom `.n1k1` manifest? Interop/robustness vs simplicity.
- **CSV typing in a JSON/SQL++ world.** How aggressively to infer types vs treat cells
  as strings; how to expose overrides.
- **Predicate pushdown through a VIEW.** Does a `WHERE`/partition predicate on the view
  reach the sub-source scans so whole eras/partitions prune — or does the view read all
  history each query? Depends on cbq's rewrite rules + the §5 predicate-to-scan work.
  The gating perf question; materialization is the fallback. (Correctness is fine
  either way.)
- **View definition home & DDL.** Views live in `.n1k1/catalog.json` as a stored
  `SELECT` string (no `CREATE VIEW` DDL — n1k1 doesn't execute DDL). Is a `views` map
  the right surface, and how do view names coexist with keyspace names (shadow?
  separate namespace)?
- **Native vs cgo extractors/OCR.** Accept the `extractous`/Tika native dependency for
  breadth + OCR, or stay pure-Go and narrower?
- **Default doc-ID scheme.** Positional `<relpath>#<offset>` (addressable, shifts on
  edit) vs content-hash (stable, not seekable) vs requiring a natural key — and how
  aggressively to default per source/mutation pattern.
- **Encryption scope & seekability.** Which segmented-encryption format (Tink vs age),
  and whether to require seekable compression/encryption for large encrypted sources vs
  accepting rescan-from-checkpoint.
- **`disorder_bound`: declared, measured, or both — and what happens when it's wrong.**
  A sampled bound can under-estimate; the merge must validate (widen+warn / error /
  spill-sort fallback). How conservative should the default be, and is a per-source
  strictness knob the right surface? (§4, [sorted sources](#sorted-sources).)
- **`ExtractSpec` expressiveness vs the imperative escape hatch.** How much do the
  declarative `framing`/`fields`/`time` primitives cover before a format forces
  imperative `extract(file, meta, emit)` (and the per-row JS/boxed cost)? Grok-completeness,
  stateful multiline, nested framing (a `section` whose body is itself `multiline`).
- **Log time model.** Normalizing wildly different timestamp formats/timezones/precision
  into one int64 epoch-nanos key — per-source `time` spec (chosen here) vs inference; how
  to handle missing/renamed/rolled-over timezones and clock skew across nodes (the merge
  compares keys *across* nodes, so skew is a correctness risk). (Mirrors
  `DESIGN-prepare.md`'s open question.)
- **Extract-recipe repo governance.** The JS extractor repo is a trusted-code surface
  (like the detector corpus). Signing/pinning, golden-fixture CI per extractor
  (`DESIGN-testing.md`), and how describe-spec invalidation interacts with the
  content-addressed sidecar cache.
- **Concatenate vs merge threshold.** Disjoint time ranges → concatenate (no heap); but
  near-boundary overlap within `disorder_bound` still needs a merge at the seam. Detect
  and localize the merge to just the overlapping tail/head?

## Sources

- DuckDB — Reading Multiple Files (glob `**`, lists, `union_by_name`, `filename`):
  https://duckdb.org/docs/current/data/multiple_files/overview
- DuckDB — Hive Partitioning (auto-detect `key=value`, pruning):
  https://duckdb.org/docs/current/data/partitioning/hive_partitioning
- DuckDB — Loading JSON (gzip/zstd auto-detect, newline-delimited vs array):
  https://duckdb.org/docs/current/data/json/loading_json
- DuckDB — Directly Reading Files / replacement scans:
  https://duckdb.org/docs/current/guides/file_formats/read_file
- AWS Athena — Partition Projection (date templates, avoid listing):
  https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html
- Open table formats overview (Iceberg/Delta/Hudi/Paimon/DuckLake):
  https://datalakehousehub.com/blog/2025-09-ultimate-guide-to-open-table-formats/
- Apache iceberg-go (read/write, V3, manifests):
  https://github.com/apache/iceberg-go ; release notes:
  https://iceberg.apache.org/blog/apache-iceberg-go-0.5.0-release/
- extractous-go (Go document extraction, Tika+Tesseract):
  https://github.com/rahulpoonia29/extractous-go
- restic/chunker (FastCDC content-defined chunking in Go):
  https://github.com/restic/chunker
- Seekable zstd (random access by decompressed offset, over klauspost/compress):
  https://github.com/SaveTheRbtz/zstd-seekable-format-go
- Tink Streaming AEAD (segmented encryption, random access):
  https://developers.google.com/tink/streaming-aead
- age STREAM / `DecryptReaderAt` (chunked, random-access decryption):
  https://github.com/FiloSottile/age
- Watermarks / bounded out-of-orderness (the `disorder_bound` model for near-sorted
  merge): Apache Flink event-time & watermarks
  https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/ ;
  Google Dataflow — The world beyond batch: Streaming 102 (watermarks, allowed lateness)
  https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/
- ASOF joins (nearest-preceding temporal join as an ordered merge): DuckDB ASOF JOIN
  https://duckdb.org/docs/current/guides/sql_features/asof_join
- grok / logfmt log-line field extraction (the `fields` primitive): Elastic grok
  https://www.elastic.co/guide/en/elasticsearch/reference/current/grok-processor.html
- lnav — log-format definitions & auto-detection (prior art for per-format extract
  specs): https://docs.lnav.org/en/latest/formats.html
