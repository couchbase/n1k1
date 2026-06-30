# Design: Data Sources for n1k1

Status: proposal / for review

This document explores the kinds of source data n1k1 should support — file
formats, directory layouts, compression, "office"/unstructured documents — and
how derived artifacts (indexes, caches) should be stored and kept in sync with
changing source data. It takes inspiration from DuckDB, and from how Spark, AWS
Athena/Glue, ClickHouse, and log tools handle the same problems.

Companion doc: `DESIGN-indexing.md` (how the planner uses indexes). This doc
covers where index/derived data lives and how we detect that source data has
changed.

## Motivation & scope

Today n1k1 reads exactly one shape of data: a directory of single-document
`*.json` files arranged as `<datastoreDir>/<namespace>/<keyspace>/<key>.json`,
via the forked `couchbase/query` file datastore. To be a useful local SQL++ CLI
("DuckDB for SQL++/JSON"), it should ingest the formats people actually have on
disk — CSV/TSV, JSON Lines, multi-record JSON, log files, compressed archives,
and eventually PDFs/Office docs — across the directory layouts those formats
arrive in.

## Starting point (n1k1 today)

There are **two separate, only-partially-connected** data paths today:

**A. The SQL++ / file-datastore path (what FROM uses):**
- **Layout:** `<dir>/<namespace>/<keyspace>/<key>.json`, one JSON object per
  file; the file's base name is the document key. Keyspaces are subdirectories
  (`loadKeyspaces`); documents are files; `Scan` does `ioutil.ReadDir` and
  iterates non-directory entries; `fetch` does `ioutil.ReadFile` of `<key>.json`.
  (`n1k1-query/datastore/file/file.go`.)
- **FROM mapping:** `FROM default:orders` → `namespace=default`,
  `keyspace=orders` → directory `<dir>/default/orders/` (case-insensitive). The
  keyspace name *is* a directory name. **No** multi-file-per-keyspace, **no**
  compression, **no** non-JSON formats on this path.

**B. The engine's direct-file scan path (NOT reachable from FROM today):**
- `engine/op_scan.go` already has scan kinds `"filePath"`, `"csvData"`,
  `"jsonsData"`; `ScanFile()` routes by extension (`.csv` → `ScanReaderAsCsv`,
  `.jsons` → `ScanReaderAsJsons`). But the CSV reader is **naive** (splits on raw
  commas via `bytes.IndexByte`, no quoting/escaping, no header/type inference) and
  the JSONL reader just passes each raw line through. These exist as low-level
  ops; they are **not** wired to keyspaces/FROM. This is a useful primitive to
  build on, not a finished format layer.

**Existing infrastructure to reuse (important):**
- The fork already contains a **working Iceberg + Arrow reader**:
  `n1k1-query/primitives/external/iceberg_reader.go` (imports `arrow-go/v18`,
  `arrow/csv`, `arrow/ipc`, `iceberg-go/io`, `iceberg-go/table`; iterates Arrow
  RecordBatches over an Iceberg table scan). It is **implemented but not wired
  into the n1k1 engine** — a natural integration target for Parquet/Iceberg/CSV
  (Arrow has its own robust CSV reader) and for the change-tracking ideas in §5.
- A schema-inferencer hook exists (`GetDefaultInferencer` →
  `infer.NewDefaultSchemaInferencer`), but it's an enterprise feature that n1k1's
  pure-Go build drops.
- **Relevant deps already present** in `go.mod` (signalling intent):
  `apache/iceberg-go`, `apache/arrow-go/v18`, `substrait-io/substrait`,
  `scritchley/orc`, `hamba/avro`, `klauspost/compress`, `blevesearch/bleve/v2`,
  `go.etcd.io/bbolt`; `buger/jsonparser` is a direct dep used for JSON decoding.

## Design principle: separate the concerns into layers

The single biggest lesson from DuckDB is to **decouple four things** that n1k1
currently fuses into "a keyspace is a directory of json files":

1. **Record format / decoder** — turns bytes into rows/JSON values (CSV, TSV,
   JSONL, JSON-array, Parquet, log-line, extracted-document).
2. **Layout / discovery** — turns a FROM term into the *set of files* to read,
   and optionally derives extra columns from the path (partitions).
3. **Compression / container** — transparently un-gzip/un-zst, or enumerate a
   `.zip`/archive, beneath the decoder.
4. **Derived artifacts** — indexes/caches/extracted text, plus the change-
   detection metadata that keeps them valid.

Each layer should be independently pluggable. The rest of this doc designs each.

## 1. File formats

### What DuckDB provides (the reference design)
- `read_csv` / `read_csv_auto`: a **sniffer** auto-detects delimiter, quoting,
  header, and column types (and gzip compression). CSV and TSV are the same
  reader with a different delimiter.
- `read_json` / `read_ndjson`: handles both newline-delimited JSON (JSONL) and
  JSON arrays/records; `format = 'auto' | 'newline_delimited' | 'array'`.
- `read_parquet`, plus ORC/Avro/Arrow via extensions.
- **Replacement scans:** `FROM 'data/foo.csv'` works directly — a bare path in
  FROM is treated as a file to read, with the reader chosen by extension.

### Recommendation for n1k1
- Define a small `RecordSource` interface: given an `io.Reader` (post-
  decompression) + options, yield a stream of `value.Value` (n1k1's JSON value)
  rows, plus an optional inferred schema/labels.
- Implement decoders in priority order: **JSONL** and **multi-doc JSON** (closest
  to today's model and to SQL++'s JSON nature; the engine's `ScanReaderAsJsons`
  is a starting point) → **CSV/TSV** (one shared reader, delimiter param, with a
  DuckDB-style sniffer for header/types — **replace the naive `op_scan.go` CSV
  splitter**; prefer Go's `encoding/csv` or Arrow's `arrow/csv` reader, which the
  fork already imports, over hand-rolled comma-splitting) → then **Parquet**
  (via `apache/arrow-go`, already a dep; columnar + pushdown is a big win) →
  ORC/Avro later (deps already present).
- **Reuse the existing `primitives/external/iceberg_reader.go`** (Arrow-batch
  iteration over Iceberg/Parquet) as the backbone for columnar formats rather
  than writing a new Parquet path — it just needs wiring into a `RecordSource`.
- **Type handling:** JSON formats are naturally typed/loose (SQL++'s home turf).
  For CSV/TSV, sniff types but always allow "everything is a string" fallback;
  expose per-column type overrides like DuckDB's `columns=`/`types=`.
- **Format selection:** primarily by file extension (`.csv`,`.tsv`,`.jsonl`,
  `.ndjson`,`.json`,`.parquet`), overridable by an explicit FROM-term option (see
  §2). Content sniffing only as a tiebreaker.

## 2. Directory layouts & FROM-term resolution

This is the crux of the user's question: flat vs two-level vs deep, auto-detect
vs explicit.

### What the ecosystem does
- **DuckDB does not force a layout.** You point a glob/list at files:
  `'dir/*.csv'`, recursive `'dir/**/*.csv'`, brace `'{a,b}/*.json'`, or a list
  `['a.csv','b.csv']`. `union_by_name=true` merges files with differing schemas
  by column name; `filename=true` adds a source-file column. There is no
  mandatory "one directory per table."
- **Hive partitioning:** for paths like `.../year=2026/month=01/file.parquet`,
  DuckDB auto-detects the `key=value` segments and exposes them as virtual
  columns, enabling **partition pruning** (`WHERE year=2026` skips whole files
  before reading). It only works for the `key=value` naming convention. Spark,
  Hive, and Trino do the same.
- **Bare date-partition dirs** (`.../ecommerce/20260101/*.log.gz`, *no*
  `key=value`) are the user's "almost invisible container" case. The ecosystem
  answer is **AWS Athena partition projection**: instead of *listing* the
  directory tree, you declare a template — a `date` column with a `range` and a
  `format`/`storage.location.template` — and the engine **computes** the candidate
  paths from the query's predicate. This both avoids expensive listing and makes
  the container dirs invisible as far as the schema is concerned.
- **Log-specific tools** (lnav, etc.) auto-detect log formats and timestamps;
  Loki/Vector attach labels rather than relying on directory structure.

### Recommendation for n1k1: convention by default, explicit when needed
Support **three resolution modes**, in increasing power:

1. **Convention (zero-config), backward-compatible.** Keep today's
   `<dir>/<namespace>/<keyspace>/...` as the default. Relax it so a keyspace
   directory may contain *any* supported format (not just `*.json`), and may
   contain **many records across many files** (a directory = a keyspace = the
   union of its files), not just one-doc-per-file. Recurse into subdirectories by
   default so deep/partitioned layouts "just work" as an unkeyed union.
   - **Do NOT force two levels.** Allow a flat root too: if `<dir>` directly
     contains data files (no namespace/keyspace subdirs), treat the directory
     name (or a default) as the keyspace. Auto-detect: if subdirs contain data
     files, they're keyspaces; if `<dir>` itself contains data files, it's a
     single flat keyspace. This mirrors DuckDB's "no mandatory layout."

2. **Explicit table functions / globs in FROM** (DuckDB-style power mode). Let
   the FROM term name a path/glob and options directly, e.g. (syntax TBD within
   SQL++ constraints — likely via a table-valued function):
   `FROM read_json('logs/**/*.jsonl.gz') AS t` or
   `FROM read_csv('sales/*.csv', header=true) AS t`. The FROM term then *tells*
   us format, files, and options — no guessing. This is the escape hatch for
   anything convention can't express.

3. **Catalog / sidecar mapping** for named, reusable, partitioned sources. A
   per-root config (e.g. `.n1k1/catalog.json`) maps a keyspace name to: a root
   glob, a format, partition columns (hive or **projected** date templates à la
   Athena), and compression. This is where the "invisible date container dirs"
   case is handled cleanly: declare `ecommerce` → `ecommerce/{date:YYYYMMDD}/*.log.gz`
   with `date` as a projected partition column, so `WHERE date >= ...` prunes by
   *computing* directory names instead of listing them.

**Auto-detect vs FROM-term:** use both. Convention auto-detects the common cases;
the FROM term (mode 2) and catalog (mode 3) override when the user needs control.
Hive `key=value` partitions auto-detect within any mode; bare date partitions
require declaring a projection template (mode 3) because they're ambiguous by
construction.

## 3. Compression & containers

### What DuckDB does
- gzip (`.gz`) and zstd (`.zst`) are decompressed **transparently**, detected by
  file extension; JSON supports gzip+zstd, CSV historically gzip-focused. No temp
  files. `.zip` archives are **not** transparently read (a zip is a container of
  many entries, not a single compressed stream) — repeatedly requested but
  treated as out of scope for direct scan.

### Recommendation for n1k1
- Treat **single-stream compression** (`.gz`, `.zst`, optionally `.bz2`/`.xz`)
  as a transparent decode layer *under* the decoder, keyed off the inner
  extension (`foo.jsonl.gz` → gzip → JSONL). Use Go's `compress/gzip` and
  `klauspost/compress` (`zstd`, already a dep) — both well-tested.
- Treat **`.zip`** as a *container/layout* concern, not a codec: enumerate its
  entries (`archive/zip`) and feed each entry through format detection, exactly
  as if the zip were a directory. This also covers the common "office" formats
  in §4 (docx/xlsx/pptx are zip files).
- Caveat: gzip/zstd streams aren't seekable, so columnar formats (Parquet) lose
  random-access/pushdown when gzipped — fine for row formats (CSV/JSONL/logs),
  document this limitation.

## 4. "Office" / unstructured documents (PDF, DOCX, XLSX, PPTX, …)

Aspiration: crack open unstructured files, extract their content as queryable
rows, and optionally full-text index them (ties directly to the FTS/bleve work in
`DESIGN-indexing.md`).

### Model
Add an **extractor** kind of `RecordSource`: input is one document file, output
is one row (with `filename`, extracted `text`, and metadata like author/title/
page-count/sheet/row), or *many* rows for inherently tabular docs (one row per
spreadsheet row, one row per slide/page). The extracted rows then flow through
the normal pipeline and can be fed to a bleve FTS index for content search.

### Libraries (the user asked for well-tested options)
- **Breadth, "bulletproof":** Apache **Tika** is the gold standard (60+ formats)
  but is Java — run it as a sidecar server, or use **`extractous-go`** (Go
  bindings over the Rust "Extractous" engine, which wraps Tika + Tesseract OCR;
  actively maintained as of late 2025, supports PDF/DOCX/XLSX/PPTX/HTML + OCR for
  scanned docs). This is the recommended path for *breadth* and for scanned-PDF
  OCR, at the cost of a cgo/native dependency.
- **Pure-Go, narrower:** `xuri/excelize` (XLSX, excellent), `ledongthuc/pdf` /
  `pdfcpu` / `go-fitz` (PDF, varying robustness/cgo), `sajari/docconv`
  (aggregator wrapping several tools). Good when staying pure-Go matters more
  than format coverage.
- **Recommendation:** make extraction a pluggable backend with two
  implementations — a pure-Go default for the common cases (xlsx via excelize,
  basic PDF/text) and an optional `extractous`/Tika backend (build tag) for
  breadth + OCR. Office docs being zip-based dovetails with the §3 zip handling.

## 5. Indexes & derived artifacts: storage + change detection

This is the part most coupled to `DESIGN-indexing.md`. Two questions: **where** do
derived artifacts live, and **how** do we know source data changed?

### Where: a sidecar directory, content-addressed
- Recommend a **single sidecar root per dataset**, e.g. `<dir>/.n1k1/` (hidden,
  co-located, easy to `.gitignore`/delete), holding: the catalog (§2.3), index
  files (bbolt for GSI, bleve dirs for FTS — see `DESIGN-indexing.md`), extracted
  document caches, and a **manifest** describing the source state each artifact
  was built from. A co-located hidden dir is cleaner than a parallel
  `<dir>-INDEXES` sibling (keeps everything movable as one tree) — but make the
  location configurable.

### How: a manifest with per-file fingerprints, Merkle-rolled
- **Per-file fingerprint:** record `(relative_path, size, mtime, content_hash?)`.
  - `(size, mtime)` alone is the cheap check Spark/DuckDB-class tools effectively
    rely on — fast, but can miss same-size same-mtime edits. Good default.
  - Add an optional **content hash** (xxhash/blake3) for correctness-critical
    use; only compute it when `(size, mtime)` says a file might have changed.
- **Merkle rollup for cheap subtree skipping:** hash each directory node from its
  children's fingerprints (git's tree-object model). A single root-hash compare
  answers "did anything change?"; on mismatch, descend only into subtrees whose
  rolled hash changed. This is what makes re-validation cheap over huge,
  mostly-static log trees.
- **Append-only / additive optimization (the log case):** for files that only
  grow, store `(size, hash_of_first_N_bytes)` or a per-file
  `(known_offset, hash_up_to_offset)`. If the prefix hash matches and size grew,
  the file is *append-only*: index only the **tail** beyond `known_offset` and
  advance the offset — never re-read old data. New dated container dirs appear as
  brand-new manifest entries (their parent subtree hash changes), so only the new
  partitions get indexed. This makes the common "logs keep coming, old data
  never changes" case incremental by construction.

### Libraries (well-tested building blocks)
- **Don't hand-roll a table format if you can avoid it.** `apache/iceberg-go`
  (already a dep; v0.5+ supports read and increasingly write, V3 spec, manifests
  with per-file stats + snapshots + time-travel) is the mature, battle-tested
  expression of exactly this manifest/snapshot idea. Leaning on Iceberg metadata
  (or DuckDB's newer **DuckLake** approach, which keeps table metadata in a SQL
  database rather than file manifests) gives partition pruning + change tracking
  for free, at the cost of adopting its on-disk conventions. Strong candidate if
  we want robustness over a bespoke `.n1k1` manifest.
- **Hashing/dedup primitives:** `cespare/xxhash` (fast non-crypto hash, already a
  dep) or blake3 for fingerprints; `restic/chunker` (FastCDC content-defined
  chunking) if we ever want sub-file dedup of large blobs. For directory Merkle
  state specifically there's no single famous Go drop-in — git's model
  (via `go-git`) and Iceberg manifests are the closest production-proven
  references; a thin custom manifest over xxhash is reasonable and small.
- **Recommendation:** start with a **thin custom manifest** in `.n1k1/`
  (per-file `size+mtime+xxhash`, Merkle-rolled, append-only offsets) — it's small,
  testable, and matches our needs exactly. Keep **Iceberg-go** in mind as the
  upgrade path when/if users want a real interoperable table format and don't
  want bespoke metadata.

## Phasing (suggested)

1. Relax the file datastore: directory = keyspace = union of *all* supported
   files; recurse; keep `<ns>/<keyspace>` convention + flat-root auto-detect.
2. Add decoders: JSONL + multi-doc JSON, then CSV/TSV (with sniffer), then
   Parquet (arrow-go).
3. Transparent gzip/zstd decode; `.zip` as a container.
4. Explicit `read_*('glob', opts)` table functions in FROM (power mode).
5. Catalog/sidecar (`.n1k1/catalog.json`) with hive + projected-date partitions.
6. Index/cache sidecar + manifest with Merkle + append-only offsets (joins
   `DESIGN-indexing.md`).
7. Office/unstructured extraction (pure-Go default + optional Tika/extractous),
   feeding FTS.

## Open questions

- **SQL++ surface for table functions / globs.** Does the n1k1-query parser
  accept DuckDB-style `read_csv('glob', …)` table-valued functions in FROM, or do
  we route everything through keyspace names + the catalog? Determines whether
  mode 2 (§2) is viable as-is. (Needs a parser/algebra check.)
- **Partition columns vs document shape.** Hive/projected partition values become
  virtual columns — how do they coexist with SQL++'s schemaless document model?
- **Bespoke manifest vs Iceberg-go.** Adopt Iceberg's proven metadata, or keep a
  minimal custom `.n1k1` manifest? Trade interop/robustness vs simplicity.
- **CSV typing in a JSON/SQL++ world.** How aggressively to infer types vs treat
  CSV cells as strings; how to expose overrides.
- **Native vs cgo extractors/OCR.** Whether to accept the `extractous`/Tika
  native dependency for document breadth + OCR, or stay pure-Go and narrower.

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
