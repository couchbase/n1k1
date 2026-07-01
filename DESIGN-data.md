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

### Libraries (well-tested **and** permissively licensed — see the licensing note)
Document extraction is where the licensing landmines are, so the choices below are
constrained to MIT/Apache-2.0/BSD only.
- **Breadth, "bulletproof":** Apache **Tika** (Apache-2.0, 60+ formats) — Java,
  run as a sidecar server — or **`extractous`** (core Apache-2.0; Rust engine
  wrapping Tika + Tesseract OCR, both Apache-2.0; Go bindings exist). Recommended
  for *breadth* and scanned-PDF OCR, at the cost of a cgo/native dependency.
- **Pure-Go, narrower:** `xuri/excelize` (XLSX; **BSD-3**), `ledongthuc/pdf`
  (**BSD-3**) and/or `pdfcpu` (**Apache-2.0**) for PDF text.
- **Avoid (viral / non-permissive):** `go-fitz` and anything else wrapping
  **MuPDF** — **AGPLv3**. **UniDoc/unipdf** — **AGPL/commercial** dual license.
  `sajari/docconv`, though MIT itself, shells out to GPL binaries (`wv`,
  `poppler-utils`, `unrtf`, `antiword`) — avoid unless restricted to
  permissive-only backends. These are the common PDF/office traps; don't reach
  for them.
- **Recommendation:** a pluggable extraction backend with two implementations — a
  pure-Go default (excelize + ledongthuc/pdf or pdfcpu) and an optional
  Tika/extractous backend (build tag) for breadth + OCR. Both stay within the
  permissive-license policy. Office docs being zip-based dovetails with §3.

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

### Manifest contents — what to track (per file, per partition, per root)
The manifest is the memory of "what we saw last time." The richer it is, the more
work we can *skip* — not just change detection, but **predicate/partition pruning**
(don't even open files that can't match a `WHERE`), **cardinality** for planning,
and **incremental** index builds. Three levels:

**Per source file:**
- `relpath`, and stable identity: `size`, `mtime`; optionally `inode`/`dev` (to
  distinguish rename from rewrite) and `ctime`.
- `content_hash` (xxhash/blake3), plus `prefix_hash` + `known_offset` for the
  append-only tail optimization.
- `format`, `compression`, `encryption` (as detected), and `codec_seekable?`
  (whether we can random-seek by logical offset — drives the doc-ID scheme in §6).
- `doc_count` (records contributed by this file) — feeds cardinality & `LIMIT`
  short-circuits.
- **Zone map / min–max stats:** `min_id`/`max_id` (the synthetic or natural doc
  IDs) **and** min/max (and null-count, distinct-estimate) for indexed/key columns
  — this is what lets a query prune the file without reading it (Parquet/Iceberg
  do exactly this).
- `schema_fingerprint` (columns + inferred types seen) — detects schema drift and
  drives `union_by_name`.
- `partition_values` (derived from the path: hive `k=v` or projected date).
- Per-index build state: for each index name, `built_through_offset` / `built?` —
  so incremental/partial index builds know what's already covered.
- `status` + `error` (files that failed to parse are recorded, not silently
  dropped — surfaces "we skipped N files").
- `last_scanned_at` (wall clock of the visit that produced this row).

**Per partition / subdirectory (rollup, Merkle-style):**
- `merkle_hash` rolled from child fingerprints (subtree-skip: unchanged hash ⇒
  skip the whole subtree without `stat`-ing every file).
- Aggregates: `doc_count`, `byte_count`, `file_count`, and rolled
  `min_id`/`max_id` + column min/max (partition-level zone map → partition
  pruning).
- `partition_key`/`value` (e.g. `date=20260101`).
- `sealed?` — an immutability hint: a *past* date partition that by policy will
  never change can be trusted from cache and skipped even for the cheap
  `(size,mtime)` check. Big win for huge historical log trees.
- `last_visited_at`.

**Per manifest (root):**
- `manifest_schema_version` and `producer_version` (n1k1 build) — bump ⇒ rebuild
  derived artifacts whose format changed.
- `root_merkle_hash` (one compare answers "did anything change at all?").
- `config_fingerprint` — hash of the catalog + index/extraction definitions the
  artifacts were built from; if the *definitions* change, invalidate derived data
  even when source bytes didn't.
- `encryption` info (wrapped-DEK / key id) if the sidecar is itself encrypted
  (§6's hard requirement).
- Global aggregates (total docs/bytes) and `last_full_scan_at`.

Rule of thumb: **stat-level fields** (`size`, `mtime`, hashes, offsets, merkle)
serve *change detection*; **stats fields** (min/max zone maps, counts, null/
distinct, schema) serve *pruning + planning*; **build-state fields** serve
*incremental indexing*. Start minimal (identity + hash + merkle + offset) and add
zone maps/counts when the planner can exploit them.

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

## 6. Primary keys / document IDs (`META().id`)

Once we move past one-doc-per-file, "what is a record's key?" stops being
obvious — and it couples to almost everything else: fetch, indexing, compression,
and encryption.

### Why it matters
SQL++ exposes `META().id`; `USE KEYS`, `JOIN … ON KEYS`, and the fetch-after-scan
path all need a stable per-record key. In `DESIGN-indexing.md` an index `Scan()`
emits a `PrimaryKey` **string** that `Fetch` later resolves back to a document.
Today, one-doc-per-file makes the **filename stem** the key. Multi-record formats
(CSV, JSONL, Parquet, logs) have **no natural key**, so we must synthesize one.

### Requirements for a synthesized ID
- **Deterministic** — same input ⇒ same ID, so an index built in one run matches
  fetches in another.
- **Unique within a keyspace** — which may span many files ⇒ composite with the
  source file identity.
- **Self-describing / addressable (ideally)** — encodes enough for `Fetch` to
  re-read *just that record* (O(1)) without rescanning the keyspace.
- **Stable under the expected mutation pattern** — append-only vs editable.

### Strategies (configurable per source)
1. **Filename stem** (today) — for one-doc-per-file. Human-meaningful, stable.
2. **User-designated natural key** — let the catalog (§2.3) name a key
   column/expression (a real PK), reusing the same expression parsing as the
   secondary-index design. Best when the data has a true key; keeps `META().id`
   stable across re-ingest.
3. **Ordinal / line number** within a file — zero-padded for lexicographic order
   (the "`0`-prefixed" idea). Simple, cheap, stable for append-only; but `Fetch`
   needs a rescan-to-line unless paired with a sync index.
4. **Byte offset** of the record's start in the **logical (decompressed/
   decrypted) stream** — enables O(1) `Fetch` (seek + decode one record) given a
   seekable substrate. Preferred for large files.
5. **Content hash** (xxhash/blake3) of the record — stable across reorder/move,
   dedup-friendly; but not addressable (no seek) and needs disambiguation for
   identical rows. Good for dedup/idempotency.

**Recommended default** for multi-record sources: a composite, self-describing ID
`<source-relpath>#<logical-offset>` (offset form) — globally unique, and `Fetch`
parses it to open the file (through the decrypt→decompress layers), seek to the
offset, and decode one record. Fall back to `#L<lineno>` when offsets aren't
seekable. Offer the **natural-key** option (strategy 2) for keyed data.

### Tweak: compressed containers
Plain gzip/zstd streams are **not** randomly seekable, so a byte offset alone
can't give O(1) fetch. Two fixes, both reusing §5's manifest checkpoints:
- **Seekable container formats for data we write/own:** BGZF (block-gzip) or the
  seekable-zstd format (`SaveTheRbtz/zstd-seekable-format-go` exposes
  `ReadAt`/`Seek` by *decompressed* offset, layered on `klauspost/compress` which
  we already depend on). The doc-ID stores the logical offset; the format's seek
  table maps it to the compressed block.
- **Opaque/plain-gzip inputs we don't control:** keep ordinal/line IDs and store
  periodic **sync points** (offset every N records) in the manifest, bounding
  `Fetch` re-scan to one inter-checkpoint span — the approach `zindex`/`gztool`
  use for gzip.
- **`.zip` containers:** the ID must include the entry name, e.g.
  `<zip-path>!<entry>#<offset>`. The central directory gives per-entry start
  offsets (random access *between* entries); *within* an entry the stream caveats
  above apply.

### Tweak: encrypted containers (encryption-at-rest)
A recurring enterprise ask; design it as another transparent layer.
- **Layering:** raw → **decrypt** → decompress → decode (mirrors §3). Don't invent
  crypto.
- **Random access needs segmented/chunked encryption, not whole-file AEAD:**
  - **Google Tink** `streamingaead` (AES-GCM-HKDF, ~1 MB segments; segments are
    position-bound and individually decryptable ⇒ random access by plaintext
    offset).
  - **age**'s STREAM (chunked; its `DecryptReaderAt` implements `io.ReaderAt` for
    random-access decryption).
  Both give plaintext-offset random access — exactly what offset doc IDs need. So
  **seekable-compression and seekable-encryption share one mechanism:** the
  doc-ID's logical (plaintext) offset is mapped through the format's segment/block
  table.
- **Key management:** envelope encryption — a data key (DEK) wrapped by a KEK from
  a KMS / keyring / passphrase. Use **`gocloud.dev/secrets`** (already a dep) for
  KMS-backed wrapping, or age recipients/passphrase for the simple local case.
  (Couchbase's `cbauth`/`gocbcrypto` are in the tree but heavier than a standalone
  CLI needs.)
- **Critical coupling — derived artifacts leak plaintext.** Indexes (bbolt/bleve),
  extracted Office text, and the manifest are all built from *decrypted* content;
  storing them in the clear would defeat encryption-at-rest. The `.n1k1` sidecar
  must itself be encrypted at rest (same DEK/KEK) or kept only in memory. Treat
  this as a hard requirement, not an afterthought.

### Stability coupling with §5
Positional IDs (offset/line) are durable only if the content *above* them is
immutable — exactly the append-only log case §5 optimizes, where per-file offset
checkpoints double as both the change-detection state **and** the `Fetch` seek
index. For mutable files, prefer a natural key (strategy 2) or content-hash IDs
(strategy 5), and document that synthetic positional IDs may shift on edit.

## Dependency licensing (policy: permissive only — no GPL / AGPL)

Every library proposed here is intended to be **MIT / Apache-2.0 / BSD** — no
copyleft/viral licenses. Verified below (module-cache `LICENSE` files for deps
already in the graph; upstream repos otherwise):

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

**Excluded (viral / non-permissive) — do NOT use:**
- `go-fitz` and any **MuPDF** wrapper — **AGPLv3**.
- **UniDoc / unipdf** — **AGPL / commercial** dual license.
- `sajari/docconv` — MIT itself, but shells out to **GPL** binaries (`wv`,
  `poppler-utils`, `unrtf`, `antiword`); avoid unless limited to permissive tools.

Note: DuckDB (MIT) is referenced only as design inspiration, not a dependency.

## Phasing (suggested)

1. Relax the file datastore: directory = keyspace = union of *all* supported
   files; recurse; keep `<ns>/<keyspace>` convention + flat-root auto-detect.
2. Add decoders: JSONL + multi-doc JSON, then CSV/TSV (with sniffer), then
   Parquet (arrow-go).
3. Transparent gzip/zstd decode; `.zip` as a container.
4. Explicit `read_*('glob', opts)` table functions in FROM (power mode).
5. Catalog/sidecar (`.n1k1/catalog.json`) with hive + projected-date partitions.
6. Synthetic document IDs for multi-record sources: composite
   `<relpath>#<offset|line>` populating `META().id`; natural-key option in the
   catalog. (Needed as soon as step 2 lands multi-record files.)
7. Index/cache sidecar + manifest with Merkle + append-only offsets, where the
   offset checkpoints double as the `Fetch` seek index (joins
   `DESIGN-indexing.md`).
8. Office/unstructured extraction (pure-Go default + optional Tika/extractous),
   feeding FTS.
9. Encryption-at-rest: transparent decrypt layer (Tink/age segmented),
   envelope keys via `gocloud.dev/secrets`, and **encrypted sidecar artifacts**.

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
- **Default doc-ID scheme.** Positional `<relpath>#<offset>` (addressable, but
  shifts on edit) vs content-hash (stable, not seekable) vs requiring a natural
  key — and how aggressively to default per source/mutation pattern.
- **Encryption scope & seekability.** Which segmented-encryption format (Tink
  vs age) and whether to require seekable compression/encryption for large
  encrypted sources, vs accepting rescan-from-checkpoint when inputs are opaque.

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
