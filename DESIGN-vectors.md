# DESIGN-vectors.md — embeddings & vector search in n1k1

_Status: design / not started. Companion to DESIGN-data.md (keyspaces, extract),
DESIGN-extensions.md (UDFs, `*.stream.js`, macros), DESIGN-col.md (columnar/SIMD)._

## Intent

Let a user turn records (log lines, doc fields, …) into vectors with a **local,
swappable** embedding model, store them, and run **semantic / nearest-neighbour
search** — all in n1k1's pure-Go, `CGO_ENABLED=0` world, at dev/debug scale (a
cbcollect bundle: 10K–1M rows). Two distinct sub-problems with opposite constraints:

1. **Embedding** (text → `float32[dim]`): heavy (~1–10 ms/item, best batched), external
   (model lives in ollama/llama.cpp/ONNX/OpenAI), deterministic in `(text, model)`. A
   **cold, throughput-bound ETL transform** — the opposite of n1k1's ~1 µs/row byte lane.
2. **Search** (distance + top-K): either brute-force or an ANN index.

## Grounding facts (verified 2026)

- **Distance is already solved, pure-Go, no new code.** cbq's `VECTOR_DISTANCE(field,
  query, metric)` and `APPROX_VECTOR_DISTANCE(...)` both evaluate through one pure-Go
  helper (`expression/func_vector.go` `vectorDistance()`): array iteration + float64 math,
  metrics `l2`/`l2_squared`/`cosine` (returns `1 − cosine_sim`, **lower = closer**)/`dot`.
  n1k1's boxed evaluator runs it out of the box:
  `SELECT t.id FROM ks t ORDER BY VECTOR_DISTANCE(t.v, [1,0,0], "cosine") ASC LIMIT 5`
  works **today** (verified: a=0, near=0.006, orthogonal=1). So there is **no COSINE_SIM
  to build** — reuse the grammar's function.
  - Quirk: the **first** operand must be a *field reference*, not an array literal
    (a planner/index-eligibility check); the query vector is the 2nd operand. That matches
    the real shape (stored vec field vs query vec).
  - `VECTOR_DISTANCE` = exact; `APPROX_VECTOR_DISTANCE` = the index-backed name (identical
    math today, since we have no ANN index). Phase 0 uses **VECTOR_DISTANCE** (honest:
    exact, full scan); `APPROX_` is the forward name for a future ANN tier.
- **FAISS is dark under `CGO_ENABLED=0`.** `go-faiss` is in the module graph (via bleve
  v2.6.1) but is cgo/C++; n1k1's cgo-free build gets bleve *text* FTS, **not** the FAISS
  vector index. This is the central constraint: **no ANN index without a cgo decision.**

## Principles

- **No grammar changes** (can't touch cbq's parser). Vectors are ordinary SQL++ arrays;
  `dim` is never in the SQL type — `VECTOR_DISTANCE` infers it from array length; the
  columnar side-file records the fixed width in its own metadata.
- **No optimizer magic.** Batching is expressed *explicitly* in SQL (GROUP-BY pages), not
  hidden in an operator. A macro sugars it; `.macro expand` shows the honest SQL.
- **Compute-once, materialize.** Embeddings are deterministic → compute once, persist to a
  side-file, skip on re-run. Essential for large/remote sources (S3/Box/Drive/HF).
- **Model-agnostic.** No hardcoded ollama. An options object carries `endpoint` + `model`;
  swap freely (ollama, llama.cpp-server, OpenAI, local ONNX all speak HTTP/JSON).
- **cgo-free at dev scale.** Brute-force distance over a columnar float32 column; defer
  FAISS/ANN (and its cgo cost) to a later scale tier.
- **Reuse existing lanes:** the vector functions above, GROUP BY / `ARRAY_AGG` / `UNNEST`,
  the columnar/SIMD lane, `INSERT INTO <file>`, the extension + macro registries.

## Search (Phase 0 — free)

Brute-force top-K via the existing function; no index, no cgo, tens of ms at bundle scale:
```sql
SELECT t.id, t.line FROM ks t
ORDER BY VECTOR_DISTANCE(t.v, $qvec, "cosine") ASC LIMIT 10;
```
Perf later: port `vectorDistance` to the native columnar float32/SIMD lane (DESIGN-col).
ANN index much later, only if N forces it (see cgo decision).

**Boxing / "recycled box" (Phase 1, not Phase 0).** Phase 0 goes through cbq's *boxed*
evaluator: each row parses its stored vector field into a `value.Value` array, and
`vectorDistance` touches ~`dim` boxed `value.Value` elements per row (`.Index(i)` →
`float64`). Fine for correctness, garbage-heavy at scale. But a "recycled cbq `Value`
backed by a reusable `[]float32`" is **not available** — cbq's value model has no native
`[]float32` array (`value/doc.go`: float32 "not used"; arrays are `[]interface{}` of
float64-boxed numbers). So the win is not *recycling a box* but **skipping the box**: store
the vector column as raw `float32` bytes in the columnar side-file, and run a native
distance kernel over a borrowed `[]byte`→`[]float32` view with a **reused** query-vector
`[]float32` + scalar accumulators (no per-row `value.Value`, SIMD-friendly). That is the
DESIGN-col native port — the right home for the reuse instinct.

## Embedding — a batched callout at ingest, materialized once

**Not** the extract hot-loop (native/cheap) and **not** a per-row scalar UDF with
fork-per-row. Instead a scalar **`VECTORIZE_BATCH(array, opts) → array`** (array of texts →
parallel array of vectors): **one goja call + one model round-trip per batch**. Backed by
a native pure-Go `http.post` (no curl subprocess) to `opts.endpoint`, e.g.
`{"endpoint":"http://localhost:11434/api/embed","model":"nomic-embed-text"}`. A
`{"fake":true}` mode returns deterministic pseudo-vectors (hash → unit vector) so the whole
pipeline is testable with **no model and no network** — the key de-risk.

### Batching without magic (explicit GROUP-BY pages)

```sql
INSERT INTO vecs
SELECT u.id, u.vec FROM (
  SELECT VECTORIZE_BATCH(g.batch, {"model":"…"}) AS vecs, g.batch
  FROM ( SELECT ARRAY_AGG({"id":t.id,"text":t.line}) AS batch, FLOOR(t.pos/256) AS page
         FROM ks t GROUP BY FLOOR(t.pos/256) ) g
) e UNNEST e.vecs AS u;
```
- `ARRAY_AGG` of `{id,text}` **objects** (not two parallel arrays) keeps id/text/vec glued.
- Page size (256) is a user-controlled literal. Last page = leftover rows.
- **`@vectorize_column(ks, text => line, model => "…", batch => 256)`** macro sugars this wall.
- **Load-bearing, verify first:** (a) `UNNEST` support (NEST is unsupported in n1k1, but
  UNNEST is separate — confirm, or wire it); (b) a stable row ordinal to page by
  (`_meta.pos`, else `ROW_NUMBER()`).

## Storage & caching

- **Side-file:** materialize vectors as a columnar/Parquet keyspace (fixed-width
  `float32[dim]`; `dim` in the file's metadata, discovered at write or declared). Reuses
  the Parquet-keyspace work. Vectors are usually far smaller than the source text.
- **Caching mostly already exists.** `INSERT INTO <file>` default mode `"new"` opens with
  `O_CREATE|O_EXCL` and **errors if the target already exists** (`glue/insert.go`). So the
  cache check is free: the macro/CLI **names the destination by a config-address** of its
  args — `vecs/<source>.<model>.<recipe-version>.jsonl` (computable at expand time from
  `source`+`model`+`opts`) — and the INSERT's own error-if-exists *is* the skip: a wrapper
  runs the generated `INSERT` and treats "target file already exists" as a **cache hit**
  (or checks existence first). A macro can't touch the filesystem, but it doesn't need to —
  it just produces the deterministic destination name; the existing INSERT semantics do the
  rest. For a stronger data-level address (catch "the source changed"), expose a
  **`CONTENT_HASH(...)` scalar UDF** (surfaced in SQL, not trapped in a Go inner tier).
  Config-address (source + model + version [+ mtime/size]) is cheap and adequate for dev.

## The cgo fork in the road (deferred)

An ANN index at ~10M+ vectors needs one of: (a) an opt-in `CGO_ENABLED=1` FAISS build
variant (breaks the single pure-Go binary), (b) a **pure-Go HNSW** library (cgo-free,
slower index build), or (c) a sidecar index process. **Decide later** — brute-force ships
first and covers dev/debug scale.

## Phased plan

- **Phase 0 (de-risk, cgo-free, no model):** `VECTORIZE_BATCH` with `{"fake":true}` + native
  `http.post`; verify `UNNEST`; `@vectorize_column` macro; end-to-end
  `INSERT INTO vecs SELECT @vectorize_column(ks, …)` then brute-force `VECTOR_DISTANCE` top-K.
  Vectors as plain SQL++ float64 arrays first (correctness), separate from columnar packing.
- **Phase 1:** real model via `http.post` (ollama/nomic-embed-text); columnar float32
  vector column + native-lane distance; caching (config-address naming + skip-if-present);
  `CONTENT_HASH()` UDF.
- **Phase 2:** remote-source ingest (S3/Box/Drive/HF → local vector side-file); then the
  ANN-index cgo decision, only if N demands.

## Prior art

- **cbq/Couchbase:** provides the vector *index + search* (bleve + FAISS); embedding is
  **BYO** — you store precomputed vectors. n1k1 inherits the functions, not the embedder.
- **DuckDB:** `FLOAT[N]` + `array_cosine_similarity` (brute-force, fast) + `vss` HNSW for
  scale; embedding via extension/UDF callout.
- **pgvector:** `vector` type + ivfflat/hnsw; embedding via BYO / `pgai` callout.
- **LanceDB:** columnar file + an **embedding-function registry** applied at write,
  materialized into the file — the closest model to what we want, doable cgo-free.

## Open questions

1. `UNNEST` support in n1k1 (load-bearing for batch-explode).
2. Native-lane / columnar float32 port of `vectorDistance` (Phase 1 perf).
3. Row-ordinal for paging (`_meta.pos` vs `ROW_NUMBER()`).
4. `INSERT INTO <file>` content-addressed skip-if-present: `.embed` command vs an
   INSERT-handler mode.
5. Parquet fixed-size-list encoding of a vector column (DESIGN-col).
